/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.regionserver.wal;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Chore;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.compactions.OffPeakHours;
import org.apache.hadoop.hbase.regionserver.wal.HLog.Reader;
import org.apache.hadoop.hbase.regionserver.wal.HLog.Writer;
import org.apache.hadoop.hbase.util.FSUtils;

/**
 * A background thread to compact the hlog.<br>
 * remove the hlog entries whose data have been flushed to hfile. <br>
 * The scenario is that in a share cluster, write requests of some regions may very little and
 * periodical, a lots of hlogs can not be cleaned for entries of those regions in hlogs.
 */

public class HLogCompactor extends Chore {
  private final static Log LOG = LogFactory.getLog(HLogCompactor.class);

  public final static String HLOG_COMPACT_ENABLE = "hbase.hlog.compact.enable";
  public final static String HLOG_COMPACT_RATIO = "hbase.hlog.compact.ratio";
  public final static String HLOG_COMPACT_RATIO_OFFPEAK =
      "hbase.hlog.compact.ratio.offpeak";
  
  private HRegionServer server;
  private Configuration conf;
  private FileSystem fs;
  private HLog hlog;
 
  private float compactRatio;
  private float compactRatioOffPeak;
  private OffPeakHours offPeak;

  private final long logrollsize;
  private final Path tmpDir;

  public HLogCompactor(final HRegionServer regionServer, int period) {
    super("HLogCompactor", period, regionServer);
    this.server = regionServer;
    this.conf = regionServer.getConfiguration();
    this.fs = regionServer.getFileSystem();
    this.hlog = regionServer.getWAL();
    
    this.compactRatio =
        conf.getFloat(HLOG_COMPACT_RATIO, 3.0f);
    this.compactRatioOffPeak =
        conf.getFloat(HLOG_COMPACT_RATIO_OFFPEAK, 2.0f);
    this.offPeak = OffPeakHours.getInstance(conf);

    this.logrollsize = hlog.getLogRollSize();
    this.tmpDir = new Path(hlog.getDir(), HLog.HLOG_TEMP_DIR);
  }

  public HLogCompactor(final HRegionServer regionServer, final FileSystem fs,
      final HLog hlog, int period) {
    super("HLogCompactor", period, regionServer);
    this.server = regionServer;
    this.conf = regionServer.getConfiguration();
    this.fs = fs;
    this.hlog = hlog;
 
    this.compactRatio =
        conf.getFloat(HLOG_COMPACT_RATIO, 3.0f);
    this.compactRatioOffPeak =
        conf.getFloat(HLOG_COMPACT_RATIO_OFFPEAK, 2.0f);
    this.offPeak = OffPeakHours.getInstance(conf);

    this.logrollsize = hlog.getLogRollSize();
    this.tmpDir = new Path(hlog.getDir(), HLog.HLOG_TEMP_DIR);
  }

  /**
   * Merge multi hlogs to a new hlog
   * @param hlogs
   * @param newHlog
   * @return max sequence id in the new hlog
   * @throws IOException
   */
  private SortedMap<Long, Path> compact(final SortedMap<Long, Path> hlogs) throws IOException {
    long seqId = 0L;
    SortedMap<Long, Path> newLogs = new TreeMap<Long, Path>();
    Path logPath = null;
    Writer writer = null;

    for (Path oldLog : hlogs.values()) {
      if (!fs.exists(oldLog)) {
        throw new FileNotFoundException(oldLog.getName());
      }
      if (!fs.isFile(oldLog)) {
        throw new IOException(oldLog + " is not a file");
      }
      Reader reader = HLog.getReader(fs, oldLog, conf);
      try {
        HLog.Entry entry;
        // If current hlog file is archived by log roller thread, the read of current block of hlog
        // will be not interrupted. But the read of next block will throw a FileNotFoundException and this 
        // hlog compaction will be aborted.
        while ((entry = reader.next()) != null) {
          HLogKey key = entry.getKey();
          
          if (!isFlushed(key.getEncodedRegionName(), key.getLogSeqNum())) {
            continue;
          }

          if (writer == null) {
            logPath = new Path(this.tmpDir, hlog.computeFilename().getName());
            writer = HLog.createWriter(fs, logPath, conf);
          }
          writer.append(entry);
          seqId = Math.max(seqId, key.getLogSeqNum());
          
          if (writer.getLength() > logrollsize) {
            writer.close();
            newLogs.put(seqId, logPath);
            writer = null;
            seqId = 0L;
          }
        }
      } finally {
        reader.close();
      }
    }
    if (writer != null) {
      writer.close();
      newLogs.put(seqId, logPath);
    }
    return newLogs;
  }

  /**
   * Cleanup tmp hlog files left by aborted hlog compaction
   * @throws IOException
   */
  private void cleanupTmpHLogs() {
    FileStatus[] logfiles;
    try {
      logfiles = FSUtils.listStatus(fs, tmpDir);
    } catch (IOException e) {
      LOG.error("List dir failed. Dir: " + tmpDir, e);
      return;
    }

    if (logfiles == null) {
      return;
    }
    for (FileStatus fstatus : logfiles) {
      try {
        FSUtils.delete(fs, fstatus.getPath(), false);
      } catch (IOException e) {
        LOG.warn("Delete tmp hlog failed. path: " + fstatus.getPath());
      }
    }
  }

  /**
   * Check if the data with this seq number have been flushed
   * @param regionName
   * @param seqNum
   * @return
   */
  private boolean isFlushed(byte[] regionName, long seqNum) {
    HRegion region = server.getOnlineRegion(regionName);
    if (region == null) return true;
    return seqNum <= region.getLastFlushSequenceId();
  }

  /**
   * Try to compact hlog
   * @throws IOException
   */

  public boolean compact() {
    SortedMap<Long, Path> newLogs = new TreeMap<Long, Path>();
    try {
      FSUtils.checkdir(fs, tmpDir);

      SortedMap<Long, Path> logs = hlog.getCompactHLogFiles();
      if (logs.size() == 0 ) {
        LOG.info("No hlog files to compact.");
        return false;
      }
      LOG.info("Start hlog compaction. Hlog file num: " + logs.size());
      newLogs = compact(logs);
      hlog.completeHLogCompaction(logs, newLogs);
      LOG.info("Complete hlog compaction. New hlog file num: " + newLogs.size());
    } catch (IOException e) {
      LOG.error("Compact hlogs failed", e);
      return false;
    } finally {
      cleanupTmpHLogs();
    }
    return true;
  }

  @Override
  protected void chore() {
    if (server.isStopped()){
      return;
    }
    if (!conf.getBoolean(HLOG_COMPACT_ENABLE, false)) {
      return;
    }
    int compactionTaskNum =
        this.server.compactSplitThread.getCompactionQueueSize()
            + this.server.compactSplitThread.getRunningCompactionSize();
    int flushTaskNum =
        this.server.getFlushingRegionNum()
            + this.server.cacheFlusher.getFlushQueueSize();
    // Avoid the hlog compaction when RS are doing store compaction or region flush. 
    if (compactionTaskNum + flushTaskNum > 0) {
      return;
    }
    float ratio = compactRatio;
    if (offPeak.isOffPeakHour()) {
      ratio = compactRatioOffPeak;
    }
    long totalMemStoreSize =
        this.server.getRegionServerAccounting().getGlobalMemstoreSize();
    long totalHLogSize = this.hlog.getOutputLogFileSize();
    if (totalHLogSize >= totalMemStoreSize * ratio) {
      compact();
    }
  }
}
