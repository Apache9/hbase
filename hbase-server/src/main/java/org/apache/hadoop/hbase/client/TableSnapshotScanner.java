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

package org.apache.hadoop.hbase.client;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.mapreduce.TableSnapshotInputFormat;
import org.apache.hadoop.hbase.protobuf.generated.SnapshotProtos;
import org.apache.hadoop.hbase.protobuf.generated.SnapshotProtos.SnapshotRegionManifest;
import org.apache.hadoop.hbase.snapshot.ExportSnapshot;
import org.apache.hadoop.hbase.snapshot.RestoreSnapshotHelper;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.SnapshotManifest;
import org.apache.hadoop.hbase.util.FSUtils;

/**
 * A Scanner which performs a scan over snapshot files. Using this class requires copying the
 * snapshot to a temporary empty directory, which will copy the snapshot reference files into that
 * directory. Actual data files are not copied.
 *
 * <p>
 * This also allows one to run the scan from an
 * online or offline hbase cluster. The snapshot files can be exported by using the
 * {@link ExportSnapshot} tool, to a pure-hdfs cluster, and this scanner can be used to
 * run the scan directly over the snapshot files. The snapshot should not be deleted while there
 * are open scanners reading from snapshot files.
 *
 * <p>
 * An internal RegionScanner is used to execute the {@link Scan} obtained
 * from the user for each region in the snapshot.
 * <p>
 * HBase owns all the data and snapshot files on the filesystem. Only the HBase user can read from
 * snapshot files and data files. HBase also enforces security because all the requests are handled
 * by the server layer, and the user cannot read from the data files directly. To read from snapshot
 * files directly from the file system, the user who is running the MR job must have sufficient
 * permissions to access snapshot and reference files. This means that to run mapreduce over
 * snapshot files, the job has to be run as the HBase user or the user must have group or other
 * priviledges in the filesystem (See HBASE-8369). Note that, given other users access to read from
 * snapshot/data files will completely circumvent the access control enforced by HBase.
 * @see TableSnapshotInputFormat
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class TableSnapshotScanner extends AbstractClientScanner {

  private static final Log LOG = LogFactory.getLog(TableSnapshotScanner.class);
  public static final String TABLE_SNAPSHOT_SCANNER_BANDWIDTH =
      "table.snapshot.scanner.bandwidth.kb";
  public static final long SLEEP_DURATION_MS = 1000;

  private Configuration conf;
  private String snapshotName;
  private FileSystem fs;
  private Path rootDir;
  private Path restoreDir;
  private Scan scan;
  private List<SnapshotRegionManifest> regionManifests;
  private HTableDescriptor htd;
  private boolean snapshotAlreadyRestored = false;
  private long maxBytesPerSec = 0;
  private long bytesRead = 0;
  private long startTime = System.currentTimeMillis();

  private ClientSideRegionScanner currentRegionScanner = null;
  private int currentRegion = -1;

  /**
   * Creates a TableSnapshotScanner.
   * @param conf the configuration
   * @param restoreDir a temporary directory to copy the snapshot files into. Current user should
   *          have write permissions to this directory, and this should not be a subdirectory of
   *          rootdir. The scanner deletes the contents of the directory once the scanner is closed.
   * @param snapshotName the name of the snapshot to read from
   * @param scan a Scan representing scan parameters
   * @throws IOException in case of error
   */
  public TableSnapshotScanner(Configuration conf, Path restoreDir, String snapshotName, Scan scan)
      throws IOException {
    this(conf, FSUtils.getRootDir(conf), restoreDir, snapshotName, scan);
  }

  /**
   * Creates a TableSnapshotScanner.
   * @param conf the configuration
   * @param rootDir root directory for HBase.
   * @param restoreDir a temporary directory to copy the snapshot files into. Current user should
   *          have write permissions to this directory, and this should not be a subdirectory of
   *          rootDir. The scanner deletes the contents of the directory once the scanner is closed.
   * @param snapshotName the name of the snapshot to read from
   * @param scan a Scan representing scan parameters
   * @throws IOException in case of error
   */
  public TableSnapshotScanner(Configuration conf, Path rootDir, Path restoreDir,
      String snapshotName, Scan scan) throws IOException {
    this(conf, rootDir, restoreDir, snapshotName, scan, false);
  }

  /**
   * Creates a TableSnapshotScanner.
   * @param conf the configuration
   * @param rootDir root directory for HBase.
   * @param restoreDir a temporary directory to copy the snapshot files into. Current user should
   *          have write permissions to this directory, and this should not be a subdirectory of
   *          rootDir. The scanner deletes the contents of the directory once the scanner is closed.
   * @param snapshotName the name of the snapshot to read from
   * @param scan a Scan representing scan parameters
   * @param snapshotAlreadyRestored true to indicate that snapshot has been restored.
   * @throws IOException in case of error
   */
  public TableSnapshotScanner(Configuration conf, Path rootDir, Path restoreDir,
      String snapshotName, Scan scan, boolean snapshotAlreadyRestored) throws IOException {
    this.conf = conf;
    this.snapshotName = snapshotName;
    this.rootDir = rootDir;
    this.scan = scan;
    this.fs = rootDir.getFileSystem(conf);
    this.snapshotAlreadyRestored = snapshotAlreadyRestored;
    this.maxBytesPerSec = conf.getLong(TABLE_SNAPSHOT_SCANNER_BANDWIDTH, -1) * 1024;

    if (snapshotAlreadyRestored) {
      this.restoreDir = restoreDir;
      openWithoutRestoringSnapshot();
    } else {
      // restoreDir will be deleted in close(), use a unique sub directory
      this.restoreDir = new Path(restoreDir, UUID.randomUUID().toString());
      openWithRestoringSnapshot();
    }
    initScanMetrics(scan);
  }

  private boolean isValidRegion(HRegionInfo hri) {
    // An offline split parent region should be excluded.
    if (hri.isOffline() && (hri.isSplit() || hri.isSplitParent())) {
      return false;
    }
    return CellUtil.overlappingKeys(scan.getStartRow(), scan.getStopRow(), hri.getStartKey(),
      hri.getEndKey());
  }

  private void openWithoutRestoringSnapshot() throws IOException {
    Path snapshotDir = SnapshotDescriptionUtils.getCompletedSnapshotDir(snapshotName, rootDir);
    SnapshotProtos.SnapshotDescription snapshotDesc =
        SnapshotDescriptionUtils.readSnapshotInfo(fs, snapshotDir);

    SnapshotManifest manifest = SnapshotManifest.open(conf, fs, snapshotDir, snapshotDesc);
    List<SnapshotRegionManifest> regionManifests = manifest.getRegionManifests();
    if (regionManifests == null) {
      throw new IllegalArgumentException("Snapshot seems empty, snapshotName: " + snapshotName);
    }

    this.regionManifests = regionManifests.stream()
        .filter(rmf -> isValidRegion(HRegionInfo.convert(rmf.getRegionInfo())))
        .collect(Collectors.toList());
    htd = manifest.getTableDescriptor();
  }

  private void openWithRestoringSnapshot() throws IOException {
    openWithoutRestoringSnapshot();
    RestoreSnapshotHelper.copySnapshotForScanner(conf, fs, rootDir, restoreDir, snapshotName);
  }

  @Override
  public Result next() throws IOException {
    Result result = null;
    while (true) {
      if (currentRegionScanner == null) {
        currentRegion++;
        if (currentRegion >= regionManifests.size()) {
          return null;
        }

        HRegionInfo hri = HRegionInfo.convert(regionManifests.get(currentRegion).getRegionInfo());
        currentRegionScanner = new ClientSideRegionScanner(conf, fs, restoreDir, htd, hri, scan,
            scanMetrics, regionManifests.get(currentRegion));
        if (this.scanMetrics != null) {
          this.scanMetrics.countOfRegions.incrementAndGet();
        }
      }

      throttle();

      try {
        result = currentRegionScanner.next();
        updateBytesRead(result);
        if (result != null) {
          return result;
        }
      } finally {
        if (result == null) {
          currentRegionScanner.close();
          currentRegionScanner = null;
        }
      }
    }
  }

  private void updateBytesRead(Result result) {
    if (result == null || maxBytesPerSec <= 0) return;
    for (Cell c : result.listCells()) {
      bytesRead += KeyValueUtil.length(c);
    }
  }

  private void throttle() throws IOException {
    if (maxBytesPerSec <= 0) return;
    while (getBytesPerSec() > maxBytesPerSec) {
      try {
        Thread.sleep(SLEEP_DURATION_MS);
      } catch (InterruptedException e) {
        throw new InterruptedIOException("Thread aborted");
      }
    }
  }

  public long getBytesRead() {
    return bytesRead;
  }

  private long getBytesPerSec() {
    long elapsed = (System.currentTimeMillis() - startTime) / 1000;
    if (elapsed == 0) {
      return bytesRead;
    } else {
      return bytesRead / elapsed;
    }
  }

  private void cleanup() {
    try {
      if (fs.exists(this.restoreDir)) {
        if (!fs.delete(this.restoreDir, true)) {
          LOG.warn("Delete restore directory for the snapshot failed. restoreDir: "
              + this.restoreDir + ", snapshotName: " + this.snapshotName);
        }
      }
    } catch (IOException ex) {
      LOG.warn("Could not delete restore directory for the snapshot. restoreDir: " + this.restoreDir
          + ", snapshotName: " + this.snapshotName,
        ex);
    }
  }

  @Override
  public void close() {
    if (currentRegionScanner != null) {
      currentRegionScanner.close();
    }
    if (!this.snapshotAlreadyRestored) {
      cleanup();
    }
  }

  @Override
  public boolean renewLease() {
    throw new UnsupportedOperationException();
  }
}
