/*
 *
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
package org.apache.hadoop.hbase.replication.regionserver;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.replication.ReplicationException;

public class RecoveredReplicationSource extends ReplicationSource {

  private static final Log LOG =
      LogFactory.getLog(RecoveredReplicationSource.class);

  @Override
  protected long getStartPosition() throws ReplicationException {
    // If this is recovered, the queue is already full and the first log
    // normally has a position (unless the RS failed between 2 logs)
    long position = this.replicationQueues.getLogPosition(this.peerClusterZnode, this.queue.peek()
        .getName());
    LOG.info("Recovered queue started with log " + this.queue.peek() + " at position "
        + this.repLogReader.getPosition());
    return position;
  }

  @Override
  protected boolean isCurrentWALisBeingWrittenTo() {
    return false;
  }

  @Override
  protected boolean handleEOFException(int sleepMultiplier) {
    boolean considerDumping = false;
    try {
      FileStatus stat = this.fs.getFileStatus(this.currentPath);
      if (stat.getLen() == 0) {
        LOG.warn(this.peerClusterZnode + " Got EOF and the file was empty");
      }
      considerDumping = true;
    } catch (IOException e) {
      LOG.warn(this.peerClusterZnode + " Got while getting file size: ", e);
    }

    return considerDumping && sleepMultiplier == this.maxRetriesMultiplier && processEndOfFile();
  }

  @Override
  protected void tryCleanOldLogs() {
    this.manager.cleanOldLogs(this.currentPath.getName(), this.peerClusterZnode, true);
  }

  @Override
  protected boolean processEndOfFile() {
    if (queue.size() != 0) {
      return super.processEndOfFile();
    } else {
      this.manager.closeRecoveredQueue(this);
      LOG.info("Finished recovering the queue with the following stats " + getStats());
      this.running = false;
      return true;
    }
  }

  @Override
  protected boolean openReader(int sleepMultiplier) {
    try {
      try {
        this.reader = repLogReader.openReader(this.currentPath);
      } catch (FileNotFoundException fnfe) {
        // We didn't find the log in the archive directory, look if it still
        // exists in the dead RS folder (there could be a chain of failures
        // to look at)
        List<String> deadRegionServers = this.replicationQueueInfo.getDeadRegionServers();
        LOG.info("NB dead servers : " + deadRegionServers.size());
        for (String curDeadServerName : deadRegionServers) {
          Path deadRsDirectory = new Path(manager.getLogDir().getParent(), curDeadServerName);
          Path[] locs = new Path[] { new Path(deadRsDirectory, currentPath.getName()),
              new Path(deadRsDirectory.suffix(HLog.SPLITTING_EXT), currentPath.getName()), };
          for (Path possibleLogLocation : locs) {
            LOG.info("Possible location " + possibleLogLocation.toUri().toString());
            if (this.manager.getFs().exists(possibleLogLocation)) {
              // We found the right new location
              LOG.info("Log " + this.currentPath + " still exists at " + possibleLogLocation);
              // Breaking here will make us sleep since reader is null
              return true;
            }
          }
        }
        // In the case of disaster/recovery, HMaster may be shutdown/crashed before flush data
        // from .logs to .oldlogs. Loop into .logs folders and check whether a match exists
        if (stopper instanceof ReplicationSyncUp.DummyServer) {
          FileStatus[] rss = fs.listStatus(manager.getLogDir());
          for (FileStatus rs : rss) {
            Path p = rs.getPath();
            FileStatus[] logs = fs.listStatus(p);
            for (FileStatus log : logs) {
              p = new Path(p, log.getPath().getName());
              if (p.getName().equals(currentPath.getName())) {
                currentPath = p;
                LOG.info("Log " + this.currentPath + " exists under " + manager.getLogDir());
                // Open the log at the new location
                this.openReader(sleepMultiplier);
                return true;
              }
            }
          }
        }

        // TODO What happens if the log was missing from every single location?
        // Although we need to check a couple of times as the log could have
        // been moved by the master between the checks
        // It can also happen if a recovered queue wasn't properly cleaned,
        // such that the znode pointing to a log exists but the log was
        // deleted a long time ago.
        // For the moment, we'll throw the IO and processEndOfFile
        throw new IOException("File from recovered queue is " + "nowhere to be found", fnfe);
      }
    } catch (IOException ioe) {
      if (ioe instanceof EOFException) {
        boolean atTail = this.queue.size() == 0;
        LOG.warn("EOF in recover queue:" + this.peerClusterZnode + ", atTail=" + atTail
            + ", file path:" + this.currentPath, ioe);
        processEndOfFile();
        return false;
      }

      LOG.warn(this.peerClusterZnode + " Got: ", ioe);
      this.reader = null;
      if (ioe.getCause() instanceof NullPointerException) {
        // Workaround for race condition in HDFS-4380
        // which throws a NPE if we open a file before any data node has the most recent block
        // Just sleep and retry. Will require re-reading compressed HLogs for compressionContext.
        LOG.warn("Got NPE opening reader, will retry.");
      }

      this.metrics.incrOpenReaderIOE();
      // Throttle the failure logs
      try {
        if (ioeSleepBeforeRetry > 0) {
          TimeUnit.MILLISECONDS.sleep(ioeSleepBeforeRetry);
        }
      } catch (Exception e) {
        // Ignore
      }
    }
    return true;
  }
}