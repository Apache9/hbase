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
package org.apache.hadoop.hbase.master.cleaner;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

/**
 * Restore File cleaner that uses the timestamp of the hfile to determine if it should be deleted.
 * By default they are allowed to live for 30 days.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class TimeToLiveSnapshotRestoreFileCleaner extends BaseFileCleanerDelegate {

  public static final Log LOG =
      LogFactory.getLog(TimeToLiveSnapshotRestoreFileCleaner.class.getName());

  public static final String SNAPSHOT_RESTORE_FILE_CLEANER_TTL =
      "hbase.master.snapshotrestorefile.cleaner.ttl";
  public static final long SNAPSHOT_RESTORE_FILE_CLEANER_TTL_DEFAULT = 2592000000L; // 30 days

  private boolean stopped = false;
  private long ttl;

  @Override
  public void setConf(Configuration conf) {
    this.ttl =
        conf.getLong(SNAPSHOT_RESTORE_FILE_CLEANER_TTL, SNAPSHOT_RESTORE_FILE_CLEANER_TTL_DEFAULT);
    super.setConf(conf);
    LOG.info("Initialize TimeToLiveSnapshotRestoreFileCleaner, ttl=" + ttl);
  }

  @Override
  public boolean isFileDeletable(FileStatus fStat) {
    long currentTime = EnvironmentEdgeManager.currentTimeMillis();
    long time = fStat.getModificationTime();
    long life = currentTime - time;
    if (LOG.isTraceEnabled()) {
      LOG.trace("SnapshotRestoreFile life:" + life + ", ttl:" + ttl + ", current:" + currentTime
          + ", from: " + time);
    }
    if (life < 0) {
      LOG.warn("Found a snapshot restore file (" + fStat.getPath() + ") newer than current time ("
          + currentTime + " < " + time + "), probably a clock skew");
      return false;
    }
    return life > ttl;
  }

  @Override
  public void stop(String why) {
    this.stopped = true;
  }

  @Override
  public boolean isStopped() {
    return stopped;
  }
}
