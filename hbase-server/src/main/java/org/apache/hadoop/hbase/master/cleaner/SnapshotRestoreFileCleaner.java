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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.classification.InterfaceAudience;

/**
 * This Chore, every time it runs, will clear the Restore Files in the snapshot restore tmp dir that
 * are deletable for each Restore File cleaner in the chain.
 */
@InterfaceAudience.Private
public class SnapshotRestoreFileCleaner extends CleanerChore<BaseFileCleanerDelegate> {

  public static final String MASTER_SNAPSHOT_RESTORE_FILE_CLEANER_PLUGINS =
      "hbase.master.snapshotrestorefile.cleaner.plugins";
  public static final String SNAPSHOT_RESTORE_FILE_CLEANER_INTERVAL =
      "hbase.master.snapshotrestorefile.cleaner.interval";
  public static final int SNAPSHOT_RESTORE_FILE_CLEANER_INTERVAL_DEFAULT = 86400000; // 1 day

  /**
   * @param period the period of time to sleep between each run
   * @param stopper the stopper
   * @param conf configuration to use
   * @param fs handle to the FS
   * @param directory directory to be cleaned
   */
  public SnapshotRestoreFileCleaner(final int period, final Stoppable stopper, Configuration conf,
      FileSystem fs, Path directory, DirScanPool pool) {
    super("SnapshotRestoreFileCleaner", period, stopper, conf, fs, directory,
        MASTER_SNAPSHOT_RESTORE_FILE_CLEANER_PLUGINS,
        conf.getLong(TimeToLiveSnapshotRestoreFileCleaner.SNAPSHOT_RESTORE_FILE_CLEANER_TTL,
          TimeToLiveSnapshotRestoreFileCleaner.SNAPSHOT_RESTORE_FILE_CLEANER_TTL_DEFAULT),
        pool);
  }

  @Override
  protected boolean validate(Path file) {
    return true;
  }
}
