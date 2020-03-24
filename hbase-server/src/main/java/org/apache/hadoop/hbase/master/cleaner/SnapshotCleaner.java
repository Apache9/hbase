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

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Chore;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.master.snapshot.SnapshotManager;
import org.apache.hadoop.hbase.protobuf.generated.SnapshotProtos.SnapshotDescription;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

/**
 * Cleaner for snapshot.
 * <p>
 * We can not use CleanChore as we clean snapshot rather than file here.
 */
@InterfaceAudience.Private
public class SnapshotCleaner extends Chore {

  private static final Log LOG = LogFactory.getLog(SnapshotCleaner.class);

  public static final String SNAPSHOT_FOR_DELETED_TABLE_TTL_MS =
      "hbase.master.snapshot.for.delete.table.ttl.ms";

  private final SnapshotManager snapshotManager;
  // ttl for snapshot of deleted table
  private final long snapshotForDeletedTableTtlMs;

  public SnapshotCleaner(int period, Stoppable stopper, SnapshotManager snapshotManager,
      Configuration conf) {
    super("SnapshotCleaner", period, stopper);
    this.snapshotManager = snapshotManager;
    this.snapshotForDeletedTableTtlMs =
        conf.getLong(SNAPSHOT_FOR_DELETED_TABLE_TTL_MS, TimeUnit.DAYS.toMillis(7));
  }

  private void cleanExpiredSnapshot() throws IOException {
    long currentTime = EnvironmentEdgeManager.currentTimeMillis();
    for (SnapshotDescription snapshot : snapshotManager.getCompletedSnapshots()) {
      long duration = currentTime - snapshot.getCreationTime();
      if (SnapshotDescriptionUtils.isSnapshotForDeletedTable(snapshot)
          && duration >= snapshotForDeletedTableTtlMs) {
        LOG.info("Delete expired snapshot " + snapshot.getName());
        snapshotManager.deleteSnapshot(snapshot);
      } else if (snapshot.getTtl() > 0 && duration >= snapshot.getTtl()) {
        LOG.info("Delete expired snapshot: " + snapshot.getName() + ", createTime: "
            + snapshot.getCreationTime() + ", ttl: " + snapshot.getTtl());
        snapshotManager.deleteSnapshot(snapshot);
      }
    }
  }

  @Override
  protected void chore() {
    try {
      cleanExpiredSnapshot();
    } catch (Exception e) {
      LOG.warn("clean expired snapshot failed", e);
    }
  }

}
