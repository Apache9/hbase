/*
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

import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.replication.ReplicationGroupOffset;
import org.apache.hadoop.hbase.replication.ReplicationPeer;
import org.apache.hadoop.hbase.replication.ReplicationQueueId;
import org.apache.hadoop.hbase.replication.ReplicationQueueStorage;
import org.apache.hadoop.hbase.wal.AbstractFSWALProvider;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class that handles the recovered source of a replication stream, which is transfered from another
 * dead region server. This will be closed when all logs are pushed to peer cluster.
 */
@InterfaceAudience.Private
public class RecoveredReplicationSource extends ReplicationSource {

  private static final Logger LOG = LoggerFactory.getLogger(RecoveredReplicationSource.class);

  @Override
  public void init(Configuration conf, FileSystem fs, ReplicationSourceManager manager,
    ReplicationQueueStorage queueStorage, ReplicationPeer replicationPeer, Server server,
    ReplicationQueueId queueId, Map<String, ReplicationGroupOffset> startPositions, UUID clusterId,
    WALFileLengthProvider walFileLengthProvider, MetricsSource metrics) throws IOException {
    super.init(conf, fs, manager, queueStorage, replicationPeer, server, queueId, startPositions,
      clusterId, walFileLengthProvider, metrics);
  }

  @Override
  public void enqueueLog(Path wal) {
    String walPrefix = AbstractFSWALProvider.getWALPrefixFromWALName(wal.getName());
    ReplicationGroupOffset offset = startPositions.get(walPrefix);
    // if the wal is before the start offset, just ignore it
    if (offset != null) {
      long offsetStartTime = AbstractFSWALProvider.getTimestamp(offset.getWal());
      long startTime = AbstractFSWALProvider.getTimestamp(wal.getName());
      if (offsetStartTime > startTime) {
        LOG.info("Skip enqueuing {} because it is before the start position {}", wal, offset);
        return;
      }
    }
    super.enqueueLog(wal);
  }

  @Override
  protected RecoveredReplicationSourceShipper createNewShipper(String walGroupId) {
    return new RecoveredReplicationSourceShipper(conf, walGroupId, logQueue, this);
  }

  void tryFinish() {
    if (workerThreads.isEmpty()) {
      this.getSourceMetrics().clear();
      manager.finishRecoveredSource(this);
    }
  }
}
