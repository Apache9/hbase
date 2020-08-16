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
package org.apache.hadoop.hbase.replication;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.ReflectionUtils;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Used to create replication storage(peer, queue) classes.
 */
@InterfaceAudience.Private
public final class ReplicationStorageFactory {

  public static final String PEER_STORAGE_IMPL = "hbase.replication.peer.storage.impl";

  private ReplicationStorageFactory() {
  }

  /**
   * Create a new {@link ReplicationPeerStorage}.
   */
  public static ReplicationPeerStorage getReplicationPeerStorage(ReplicationFactoryConfig config) {
    Configuration conf = config.getConfiguration();
    Class<? extends ReplicationPeerStorage> clazz = conf.getClass(PEER_STORAGE_IMPL,
      TableReplicationPeerStorage.class, ReplicationPeerStorage.class);
    return ReflectionUtils.newInstance(clazz, config);
  }

  /**
   * Create a new {@link ReplicationQueueStorage}.
   */
  public static ReplicationQueueStorage getReplicationQueueStorage(ZKWatcher zk,
    Configuration conf) {
    return new ZKReplicationQueueStorage(zk, conf);
  }
}
