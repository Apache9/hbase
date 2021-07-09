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
package org.apache.hadoop.hbase.testing;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * A mini hbase cluster used for testing.
 * <p/>
 * It will also start the necessary zookeeper cluster and dfs cluster. But we will not provide
 * methods for controlling the zookeeper cluster and dfs cluster, as end users do not need to test
 * the HBase behavior when these systems are broken.
 */
@InterfaceAudience.Public
public interface TestingHBaseCluster {

  Configuration getConf();

  Optional<ServerName> getActiveMaster();

  List<ServerName> getBackupMasters();

  List<ServerName> getRegionServers();

  CompletableFuture<Void> stopMaster(ServerName serverName) throws Exception;

  CompletableFuture<Void> stopRegionServer(ServerName serverName) throws Exception;

  CompletableFuture<Void> stopHBaseCluster() throws Exception;

  void startHBaseCluster() throws Exception;

  void start() throws Exception;

  default void stop() throws Exception {
    stop(true);
  }

  void stop(boolean cleanup) throws Exception;

  static TestingHBaseCluster create(TestingHBaseClusterOption option) {
    return null;
  }
}
