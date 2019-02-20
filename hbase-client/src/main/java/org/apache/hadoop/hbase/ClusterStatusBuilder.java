/**
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

package org.apache.hadoop.hbase;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.hbase.master.RegionState;

import edu.umd.cs.findbugs.annotations.Nullable;

public class ClusterStatusBuilder {

  @Nullable
  private String hbaseVersion;
  private Set<ServerName> deadServers = Collections.emptySet();
  private Map<ServerName, ServerLoad> liveServers = new TreeMap<>();
  @Nullable
  private ServerName master;
  private List<ServerName> backupMasters = Collections.emptyList();
  private Map<String, RegionState> regionsInTransition = new HashMap<>();
  @Nullable
  private String clusterId;
  private String[] masterCoprocessors = new String[0];
  @Nullable
  private Boolean balancerOn;
  private Set<ServerName> serversName = Collections.emptySet();
  
  public static ClusterStatusBuilder newBuilder() {
    return new ClusterStatusBuilder();
  }

  public ClusterStatusBuilder setHBaseVersion(String value) {
    this.hbaseVersion = value;
    return this;
  }

  public ClusterStatusBuilder setDeadServers(Set<ServerName> value) {
    this.deadServers = value;
    return this;
  }

  public ClusterStatusBuilder setLiveServers(Map<ServerName, ServerLoad> value) {
    liveServers.putAll(value);
    return this;
  }

  public ClusterStatusBuilder setMaster(ServerName value) {
    this.master = value;
    return this;
  }

  public ClusterStatusBuilder setBackupMasters(List<ServerName> value) {
    this.backupMasters = value;
    return this;
  }

  public ClusterStatusBuilder setRegionsInTransition(Map<String, RegionState> value) {
    this.regionsInTransition = value;
    return this;
  }

  public ClusterStatusBuilder setClusterId(String value) {
    this.clusterId = value;
    return this;
  }

  public ClusterStatusBuilder setMasterCoprocessors(String[] value) {
    this.masterCoprocessors = value;
    return this;
  }

  public ClusterStatusBuilder setBalancerOn(@Nullable Boolean value) {
    this.balancerOn = value;
    return this;
  }

  public ClusterStatusBuilder setServersName(Set<ServerName> serversName) {
    this.serversName = serversName;
    return this;
  }

  public ClusterStatus build() {
    return new ClusterStatus(hbaseVersion, clusterId, liveServers, deadServers, master,
        backupMasters, regionsInTransition, masterCoprocessors, balancerOn, serversName);
  }
}
