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

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.protobuf.generated.ReplicationProtos.ReplicationState;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * A configuration for the replication peer cluster.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class ReplicationPeerConfig {

  private String clusterKey;
  private String replicationEndpointImpl;
  private ReplicationPeer.PeerProtocol protocol = ReplicationPeer.PeerProtocol.NATIVE;
  private final Map<byte[], byte[]> peerData;
  private final Map<String, String> configuration;
  private Map<TableName, ? extends Collection<String>> tableCFsMap = null;
  private long bandwidth = 0;
  private Set<String> namespaces = null;
  // Default value is true, means replicate all user tables to peer cluster.
  private boolean replicateAllUserTables = true;
  private Map<TableName, ? extends Collection<String>> excludeTableCFsMap = null;
  private Set<String> excludeNamespaces = null;
  private boolean serial = false;

  public ReplicationPeerConfig() {
    this.peerData = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    this.configuration = new HashMap<>(0);
  }

  /**
   * Set the clusterKey which is the concatenation of the slave cluster's:
   *          hbase.zookeeper.quorum:hbase.zookeeper.property.clientPort:zookeeper.znode.parent
   */
  public ReplicationPeerConfig setClusterKey(String clusterKey) {
    this.clusterKey = clusterKey;
    return this;
  }

  /**
   * Sets the ReplicationEndpoint plugin class for this peer.
   * @param replicationEndpointImpl a class implementing ReplicationEndpoint
   */
  public ReplicationPeerConfig setReplicationEndpointImpl(String replicationEndpointImpl) {
    this.replicationEndpointImpl = replicationEndpointImpl;
    return this;
  }

  /**
   * Sets the ReplicationRPCProtocol for this peer.
   * @param protocol [NATIVE, THRIFT]
   */
  public ReplicationPeerConfig setProtocol(ReplicationPeer.PeerProtocol protocol) {
    this.protocol = protocol;
    if (protocol.getReplicationEndpointImpl() != null) {
      setReplicationEndpointImpl(protocol.getReplicationEndpointImpl());
    }
    return this;
  }

  public String getClusterKey() {
    return clusterKey;
  }

  public String getReplicationEndpointImpl() {
    return replicationEndpointImpl;
  }

  public Map<byte[], byte[]> getPeerData() {
    return peerData;
  }

  public Map<String, String> getConfiguration() {
    return configuration;
  }

  public ReplicationPeer.PeerProtocol getProtocol() {
    return protocol;
  }

  public Map<TableName, List<String>> getTableCFsMap() {
    return (Map<TableName, List<String>>) tableCFsMap;
  }

  public ReplicationPeerConfig setTableCFsMap(
      Map<TableName, ? extends Collection<String>> tableCFsMap) {
    this.tableCFsMap = tableCFsMap;
    return this;
  }

  public Set<String> getNamespaces() {
    return namespaces;
  }

  public ReplicationPeerConfig setNamespaces(Set<String> namespaces) {
    this.namespaces = namespaces;
    return this;
  }

  public long getBandwidth() {
    return this.bandwidth;
  }

  public void setBandwidth(long bandwidth) {
    this.bandwidth = bandwidth;
  }

  public boolean replicateAllUserTables() {
    return this.replicateAllUserTables;
  }

  public void setReplicateAllUserTables(boolean replicateAllUserTables) {
    this.replicateAllUserTables = replicateAllUserTables;
  }

  public Map<TableName, List<String>> getExcludeTableCFsMap() {
    return (Map<TableName, List<String>>) excludeTableCFsMap;
  }

  public ReplicationPeerConfig setExcludeTableCFsMap(
      Map<TableName, ? extends Collection<String>> tableCFsMap) {
    this.excludeTableCFsMap = tableCFsMap;
    return this;
  }

  public Set<String> getExcludeNamespaces() {
    return excludeNamespaces;
  }

  public ReplicationPeerConfig setExcludeNamespaces(Set<String> namespaces) {
    this.excludeNamespaces = namespaces;
    return this;
  }

  public boolean isSerial() {
    return this.serial;
  }

  public void setSerial(boolean serial) {
    this.serial = serial;
  }

  /**
   * Decide whether the table need replicate to the peer cluster
   * @param table name of the table
   * @return true if the table need replicate to the peer cluster
   */
  public boolean needToReplicate(TableName table) {
    if (replicateAllUserTables) {
      if (excludeNamespaces == null && excludeTableCFsMap == null) {
        return true;
      }
      if (excludeNamespaces != null && !excludeNamespaces.contains(table.getNamespaceAsString())) {
        return true;
      }
      if (excludeTableCFsMap != null && !excludeTableCFsMap.containsKey(table)) {
        return true;
      }
    } else {
      // If null means user has explicitly not configured any namespaces and table CFs
      // so all the tables data are applicable for replication
      if (namespaces == null && tableCFsMap == null) {
        return true;
      }
      if (namespaces != null && namespaces.contains(table.getNamespaceAsString())) {
        return true;
      }
      if (tableCFsMap != null && tableCFsMap.containsKey(table)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder("clusterKey=").append(clusterKey).append(",");
    builder.append("replicateAllUserTables=").append(replicateAllUserTables).append(",");
    if (this.replicateAllUserTables) {
      if (excludeNamespaces != null) {
        builder.append("excludeNamespaces=").append(excludeNamespaces.toString()).append(",");
      }
      if (excludeTableCFsMap != null) {
        builder.append("excludeTableCFsMap=").append(excludeTableCFsMap.toString()).append(",");
      }
    } else {
      if (namespaces != null) {
        builder.append("namespaces=").append(namespaces.toString()).append(",");
      }
      if (tableCFsMap != null) {
        builder.append("tableCFs=").append(tableCFsMap.toString()).append(",");
      }
    }
    builder.append("rpcProtocol=").append(protocol.name()).append(",");
    builder.append("bandwidth=").append(bandwidth).append(",");
    builder.append("serial=").append(serial);
    return builder.toString();
  }
}
