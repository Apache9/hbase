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

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos.ReplicationState;
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
  private ReplicationState.State state = ReplicationState.State.ENABLED;
  private final Map<byte[], byte[]> peerData;
  private final Map<String, String> configuration;
  private ZooKeeperProtos.TableCFs tableCFs;
  private long bandwidth = 0;

  public ReplicationPeerConfig() {
    this.peerData = new TreeMap<byte[], byte[]>(Bytes.BYTES_COMPARATOR);
    this.configuration = new HashMap<String, String>(0);
    this.tableCFs = ZooKeeperProtos.TableCFs.newBuilder().build();
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
  
  public ReplicationPeerConfig setState(ReplicationState.State state) {
    this.state = state;
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
  
  public ReplicationState.State getState() {
    return state;
  }

  public ZooKeeperProtos.TableCFs getTableCFs() {
    return tableCFs;
  }

  public void setTableCFs(ZooKeeperProtos.TableCFs tableCFs) {
    this.tableCFs = tableCFs;
  }

  public long getBandwidth() {
    return this.bandwidth;
  }

  public void setBandwidth(long bandwidth) {
    this.bandwidth = bandwidth;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder("clusterKey=").append(clusterKey).append(",");
    builder.append("state=").append(state).append(",");
    builder.append("tableCFs=").append(tableCFs.toString()).append(",");
    builder.append("rpcProtocol=").append(protocol.name()).append(",");
    builder.append("bandwidth=").append(bandwidth);
    return builder.toString();
  }
}
