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
package org.apache.hadoop.hbase.replication.master;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.ReplicationPeerNotFoundException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationFactory;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.ReplicationPeerDescription;
import org.apache.hadoop.hbase.replication.ReplicationPeers;
import org.apache.hadoop.hbase.replication.ReplicationQueuesClient;
import org.apache.hadoop.hbase.replication.TalosReplicationEndpoint;
import org.apache.hadoop.hbase.util.TalosUtil;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;

/**
 * Manages and performs all replication admin operations.
 * Used to add/remove a replication peer.
 */
@InterfaceAudience.Private
public class ReplicationManager {

  private final Configuration conf;
  private final ZooKeeperWatcher zkw;
  private final ReplicationQueuesClient replicationQueuesClient;
  private final ReplicationPeers replicationPeers;

  private static final String REPLICATION_ENALBE_PEER_MODIFICATION =
      "hbase.replication.enable.peer.modification";
  private static final boolean DEFAULT_REPLICATION_ENALBE_PEER_MODIFICATION = true;
  private final boolean peerModificationEnabled;

  public ReplicationManager(Configuration conf, ZooKeeperWatcher zkw, Abortable abortable)
      throws IOException {
    this.conf = conf;
    this.zkw = zkw;
    this.peerModificationEnabled = conf.getBoolean(REPLICATION_ENALBE_PEER_MODIFICATION,
        DEFAULT_REPLICATION_ENALBE_PEER_MODIFICATION);
    try {
      this.replicationQueuesClient = ReplicationFactory.getReplicationQueuesClient(zkw, conf,
        abortable);
      this.replicationQueuesClient.init();
      this.replicationPeers = ReplicationFactory.getReplicationPeers(zkw, conf,
        this.replicationQueuesClient, abortable);
      this.replicationPeers.init();
    } catch (Exception e) {
      throw new IOException("Failed to construct ReplicationManager", e);
    }
  }

  public void addReplicationPeer(String peerId, ReplicationPeerConfig peerConfig, boolean enabled)
      throws ReplicationException, IOException {
    if (!peerModificationEnabled) {
      throw new ReplicationException("Replication peer modification is disabled");
    }
    if(peerConfig.getReplicationEndpointImpl() != null) {
      checkReplicationEndpoint(peerConfig);
    } else {
      checkClusterKey(peerConfig.getClusterKey());
    }
    checkPeerConfigConflict(peerConfig);
    replicationPeers.addPeer(peerId, peerConfig, enabled);
    replicationPeers.peerAdded(peerId);
  }

  private void checkReplicationEndpoint(ReplicationPeerConfig peerConfig)
      throws DoNotRetryIOException {
    try {
      Class endpointImpl = Class.forName(peerConfig.getReplicationEndpointImpl());
      if(endpointImpl == TalosReplicationEndpoint.class) {
        TalosUtil.checkConfig(peerConfig);
      }
    } catch (ClassNotFoundException e) {
      throw new DoNotRetryIOException(
          "Replication endpoint implemented class not found, please check class name, ", e);
    }
  }

  public void removeReplicationPeer(String peerId) throws ReplicationException {
    if (!peerModificationEnabled) {
      throw new ReplicationException("Replication peer modification is disabled");
    }
    replicationPeers.removePeer(peerId);
  }

  public void enableReplicationPeer(String peerId) throws ReplicationException {
    if (!peerModificationEnabled) {
      throw new ReplicationException("Replication peer modification is disabled");
    }
    replicationPeers.enablePeer(peerId);
  }

  public void disableReplicationPeer(String peerId) throws ReplicationException {
    if (!peerModificationEnabled) {
      throw new ReplicationException("Replication peer modification is disabled");
    }
    replicationPeers.disablePeer(peerId);
  }

  public ReplicationPeerConfig getPeerConfig(String peerId) throws ReplicationException,
      ReplicationPeerNotFoundException {
    ReplicationPeerConfig peerConfig = replicationPeers.getReplicationPeerConfig(peerId);
    if (peerConfig == null) {
      throw new ReplicationPeerNotFoundException(peerId);
    }
    return peerConfig;
  }

  public void updatePeerConfig(String peerId, ReplicationPeerConfig peerConfig)
      throws ReplicationException, IOException {
    if (!peerModificationEnabled) {
      throw new ReplicationException("Replication peer modification is disabled");
    }
    checkPeerConfigConflict(peerConfig);
    if (peerConfig.getReplicationEndpointImpl() != null) {
      checkReplicationEndpoint(peerConfig);
    }
    replicationPeers.updatePeerConfig(peerId, peerConfig);
  }

  public List<ReplicationPeerDescription> listReplicationPeers(Pattern pattern)
      throws ReplicationException {
    List<ReplicationPeerDescription> peers = new ArrayList<>();
    List<String> peerIds = replicationPeers.getAllPeerIds();
    for (String peerId : peerIds) {
      if (pattern == null || (pattern != null && pattern.matcher(peerId).matches())) {
        peers.add(new ReplicationPeerDescription(peerId, replicationPeers
            .getStatusOfPeerFromBackingStore(peerId), replicationPeers
            .getReplicationPeerConfig(peerId)));
      }
    }
    return peers;
  }

  private void checkPeerConfigConflict(ReplicationPeerConfig peerConfig)
      throws DoNotRetryIOException {
    if (peerConfig.replicateAllUserTables()) {
      if ((peerConfig.getNamespaces() != null && !peerConfig.getNamespaces().isEmpty())
          || (peerConfig.getTableCFsMap() != null && !peerConfig.getTableCFsMap().isEmpty())) {
        throw new DoNotRetryIOException(
            "Need clean namespaces or table-cfs config fisrtly when you want replicate all cluster");
      }
      checkNamespacesAndTableCfsConfigConflict(peerConfig.getExcludeNamespaces(),
        peerConfig.getExcludeTableCFsMap());
    } else {
      if ((peerConfig.getExcludeNamespaces() != null && !peerConfig.getExcludeNamespaces()
          .isEmpty())
          || (peerConfig.getExcludeTableCFsMap() != null && !peerConfig.getExcludeTableCFsMap()
              .isEmpty())) {
        throw new DoNotRetryIOException(
            "Need clean exclude-namespaces or exclude-table-cfs config firstly when you want not replicate all cluster");
      }
      checkNamespacesAndTableCfsConfigConflict(peerConfig.getNamespaces(),
        peerConfig.getTableCFsMap());
    }
  }

  /**
   * Set a namespace in the peer config means that all tables in this namespace
   * will be replicated to the peer cluster.
   *
   * 1. If you already have set a namespace in the peer config, then you can't set any table
   *    of this namespace to the peer config.
   * 2. If you already have set a table in the peer config, then you can't set this table's
   *    namespace to the peer config.
   *
   * @param namespaces
   * @param tableCfs
   * @throws DoNotRetryIOException
   */
  private void checkNamespacesAndTableCfsConfigConflict(Set<String> namespaces,
      Map<TableName, ? extends Collection<String>> tableCfs) throws DoNotRetryIOException {
    if (namespaces == null || namespaces.isEmpty()) {
      return;
    }
    if (tableCfs == null || tableCfs.isEmpty()) {
      return;
    }
    for (Map.Entry<TableName, ? extends Collection<String>> entry : tableCfs.entrySet()) {
      TableName table = entry.getKey();
      if (namespaces.contains(table.getNamespaceAsString())) {
        throw new DoNotRetryIOException("Table " + table + " 's namespace "
            + table.getNamespaceAsString() + " was already in namespaces config in peer");
      }
    }
  }

  private void checkClusterKey(String clusterKey) throws DoNotRetryIOException {
    Configuration otherConf = new Configuration(this.conf);
    try {
      ZKUtil.applyClusterKeyToConf(otherConf, clusterKey);
    } catch (IOException e) {
      throw new DoNotRetryIOException("Invalid cluster key: " + clusterKey, e);
    }
  }
}
