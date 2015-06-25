/**
 * Copyright 2010 The Apache Software Foundation
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
package org.apache.hadoop.hbase.client.replication;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.HashMap;
import java.lang.Integer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.replication.ReplicationZookeeper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.HColumnDescriptor;

/**
 * <p>
 * This class provides the administrative interface to HBase cluster
 * replication. In order to use it, the cluster and the client using
 * ReplicationAdmin must be configured with <code>hbase.replication</code>
 * set to true.
 * </p>
 * <p>
 * Adding a new peer results in creating new outbound connections from every
 * region server to a subset of region servers on the slave cluster. Each
 * new stream of replication will start replicating from the beginning of the
 * current HLog, meaning that edits from that past will be replicated.
 * </p>
 * <p>
 * Removing a peer is a destructive and irreversible operation that stops
 * all the replication streams for the given cluster and deletes the metadata
 * used to keep track of the replication state.
 * </p>
 * <p>
 * Enabling and disabling peers is currently not supported.
 * </p>
 * <p>
 * As cluster replication is still experimental, a kill switch is provided
 * in order to stop all replication-related operations, see
 * {@link #setReplicating(boolean)}. When setting it back to true, the new
 * state of all the replication streams will be unknown and may have holes.
 * Use at your own risk.
 * </p>
 * <p>
 * To see which commands are available in the shell, type
 * <code>replication</code>.
 * </p>
 */
public class ReplicationAdmin implements Closeable {

  private final static Log LOG = LogFactory.getLog(ReplicationAdmin.class);
  public static final String TNAME = "tableName";
  public static final String CFNAME = "columnFamlyName";

  // only Global for now, can add other type
  // such as, 1) no global replication, or 2) the table is replicated to this cluster, etc.
  public static final String REPLICATIONTYPE = "replicationType";
  public static final String REPLICATIONGLOBAL = Integer
      .toString(HConstants.REPLICATION_SCOPE_GLOBAL);
      
  private final ReplicationZookeeper replicationZk;
  private final HConnection connection;
  private final HBaseAdmin hbaseAdmin;

  /**
   * Constructor that creates a connection to the local ZooKeeper ensemble.
   * @param conf Configuration to use
   * @throws IOException if the connection to ZK cannot be made
   * @throws RuntimeException if replication isn't enabled.
   */
  public ReplicationAdmin(Configuration conf) throws IOException {
    if (!conf.getBoolean(HConstants.REPLICATION_ENABLE_KEY, false)) {
      throw new RuntimeException("hbase.replication isn't true, please " +
          "enable it in order to use replication");
    }
    this.connection = HConnectionManager.getConnection(conf);
    ZooKeeperWatcher zkw = this.connection.getZooKeeperWatcher();
    try {
      this.replicationZk = new ReplicationZookeeper(this.connection, conf, zkw);
      this.hbaseAdmin = new HBaseAdmin(this.connection);
    } catch (KeeperException e) {
      throw new IOException("Unable setup the ZooKeeper connection", e);
    } catch (MasterNotRunningException e) {
      throw new IOException("Unable new HBaseAdmin", e);
    } catch (ZooKeeperConnectionException e) {
      throw new IOException("Unable new HBaseAdmin", e);
    }
  }

  /**
   * Add a new peer cluster to replicate to.
   * @param id a short that identifies the cluster
   * @param clusterKey the concatenation of the slave cluster's
   * <code>hbase.zookeeper.quorum:hbase.zookeeper.property.clientPort:zookeeper.znode.parent</code>
   * @throws IllegalStateException if there's already one slave since
   * multi-slave isn't supported yet.
   */
  public void addPeer(String id, String clusterKey) throws IOException {
    this.replicationZk.addPeer(id, clusterKey);
  }

  public void addPeer(String id, String clusterKey, String peerState) throws IOException {
    this.replicationZk.addPeer(id, clusterKey, peerState);
  }

  public void addPeer(String id, String clusterKey, String peerState, String tableCFs)
    throws IOException {
    addPeer(id, clusterKey, peerState, tableCFs, 0l);
  }
  
  public void addPeer(String id, String clusterKey, String peerState, String tableCFs,
      Long bandwidth) throws IOException {
    checkTableCFs(tableCFs);
    this.replicationZk.addPeer(id, clusterKey, peerState, tableCFs, bandwidth);    
  }
  
  public void addPeer(String id, String clusterKey, String peerState, String tableCFs,
      Long bandwidth, String protocol) throws IOException {
    checkTableCFs(tableCFs);
    this.replicationZk.addPeer(id, clusterKey, peerState, tableCFs, bandwidth,
      ReplicationZookeeper.PeerProtocol.valueOf(protocol));
  }
  
  /**
   * Get the protocol for the peer
   * @param id peer's identifier
   * @return current protocol of the peer
   */
  public String getPeerProtocol(String id) throws IOException {
    try {
      return this.replicationZk.getPeerProtocol(id).name();
    } catch (KeeperException e) {
      throw new IOException("Couldn't get the protocol of the peer " + id, e);
    }
  }
  
  /**
   * Removes a peer cluster and stops the replication to it.
   * @param id a short that identifies the cluster
   */
  public void removePeer(String id) throws IOException {
    this.replicationZk.removePeer(id);
  }

  /**
   * Restart the replication stream to the specified peer.
   * @param id a short that identifies the cluster
   */
  public void enablePeer(String id) throws IOException {
    this.replicationZk.enablePeer(id);
  }

  /**
   * Stop the replication stream to the specified peer.
   * @param id a short that identifies the cluster
   */
  public void disablePeer(String id) throws IOException {
    this.replicationZk.disablePeer(id);
  }

  /**
   * Get the number of slave clusters the local cluster has.
   * @return number of slave clusters
   */
  public int getPeersCount() {
    return this.replicationZk.listPeersIdsAndWatch().size();
  }

  /**
   * Map of this cluster's peers for display.
   * @return A map of peer ids to peer cluster keys
   */
  public Map<String, String> listPeers() {
    return this.replicationZk.listPeers();
  }

  /**
   * Get state of the peer
   *
   * @param id peer's identifier
   * @return current state of the peer
   */
  public String getPeerState(String id) throws IOException {
    try {
      return this.replicationZk.getPeerState(id).name();
    } catch (KeeperException e) {
      throw new IOException("Couldn't get the state of the peer " + id, e);
    }
  }

  /**
   * Get the replicable table-cf config of the specified peer.
   * @param id a short that identifies the cluster
   */
  public String getPeerTableCFs(String id)
    throws IOException, KeeperException {
    return this.replicationZk.getTableCFsStr(id);
  }

  public long getPeerBandwidth(String id)
      throws IOException, KeeperException {
      return this.replicationZk.getPeerBandwidthFromZK(id);
    }
  
  /**
   * Set the replicable table-cf config of the specified peer
   * @param id a short that identifies the cluster
   */
  public void setPeerTableCFs(String id, String tableCFs)
    throws IOException {
    checkTableCFs(tableCFs);
    this.replicationZk.setTableCFsStr(id, tableCFs);
  }
  
  public void setPeerBandwidth(String id, Long bandwidth) throws IOException {
    this.replicationZk.setPeerBandwidth(id, bandwidth);
  }

  /**
   * Remote the replicable table-cf config of the specified peer
   * @param id a short that identifies the cluster
   * @throws KeeperException
  */
  public void removePeerTableCFs(String id, String tableCFs)
      throws IOException, KeeperException {
    if (tableCFs == null || tableCFs.isEmpty()) {
      return;
    }
    
    String prevTableCFs = getPeerTableCFs(id);
    if (prevTableCFs != null && !prevTableCFs.isEmpty()) {
      if (prevTableCFs.contains(tableCFs)) {
        tableCFs = prevTableCFs.replace(tableCFs, "");
        // the empty tableCFs means all tables with REPLICATION_SCOPE = 1 will be replicated
        if (isTableCFsEmpty(tableCFs)) {
          LOG.warn("tableCFs will become empty after " + tableCFs
              + " removed, please use 'removePeer' instead!");
          return;
        }
        LOG.info("The new table-cf config for peer: " + id + " is: " + tableCFs);
        checkTableCFs(tableCFs);
        this.replicationZk.setTableCFsStr(id, tableCFs);
      } else {
        LOG.warn(tableCFs + " not contained in the tableCFs of peerId " + id);
      }
    } else {
      LOG.warn("no previous TableCFs, could not remove " + tableCFs);
    }
  }
  
  private boolean isTableCFsEmpty(String tableCFs) throws IOException {
    String[] tables = tableCFs.split(";");
    for (String tab : tables) {
      if (!tab.trim().isEmpty()) {
        return false;
      }
    }
    return true;
  }
  
  /**
   * Append the replicable table-cf config of the specified peer
   * @param id a short that identifies the cluster
   * @throws KeeperException
   */
  public void appendPeerTableCFs(String id, String tableCFs)
      throws IOException, KeeperException {
    String prevTableCFs = getPeerTableCFs(id);
    if (prevTableCFs != null && !prevTableCFs.isEmpty()) {
      tableCFs = getPeerTableCFs(id) + ";" + tableCFs;
    }
    LOG.info("The new table-cf config for peer: " + id + " is: " + tableCFs);
    checkTableCFs(tableCFs);
    this.replicationZk.setTableCFsStr(id, tableCFs);
  }
  
  private void checkTableCFs(String tableCFs) throws IOException {
    String[] tables = tableCFs.split(";");
    for (String tab : tables) {
      // 1. split to "table" and "cf1,cf2"
      //    for each table: "table:cf1,cf2" or "table"
      String[] pair = tab.split(":");
      if (pair.length > 2) {
        throw new IOException("invalid tableCFs format: '" + tab + "'");
      }
      // 2. get "table" part
      String tabName = pair[0].trim();

      if (tabName.isEmpty()) {
        continue;
      }

      // 3. check table existence
      if (!hbaseAdmin.tableExists(tabName)) {
        throw new IOException("table '" + tabName + "' not exist");
      }

      HTableDescriptor tabDesc =
        hbaseAdmin.getTableDescriptor(Bytes.toBytes(tabName));

      // 4. parse "cf1,cf2" part
      if (pair.length == 2) {
        String[] cfsList = pair[1].split(",");
        for (String cf : cfsList) {
          String cfName = cf.trim();
          if (!cfName.isEmpty() &&
              !tabDesc.hasFamily(Bytes.toBytes(cfName))) {
            throw new IOException("table '" + tabName +
                "' has no such column family: '" + cfName + "'");
          }
        }
      }
    }
  }

  /**
   * Get the current status of the kill switch, if the cluster is replicating
   * or not.
   * @return true if the cluster is replicated, otherwise false
   */
  public boolean getReplicating() throws IOException {
    try {
      return this.replicationZk.getReplication();
    } catch (KeeperException e) {
      throw new IOException("Couldn't get the replication status");
    }
  }

  /**
   * Kill switch for all replication-related features
   * @param newState true to start replication, false to stop it.
   * completely
   * @return the previous state
   */
  public boolean setReplicating(boolean newState) throws IOException {
    boolean prev = true;
    try {
      prev = getReplicating();
      this.replicationZk.setReplicating(newState);
    } catch (KeeperException e) {
      throw new IOException("Unable to set the replication state", e);
    }
    return prev;
  }

  /**
   * Get the ZK-support tool created and used by this object for replication.
   * @return the ZK-support tool
   */
  ReplicationZookeeper getReplicationZk() {
    return replicationZk;
  }

  @Override
  public void close() throws IOException {
    if (this.connection != null) {
      this.connection.close();
    }
  }
  
  /**
   * Find all column families that are replicated from this cluster
   * @return the full list of the replicated column families of this cluster as:
   *        tableName, family name, replicationType
   *
   * Currently replicationType is Global. In the future, more replication
   * types may be extended here. For example
   *  1) the replication may only apply to selected peers instead of all peers
   *  2) the replicationType may indicate the host Cluster servers as Slave
   *     for the table:columnFam.         
   */
  public List<HashMap<String, String>> listReplicated() throws IOException {
    List<HashMap<String, String>> replicationColFams = new ArrayList<HashMap<String, String>>();
    HTableDescriptor[] tables = this.connection.listTables();
  
    for (HTableDescriptor table : tables) {
      HColumnDescriptor[] columns = table.getColumnFamilies();
      String tableName = table.getNameAsString();
      for (HColumnDescriptor column : columns) {
        if (column.getScope() != HConstants.REPLICATION_SCOPE_LOCAL) {
          // At this moment, the columfam is replicated to all peers
          HashMap<String, String> replicationEntry = new HashMap<String, String>();
          replicationEntry.put(TNAME, tableName);
          replicationEntry.put(CFNAME, column.getNameAsString());
          replicationEntry.put(REPLICATIONTYPE, REPLICATIONGLOBAL);
          replicationColFams.add(replicationEntry);
        }
      }
    }
 
    return replicationColFams;
  } 
}
