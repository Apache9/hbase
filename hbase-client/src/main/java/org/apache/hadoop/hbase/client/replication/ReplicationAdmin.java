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
package org.apache.hadoop.hbase.client.replication;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ReplicationPeerNotFoundException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.ReplicationPeerDescription;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;

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
 * To see which commands are available in the shell, type
 * <code>replication</code>.
 * </p>
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class ReplicationAdmin implements Closeable {
  private static final Log LOG = LogFactory.getLog(ReplicationAdmin.class);

  public static final String TNAME = "tableName";
  public static final String CFNAME = "columnFamlyName";

  // only Global for now, can add other type
  // such as, 1) no global replication, or 2) the table is replicated to this cluster, etc.
  public static final String REPLICATIONTYPE = "replicationType";
  public static final String REPLICATIONGLOBAL = Integer.toString(HConstants.REPLICATION_SCOPE_GLOBAL);
  public static final String REPLICATIONSERIAL = Integer.toString(HConstants.REPLICATION_SCOPE_SERIAL);

  private final HConnection connection;

  /**
   * A watcher used by replicationPeers and replicationQueuesClient. Keep reference so can dispose
   * on {@link #close()}.
   */
  private final ZooKeeperWatcher zkw;

  private final boolean cleanupConnectionOnClose; // close the connection in close()

  private final HBaseAdmin admin;

  /**
   * Constructor that creates a connection to the local ZooKeeper ensemble. This will close the
   * connection and zkw when in close()
   * @param conf Configuration to use
   * @throws IOException if an internal replication error occurs
   * @throws RuntimeException if replication isn't enabled.
   */
  public ReplicationAdmin(Configuration conf) throws IOException {
    this(HConnectionManager.getConnection(conf), conf, true);
  }

  /**
   * Constructor that create the local ZooKeeperWatcher.
   * @param connection
   * @param conf Configuration to use
   * @param cleanupConnectionOnClose true means close the connection in close()
   * @throws IOException if an internal replication error occurs
   */
  public ReplicationAdmin(final HConnection connection, final Configuration conf,
      final boolean cleanupConnectionOnClose)
      throws IOException {
    if (!conf.getBoolean(HConstants.REPLICATION_ENABLE_KEY, HConstants.REPLICATION_ENABLE_DEFAULT)) {
      throw new RuntimeException("hbase.replication isn't true, please "
          + "enable it in order to use replication");
    }
    ZooKeeperWatcher zkw = null;
    boolean succeed = false;
    HBaseAdmin admin = null;
    try {
      admin = new HBaseAdmin(connection);
      zkw = createZooKeeperWatcher(conf);
      succeed = true;
    } catch (Exception exception) {
      if (exception instanceof IOException) {
        throw (IOException) exception;
      } else if (exception instanceof RuntimeException) {
        throw (RuntimeException) exception;
      } else {
        throw new IOException("Error initializing the replication admin client.", exception);
      }
    } finally {
      if (!succeed) {
        IOUtils.closeQuietly(admin);
        IOUtils.closeQuietly(zkw);
        if (cleanupConnectionOnClose) {
          IOUtils.closeQuietly(connection);
        }
      }
    }
    this.connection = connection;
    this.zkw = zkw;
    this.cleanupConnectionOnClose = cleanupConnectionOnClose;
    this.admin = admin;
  }

  private ZooKeeperWatcher createZooKeeperWatcher(Configuration conf) throws IOException {
    // This Abortable doesn't 'abort'... it just logs.
    return new ZooKeeperWatcher(conf, "ReplicationAdmin", new Abortable() {
      @Override
      public void abort(String why, Throwable e) {
        LOG.error(why, e);
        // We used to call system.exit here but this script can be embedded by other programs that
        // want to do replication stuff... so inappropriate calling System.exit. Just log for now.
      }

      @Override
      public boolean isAborted() {
        return false;
      }
    });
  }

  /**
   * Add a new remote slave cluster for replication.
   * @param id a short name that identifies the cluster
   * @param peerConfig configuration for the replication slave cluster
   * @param tableCfs the table and column-family list which will be replicated for this peer.
   * A map from tableName to column family names. An empty collection can be passed
   * to indicate replicating all column families. Pass null for replicating all table and column
   * families
   * @throws IOException 
   */
  @Deprecated
  public void addPeer(String id, ReplicationPeerConfig peerConfig,
      Map<TableName, ? extends Collection<String>> tableCfs) throws ReplicationException,
      IOException {
    if (tableCfs != null) {
      peerConfig.setTableCFsMap(tableCfs);
    }
    addPeer(id, peerConfig);
  }

  /**
   * Add a new remote slave cluster for replication.
   * @param id a short name that identifies the cluster
   * @param peerConfig configuration for the replication slave cluster
   * @throws IOException 
   */
  @Deprecated
  public void addPeer(String id, ReplicationPeerConfig peerConfig) throws ReplicationException,
      IOException {
    admin.addReplicationPeer(id, peerConfig);
  }

  /**
   * Removes a peer cluster and stops the replication to it.
   * @param id a short name that identifies the cluster
   * @throws IOException 
   */
  @Deprecated
  public void removePeer(String id) throws ReplicationException, IOException {
    admin.removeReplicationPeer(id);
  }

  /**
   * Restart the replication stream to the specified peer.
   * @param id a short name that identifies the cluster
   * @throws IOException 
   */
  @Deprecated
  public void enablePeer(String id) throws ReplicationException, IOException {
    admin.enableReplicationPeer(id);
  }

  /**
   * Stop the replication stream to the specified peer.
   * @param id a short name that identifies the cluster
   * @throws IOException 
   */
  @Deprecated
  public void disablePeer(String id) throws ReplicationException, IOException {
    admin.disableReplicationPeer(id);
  }

  /**
   * Get the number of slave clusters the local cluster has.
   * @return number of slave clusters
   * @throws IOException 
   */
  @Deprecated
  public int getPeersCount() throws IOException {
    return admin.listReplicationPeers().size();
  }

  @Deprecated
  public Map<String, ReplicationPeerConfig> listPeerConfigs() throws IOException {
    List<ReplicationPeerDescription> peers = admin.listReplicationPeers();
    Map<String, ReplicationPeerConfig> result = new TreeMap<>();
    for (ReplicationPeerDescription peer : peers) {
      result.put(peer.getPeerId(), peer.getPeerConfig());
    }
    return result;
  }

  @Deprecated
  public ReplicationPeerConfig getPeerConfig(String id) throws ReplicationException, IOException {
    return admin.getReplicationPeerConfig(id);
  }

  @Deprecated
  public void updatePeerConfig(String id, ReplicationPeerConfig peerConfig)
      throws ReplicationException, IOException {
    admin.updateReplicationPeerConfig(id, peerConfig);
  }

  /**
   * Get the replicable table-cf config of the specified peer.
   * @param id a short name that identifies the cluster
   * @throws IOException 
   */
  @Deprecated
  public Map<TableName, List<String>> getPeerTableCFs(String id)
      throws ReplicationException, IOException {
    return getPeerConfig(id).getTableCFsMap();
  }

  /**
   * Append the replicable table-cf config of the specified peer
   * @param id a short that identifies the cluster
   * @param tableCfs A map from tableName to column family names
   * @throws ReplicationException
   * @throws IOException 
   */
  @Deprecated
  public void appendPeerTableCFs(String id, Map<TableName, ? extends Collection<String>> tableCfs)
      throws ReplicationException, IOException {
    admin.appendReplicationPeerTableCFs(id, tableCfs);
  }

  /**
   * Remove some table-cfs from config of the specified peer
   * @param id a short name that identifies the cluster
   * @param cfs A map from tableName to column family names
   * @throws ReplicationException
   * @throws IOException 
   */
  @Deprecated
  public void removePeerTableCFs(String id, Map<TableName, ? extends Collection<String>> tableCfs)
      throws ReplicationException, IOException {
    admin.removeReplicationPeerTableCFs(id, tableCfs);
  }

  /**
   * Set the replicable table-cf config of the specified peer
   * @param id a short name that identifies the cluster
   * @param tableCfs the table and column-family list which will be replicated for this peer.
   * A map from tableName to column family names. An empty collection can be passed
   * to indicate replicating all column families. Pass null for replicating all table and column
   * families
   * @throws IOException 
   */
  @Deprecated
  public void setPeerTableCFs(String id, Map<TableName, ? extends Collection<String>> tableCfs)
      throws ReplicationException, IOException {
    ReplicationPeerConfig peerConfig = getPeerConfig(id);
    peerConfig.setTableCFsMap(tableCfs);
    admin.updateReplicationPeerConfig(id, peerConfig);
  }

  /**
   * Set the replication source per node bandwidth for the specified peer
   * @param id a short name that identifies the cluster
   * @param bandwidth the replication source per node bandwidth
   * @throws IOException 
   */
  @Deprecated
  public void setPeerBandwidth(String id, long bandwidth) throws ReplicationException, IOException {
    ReplicationPeerConfig peerConfig = getPeerConfig(id);
    peerConfig.setBandwidth(bandwidth);
    admin.updateReplicationPeerConfig(id, peerConfig);
  }

  /**
   * Get the state of the specified peer cluster
   * @param id String format of the Short name that identifies the peer,
   * an IllegalArgumentException is thrown if it doesn't exist
   * @return true if replication is enabled to that peer, false if it isn't
   * @throws IOException 
   */
  @Deprecated
  public boolean getPeerState(String id) throws ReplicationException, IOException {
    List<ReplicationPeerDescription> peers = admin.listReplicationPeers(id);
    if (peers.isEmpty() || !id.equals(peers.get(0).getPeerId())) {
      throw new ReplicationPeerNotFoundException(id);
    }
    return peers.get(0).isEnabled();
  }

  @Override
  public void close() throws IOException {
    IOUtils.closeQuietly(admin);
    IOUtils.closeQuietly(zkw);
    if (cleanupConnectionOnClose) {
      IOUtils.closeQuietly(connection);
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
  @Deprecated
  public List<HashMap<String, String>> listReplicated() throws IOException {
    List<HashMap<String, String>> replicationColFams = new ArrayList<>();
    admin.listReplicatedTableCFs().forEach(
      (tableCFs) -> {
        String table = tableCFs.getTable().getNameAsString();
        tableCFs.getColumnFamilyMap()
            .forEach(
              (cf, scope) -> {
                HashMap<String, String> replicationEntry = new HashMap<>();
                replicationEntry.put(TNAME, table);
                replicationEntry.put(CFNAME, cf);
                replicationEntry.put(REPLICATIONTYPE,
                  scope == HConstants.REPLICATION_SCOPE_GLOBAL ? REPLICATIONGLOBAL
                      : REPLICATIONSERIAL);
                replicationColFams.add(replicationEntry);
              });
      });
    return replicationColFams;
  }

  /**
   * Enable a table's replication switch.
   * @param tableName name of the table
   * @throws IOException if a remote or network exception occurs
   */
  @Deprecated
  public void enableTableRep(final TableName tableName) throws IOException {
    admin.enableTableReplication(tableName);
  }

  /**
   * Disable a table's replication switch.
   * @param tableName name of the table
   * @throws IOException if a remote or network exception occurs
   */
  @Deprecated
  public void disableTableRep(final TableName tableName) throws IOException {
    admin.disableTableReplication(tableName);
  }

  @Deprecated
  public void upgradeTableCFs() {
    ReplicationPeerConfigUpgrader tableCFsUpdater = new ReplicationPeerConfigUpgrader(zkw,
        connection.getConfiguration(), connection);
    tableCFsUpdater.copyTableCFs();
  }
}
