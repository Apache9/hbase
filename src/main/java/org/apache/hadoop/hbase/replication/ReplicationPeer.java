/*
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
package org.apache.hadoop.hbase.replication;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.replication.ReplicationZookeeper.PeerState;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperNodeTracker;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

/**
 * This class acts as a wrapper for all the objects used to identify and
 * communicate with remote peers and is responsible for answering to expired
 * sessions and re-establishing the ZK connections.
 */
public class ReplicationPeer implements Abortable {
  private static final Log LOG = LogFactory.getLog(ReplicationPeer.class);

  private final String clusterKey;
  private final String id;
  private List<ServerName> regionServers = new ArrayList<ServerName>(0);
  private final AtomicBoolean peerEnabled = new AtomicBoolean();
  private Map<String, List<String>> tableCFs =
    new HashMap<String, List<String>>();
  private Map<String, List<String>> excludedTableCFs =
      new HashMap<String, List<String>>();
  private AtomicLong bandwidth = new AtomicLong();
  // Cannot be final since a new object needs to be recreated when session fails
  private ZooKeeperWatcher zkw;
  private final Configuration conf;

  private PeerStateTracker peerStateTracker;
  private TableCFsTracker tableCFsTracker;
  private ExcludedTableCFsTracker excludedTableCFsTracker;
  private BandwidthTracker bandwidthTracker;
  
  /**
   * Constructor that takes all the objects required to communicate with the
   * specified peer, except for the region server addresses.
   * @param conf configuration object to this peer
   * @param key cluster key used to locate the peer
   * @param id string representation of this peer's identifier
   */
  public ReplicationPeer(Configuration conf, String key,
      String id) throws IOException {
    this.conf = conf;
    this.clusterKey = key;
    this.id = id;
    this.reloadZkWatcher();
  }
  
  /**
   * start a state tracker to check whether this peer is enabled or not
   *
   * @param zookeeper zk watcher for the local cluster
   * @param peerStateNode path to zk node which stores peer state
   * @throws KeeperException
   * @throws IOException
   */
  public void startStateTracker(ZooKeeperWatcher zookeeper, String peerStateNode)
      throws KeeperException, IOException {
    if (!waitZnodeAvailable(zookeeper, peerStateNode)) {
      LOG.error("wait 5s for client to create" + peerStateNode + " but failed!" +
          " give up startStateTracker for this peer");
      throw new IOException("Failed to start peer state tracker");
    }
    this.peerStateTracker = new PeerStateTracker(peerStateNode, zookeeper,
        this);
    this.peerStateTracker.start();
    this.readPeerStateZnode();
  }

  private void readPeerStateZnode() {
    String currentState = Bytes.toString(peerStateTracker.getData(false));
    this.peerEnabled.set(PeerState.ENABLED.equals(PeerState
        .valueOf(currentState)));
  }
  
  private void readBandwidthZnode() {
    String currentBandwidth = Bytes.toString(bandwidthTracker.getData(false));
    this.bandwidth.set(Long.parseLong(currentBandwidth));
  }

  /**
   * start a table-cfs tracker to listen the (table, cf-list) map change
   *
   * @param zookeeper zk watcher for the local cluster
   * @param tableCFsNode path to zk node which stores table-cfs
   * @throws KeeperException
   * @throws IOException
   */
  public void startTableCFsTracker(ZooKeeperWatcher zookeeper, String tableCFsNode)
      throws KeeperException, IOException {
    if (!waitZnodeAvailable(zookeeper, tableCFsNode)) {
      LOG.error("wait 5s for client to create" + tableCFsNode + " but failed!" +
          " give up startTableCFsTracker for this peer");
      throw new IOException("Failed to start peer table-cfs tracker");
    }

    this.tableCFsTracker = new TableCFsTracker(tableCFsNode, zookeeper,
        this);
    this.tableCFsTracker.start();
    this.readTableCFsZnode();
  }

  // TODO : reuse the common code among startStateTracker/startTableCFsTracker/startBandwidthTracker
  public void startBandwidthTracker(ZooKeeperWatcher zookeeper, String bandwidthNode)
      throws KeeperException {
    if (!waitZnodeAvailable(zookeeper, bandwidthNode)) {
      LOG.error("wait 5s for client to create" + bandwidthNode + " but failed!" +
          " give up startBandwidthTracker for this peer");
      return;
    }

    this.bandwidthTracker = new BandwidthTracker(bandwidthNode, zookeeper, this);
    this.bandwidthTracker.start();
    this.readBandwidthZnode();
  }

  private void readTableCFsZnode() {
    String currentTableCFs = Bytes.toString(tableCFsTracker.getData(false));

    if (currentTableCFs == null || currentTableCFs.trim().isEmpty()) {
      this.tableCFs.clear();
      return;
    }

    this.tableCFs = translateTableCFs(currentTableCFs);
  }

  /**
   * start a exclude table-cfs tracker to listen the (table, cf-list) map change
   *
   * @param zookeeper zk watcher for the local cluster
   * @param tableCFsNode path to zk node which stores table-cfs
   * @throws KeeperException
   * @throws IOException
   */
  public void startExcludedTableCFsTracker(ZooKeeperWatcher zookeeper, String excludedTableCFsNode)
      throws KeeperException, IOException {
    if (!waitZnodeAvailable(zookeeper, excludedTableCFsNode)) {
      LOG.error("wait 5s for client to create" + excludedTableCFsNode + " but failed!" +
          " give up startExcludeTableCFsTracker for this peer");
      throw new IOException("Failed to start peer excluded table-cfs tracker");
    }

    this.excludedTableCFsTracker = new ExcludedTableCFsTracker(excludedTableCFsNode, zookeeper,
        this);
    this.excludedTableCFsTracker.start();
    this.readExcludedTableCFsZnode();
  }

  private void readExcludedTableCFsZnode() {
    String currentExcludedTableCFs = Bytes.toString(excludedTableCFsTracker.getData(false));

    if (currentExcludedTableCFs == null || currentExcludedTableCFs.trim().isEmpty()) {
      this.excludedTableCFs.clear();
      return;
    }

    this.excludedTableCFs = translateTableCFs(currentExcludedTableCFs);
  }

  private Map<String, List<String>> translateTableCFs(String tableCFs) {
    // 1. parse out (table, cf-list) pairs from currentTableCFs
    // format: "table1:cf1,cf2;table2:cfA,cfB"
    String[] tables = tableCFs.split(";");
    Map<String, List<String>> curMap = new HashMap<String, List<String>>();
    for (String tab : tables) {
      // 1.1 split to "table" and "cf1,cf2"
      // for each table: "table:cf1,cf2" or "table"
      String[] pair = tab.split(":");
      if (pair.length > 2) {
        LOG.error("ignore invalid tableCFs setting: " + tab);
        continue;
      }

      // 1.2 get "table" part
      String tabName = pair[0].trim();

      // 1.3 parse "cf1,cf2" part to List<cf>
      List<String> cfs = new ArrayList<String>();
      if (pair.length == 2) {
        String[] cfsList = pair[1].split(",");
        for (String cf : cfsList) {
          String cfName = cf.trim();
          if (cfName.length() > 0) {
            cfs.add(cfName);
          }
        }
      }

      // 1.4 put <table, List<cf>> to map
      curMap.put(tabName, cfs);
    }
    return curMap;
  }

  /**
   * Wait 5000ms for client to add zknode
   * @param zookeeper
   * @param zNode
   * @throws KeeperException
   */
  private boolean waitZnodeAvailable(ZooKeeperWatcher zookeeper, String zNode)
      throws KeeperException {
    int times = 500;
    while (ZKUtil.checkExists(zookeeper, zNode) == -1) {
      if (times-- < 0) {
        return false;
      }
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
      }
    }
    return true;
  }

  /**
   * Get the cluster key of that peer
   * @return string consisting of zk ensemble addresses, client port
   * and root znode
   */
  public String getClusterKey() {
    return clusterKey;
  }

  /**
   * Get the state of this peer
   * @return atomic boolean that holds the status
   */
  public AtomicBoolean getPeerEnabled() {
    return peerEnabled;
  }

  /**
   * Get replicable (table, cf-list) map of this peer
   * @return the replicable (table, cf-list) map
   */
  public Map<String, List<String>> getTableCFs() {
    return this.tableCFs;
  }

  /**
   * Get excluded (table, cf-list) map of this peer
   * @return the excluded (table, cf-list) map
   */
  public Map<String, List<String>> getExcludedTableCFs() {
    return this.excludedTableCFs;
  }

  public long getBandwidth() {
    return this.bandwidth.get();
  }

  /**
   * Get a list of all the addresses of all the region servers
   * for this peer cluster
   * @return list of addresses
   */
  public List<ServerName> getRegionServers() {
    return regionServers;
  }

  /**
   * Set the list of region servers for that peer
   * @param regionServers list of addresses for the region servers
   */
  public void setRegionServers(List<ServerName> regionServers) {
    this.regionServers = regionServers;
  }

  /**
   * Get the ZK connection to this peer
   * @return zk connection
   */
  public ZooKeeperWatcher getZkw() {
    return zkw;
  }

  /**
   * Get the identifier of this peer
   * @return string representation of the id (short)
   */
  public String getId() {
    return id;
  }

  /**
   * Get the configuration object required to communicate with this peer
   * @return configuration object
   */
  public Configuration getConfiguration() {
    return conf;
  }

  @Override
  public void abort(String why, Throwable e) {
    LOG.fatal("The ReplicationPeer coresponding to peer " + clusterKey
        + " was aborted for the following reason(s):" + why, e);
  }

  /**
   * Closes the current ZKW (if not null) and creates a new one
   * @throws IOException If anything goes wrong connecting
   */
  public void reloadZkWatcher() throws IOException {
    if (zkw != null) zkw.close();
    zkw = new ZooKeeperWatcher(conf,
        "connection to cluster: " + id, this);
  }

  @Override
  public boolean isAborted() {
    // Currently the replication peer is never "Aborted", we just log when the
    // abort method is called.
    return false;
  }

  /**
   * Tracker for state of this peer
   */
  public class PeerStateTracker extends ZooKeeperNodeTracker {

    public PeerStateTracker(String peerStateZNode, ZooKeeperWatcher watcher,
        Abortable abortable) {
      super(watcher, peerStateZNode, abortable);
    }

    @Override
    public synchronized void nodeDataChanged(String path) {
      if (path.equals(node)) {
        super.nodeDataChanged(path);
        readPeerStateZnode();
      }
    }
  }

  /**
   * Tracker for (table, cf-list) map of this peer
   */
  public class TableCFsTracker extends ZooKeeperNodeTracker {

    public TableCFsTracker(String tableCFsZNode, ZooKeeperWatcher watcher,
        Abortable abortable) {
      super(watcher, tableCFsZNode, abortable);
    }

    @Override
    public synchronized void nodeDataChanged(String path) {
      if (path.equals(node)) {
        super.nodeDataChanged(path);
        readTableCFsZnode();
      }
    }
  }

  /**
   * Tracker for the exclude (table, cf-list) map of this peer
   */
  public class ExcludedTableCFsTracker extends ZooKeeperNodeTracker {

    public ExcludedTableCFsTracker(String excludedTableCFsZNode, ZooKeeperWatcher watcher,
        Abortable abortable) {
      super(watcher, excludedTableCFsZNode, abortable);
    }

    @Override
    public synchronized void nodeDataChanged(String path) {
      if (path.equals(node)) {
        super.nodeDataChanged(path);
        readExcludedTableCFsZnode();
      }
    }
  }

  public class BandwidthTracker extends ZooKeeperNodeTracker {

    public BandwidthTracker(String bandwidthZNode, ZooKeeperWatcher watcher,
        Abortable abortable) {
      super(watcher, bandwidthZNode, abortable);
    }

    @Override
    public synchronized void nodeDataChanged(String path) {
      if (path.equals(node)) {
        super.nodeDataChanged(path);
        readBandwidthZnode();
      }
    }
  }
}
