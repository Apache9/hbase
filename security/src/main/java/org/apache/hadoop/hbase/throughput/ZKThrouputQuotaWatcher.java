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

package org.apache.hadoop.hbase.throughput;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperListener;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.List;

/**
 * Handles synchronization of throughput quota entries and updates throughout all nodes in the
 * cluster. The {@link AccessController} instance on the {@code _quota_} table regions, creates a
 * znode for each table as {@code /hbase/quota/throughput/tablename}, with the znode data containing
 * a serialized list of the throughput quota set for the current user on this table (data in node
 * {@code /hbase/quota/throughput} is used for per user quota). The {@code AccessController}
 * instances on all other cluster hosts watch the znodes for updates, which trigger updates in the
 * {@link ThroughputManager} quota cache.
 */
public class ZKThrouputQuotaWatcher extends ZooKeeperListener {
  private static Log LOG = LogFactory.getLog(ZKThrouputQuotaWatcher.class);
  // parent node for throughput quota
  private static final String QUOTA_NODE = "quota/throughput";
  private ThroughputManager quotaManager;
  private String quotaZNode;

  public ZKThrouputQuotaWatcher(ZooKeeperWatcher watcher,
      ThroughputManager quotaManager, Configuration conf) {
    super(watcher);
    this.quotaManager = quotaManager;
    String quotaZnodeParent = conf.get(Constants.CONF_THROUGHPUT_QUOTA_ZKROOT, QUOTA_NODE);
    this.quotaZNode = ZKUtil.joinZNode(watcher.baseZNode, quotaZnodeParent);
  }

  public void start() throws KeeperException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Start zookeeper watcher with root path " + quotaZNode);
    }
    watcher.registerListener(this);
    if (ZKUtil.watchAndCheckExists(watcher, quotaZNode)) {
      List<ZKUtil.NodeAndData> existing =
          ZKUtil.getChildDataAndWatchForNewChildren(watcher, quotaZNode);
      if (existing != null) {
        refreshNodes(existing);
      }
    }
  }

  @Override
  public void nodeCreated(String path) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Zookeeper node created with path " + path);
    }
    if (path.equals(quotaZNode)) {
      try {
        List<ZKUtil.NodeAndData> nodes =
            ZKUtil.getChildDataAndWatchForNewChildren(watcher, quotaZNode);
        refreshNodes(nodes);
      } catch (KeeperException ke) {
        LOG.error("Failed to get children for zookeeper node " + path, ke);
        // only option is to abort
        watcher.abort("Failed to get children for zookeeper node " + path, ke);
      }
    }
  }

  @Override
  public void nodeDeleted(String path) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Zookeeper node deleted for path " + path);
    }
    if (quotaZNode.equals(ZKUtil.getParent(path))) {
      String table = ZKUtil.getNodeName(path);
      quotaManager.removeTableLimiters(table);
    }
  }

  @Override
  public void nodeDataChanged(String path) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Zookeeper node data changed for path " + path);
    }
    try {
      if (quotaZNode.equals(ZKUtil.getParent(path))) {
        String table = ZKUtil.getNodeName(path);
        byte[] data = ZKUtil.getDataAndWatch(watcher, path);
        refreshNode(table, data);
      }
    } catch (KeeperException ke) {
      LOG.error("Failed to read data from zookeeper node " + path, ke);
      // only option is to abort
      watcher.abort("Failed to read data from zookeeper node " + path, ke);
    }
  }

  @Override
  public void nodeChildrenChanged(String path) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Zookeeper node children changed with path " + path);
    }
    if (path.equals(quotaZNode)) {
      // table throughput quota changed
      try {
        List<ZKUtil.NodeAndData> nodes =
            ZKUtil.getChildDataAndWatchForNewChildren(watcher, quotaZNode);
        refreshNodes(nodes);
      } catch (KeeperException ke) {
        LOG.error("Failed to get children for zookeeper node " + path, ke);
        watcher.abort("Failed to get children for zookeeper node " + path, ke);
      }
    }
  }

  /**
   * Refresh throughput quota from zk node.
   */
  private void refreshNode(String table, byte[] data) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Updating throughput quota cache from zookeeper node " + table
          + " with data: " + Bytes.toStringBinary(data));
    }
    try {
      quotaManager.refreshTableLimiters(table, data);
    } catch (IOException ioe) {
      LOG.error("Failed to parse throughput quota from zookeeper node " + table, ioe);
    }
  }

  private void refreshNodes(List<ZKUtil.NodeAndData> nodes) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Refresh from zookeeper nodes: " + nodes);
    }
    for (ZKUtil.NodeAndData node : nodes) {
      if (!node.isEmpty()) {
        String path = node.getNode();
        byte[] data = node.getData();
        if (quotaZNode.equals(ZKUtil.getParent(path))) {
          String table = ZKUtil.getNodeName(path);
          refreshNode(table, data);
        }
      }
    }
  }

  /**
   * Write per user throughput quota mirror in zookeeper. The caller can only be: 1) postOpen of
   * quota region 2) postPut and postDeelte of quota region 3) Test helper methods
   */
  public void writeToZookeeper(String tableName, byte[] data) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Write to zookeeper for table '" + tableName + "' with data: "
          + Bytes.toStringBinary(data));
    }
    String zkNode = ZKUtil.joinZNode(watcher.baseZNode, QUOTA_NODE);
    zkNode = ZKUtil.joinZNode(zkNode, tableName);
    try {
      ZKUtil.createWithParents(watcher, zkNode);
      ZKUtil.updateExistingNodeData(watcher, zkNode, data, -1);
    } catch (KeeperException e) {
      LOG.error("Failed to update throughput quota to node " + zkNode, e);
      watcher.abort("Failed to update throughput quota to node " + zkNode, e);
    }
  }

  public void removeFromZookeeper(String tableName) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Remove node from zookeeper for table '" + tableName);
    }
    String zkNode = ZKUtil.joinZNode(watcher.baseZNode, QUOTA_NODE);
    zkNode = ZKUtil.joinZNode(zkNode, tableName);
    try {
      ZKUtil.deleteNode(watcher, zkNode);
    } catch (KeeperException e) {
      try {
        if (ZKUtil.checkExists(watcher, zkNode) == -1) {
          return;
        }
      } catch (KeeperException e1) {
      }
      LOG.error("Failed to delete throughput quota from node " + zkNode, e);
      watcher.abort("Failed to delete throughput quota from node " + zkNode, e);
    }
  }
}
