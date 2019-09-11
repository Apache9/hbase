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
package org.apache.hadoop.hbase.zookeeper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.classification.InterfaceAudience;

/**
 * Class that hold all the paths of znode for HBase.
 */
@InterfaceAudience.Private
public class ZNodePaths {

  // base znode for this cluster
  public final String baseZNode;
  // znode containing location of server hosting meta region
  public final String metaServerZNode;
  // znode containing ephemeral nodes of the regionservers
  public final String rsZNode;
  // znode containing ephemeral nodes of the draining regionservers
  public final String drainingZNode;
  // znode of currently active master
  public final String masterAddressZNode;
  // znode of this master in backup master directory, if not the active master
  public final String backupMasterAddressesZNode;
  // znode containing the current cluster state
  public final String clusterStateZNode;
  // znode used for region transitioning and assignment
  public final String assignmentZNode;
  // znode used for table disabling/enabling
  public final String tableZNode;
  // znode containing the unique cluster ID
  public final String clusterIdZNode;
  // znode used for log splitting work assignment
  public final String splitLogZNode;
  // znode containing the state of the load balancer
  public final String balancerZNode;
  // znode containing the lock for the tables
  public final String tableLockZNode;
  // znode containing the state of recovering regions
  public final String recoveringRegionsZNode;
  // znode containing namespace descriptors
  public final String namespaceZNode;
  // znode containing throttle state
  public final String throttleZNode;
  // znode containing the state of all switches, currently there are split and merge child node.
  public final String switchZNode;

  public ZNodePaths(Configuration conf) {
    baseZNode =
        conf.get(HConstants.ZOOKEEPER_ZNODE_PARENT, HConstants.DEFAULT_ZOOKEEPER_ZNODE_PARENT);
    metaServerZNode =
        ZKUtil.joinZNode(baseZNode, conf.get("zookeeper.znode.metaserver", "meta-region-server"));
    rsZNode = ZKUtil.joinZNode(baseZNode, conf.get("zookeeper.znode.rs", "rs"));
    drainingZNode =
        ZKUtil.joinZNode(baseZNode, conf.get("zookeeper.znode.draining.rs", "draining"));
    masterAddressZNode = ZKUtil.joinZNode(baseZNode, conf.get("zookeeper.znode.master", "master"));
    backupMasterAddressesZNode =
        ZKUtil.joinZNode(baseZNode, conf.get("zookeeper.znode.backup.masters", "backup-masters"));
    clusterStateZNode = ZKUtil.joinZNode(baseZNode, conf.get("zookeeper.znode.state", "running"));
    assignmentZNode =
        ZKUtil.joinZNode(baseZNode, conf.get("zookeeper.znode.unassigned", "region-in-transition"));
    tableZNode =
        ZKUtil.joinZNode(baseZNode, conf.get("zookeeper.znode.tableEnableDisable", "table"));
    clusterIdZNode = ZKUtil.joinZNode(baseZNode, conf.get("zookeeper.znode.clusterId", "hbaseid"));
    splitLogZNode = ZKUtil.joinZNode(baseZNode,
      conf.get("zookeeper.znode.splitlog", HConstants.SPLIT_LOGDIR_NAME));
    balancerZNode = ZKUtil.joinZNode(baseZNode, conf.get("zookeeper.znode.balancer", "balancer"));
    tableLockZNode =
        ZKUtil.joinZNode(baseZNode, conf.get("zookeeper.znode.tableLock", "table-lock"));
    recoveringRegionsZNode = ZKUtil.joinZNode(baseZNode,
      conf.get("zookeeper.znode.recovering.regions", "recovering-regions"));
    namespaceZNode =
        ZKUtil.joinZNode(baseZNode, conf.get("zookeeper.znode.namespace", "namespace"));
    throttleZNode = ZKUtil.joinZNode(baseZNode, conf.get("zookeeper.znode.throttle", "throttle"));
    switchZNode = ZKUtil.joinZNode(baseZNode, conf.get("zookeeper.znode.switch", "switch"));
  }
}
