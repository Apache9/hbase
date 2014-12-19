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

package org.apache.hadoop.hbase.replication.master;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.replication.ReplicationZookeeper;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

/**
 * A tool to clean up left replication node of a peer
 */
public class ReplicationZKCleaner {
  private final static Log LOG = LogFactory.getLog(ReplicationZKCleaner.class);

  public static void main(String[] args) throws ZooKeeperConnectionException, IOException,
      KeeperException {
    if (args.length != 1) {
      System.out.println("Usage: ReplicationZKCleaner $peerId");
      return;
    }
    String peerId = args[0];
    Configuration conf = HBaseConfiguration.create();
    ZooKeeperWatcher zkw = new ZooKeeperWatcher(conf, "ReplicationZKCleaner", null);
    String replicationZNodeName = conf.get("zookeeper.znode.replication", "replication");
    String rsZNodeName = conf.get("zookeeper.znode.replication.rs", "rs");
    String replicationZNode = ZKUtil.joinZNode(zkw.baseZNode, replicationZNodeName);
    String rsZNode = ZKUtil.joinZNode(replicationZNode, rsZNodeName);

    for (ServerName sn : ReplicationZookeeper.listChildrenAndGetAsServerNames(zkw, rsZNode)) {
      String node = ZKUtil.joinZNode(rsZNode, sn.toString());
      for (String child : ZKUtil.listChildrenNoWatch(zkw, node)) {
        if (child.equals(peerId) || child.startsWith(peerId + "-")) {
          String zPath = ZKUtil.joinZNode(node, child);
          LOG.info("delete " + zPath);
          ZKUtil.deleteNodeRecursively(zkw, zPath);
        }
      }
    }
  }
}
