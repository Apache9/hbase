/*
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ReplicationProtos;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.ReplicationPeerDescription;
import org.apache.hadoop.hbase.replication.ReplicationStateZKBase;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooDefs.Perms;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;

import java.io.IOException;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * This class is used to upgrade TableCFs from HBase 1.x to HBase 2.x
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class ReplicationPeerConfigUpgrader extends ReplicationStateZKBase {

  private static final Log LOG = LogFactory.getLog(ReplicationPeerConfigUpgrader.class);

  public ReplicationPeerConfigUpgrader(ZooKeeperWatcher zookeeper,
                         Configuration conf, Abortable abortable) {
    super(zookeeper, conf, abortable);
  }

  public void setOwner(String owner) throws Exception {
    if (!ZKUtil.isSecureZooKeeper(conf)) {
      return;
    }
    List<ACL> acls = createACL(owner);
    try {
      ZKUtil.setACL(zookeeper, peersZNode, acls, -1);
      LOG.info("Successfully set owner: " + owner + " for replication peers znode " + peersZNode);
      List<String> znodes = ZKUtil.listChildrenNoWatch(this.zookeeper, this.peersZNode);
      for (String peerId : znodes) {
        String peerNode = getPeerNode(peerId);
        ZKUtil.setACL(zookeeper, peerNode, acls, -1);
        LOG.info("Successfully set owner: " + owner + " for replication peer: " + peerId
            + " znode " + peerNode);
        String peerStateNode = getPeerStateNode(peerId);
        ZKUtil.setACL(zookeeper, peerStateNode, acls, -1);
        LOG.info("Successfully set owner: " + owner + " for replication peer: " + peerId
            + " state znode " + peerStateNode);
      }
    } catch (KeeperException e) {
      LOG.error("Failed to set owner for replication peers znode", e);
    }
  }

  private ArrayList<ACL> createACL(String owner) {
    ArrayList<ACL> acls = new ArrayList<ACL>();
    acls.add(new ACL(ZooDefs.Perms.READ, ZooDefs.Ids.ANYONE_ID_UNSAFE));
    acls.add(new ACL(Perms.ALL, new Id("sasl", owner)));
    StringBuilder sb = new StringBuilder("Created ACLs: ");
    acls.forEach(acl -> sb.append(acl));
    LOG.info(sb.toString());
    return acls;
  }

  public void upgradeAllPeersToBranch2() throws Exception {
    try (HBaseAdmin admin = new HBaseAdmin(conf)) {
      List<ReplicationPeerDescription> peers = admin.listReplicationPeers();
      peers.forEach(peer -> {
        upgradePeerToBranch2(peer.getPeerId(), peer.getPeerConfig());
      });
    }
  }

  public void upgradePeerToBranch2(String peerId) throws Exception {
    try (HBaseAdmin admin = new HBaseAdmin(conf)) {
      ReplicationPeerConfig peerConfig = admin.getReplicationPeerConfig(peerId);
      upgradePeerToBranch2(peerId, peerConfig);
    }
  }

  private void upgradePeerToBranch2(String peerId, ReplicationPeerConfig peerConfig) {
    try {
      saveToLocalFile(peerId, ZKUtil.getData(this.zookeeper, getPeerNode(peerId)));
      ZKUtil.setData(this.zookeeper, getPeerNode(peerId),
          ReplicationSerDeHelper.toNewByteArray(peerConfig));
      LOG.info("Successfully upgrade replication peer " + peerId + " config " + peerConfig
          + " to new branch-2 format");
    } catch (IOException | KeeperException e) {
      LOG.error("Failed upgrade replication peer " + peerId + " config " + peerConfig
          + " to new branch-2 format", e);
    }
  }

  public void downgradeAllPeersTo98() throws Exception {
    try (HBaseAdmin admin = new HBaseAdmin(conf)) {
      List<ReplicationPeerDescription> peers = admin.listReplicationPeers();
      peers.forEach(peer -> {
        downgradePeerTo98(peer.getPeerId());
      });
    }
  }

  public void downgradePeerTo98(String peerId) {
    try {
      byte[] peerConfigBytes = ZKUtil.getData(this.zookeeper, getPeerNode(peerId));
      saveToLocalFile(peerId, peerConfigBytes);
      ReplicationPeerConfig newPeerConfig = ReplicationSerDeHelper
          .parsePeerConfigFromNewProtobuf(peerConfigBytes, ProtobufUtil.lengthOfPBMagic());
      ZKUtil.setData(this.zookeeper, getPeerNode(peerId),
          ReplicationSerDeHelper.toByteArray(newPeerConfig));
      LOG.info("Successfully downgrade replication peer " + peerId + " config to " + newPeerConfig);
    } catch (IOException | KeeperException | DeserializationException e) {
      LOG.error("Failed downgrade replication peer " + peerId + " config", e);
    }
  }

  private void saveToLocalFile(String peerId, byte[] bytes) throws IOException {
    String fileName = peerId + "." + System.currentTimeMillis() + ".peerconfig";
    try (FileOutputStream fos = new FileOutputStream(fileName)) {
      fos.write(bytes);
    }
  }

  public void upgrade() throws Exception {
    try (ReplicationAdmin admin = new ReplicationAdmin(conf)) {
      Map<String, ReplicationPeerConfig> peers = admin.listPeerConfigs();
      peers
          .forEach((peerId, peerConfig) -> {
            if ((peerConfig.getNamespaces() != null && !peerConfig.getNamespaces().isEmpty())
                || (peerConfig.getTableCFsMap() != null && !peerConfig.getTableCFsMap().isEmpty())) {
              peerConfig.setReplicateAllUserTables(false);
              try {
                admin.updatePeerConfig(peerId, peerConfig);
                LOG.info("Successfully upgrade replication peer " + peerId + " config to "
                    + peerConfig);
              } catch (Exception e) {
                LOG.error("Failed to upgrade replication peer config for peerId=" + peerId, e);
              }
            }
          });
    }
  }

  public void copyTableCFs() {
    List<String> znodes = null;
    try {
      znodes = ZKUtil.listChildrenNoWatch(this.zookeeper, this.peersZNode);
    } catch (KeeperException e) {
      LOG.error("Failed to list replication peers znode", e);
    }
    if (znodes != null) {
      for (String peerId : znodes) {
        if (!copyTableCFs(peerId)) {
          LOG.error("upgrade tableCFs failed for peerId=" + peerId);
        }
      }
    }
  }

  public boolean copyTableCFs(String peerId) {
    String tableCFsNode = getTableCFsNode(peerId);
    try {
      if (ZKUtil.checkExists(zookeeper, tableCFsNode) != -1) {
        String peerNode = getPeerNode(peerId);
        ReplicationPeerConfig rpc = getReplicationPeerConig(peerNode);
        if (rpc.getTableCFsMap() == null || rpc.getTableCFsMap().size() == 0) {
          // we copy TableCFs node into PeerNode
          LOG.info("copy tableCFs into peerNode:" + peerId);
          ReplicationProtos.TableCF[] tableCFs = ReplicationSerDeHelper.parseTableCFs(
            ZKUtil.getData(this.zookeeper, tableCFsNode));
          if (tableCFs != null && tableCFs.length > 0) {
            rpc.setTableCFsMap(ReplicationSerDeHelper.convert2Map(tableCFs));
            ZKUtil.setData(this.zookeeper, peerNode, ReplicationSerDeHelper.toByteArray(rpc));
          }
        } else {
          LOG.info("No tableCFs in peerNode:" + peerId);
        }
      }
    } catch (KeeperException e) {
      LOG.error("NOTICE!! Update peerId failed, peerId=" + peerId, e);
      return false;
    } catch (InterruptedException e) {
      LOG.error("NOTICE!! Update peerId failed, peerId=" + peerId, e);
      return false;
    } catch (IOException e) {
      LOG.error("NOTICE!! Update peerId failed, peerId=" + peerId, e);
      return false;
    }
    return true;
  }

  private ReplicationPeerConfig getReplicationPeerConig(String peerNode)
          throws KeeperException, InterruptedException {
    byte[] data = null;
    data = ZKUtil.getData(this.zookeeper, peerNode);
    if (data == null) {
      LOG.error("Could not get configuration for " +
              "peer because it doesn't exist. peer=" + peerNode);
      return null;
    }
    try {
      return ReplicationSerDeHelper.parsePeerFrom(data);
    } catch (DeserializationException e) {
      LOG.error("Failed to parse cluster key from peer=" + peerNode, e);
      return null;
    }
  }

  private static void printUsageAndExit() {
    System.err.printf(
        "Usage: bin/hbase org.apache.hadoop.hbase.replication.master.ReplicationPeerConfigUpgrader [options]");
    System.err.println(" where [options] are:");
    System.err.println("  -h|-help    Show this help and exit.");
    System.err.println("  copyTableCFs        Copy table-cfs to replication peer config");
    System.err.println("  upgrade             Upgrade replication peer config to new format");
    System.err.println("  setOwner [owner]    Update replication peers znode's owner");
    System.err.println(
        "  upgradeToBranch2 [peerId] Upgrade 98 replication peer config to new branch-2 format");
    System.err.println(
        "  downgradeTo98 [peerId] Downgrade replication peer config from new branch-2 format to old 98 format");
    System.err.println();
    System.exit(1);
  }

  public static void main(String[] args) throws Exception {
    if (args.length < 1) {
      printUsageAndExit();
    }
    if (args[0].equals("-help") || args[0].equals("-h")) {
      printUsageAndExit();
    }
    Configuration conf = HBaseConfiguration.create();
    try (ZooKeeperWatcher zkw = new ZooKeeperWatcher(conf, "ReplicationPeerConfigUpgrader", null)) {
      ReplicationPeerConfigUpgrader upgrader = new ReplicationPeerConfigUpgrader(zkw, conf, null);
      if (args[0].equals("copyTableCFs")) {
        upgrader.copyTableCFs();
      } else if (args[0].equals("upgrade")) {
        upgrader.upgrade();
      } else if (args[0].equals("setOwner")) {
        if (args.length != 2) {
          printUsageAndExit();
        }
        upgrader.setOwner(args[1]);
      } else if (args[0].equals("upgradeToBranch2")) {
        if (args.length == 1) {
          upgrader.upgradeAllPeersToBranch2();
        } else if (args.length == 2) {
          upgrader.upgradePeerToBranch2(args[1]);
        } else {
          printUsageAndExit();
        }
      } else if (args[0].equals("downgradeTo98")) {
        if (args.length == 1) {
          upgrader.downgradeAllPeersTo98();
        } else if (args.length == 2) {
          upgrader.downgradePeerTo98(args[1]);
        } else {
          printUsageAndExit();
        }
      } else {
        printUsageAndExit();
      }
    }
  }
}
