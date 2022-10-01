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
package org.apache.hadoop.hbase.replication;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseZKTestingUtil;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.replication.ZKReplicationQueueStorage.MigrationIterator;
import org.apache.hadoop.hbase.replication.ZKReplicationQueueStorage.ZkLastPushedSeqId;
import org.apache.hadoop.hbase.replication.ZKReplicationQueueStorage.ZkReplicationQueueData;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.MD5Hash;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.hadoop.hbase.zookeeper.ZNodePaths;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import org.apache.hbase.thirdparty.com.google.common.base.Splitter;
import org.apache.hbase.thirdparty.com.google.common.collect.Iterables;
import org.apache.hbase.thirdparty.com.google.common.collect.Sets;
import org.apache.hbase.thirdparty.com.google.common.io.Closeables;

@Category({ ReplicationTests.class, MediumTests.class })
public class TestZKReplicationQueueStorage {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestZKReplicationQueueStorage.class);

  private static final HBaseZKTestingUtil UTIL = new HBaseZKTestingUtil();

  private ZKWatcher zk;

  private ZKReplicationQueueStorage storage;

  @Rule
  public final TestName name = new TestName();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    UTIL.startMiniZKCluster();
  }

  @AfterClass
  public static void tearDownAfterClass() throws IOException {
    UTIL.shutdownMiniZKCluster();
  }

  @Before
  public void setUp() throws IOException {
    Configuration conf = UTIL.getConfiguration();
    conf.set(ZKReplicationStorageBase.REPLICATION_ZNODE, name.getMethodName());
    zk = new ZKWatcher(conf, name.getMethodName(), null);
    storage = new ZKReplicationQueueStorage(zk, conf);
  }

  @After
  public void tearDown() throws Exception {
    ZKUtil.deleteNodeRecursively(zk, storage.replicationZNode);
    Closeables.close(zk, true);
  }

  @Test
  public void testDeleteAllData() throws Exception {
    assertFalse(storage.hasData());
    ZKUtil.createWithParents(zk, storage.getQueuesZNode());
    assertTrue(storage.hasData());
    storage.deleteAllData();
    assertFalse(storage.hasData());
  }

  @Test
  public void testEmptyIter() throws Exception {
    ZKUtil.createWithParents(zk, storage.getQueuesZNode());
    ZKUtil.createWithParents(zk, storage.getRegionsZNode());
    ZKUtil.createWithParents(zk, storage.getHfileRefsZNode());
    assertNull(storage.listAllQueues().next());
    assertEquals(-1, ZKUtil.checkExists(zk, storage.getQueuesZNode()));
    assertNull(storage.listAllLastPushedSeqIds().next());
    assertEquals(-1, ZKUtil.checkExists(zk, storage.getRegionsZNode()));
    assertNull(storage.listAllHFileRefs().next());
    assertEquals(-1, ZKUtil.checkExists(zk, storage.getHfileRefsZNode()));
  }

  @Test
  public void testListAllQueues() throws Exception {
    String peerId = "1";
    ServerName deadServer =
      ServerName.valueOf("test-hbase-dead", 12345, EnvironmentEdgeManager.currentTime());
    int nServers = 10;
    for (int i = 0; i < nServers; i++) {
      ServerName sn =
        ServerName.valueOf("test-hbase-" + i, 12345, EnvironmentEdgeManager.currentTime());
      String rsZNode = ZNodePaths.joinZNode(storage.getQueuesZNode(), sn.toString());
      String peerZNode = ZNodePaths.joinZNode(rsZNode, peerId);
      ZKUtil.createWithParents(zk, peerZNode);
      for (int j = 0; j < i; j++) {
        String wal = ZNodePaths.joinZNode(peerZNode, "wal-" + j);
        ZKUtil.createSetData(zk, wal, ZKUtil.positionToByteArray(j));
      }
      String deadServerPeerZNode = ZNodePaths.joinZNode(rsZNode, peerId + "-" + deadServer);
      ZKUtil.createWithParents(zk, deadServerPeerZNode);
      for (int j = 0; j < i; j++) {
        String wal = ZNodePaths.joinZNode(deadServerPeerZNode, "wal-" + j);
        if (j > 0) {
          ZKUtil.createSetData(zk, wal, ZKUtil.positionToByteArray(j));
        } else {
          ZKUtil.createWithParents(zk, wal);
        }
      }
    }
    ZKUtil.createWithParents(zk,
      ZNodePaths.joinZNode(storage.getQueuesZNode(), deadServer.toString()));
    MigrationIterator<Pair<ServerName, List<ZkReplicationQueueData>>> iter =
      storage.listAllQueues();
    ServerName previousServerName = null;
    for (int i = 0; i < nServers + 1; i++) {
      Pair<ServerName, List<ZkReplicationQueueData>> pair = iter.next();
      assertNotNull(pair);
      if (previousServerName != null) {
        assertEquals(-1, ZKUtil.checkExists(zk,
          ZNodePaths.joinZNode(storage.getQueuesZNode(), previousServerName.toString())));
      }
      ServerName sn = pair.getFirst();
      previousServerName = sn;
      if (sn.equals(deadServer)) {
        assertThat(pair.getSecond(), empty());
      } else {
        assertEquals(2, pair.getSecond().size());
        int n = Integer.parseInt(Iterables.getLast(Splitter.on('-').split((sn.getHostname()))));
        ZkReplicationQueueData data0 = pair.getSecond().get(0);
        assertEquals(peerId, data0.getQueueId().getPeerId());
        assertEquals(sn, data0.getQueueId().getServerName());
        assertEquals(n, data0.getWalOffsets().size());
        for (int j = 0; j < n; j++) {
          assertEquals(j, data0.getWalOffsets().get("wal-" + j).intValue());
        }
        ZkReplicationQueueData data1 = pair.getSecond().get(1);
        assertEquals(peerId, data1.getQueueId().getPeerId());
        assertEquals(sn, data1.getQueueId().getServerName());
        assertEquals(n, data1.getWalOffsets().size());
        for (int j = 0; j < n; j++) {
          assertEquals(j, data1.getWalOffsets().get("wal-" + j).intValue());
        }
        // the order of the returned result is undetermined
        if (data0.getQueueId().getSourceServerName().isPresent()) {
          assertEquals(deadServer, data0.getQueueId().getSourceServerName().get());
          assertFalse(data1.getQueueId().getSourceServerName().isPresent());
        } else {
          assertEquals(deadServer, data1.getQueueId().getSourceServerName().get());
        }
      }
    }
    assertNull(iter.next());
    assertEquals(-1, ZKUtil.checkExists(zk, storage.getQueuesZNode()));
  }

  private String getLastPushedSeqIdZNode(String encodedName, String peerId) {
    return ZNodePaths.joinZNode(storage.getRegionsZNode(), encodedName.substring(0, 2),
      encodedName.substring(2, 4), encodedName.substring(4) + "-" + peerId);
  }

  @Test
  public void testListAllLastPushedSeqIds() throws Exception {
    String peerId1 = "1";
    String peerId2 = "2";
    Map<String, Set<String>> name2PeerIds = new HashMap<>();
    byte[] bytes = new byte[32];
    for (int i = 0; i < 100; i++) {
      ThreadLocalRandom.current().nextBytes(bytes);
      String encodeName = MD5Hash.getMD5AsHex(bytes);
      String znode1 = getLastPushedSeqIdZNode(encodeName, peerId1);
      ZKUtil.createSetData(zk, znode1, ZKUtil.positionToByteArray(1));
      String znode2 = getLastPushedSeqIdZNode(encodeName, peerId2);
      ZKUtil.createSetData(zk, znode2, ZKUtil.positionToByteArray(2));
      name2PeerIds.put(encodeName, Sets.newHashSet(peerId1, peerId2));
    }
    int addedEmptyZNodes = 0;
    for (int i = 0; i < 256; i++) {
      String level1ZNode =
        ZNodePaths.joinZNode(storage.getRegionsZNode(), String.format("%02x", i));
      if (ZKUtil.checkExists(zk, level1ZNode) == -1) {
        ZKUtil.createWithParents(zk, level1ZNode);
        addedEmptyZNodes++;
        if (addedEmptyZNodes <= 10) {
          ZKUtil.createWithParents(zk, ZNodePaths.joinZNode(level1ZNode, "ab"));
        }
        if (addedEmptyZNodes >= 20) {
          break;
        }

      }
    }
    MigrationIterator<List<ZkLastPushedSeqId>> iter = storage.listAllLastPushedSeqIds();
    int emptyListCount = 0;
    for (;;) {
      List<ZkLastPushedSeqId> list = iter.next();
      if (list == null) {
        break;
      }
      if (list.isEmpty()) {
        emptyListCount++;
        continue;
      }
      for (ZkLastPushedSeqId seqId : list) {
        name2PeerIds.get(seqId.getEncodedRegionName()).remove(seqId.getPeerId());
        if (seqId.getPeerId().equals(peerId1)) {
          assertEquals(1, seqId.getLastPushedSeqId());
        } else {
          assertEquals(2, seqId.getLastPushedSeqId());
        }
      }
    }
    assertEquals(10, emptyListCount);
    name2PeerIds.forEach((encodedRegionName, peerIds) -> {
      assertThat(encodedRegionName + " still has unmigrated peers", peerIds, empty());
    });
    assertEquals(-1, ZKUtil.checkExists(zk, storage.getRegionsZNode()));
  }

  @Test
  public void testListAllHFilesRefs() throws Exception {
    int nPeers = 10;
    for (int i = 0; i < nPeers; i++) {
      String peerId = "peer_" + i;
      ZKUtil.createWithParents(zk, ZNodePaths.joinZNode(storage.getHfileRefsZNode(), peerId));
      for (int j = 0; j < i; j++) {
        ZKUtil.createWithParents(zk,
          ZNodePaths.joinZNode(storage.getHfileRefsZNode(), peerId, "hfile-" + j));
      }
    }
    MigrationIterator<Pair<String, List<String>>> iter = storage.listAllHFileRefs();
    String previousPeerId = null;
    for (int i = 0; i < nPeers; i++) {
      Pair<String, List<String>> pair = iter.next();
      if (previousPeerId != null) {
        assertEquals(-1, ZKUtil.checkExists(zk,
          ZNodePaths.joinZNode(storage.getHfileRefsZNode(), previousPeerId)));
      }
      String peerId = pair.getFirst();
      previousPeerId = peerId;
      int index = Integer.parseInt(Iterables.getLast(Splitter.on('_').split(peerId)));
      assertEquals(index, pair.getSecond().size());
    }
    assertNull(iter.next());
    assertEquals(-1, ZKUtil.checkExists(zk, storage.getHfileRefsZNode()));
  }
}
