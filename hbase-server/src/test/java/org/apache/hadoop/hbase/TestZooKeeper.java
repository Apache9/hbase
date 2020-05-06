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
package org.apache.hadoop.hbase;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.master.LoadBalancer;
import org.apache.hadoop.hbase.master.balancer.SimpleLoadBalancer;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.zookeeper.EmptyWatcher;
import org.apache.hadoop.hbase.zookeeper.ZKAssign;
import org.apache.hadoop.hbase.zookeeper.ZKConfig;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZKUtil.ZKClusterKey;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.xiaomi.infra.base.nameservice.ZkClusterInfo;

@Category(LargeTests.class)
public class TestZooKeeper {
  private final Log LOG = LogFactory.getLog(this.getClass());

  private final static HBaseTestingUtility
      TEST_UTIL = new HBaseTestingUtility();

  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // Test we can first start the ZK cluster by itself
    Configuration conf = TEST_UTIL.getConfiguration();
    TEST_UTIL.startMiniDFSCluster(2);
    TEST_UTIL.startMiniZKCluster();
    conf.setBoolean("dfs.support.append", true);
    conf.setInt(HConstants.ZK_SESSION_TIMEOUT, 1000);
    conf.setClass(HConstants.HBASE_MASTER_LOADBALANCER_CLASS, MockLoadBalancer.class,
      LoadBalancer.class);
    conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 10);
    conf.setInt(HConstants.HBASE_CLIENT_SERVERSIDE_RETRIES_MULTIPLIER, 2);
  }

  /**
   * @throws java.lang.Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception {
    TEST_UTIL.startMiniHBaseCluster(2, 2);
  }

  @After
  public void after() throws Exception {
    try {
      TEST_UTIL.shutdownMiniHBaseCluster();
    } finally {
      TEST_UTIL.getTestFileSystem().delete(FSUtils.getRootDir(TEST_UTIL.getConfiguration()), true);
      ZKUtil.deleteNodeRecursively(TEST_UTIL.getZooKeeperWatcher(), "/hbase");
    }
  }

  @Test (timeout = 60000)
  public void testRegionServerSessionExpired() throws Exception {
    LOG.info("Starting testRegionServerSessionExpired");
    int metaIndex = TEST_UTIL.getMiniHBaseCluster().getServerWithMeta();
    TEST_UTIL.expireRegionServerSession(metaIndex);
    testSanity("testRegionServerSessionExpired");
  }

  /**
   * Make sure we can use the cluster
   * @throws Exception
   */
  private void testSanity(final String testName) throws Exception{
    String tableName = testName + "_" + System.currentTimeMillis();
    HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));
    HColumnDescriptor family = new HColumnDescriptor("fam");
    desc.addFamily(family);
    LOG.info("Creating table " + tableName);
    HBaseAdmin admin = new HBaseAdmin(TEST_UTIL.getConfiguration());
    try {
      admin.createTable(desc);
    } finally {
      admin.close();
    }

    HTable table =
      new HTable(new Configuration(TEST_UTIL.getConfiguration()), tableName);
    Put put = new Put(Bytes.toBytes("testrow"));
    put.add(Bytes.toBytes("fam"),
        Bytes.toBytes("col"), Bytes.toBytes("testdata"));
    LOG.info("Putting table " + tableName);
    table.put(put);
    table.close();
  }

  /**
   * Create a znode with data
   * @throws Exception
   */
  @Test
  public void testCreateWithParents() throws Exception {
    ZooKeeperWatcher zkw =
        new ZooKeeperWatcher(new Configuration(TEST_UTIL.getConfiguration()),
            TestZooKeeper.class.getName(), null);
    byte[] expectedData = new byte[] { 1, 2, 3 };
    ZKUtil.createWithParents(zkw, "/l1/l2/l3/l4/testCreateWithParents", expectedData);
    byte[] data = ZKUtil.getData(zkw, "/l1/l2/l3/l4/testCreateWithParents");
    assertTrue(Bytes.equals(expectedData, data));
    ZKUtil.deleteNodeRecursively(zkw, "/l1");

    ZKUtil.createWithParents(zkw, "/testCreateWithParents", expectedData);
    data = ZKUtil.getData(zkw, "/testCreateWithParents");
    assertTrue(Bytes.equals(expectedData, data));
    ZKUtil.deleteNodeRecursively(zkw, "/testCreateWithParents");
  }

  /**
   * Create a bunch of znodes in a hierarchy, try deleting one that has childs (it will fail), then
   * delete it recursively, then delete the last znode
   * @throws Exception
   */
  @Test
  public void testZNodeDeletes() throws Exception {
    ZooKeeperWatcher zkw = new ZooKeeperWatcher(
      new Configuration(TEST_UTIL.getConfiguration()),
      TestZooKeeper.class.getName(), null);
    ZKUtil.createWithParents(zkw, "/l1/l2/l3/l4");
    try {
      ZKUtil.deleteNode(zkw, "/l1/l2");
      fail("We should not be able to delete if znode has childs");
    } catch (KeeperException ex) {
      assertNotNull(ZKUtil.getDataNoWatch(zkw, "/l1/l2/l3/l4", null));
    }
    ZKUtil.deleteNodeRecursively(zkw, "/l1/l2");
    // make sure it really is deleted
    assertNull(ZKUtil.getDataNoWatch(zkw, "/l1/l2/l3/l4", null));

    // do the same delete again and make sure it doesn't crash
    ZKUtil.deleteNodeRecursively(zkw, "/l1/l2");

    ZKUtil.deleteNode(zkw, "/l1");
    assertNull(ZKUtil.getDataNoWatch(zkw, "/l1/l2", null));
  }

  @Test
  public void testClusterKey() throws Exception {
    testKey("server", 2181, "hbase");
    testKey("server1,server2,server3", 2181, "hbase");
    try {
      ZKUtil.transformClusterKey("2181:hbase");
    } catch (IOException ex) {
      // OK
    }
  }

  @Test
  public void testClusterKeyWithMultiplePorts() throws Exception {
    // server has different port than the default port
    testKey("server1:2182", 2181, "hbase", true);
    // multiple servers have their own port
    testKey("server1:2182,server2:2183,server3:2184", 2181, "hbase", true);
    // one server has no specified port, should use default port
    testKey("server1:2182,server2,server3:2184", 2181, "hbase", true);
    // the last server has no specified port, should use default port
    testKey("server1:2182,server2:2183,server3", 2181, "hbase", true);
    // multiple servers have no specified port, should use default port for those servers
    testKey("server1:2182,server2,server3:2184,server4", 2181, "hbase", true);
    // same server, different ports
    testKey("server1:2182,server1:2183,server1", 2181, "hbase", true);
    // mix of same server/different port and different server
    testKey("server1:2182,server2:2183,server1", 2181, "hbase", true);
  }

  private void testKey(String ensemble, int port, String znode)
      throws IOException {
    testKey(ensemble, port, znode, false); // not support multiple client ports
  }

  private void testKey(String ensemble, int port, String znode, Boolean multiplePortSupport)
      throws IOException {
    Configuration conf = new Configuration();
    String key = ensemble+":"+port+":"+znode;
    String ensemble2 = null;
    ZKUtil.ZKClusterKey zkClusterKey = ZKUtil.transformClusterKey(key);
    if (multiplePortSupport) {
      ensemble2 = ZKUtil.standardizeQuorumServerString(ensemble, Integer.toString(port));
      assertEquals(ensemble2, zkClusterKey.quorumString);
    }
    else {
      assertEquals(ensemble, zkClusterKey.quorumString);
    }
    assertEquals(port, zkClusterKey.clientPort);
    assertEquals(znode, zkClusterKey.znodeParent);

    ZKUtil.applyClusterKeyToConf(conf, key);
    assertEquals(zkClusterKey.quorumString, conf.get(HConstants.ZOOKEEPER_QUORUM));
    assertEquals(zkClusterKey.clientPort, conf.getInt(HConstants.ZOOKEEPER_CLIENT_PORT, -1));
    assertEquals(zkClusterKey.znodeParent, conf.get(HConstants.ZOOKEEPER_ZNODE_PARENT));

    String reconstructedKey = ZKUtil.getZooKeeperClusterKey(conf);
    if (multiplePortSupport) {
      String key2 = ensemble2 + ":" + port + ":" + znode;
      assertEquals(key2, reconstructedKey);
    }
    else {
      assertEquals(key, reconstructedKey);
    }
  }

  /**
   * A test for HBASE-3238
   * @throws IOException A connection attempt to zk failed
   * @throws InterruptedException One of the non ZKUtil actions was interrupted
   * @throws KeeperException Any of the zookeeper connections had a
   * KeeperException
   */
  @Test
  public void testCreateSilentIsReallySilent() throws InterruptedException,
      KeeperException, IOException {
    Configuration c = TEST_UTIL.getConfiguration();

    String aclZnode = "/aclRoot";
    String quorumServers = ZKConfig.getZKQuorumServersString(c);
    int sessionTimeout = 5 * 1000; // 5 seconds
    ZooKeeper zk = new ZooKeeper(quorumServers, sessionTimeout, EmptyWatcher.instance);
    zk.addAuthInfo("digest", "hbase:rox".getBytes());

    // Assumes the  root of the ZooKeeper space is writable as it creates a node
    // wherever the cluster home is defined.
    ZooKeeperWatcher zk2 = new ZooKeeperWatcher(TEST_UTIL.getConfiguration(),
      "testCreateSilentIsReallySilent", null);

    // Save the previous ACL
    Stat s =  null;
    List<ACL> oldACL = null;
    while (true) {
      try {
        s = new Stat();
        oldACL = zk.getACL("/", s);
        break;
      } catch (KeeperException e) {
        switch (e.code()) {
          case CONNECTIONLOSS:
          case SESSIONEXPIRED:
          case OPERATIONTIMEOUT:
            LOG.warn("Possibly transient ZooKeeper exception", e);
            Threads.sleep(100);
            break;
         default:
            throw e;
        }
      }
    }

    // I set this acl after the attempted creation of the cluster home node.
    // Add retries in case of retryable zk exceptions.
    while (true) {
      try {
        zk.setACL("/", ZooDefs.Ids.CREATOR_ALL_ACL, -1);
        break;
      } catch (KeeperException e) {
        switch (e.code()) {
          case CONNECTIONLOSS:
          case SESSIONEXPIRED:
          case OPERATIONTIMEOUT:
            LOG.warn("Possibly transient ZooKeeper exception: " + e);
            Threads.sleep(100);
            break;
         default:
            throw e;
        }
      }
    }

    while (true) {
      try {
        zk.create(aclZnode, null, ZooDefs.Ids.CREATOR_ALL_ACL, CreateMode.PERSISTENT);
        break;
      } catch (KeeperException e) {
        switch (e.code()) {
          case CONNECTIONLOSS:
          case SESSIONEXPIRED:
          case OPERATIONTIMEOUT:
            LOG.warn("Possibly transient ZooKeeper exception: " + e);
            Threads.sleep(100);
            break;
         default:
            throw e;
        }
      }
    }
    zk.close();
    ZKUtil.createAndFailSilent(zk2, aclZnode);

    // Restore the ACL
    ZooKeeper zk3 = new ZooKeeper(quorumServers, sessionTimeout, EmptyWatcher.instance);
    zk3.addAuthInfo("digest", "hbase:rox".getBytes());
    try {
      zk3.setACL("/", oldACL, -1);
    } finally {
      zk3.close();
    }
 }

  /**
   * Test should not fail with NPE when getChildDataAndWatchForNewChildren
   * invoked with wrongNode
   */
  @Test
  @SuppressWarnings("deprecation")
  public void testGetChildDataAndWatchForNewChildrenShouldNotThrowNPE()
      throws Exception {
    ZooKeeperWatcher zkw = new ZooKeeperWatcher(TEST_UTIL.getConfiguration(),
        "testGetChildDataAndWatchForNewChildrenShouldNotThrowNPE", null);
    ZKUtil.getChildDataAndWatchForNewChildren(zkw, "/wrongNode");
  }

  static class MockLoadBalancer extends SimpleLoadBalancer {
    static boolean retainAssignCalled = false;

    @Override
    public Map<ServerName, List<HRegionInfo>> retainAssignment(
        Map<HRegionInfo, ServerName> regions, List<ServerName> servers) {
      retainAssignCalled = true;
      return super.retainAssignment(regions, servers);
    }
  }
  
  @Test
  public void testClusterKeyWithNameService() throws Exception {
    System.setProperty("hadoop.property.hadoop.security.authentication", "kerberos");
    ZKClusterKey clusterKey = ZKUtil.transformClusterKey("hbase://zjyprc-xiaomi");

    ZkClusterInfo info = new ZkClusterInfo("zjyprc");
    assertEquals(clusterKey.quorumString, info.resolve());
    assertEquals(clusterKey.clientPort, info.getPort());
    assertEquals(clusterKey.znodeParent, "/hbase/zjyprc-xiaomi");

    try {
      ZKUtil.transformClusterKey("hbase://lgunkown-xiaomi");
      assertTrue(false);
    } catch (IOException ex) {
      // OK
    }
  }
  
  @Test
  public void testApplyClusterKeyToConf() throws IOException {
    Configuration conf = HBaseConfiguration.create();
    ZKUtil.applyClusterKeyToConf(conf, "server1,server2,server3:2181:/hbase");
    assertEquals(conf.get(HConstants.ZOOKEEPER_QUORUM), "server1,server2,server3");
    assertEquals(conf.get(HConstants.ZOOKEEPER_CLIENT_PORT),"2181");
    assertEquals(conf.get(HConstants.ZOOKEEPER_ZNODE_PARENT), "/hbase");
  }
  
  @Test
  public void testApplyClusterKeyToConfWithNameService() throws IOException {
    Configuration conf = HBaseConfiguration.create();
    conf.set("hadoop.security.authentication", "kerberos");
    ZKUtil.applyClusterKeyToConf(conf, "hbase://zjyprc-xiaomi");
    ZkClusterInfo info = new ZkClusterInfo("zjyprc");
    assertEquals(conf.get(HConstants.ZOOKEEPER_QUORUM), info.resolve());
    assertEquals(conf.get(HConstants.ZOOKEEPER_CLIENT_PORT), Integer.toString(info.getPort()));
    assertEquals(conf.get(HConstants.ZOOKEEPER_ZNODE_PARENT), "/hbase/zjyprc-xiaomi");
  }
}

