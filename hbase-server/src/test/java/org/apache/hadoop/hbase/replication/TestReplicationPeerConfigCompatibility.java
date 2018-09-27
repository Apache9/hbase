package org.apache.hadoop.hbase.replication;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.HTestConst;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.replication.ReplicationPeerConfigUpgrader;
import org.apache.hadoop.hbase.client.replication.ReplicationSerDeHelper;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@Category(LargeTests.class)
public class TestReplicationPeerConfigCompatibility {
  private static final Log LOG = LogFactory.getLog(TestReplicationPeerConfigCompatibility.class);

  private static Configuration conf1;
  private static Configuration conf2;

  private static HBaseTestingUtility utility1;
  private static HBaseTestingUtility utility2;

  private static ZooKeeperWatcher zkw1;

  private static final String PEER_ID = "1";
  private static final byte[] FAMILY = Bytes.toBytes("f");
  private static final byte[] QUALIFIER = Bytes.toBytes("q");
  private static final byte[] VALUE = Bytes.toBytes("v");
  private static final byte[] ROW = Bytes.toBytes("r");
  private static final int COUNT = 100;
  private static final byte[][] ROWS = HTestConst.makeNAscii(ROW, COUNT);

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    conf1 = HBaseConfiguration.create();
    conf1.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/1");
    conf1.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 5);

    utility1 = new HBaseTestingUtility(conf1);
    utility1.startMiniZKCluster();
    MiniZooKeeperCluster miniZK = utility1.getZkCluster();
    zkw1 = new ZooKeeperWatcher(conf1, "cluster1", null, true);

    conf2 = new Configuration(conf1);
    conf2.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/2");

    utility2 = new HBaseTestingUtility(conf2);
    utility2.setZkCluster(miniZK);
    new ZooKeeperWatcher(conf2, "cluster2", null, true);

    utility1.startMiniCluster(1, 1);
    utility2.startMiniCluster(1, 1);

    ReplicationPeerConfig rpc = new ReplicationPeerConfig();
    rpc.setClusterKey(utility2.getClusterKey());
    utility1.getHBaseAdmin().addReplicationPeer(PEER_ID, rpc);
  }

  @AfterClass
  public static void setUpAfterClass() throws Exception {
    utility2.shutdownMiniCluster();
    utility1.shutdownMiniCluster();
  }

  @Test
  public void testReplicationPeerConfig() throws Exception {
    ReplicationPeerConfig rpc = new ReplicationPeerConfig();
    rpc.setClusterKey(utility2.getClusterKey());

    byte[] newRpcBytes = ReplicationSerDeHelper.toNewByteArray(rpc);
    ReplicationPeerConfig newRpc = ReplicationSerDeHelper.parsePeerFrom(newRpcBytes);
    assertReplicationPeerConfigEquals(rpc, newRpc);

    rpc = new ReplicationPeerConfig();
    rpc.setClusterKey(utility2.getClusterKey());
    rpc.setReplicateAllUserTables(false);
    HashSet<String> namespaces = new HashSet<>();
    namespaces.add("ns1");
    rpc.setNamespaces(namespaces);
    Map<TableName, List<String>> tableCfsMap = new HashMap<>();
    tableCfsMap.put(TableName.valueOf("ns2:table1"), new ArrayList<>());
    rpc.setTableCFsMap(tableCfsMap);
    rpc.setBandwidth(1048576L);

    newRpcBytes = ReplicationSerDeHelper.toNewByteArray(rpc);
    newRpc = ReplicationSerDeHelper.parsePeerFrom(newRpcBytes);
    assertReplicationPeerConfigEquals(rpc, newRpc);

    rpc = new ReplicationPeerConfig();
    rpc.setClusterKey(utility2.getClusterKey());
    rpc.setReplicateAllUserTables(true);
    HashSet<String> excludeNamespaces = new HashSet<>();
    excludeNamespaces.add("ns1");
    rpc.setExcludeNamespaces(excludeNamespaces);
    Map<TableName, List<String>> excludeTableCfsMap = new HashMap<>();
    excludeTableCfsMap.put(TableName.valueOf("ns2:table1"), new ArrayList<>());
    rpc.setExcludeTableCFsMap(tableCfsMap);

    newRpcBytes = ReplicationSerDeHelper.toNewByteArray(rpc);
    newRpc = ReplicationSerDeHelper.parsePeerFrom(newRpcBytes);
    assertReplicationPeerConfigEquals(rpc, newRpc);

    rpc = new ReplicationPeerConfig();
    rpc.setClusterKey(utility2.getClusterKey());
  }

  private void assertReplicationPeerConfigEquals(ReplicationPeerConfig rpc1,
      ReplicationPeerConfig rpc2) {
    LOG.info("Rpc1 is " + rpc1 + " and rpc2 is " + rpc2);
    assertEquals(rpc1.getClusterKey(), rpc2.getClusterKey());
    assertEquals(rpc1.getReplicationEndpointImpl(), rpc2.getReplicationEndpointImpl());
    assertEquals(rpc1.getBandwidth(), rpc2.getBandwidth());
    assertEquals(rpc1.replicateAllUserTables(), rpc2.replicateAllUserTables());
    assertPeerDataEquals(rpc1.getPeerData(), rpc2.getPeerData());
    assertConfigurationEquals(rpc1.getConfiguration(), rpc2.getConfiguration());
    assertTableCfsEquals(rpc1.getTableCFsMap(), rpc2.getTableCFsMap());
    assertNamespacesEquals(rpc1.getNamespaces(), rpc2.getNamespaces());
    assertTableCfsEquals(rpc1.getExcludeTableCFsMap(), rpc2.getExcludeTableCFsMap());
    assertNamespacesEquals(rpc1.getExcludeNamespaces(), rpc2.getExcludeNamespaces());
  }

  private void assertPeerDataEquals(Map<byte[], byte[]> peerData1, Map<byte[], byte[]> peerData2) {
    if (peerData1 == null) {
      assertNull(peerData2);
    } else {
      assertNotNull(peerData2);
      assertEquals(peerData1.size(), peerData2.size());
      for (Map.Entry<byte[], byte[]> entry : peerData1.entrySet()) {
        assertTrue(peerData2.containsKey(entry.getKey()));
        assertEquals(Bytes.toString(entry.getValue()), Bytes.toString(peerData2.get(entry.getKey())));
      }
    }
  }

  private void assertConfigurationEquals(Map<String, String> configuration1, Map<String, String> configuration2) {
    if (configuration1 == null) {
      assertNull(configuration2);
    } else {
      assertNotNull(configuration2);
      assertEquals(configuration1.size(), configuration2.size());
      for (Map.Entry<String, String> entry : configuration1.entrySet()) {
        assertTrue(configuration2.containsKey(entry.getKey()));
        assertEquals(entry.getValue(), configuration2.get(entry.getKey()));
      }
    }
  }

  private void assertTableCfsEquals(Map<TableName, ? extends Collection<String>> tableCfsMap1,
      Map<TableName, ? extends Collection<String>> tableCfsMap2) {
    if (tableCfsMap1 == null) {
      assertNull(tableCfsMap2);
    } else {
      assertNotNull(tableCfsMap2);
      assertEquals(tableCfsMap1.size(), tableCfsMap2.size());
      for (Map.Entry<TableName, ? extends Collection<String>> entry : tableCfsMap2.entrySet()) {
        assertTrue(tableCfsMap2.containsKey(entry.getKey()));
      }
    }
  }

  private void assertNamespacesEquals(Set<String> namespaces1,
      Set<String> namespaces2) {
    if (namespaces1 == null) {
      assertNull(namespaces2);
    } else {
      assertNotNull(namespaces2);
      assertEquals(namespaces1.size(), namespaces2.size());
      for (String namespace : namespaces1) {
        assertTrue(namespaces2.contains(namespace));
      }
    }
  }

  @Test
  public void testUpgradeToBranch2() throws Exception {
    // create table
    TableName tableName = TableName.valueOf("testUpgradeToBranch2");
    HTableDescriptor table = new HTableDescriptor(tableName);
    HColumnDescriptor fam = new HColumnDescriptor(FAMILY);
    fam.setScope(HConstants.REPLICATION_SCOPE_GLOBAL);
    table.addFamily(fam);
    utility1.getHBaseAdmin().createTable(table);

    writeDataAndVerifyReplication(tableName, 0, COUNT / 2);

    ReplicationPeerConfigUpgrader upgrader = new ReplicationPeerConfigUpgrader(zkw1, conf1, null);
    upgrader.upgradeAllPeersToBranch2();

    // Sleep to wait for RS update replication source with new protobuf
    Thread.sleep(10000);
    writeDataAndVerifyReplication(tableName, COUNT / 2, COUNT);

    try (HBaseAdmin admin = new HBaseAdmin(conf1)) {
      // Master should read replication peer config from new protobuf
      ReplicationPeerConfig rpc = admin.getReplicationPeerConfig(PEER_ID);
      assertEquals(utility2.getClusterKey(), rpc.getClusterKey());
      assertTrue(rpc.replicateAllUserTables());
    }
  }

  private void writeDataAndVerifyReplication(TableName tableName, int start, int end)
      throws Exception {
    try (HTable t1 = new HTable(conf1, tableName); HTable t2 = new HTable(conf2, tableName)) {
      for (int i = start; i < end; i++) {
        Put put = new Put(ROWS[i]);
        put.add(FAMILY, QUALIFIER, VALUE);
        t1.put(put);
      }
      t1.flushCommits();

      // Sleep to wait for replication
      Thread.sleep(10000);

      for (int i = start; i < end; i++) {
        Result result = t2.get(new Get(ROWS[i]).addColumn(FAMILY, QUALIFIER));
        assertNotNull(result);
        assertFalse(result.isEmpty());
        assertEquals(Bytes.toString(VALUE), Bytes.toString(result.getValue(FAMILY, QUALIFIER)));
      }
    }
  }
}
