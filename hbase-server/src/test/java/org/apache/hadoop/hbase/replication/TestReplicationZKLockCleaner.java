package org.apache.hadoop.hbase.replication;

import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.replication.ReplicationAdmin;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.master.cleaner.ReplicationZKLockCleanerChore;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestReplicationZKLockCleaner {

  private static Configuration conf1;
  private static HBaseTestingUtility utility1;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    conf1 = HBaseConfiguration.create();
    conf1.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/1");
    // smaller block size and capacity to trigger more operations
    // and test them
    conf1.setInt("hbase.regionserver.hlog.blocksize", 1024*20);
    conf1.setInt("replication.source.size.capacity", 1024);
    conf1.setLong("replication.source.sleepforretries", 100);
    conf1.setInt("hbase.regionserver.maxlogs", 10);
    conf1.setLong("hbase.master.logcleaner.ttl", 10);
    conf1.setBoolean(HConstants.REPLICATION_ENABLE_KEY, HConstants.REPLICATION_ENABLE_DEFAULT);
    conf1.setBoolean("dfs.support.append", true);
    conf1.setLong(HConstants.THREAD_WAKE_FREQUENCY, 100);
    conf1.setStrings(CoprocessorHost.USER_REGION_COPROCESSOR_CONF_KEY,
        "org.apache.hadoop.hbase.replication.TestMasterReplication$CoprocessorCounter");
    conf1.setBoolean(HConstants.ZOOKEEPER_USEMULTI , false);// for testZKLockCleaner
    conf1.setInt("hbase.master.cleaner.interval", 5 * 1000);
    conf1.setClass("hbase.region.replica.replication.replicationQueues.class",
        ReplicationQueuesZKImpl.class, ReplicationQueues.class);
    conf1.setLong(ReplicationZKLockCleanerChore.TTL_CONFIG_KEY, 0L);

    utility1 = new HBaseTestingUtility(conf1);
    utility1.startMiniZKCluster();
    MiniZooKeeperCluster miniZK = utility1.getZkCluster();
    utility1.setZkCluster(miniZK);
    new ZooKeeperWatcher(conf1, "cluster1", null, true);
  }

  @Test
  public void testZKLockCleaner() throws Exception {
    MiniHBaseCluster cluster = utility1.startMiniCluster(1, 2);
    HTableDescriptor table = new HTableDescriptor(TableName.valueOf(Bytes.toBytes("zk")));
    HColumnDescriptor fam = new HColumnDescriptor("cf");
    fam.setScope(HConstants.REPLICATION_SCOPE_GLOBAL);
    table.addFamily(fam);
    new HBaseAdmin(conf1).createTable(table);
    ReplicationAdmin replicationAdmin = new ReplicationAdmin(conf1);
    ReplicationPeerConfig rpc = new ReplicationPeerConfig();
    rpc.setClusterKey("cluster2");
    replicationAdmin.addPeer("cluster2", rpc, null);
    HRegionServer rs = cluster.getRegionServer(0);
    ReplicationQueuesZKImpl zk = new ReplicationQueuesZKImpl(rs.getZooKeeper(), conf1, rs);
    zk.init(rs.getServerName().toString());
    List<String> replicators = zk.getListOfReplicators();
    assertEquals(2, replicators.size());
    String zNode = cluster.getRegionServer(1).getServerName().toString();

    assertTrue(zk.lockOtherRS(zNode));
    assertTrue(zk.checkLockExists(zNode));
    Thread.sleep(10000);
    assertTrue(zk.checkLockExists(zNode));
    cluster.abortRegionServer(0);
    Thread.sleep(10000);
    HRegionServer rs1 = cluster.getRegionServer(1);
    zk = new ReplicationQueuesZKImpl(rs1.getZooKeeper(), conf1, rs1);
    zk.init(rs1.getServerName().toString());
    assertFalse(zk.checkLockExists(zNode));

    utility1.shutdownMiniCluster();
  }
}
