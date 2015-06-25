package org.apache.hadoop.hbase.replication.thrift;

import static org.apache.hadoop.hbase.replication.thrift.ReplicationTestUtils.addPeerThriftPort;
import static org.apache.hadoop.hbase.replication.thrift.ReplicationTestUtils.addReplicationPeer;
import static org.apache.hadoop.hbase.replication.thrift.ReplicationTestUtils.assertContainsOnly;
import static org.apache.hadoop.hbase.replication.thrift.ReplicationTestUtils.createTableOnCluster;
import static org.apache.hadoop.hbase.replication.thrift.ReplicationTestUtils.createTestTable;
import static org.apache.hadoop.hbase.replication.thrift.ReplicationTestUtils.deleteAndWait;
import static org.apache.hadoop.hbase.replication.thrift.ReplicationTestUtils.generateRandomPut;
import static org.apache.hadoop.hbase.replication.thrift.ReplicationTestUtils.getTestTable;
import static org.apache.hadoop.hbase.replication.thrift.ReplicationTestUtils.putAndWait;
import static org.apache.hadoop.hbase.replication.thrift.ReplicationTestUtils.setupConfiguration;

import java.util.Set;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Sets;

@Category(MediumTests.class)
public class TestThriftCyclicReplication extends TestThriftReplicationBase {

  private final static HBaseTestingUtility clusterA = new HBaseTestingUtility();
  private final static HBaseTestingUtility clusterB = new HBaseTestingUtility();
  private final static HBaseTestingUtility clusterC = new HBaseTestingUtility();

  private static HTable tableA;
  private static HTable tableB;
  private static HTable tableC;

  @BeforeClass
  public static void setUpClazz() throws Exception {

    int clusterAServerPort = HBaseTestingUtility.randomFreePort();
    int clusterBServerPort = HBaseTestingUtility.randomFreePort();
    int clusterCServerPort = HBaseTestingUtility.randomFreePort();

    setupConfiguration(clusterA, clusterAServerPort);
    setupConfiguration(clusterB, clusterBServerPort);
    setupConfiguration(clusterC, clusterCServerPort);

    addPeerThriftPort(clusterA, "1", clusterBServerPort);
    addPeerThriftPort(clusterB, "1", clusterCServerPort);
    addPeerThriftPort(clusterC, "1", clusterAServerPort);

    HTableDescriptor table = createTestTable();

    clusterA.startMiniCluster();
    clusterB.startMiniCluster();
    clusterC.startMiniCluster();

    createTableOnCluster(clusterA, table);
    createTableOnCluster(clusterB, table);
    createTableOnCluster(clusterC, table);

    tableA = getTestTable(clusterA, table);
    tableB = getTestTable(clusterB, table);
    tableC = getTestTable(clusterC, table);

    addReplicationPeer("1", clusterA, clusterB);
    addReplicationPeer("1", clusterB, clusterC);
    addReplicationPeer("1", clusterC, clusterA);
  }

  @Before
  public void setUp() throws Exception {
    clusterA.truncateTable(tableA.getTableName());
    clusterB.truncateTable(tableB.getTableName());

  }

  @AfterClass
  public static void tearDown() throws Exception {
    clusterA.shutdownMiniCluster();
    clusterB.shutdownMiniCluster();
    clusterC.shutdownMiniCluster();
  }

  @Test
  public void testCyclicReplication() throws Exception {
    String firstRow = "firstRow";
    putAndWait(generateRandomPut(firstRow), firstRow, tableA, tableB);

    String secondRow = "secondRow";
    putAndWait(generateRandomPut(secondRow), secondRow, tableB, tableC);

    String thirdRow = "thirdRow";
    Put lastPut = generateRandomPut(thirdRow);
    putAndWait(lastPut, thirdRow, tableC, tableA);

    Set<String> expected = Sets.newHashSet(firstRow, secondRow, thirdRow);
    assertContainsOnly(tableA, expected);
    assertContainsOnly(tableB, expected);
    assertContainsOnly(tableC, expected);

    // lets delete one of those rows and verify it goes around

    Delete delete = new Delete(lastPut.getRow());
    deleteAndWait(delete.getRow(), tableB, tableC);

    expected = Sets.newHashSet(firstRow, secondRow);
    assertContainsOnly(tableB, expected);
    assertContainsOnly(tableC, expected);
    assertContainsOnly(tableA, expected);
  }

  @org.junit.Rule
  public org.apache.hadoop.hbase.ResourceCheckerJUnitRule cu =
      new org.apache.hadoop.hbase.ResourceCheckerJUnitRule();

}
