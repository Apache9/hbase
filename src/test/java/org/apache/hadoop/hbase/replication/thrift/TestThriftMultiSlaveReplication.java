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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Sets;
@Category(MediumTests.class)
public class TestThriftMultiSlaveReplication extends TestThriftReplicationBase {

  private static final Log LOG = LogFactory.getLog(TestThriftCyclicReplication.class);


  private final static HBaseTestingUtility clusterA = new HBaseTestingUtility();
  private final static HBaseTestingUtility clusterB = new HBaseTestingUtility();
  private final static HBaseTestingUtility clusterC = new HBaseTestingUtility();

  private static HTable tableA;
  private static HTable tableB;
  private static HTable tableC;

  @BeforeClass
  public static void setUp() throws Exception {

    int clusterAServerPort = HBaseTestingUtility.randomFreePort();
    int clusterBServerPort = HBaseTestingUtility.randomFreePort();
    int clusterCServerPort = HBaseTestingUtility.randomFreePort();

    setupConfiguration(clusterA, clusterAServerPort);
    setupConfiguration(clusterB, clusterBServerPort);
    setupConfiguration(clusterC, clusterCServerPort);

    addPeerThriftPort(clusterA, "1", clusterBServerPort);
    addPeerThriftPort(clusterA, "2", clusterCServerPort);

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
    addReplicationPeer("2", clusterA, clusterC);
  }

  @Test
  public void testReplicatingToMultipleSlaves() throws Exception {
    String firstRow = "firstRow";
    putAndWait(generateRandomPut(firstRow), firstRow, tableA, tableB);

    // make sure all tables have that one element only
    Set<String> expected = Sets.newHashSet(firstRow);
    assertContainsOnly(tableA, expected);
    assertContainsOnly(tableB, expected);
    assertContainsOnly(tableC, expected);

    String secondRow = "secondRow";
    Put lastPut = generateRandomPut(secondRow);
    putAndWait(lastPut, secondRow, tableA, tableB);

    // all tables should have both elements
    Set<String> otherExpected = Sets.newHashSet(firstRow, secondRow);
    assertContainsOnly(tableA, otherExpected);
    assertContainsOnly(tableB, otherExpected);
    assertContainsOnly(tableC, otherExpected);

    // lets delete one of those rows and verify it goes replicated to both slaves
    Delete delete = new Delete(lastPut.getRow());
    deleteAndWait(delete.getRow(), tableA, tableB);

    assertContainsOnly(tableA, expected);
    assertContainsOnly(tableB, expected);
    assertContainsOnly(tableC, expected);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    clusterA.shutdownMiniCluster();
    clusterB.shutdownMiniCluster();
    clusterC.shutdownMiniCluster();
  }

  @org.junit.Rule
  public org.apache.hadoop.hbase.ResourceCheckerJUnitRule cu =
      new org.apache.hadoop.hbase.ResourceCheckerJUnitRule();
}
