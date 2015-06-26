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
package org.apache.hadoop.hbase.replication.thrift;

import com.google.common.collect.Sets;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Set;

import static org.apache.hadoop.hbase.replication.thrift.ReplicationTestUtils.*;
import static org.apache.hadoop.hbase.replication.thrift.ReplicationTestUtils.assertContainsOnly;
import static org.apache.hadoop.hbase.replication.thrift.ReplicationTestUtils.deleteAndWait;

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
}
