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
import com.google.common.collect.Iterables;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.*;
import org.junit.experimental.categories.Category;

import static org.apache.hadoop.hbase.replication.thrift.ReplicationTestUtils.*;
import static org.junit.Assert.assertEquals;



@Category(MediumTests.class)
public class TestThriftMasterMasterReplication extends TestThriftReplicationBase {

  private final static HBaseTestingUtility clusterA = new HBaseTestingUtility();
  private final static HBaseTestingUtility clusterB = new HBaseTestingUtility();

  private static HTable tableA;
  private static HTable tableB;

  @BeforeClass
  public static void setUpClazz() throws Exception {

    int clusterAServerPort = HBaseTestingUtility.randomFreePort();
    int clusterBServerPort = HBaseTestingUtility.randomFreePort();

    setupConfiguration(clusterA, clusterAServerPort);
    setupConfiguration(clusterB, clusterBServerPort);

    addPeerThriftPort(clusterA, "1", clusterBServerPort);
    addPeerThriftPort(clusterB, "1", clusterAServerPort);

    HTableDescriptor table = createTestTable();

    clusterA.startMiniCluster();
    clusterB.startMiniCluster();

    createTableOnCluster(clusterA, table);
    createTableOnCluster(clusterB, table);

    tableA = getTestTable(clusterA, table);
    tableB = getTestTable(clusterB, table);

    addReplicationPeer("1", clusterA, clusterB);
    addReplicationPeer("1", clusterB, clusterA);
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
  }

  @Test
  public void testMasterMasterReplication() throws Exception {
    long originalTimestamp = 1l;
    String rowKey = "master-master-key";
    String value = "testMasterMaster";
    Put originalPut = new Put(Bytes.toBytes(rowKey));
    originalPut.add(DEFAULT_FAMILY, DEFAULT_QUALIFIER, originalTimestamp, Bytes.toBytes(value));
    Result originalResult = putAndWait(originalPut, value, true, tableA, tableB);
    KeyValue originalKeyVal =
        Iterables.getOnlyElement(originalResult.getColumn(DEFAULT_FAMILY, DEFAULT_QUALIFIER));
    assertEquals(originalTimestamp, originalKeyVal.getTimestamp());

    long newTimestamp = 2l;
    Put overwritePut = new Put(Bytes.toBytes(rowKey));
    overwritePut.add(DEFAULT_FAMILY, DEFAULT_QUALIFIER, newTimestamp, Bytes.toBytes(value));
    putAndWait(overwritePut, value, true, tableB, tableA);
  }
}
