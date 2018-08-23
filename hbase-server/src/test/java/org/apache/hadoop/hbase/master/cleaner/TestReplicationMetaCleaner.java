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
package org.apache.hadoop.hbase.master.cleaner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.catalog.MetaEditor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestReplicationMetaCleaner {

  private static final Log LOG = LogFactory.getLog(TestReplicationMetaCleaner.class);
  
  private static Configuration conf1;

  private static Configuration conf2;

  private static HBaseTestingUtility UTIL1;

  private static HBaseTestingUtility UTIL2;

  private static HBaseAdmin admin;

  private static HConnection conn;

  private static HTableInterface metaTable;

  private static final TableName serialTableName = TableName.valueOf("serialTable");

  private static final TableName notSerialTableName = TableName.valueOf("notSerialTable");

  private static final byte[] CF = Bytes.toBytes("F");

  private static final String PEER_ID = "1";

  private static final int PERIOD = 5000;

  @BeforeClass
  public static void setupCluster() throws Exception {
    // Setup UTIL1
    conf1 = HBaseConfiguration.create();
    conf1.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/1");
    conf1.setInt("hbase.master.cleaner.interval", PERIOD);
    UTIL1 = new HBaseTestingUtility(conf1);
    UTIL1.startMiniZKCluster();

    // Setup UTIL2
    conf2 = HBaseConfiguration.create(conf1);
    conf2.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/2");
    UTIL2 = new HBaseTestingUtility(conf2);
    UTIL2.setZkCluster(UTIL1.getZkCluster());

    UTIL1.startMiniCluster();
    UTIL2.startMiniCluster();

    // Create peer
    conn = HConnectionManager.createConnection(UTIL1.getConfiguration());
    admin = UTIL1.getHBaseAdmin();
    ReplicationPeerConfig rpc = new ReplicationPeerConfig();
    rpc.setClusterKey(UTIL2.getClusterKey());
    admin.addReplicationPeer(PEER_ID, rpc, false);

    // Create table
    HTableDescriptor tableDesc = new HTableDescriptor(serialTableName);
    HColumnDescriptor cfDesc = new HColumnDescriptor(CF);
    cfDesc.setScope(HConstants.REPLICATION_SCOPE_SERIAL);
    tableDesc.addFamily(cfDesc);
    admin.createTable(tableDesc);
    tableDesc = new HTableDescriptor(notSerialTableName);
    cfDesc = new HColumnDescriptor(CF);
    tableDesc.addFamily(cfDesc);
    admin.createTable(tableDesc);
    metaTable = conn.getTable(TableName.META_TABLE_NAME);
  }

  @AfterClass
  public static void shutdownCluster() throws Exception {
    metaTable.close();
    admin.close();
    conn.close();
    UTIL2.shutdownMiniCluster();
    UTIL1.shutdownMiniCluster();
  }

  private void writeBarriers(byte[] encodedRegionName, List<Long> barriers, byte[] tableName)
      throws IOException {
    List<Put> puts = barriers.stream()
        .map(barrier -> MetaEditor.makeBarrierPut(encodedRegionName, barrier, tableName))
        .collect(Collectors.toList());
    metaTable.put(puts);
  }

  private void verifyBarriers(byte[] encodedRegionName, List<Long> barriers) throws IOException {
    List<Long> metaBarriers = MetaEditor.getReplicationBarriers(conn, encodedRegionName);
    assertEquals(barriers.size(), metaBarriers.size());
    for (Long barrier : barriers) {
      assertTrue(metaBarriers.contains(barrier));
    }
  }

  private void verifyNoBarriers(byte[] encodedRegionName) throws IOException {
    List<Long> metaBarriers = MetaEditor.getReplicationBarriers(conn, encodedRegionName);
    assertEquals(0, metaBarriers.size());
  }

  @Test
  public void testReplicationMetaCleaner() throws Exception {
    byte[] serialTableEncodedRegionName = admin.getTableRegions(serialTableName).get(0)
        .getEncodedNameAsBytes();
    List<Long> barriers = Arrays.asList(1L, 11L, 21L);
    writeBarriers(serialTableEncodedRegionName, barriers, serialTableName.getName());
    byte[] notSerialTableEncodedRegionName = admin.getTableRegions(notSerialTableName).get(0)
        .getEncodedNameAsBytes();
    writeBarriers(notSerialTableEncodedRegionName, barriers, notSerialTableName.getName());

    Thread.sleep(PERIOD * 2);
    // There are no replication position, so nothing to delete
    verifyBarriers(serialTableEncodedRegionName, barriers);
    verifyBarriers(notSerialTableEncodedRegionName, barriers);

    Map<String, Long> positions = new HashMap<>();
    positions.put(Bytes.toString(serialTableEncodedRegionName), 5L);
    positions.put(Bytes.toString(notSerialTableEncodedRegionName), 5L);
    MetaEditor.updateReplicationPositions(conn, PEER_ID, positions);

    Thread.sleep(PERIOD * 2);
    // Replication meta for not serial table will be cleaned
    verifyBarriers(serialTableEncodedRegionName, barriers);
    verifyNoBarriers(notSerialTableEncodedRegionName);

    positions = new HashMap<>();
    positions.put(Bytes.toString(serialTableEncodedRegionName), 15L);
    MetaEditor.updateReplicationPositions(conn, PEER_ID, positions);
    
    Thread.sleep(PERIOD * 2);
    barriers = Arrays.asList(11L, 21L);
    // barrier 1L should be cleaned
    verifyBarriers(serialTableEncodedRegionName, barriers);

    positions = new HashMap<>();
    positions.put(Bytes.toString(serialTableEncodedRegionName), 25L);
    MetaEditor.updateReplicationPositions(conn, PEER_ID, positions);

    Thread.sleep(PERIOD * 2);
    barriers = Arrays.asList(21L);
    // barrier 11L should be cleaned
    verifyBarriers(serialTableEncodedRegionName, barriers);
  }
}