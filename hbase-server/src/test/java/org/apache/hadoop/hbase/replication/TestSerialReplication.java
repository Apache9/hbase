/*
 * Copyright The Apache Software Foundation
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
package org.apache.hadoop.hbase.replication;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.HTestConst;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.catalog.MetaReader;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.replication.ReplicationAdmin;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category(LargeTests.class)
public class TestSerialReplication {
  private static final Log LOG = LogFactory.getLog(TestSerialReplication.class);

  private static Configuration conf1;
  private static Configuration conf2;

  private static HBaseTestingUtility utility1;
  private static HBaseTestingUtility utility2;

  private static final byte[] famName = Bytes.toBytes("f");
  private static final byte[] VALUE = Bytes.toBytes("v");
  private static final byte[] ROW = Bytes.toBytes("r");
  private static final byte[][] ROWS = HTestConst.makeNAscii(ROW, 100);

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    conf1 = HBaseConfiguration.create();
    conf1.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/1");
    // smaller block size and capacity to trigger more operations
    // and test them
    conf1.setInt("hbase.regionserver.hlog.blocksize", 1024 * 20);
    conf1.setLong("replication.source.sleepforretries", 100);
    conf1.setInt("hbase.regionserver.maxlogs", 10);
    conf1.setLong("hbase.master.logcleaner.ttl", 10);
    conf1.setBoolean("dfs.support.append", true);
    conf1.setLong(HConstants.THREAD_WAKE_FREQUENCY, 100);
    conf1.setStrings(CoprocessorHost.USER_REGION_COPROCESSOR_CONF_KEY,
        "org.apache.hadoop.hbase.replication.TestMasterReplication$CoprocessorCounter");
    conf1.setLong("replication.source.per.peer.node.bandwidth", 100L);// Each WAL is 120 bytes
    conf1.setLong("replication.source.size.capacity", 1L);
    conf1.setLong(HConstants.REPLICATION_SERIALLY_WAITING_KEY, 1000L);
    conf1.setBoolean("hbase.assignment.usezk", false);

    utility1 = new HBaseTestingUtility(conf1);
    utility1.startMiniZKCluster();
    MiniZooKeeperCluster miniZK = utility1.getZkCluster();
    new ZooKeeperWatcher(conf1, "cluster1", null, true);

    conf2 = new Configuration(conf1);
    conf2.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/2");

    utility2 = new HBaseTestingUtility(conf2);
    utility2.setZkCluster(miniZK);
    new ZooKeeperWatcher(conf2, "cluster2", null, true);

    utility1.startMiniCluster(1, 3);
    utility2.startMiniCluster(1, 1);

    ReplicationAdmin admin1 = new ReplicationAdmin(conf1);
    ReplicationPeerConfig rpc = new ReplicationPeerConfig();
    rpc.setClusterKey(utility2.getClusterKey());
    admin1.addPeer("1", rpc, null);

    utility1.getHBaseAdmin().setBalancerRunning(false, true);
  }

  @Test
  public void testRegionMoveAndFailover() throws Exception {
    TableName tableName = TableName.valueOf("testRSFailover");
    HTableDescriptor table = new HTableDescriptor(tableName);
    HColumnDescriptor fam = new HColumnDescriptor(famName);
    fam.setScope(HConstants.REPLICATION_SCOPE_SERIAL);
    table.addFamily(fam);
    utility1.getHBaseAdmin().createTable(table);
    // Table should be created on peer cluster, too

    HTable t1 = new HTable(conf1, tableName);
    HTable t2 = new HTable(conf2, tableName);
    try {
      LOG.info("move to 1");
      moveRegion(t1, 1);
      LOG.info("move to 0");
      moveRegion(t1, 0);
      for (int i = 10; i < 20; i++) {
        Put put = new Put(ROWS[i]);
        put.add(famName, VALUE, VALUE);
        t1.put(put);
      }
      LOG.info("move to 2");
      moveRegion(t1, 2);
      for (int i = 20; i < 30; i++) {
        Put put = new Put(ROWS[i]);
        put.add(famName, VALUE, VALUE);
        t1.put(put);
      }
      utility1.getHBaseCluster().abortRegionServer(2);
      for (int i = 30; i < 40; i++) {
        Put put = new Put(ROWS[i]);
        put.add(famName, VALUE, VALUE);
        t1.put(put);
      }

      long start = System.currentTimeMillis();
      while (System.currentTimeMillis() - start < 18000000) {
        Scan scan = new Scan();
        scan.setCaching(100);
        List<Cell> list = new ArrayList<Cell>();
        ResultScanner results = t2.getScanner(scan);
        for (Result result : results) {
          assertEquals(1, result.rawCells().length);
          list.add(result.rawCells()[0]);
        }
        results.close();
        List<Integer> listOfNumbers = getRowNumbers(list);
        LOG.info(Arrays.toString(listOfNumbers.toArray()));
        assertIntegerList(listOfNumbers, 10, 1);
        if (listOfNumbers.size() != 30) {
          LOG.info("Waiting all logs pushed to slave. Expected 30 , actual " + list.size());
          Thread.sleep(200);
          continue;
        }
        return;
      }
      throw new Exception("Not all logs have been pushed");
    } finally {

    }
  }


  @Test
  public void testSKipWAL() throws Exception {
    TableName tableName = TableName.valueOf("testSKipWAL");
    HTableDescriptor table = new HTableDescriptor(tableName);
    HColumnDescriptor fam = new HColumnDescriptor(famName);
    fam.setScope(HConstants.REPLICATION_SCOPE_SERIAL);
    table.addFamily(fam);
    utility1.getHBaseAdmin().createTable(table);
    // Table should be created on peer cluster, too

    HTable t1 = new HTable(conf1, tableName);
    HTable t2 = new HTable(conf2, tableName);
    try {
      LOG.info("move to 1");
      moveRegion(t1, 1);
      LOG.info("move to 0");
      moveRegion(t1, 0);
      for (int i = 10; i < 20; i++) {
        Put put = new Put(ROWS[i]);
        if (i % 10 >= 5) {
          put.setDurability(Durability.SKIP_WAL);
        }
        put.add(famName, VALUE, VALUE);
        t1.put(put);
      }
      LOG.info("move to 2");
      moveRegion(t1, 2);
      for (int i = 20; i < 30; i++) {
        Put put = new Put(ROWS[i]);
        put.add(famName, VALUE, VALUE);
        if (i % 10 >= 5) {
          put.setDurability(Durability.SKIP_WAL);
        }
        t1.put(put);
      }
      utility1.getHBaseCluster().abortRegionServer(2);
      for (int i = 30; i < 40; i++) {
        Put put = new Put(ROWS[i]);
        put.add(famName, VALUE, VALUE);
        if (i % 10 >= 5) {
          put.setDurability(Durability.SKIP_WAL);
        }
        t1.put(put);
      }

      long start = System.currentTimeMillis();
      while (System.currentTimeMillis() - start < 18000000) {
        Scan scan = new Scan();
        scan.setCaching(100);
        List<Cell> list = new ArrayList<Cell>();
        ResultScanner results = t2.getScanner(scan);
        for (Result result : results) {
          assertEquals(1, result.rawCells().length);
          list.add(result.rawCells()[0]);
        }
        results.close();
        List<Integer> listOfNumbers = getRowNumbers(list);
        List<Integer> list1 = new ArrayList<Integer>();
        List<Integer> list2 = new ArrayList<Integer>();
        List<Integer> list3 = new ArrayList<Integer>();
        for (int num : listOfNumbers) {
          if (num < 20) {
            list1.add(num);
          } else if (num < 30) {
            list2.add(num);
          } else {
            list3.add(num);
          }
        }
        LOG.info(Arrays.toString(list1.toArray()));
        LOG.info(Arrays.toString(list2.toArray()));
        LOG.info(Arrays.toString(list3.toArray()));
        assertIntegerList(list1, 10, 1);
        assertIntegerList(list2, 20, 1);
        assertIntegerList(list3, 30, 1);
        if (!list2.isEmpty()) {
          assertEquals(5, list1.size());
        }
        if (!list3.isEmpty()) {
          assertEquals(5, list2.size());
        }
        if (listOfNumbers.size() != 15) {
          LOG.info("Waiting all logs pushed to slave. Expected 15 , actual " + list.size());
          Thread.sleep(200);
          continue;
        }
        return;
      }
      throw new Exception("Not all logs have been pushed");
    } finally {

    }
  }

  @Test
  public void testRegionSplit() throws Exception {
    TableName tableName = TableName.valueOf("testRegionSplit");
    HTableDescriptor table = new HTableDescriptor(tableName);
    HColumnDescriptor fam = new HColumnDescriptor(famName);
    fam.setScope(HConstants.REPLICATION_SCOPE_SERIAL);
    table.addFamily(fam);
    utility1.getHBaseAdmin().createTable(table);
    // Table should be created on peer cluster, too

    HTable t1 = new HTable(conf1, tableName);
    HTable t2 = new HTable(conf2, tableName);

      for (int i = 10; i < 100; i += 10) {
        Put put = new Put(ROWS[i]);
        put.add(famName, VALUE, VALUE);
        t1.put(put);
      }
      utility1.getHBaseAdmin().split(tableName.getName(), ROWS[50]);
      Thread.sleep(5000L);
      for (int i = 11; i < 100; i += 10) {
        Put put = new Put(ROWS[i]);
        put.add(famName, VALUE, VALUE);
        t1.put(put);
      }
      balanceTwoRegions(t1);
      for (int i = 12; i < 100; i += 10) {
        Put put = new Put(ROWS[i]);
        put.add(famName, VALUE, VALUE);
        t1.put(put);
      }

      long start = System.currentTimeMillis();
      while (System.currentTimeMillis() - start < 180000) {
        Scan scan = new Scan();
        scan.setCaching(100);
        List<Cell> list = new ArrayList<Cell>();
        ResultScanner results = t2.getScanner(scan);
        for (Result result : results) {
          assertEquals(1, result.rawCells().length);
          list.add(result.rawCells()[0]);
        }
        results.close();

        List<Integer> listOfNumbers = getRowNumbers(list);
        List<Integer> list0 = new ArrayList<Integer>();
        List<Integer> list1 = new ArrayList<Integer>();
        List<Integer> list21 = new ArrayList<Integer>();
        List<Integer> list22 = new ArrayList<Integer>();
        for (int num : listOfNumbers) {
          if (num % 10 == 0) {
            list0.add(num);
          } else if (num % 10 == 1) {
            list1.add(num);
          } else if (num < 50) { //num%10==2
            list21.add(num);
          } else { // num%10==1&&num>50
            list22.add(num);
          }
        }

        LOG.info(Arrays.toString(list0.toArray()));
        LOG.info(Arrays.toString(list1.toArray()));
        LOG.info(Arrays.toString(list21.toArray()));
        LOG.info(Arrays.toString(list22.toArray()));
        assertIntegerList(list0, 10, 10);
        assertIntegerList(list1, 11, 10);
        assertIntegerList(list21, 12, 10);
        assertIntegerList(list22, 52, 10);
        if (!list1.isEmpty()) {
          assertEquals(9, list0.size());
        }
        if (!list21.isEmpty() || !list22.isEmpty()) {
          assertEquals(9, list1.size());
        }

        if (list.size() == 27) {
          return;
        }
        LOG.info("Waiting all logs pushed to slave. Expected 27 , actual " + list.size());
        Thread.sleep(200);
      }
      throw new Exception("Not all logs have been pushed");
  }

  @Test
  public void testRegionMerge() throws Exception {
    TableName tableName = TableName.valueOf("testRegionMerge");
    HTableDescriptor table = new HTableDescriptor(tableName);
    HColumnDescriptor fam = new HColumnDescriptor(famName);
    fam.setScope(HConstants.REPLICATION_SCOPE_SERIAL);
    table.addFamily(fam);
    utility1.getHBaseAdmin().createTable(table);
    // Table should be created on peer cluster, too

    utility1.getHBaseAdmin().split(tableName.getName(), ROWS[50]);
    Thread.sleep(5000L);

    HTable t1 = new HTable(conf1, tableName);
    HTable t2 = new HTable(conf2, tableName);

      for (int i = 10; i < 100; i += 10) {
        Put put = new Put(ROWS[i]);
        put.add(famName, VALUE, VALUE);
        t1.put(put);
      }
      List<Pair<HRegionInfo, ServerName>> regions =
          MetaReader.getTableRegionsAndLocations(
              utility1.getMiniHBaseCluster().getMaster().getCatalogTracker(), tableName);
      assertEquals(2, regions.size());
      utility1.getHBaseAdmin().mergeRegions(regions.get(0).getFirst().getEncodedNameAsBytes(),
          regions.get(1).getFirst().getEncodedNameAsBytes(), true);
      for (int i = 11; i < 100; i += 10) {
        Put put = new Put(ROWS[i]);
        put.add(famName, VALUE, VALUE);
        t1.put(put);
      }

      long start = System.currentTimeMillis();
      while (System.currentTimeMillis() - start < 180000) {
        Scan scan = new Scan();
        scan.setCaching(100);
        List<Cell> list = new ArrayList<Cell>();
        ResultScanner results = t2.getScanner(scan);
          for (Result result : results) {
            assertEquals(1, result.rawCells().length);
            list.add(result.rawCells()[0]);
          }
        results.close();

        List<Integer> listOfNumbers = getRowNumbers(list);
        List<Integer> list0 = new ArrayList<Integer>();
        List<Integer> list1 = new ArrayList<Integer>();
        for (int num : listOfNumbers) {
          if (num % 10 == 0) {
            list0.add(num);
          } else {
            list1.add(num);
          }
        }
        LOG.info(Arrays.toString(list0.toArray()));
        LOG.info(Arrays.toString(list1.toArray()));
        assertIntegerList(list0, 10, 10);
        assertIntegerList(list1, 11, 10);
        if (!list1.isEmpty()) {
          assertEquals(9, list0.size());
        }
        if (list.size() == 18) {
          return;
        }
        LOG.info("Waiting all logs pushed to slave. Expected 18 , actual " + list.size());
        Thread.sleep(200);
      }
  }

  private List<Integer> getRowNumbers(List<Cell> cells) {
    List<Integer> listOfRowNumbers = new ArrayList<Integer>();
    for (Cell c : cells) {
      listOfRowNumbers.add(Integer.parseInt(Bytes
          .toString(c.getRowArray(), c.getRowOffset() + ROW.length,
              c.getRowLength() - ROW.length)));
    }
    return listOfRowNumbers;
  }

  @AfterClass
  public static void setUpAfterClass() throws Exception {
    utility2.shutdownMiniCluster();
    utility1.shutdownMiniCluster();
  }

  private void moveRegion(HTable table, int index) throws IOException, InterruptedException {
    List<Pair<HRegionInfo, ServerName>> regions =
        MetaReader.getTableRegionsAndLocations(
            utility1.getMiniHBaseCluster().getMaster().getCatalogTracker(), table.getName());
    assertEquals(1, regions.size());
    HRegionInfo regionInfo = regions.get(0).getFirst();
    ServerName name = utility1.getHBaseCluster().getRegionServer(index).getServerName();
    utility1.getHBaseAdmin()
        .move(regionInfo.getEncodedNameAsBytes(), Bytes.toBytes(name.getServerName()));
    try {
      Thread.sleep(5000L); // wait to complete
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private void balanceTwoRegions(HTable table) throws Exception {
    List<Pair<HRegionInfo, ServerName>> regions =
        MetaReader.getTableRegionsAndLocations(
            utility1.getMiniHBaseCluster().getMaster().getCatalogTracker(), table.getName());
    assertEquals(2, regions.size());
    HRegionInfo regionInfo1 = regions.get(0).getFirst();
    ServerName name1 = utility1.getHBaseCluster().getRegionServer(0).getServerName();
    HRegionInfo regionInfo2 = regions.get(1).getFirst();
    ServerName name2 = utility1.getHBaseCluster().getRegionServer(1).getServerName();
    utility1.getHBaseAdmin()
        .move(regionInfo1.getEncodedNameAsBytes(), Bytes.toBytes(name1.getServerName()));
    Thread.sleep(5000L);
    utility1.getHBaseAdmin()
        .move(regionInfo2.getEncodedNameAsBytes(), Bytes.toBytes(name2.getServerName()));
    Thread.sleep(5000L);
  }

  private void assertIntegerList(List<Integer> list, int start, int step) {
    for (int i = 0; i < list.size(); i++) {
      assertTrue(list.get(i) == start + step * i);
    }
  }
}