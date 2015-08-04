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
package org.apache.hadoop.hbase.master;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.catalog.MetaReader;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.base.Joiner;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.*;

@Category(MediumTests.class)
public class TestMaster {
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final Log LOG = LogFactory.getLog(TestMaster.class);
  private static final TableName TABLENAME =
      TableName.valueOf("TestMaster");
  private static final byte[] FAMILYNAME = Bytes.toBytes("fam");
  private static HBaseAdmin admin;

  @BeforeClass
  public static void beforeAllTests() throws Exception {
    // Start a cluster of two regionservers.
    TEST_UTIL.startMiniCluster(2);
    admin = TEST_UTIL.getHBaseAdmin();
    TEST_UTIL.getHBaseCluster().getMaster().assignmentManager.initializeHandlerTrackers();
  }

  @AfterClass
  public static void afterAllTests() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testMasterOpsWhileSplitting() throws Exception {
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    HMaster m = cluster.getMaster();

    HTable ht = TEST_UTIL.createTable(TABLENAME, FAMILYNAME);
    assertTrue(m.assignmentManager.getZKTable().isEnabledTable(TABLENAME));
    TEST_UTIL.loadTable(ht, FAMILYNAME, false);
    ht.close();

    List<Pair<HRegionInfo, ServerName>> tableRegions =
      MetaReader.getTableRegionsAndLocations(m.getCatalogTracker(), TABLENAME);
    LOG.info("Regions after load: " + Joiner.on(',').join(tableRegions));
    assertEquals(1, tableRegions.size());
    assertArrayEquals(HConstants.EMPTY_START_ROW,
        tableRegions.get(0).getFirst().getStartKey());
    assertArrayEquals(HConstants.EMPTY_END_ROW,
        tableRegions.get(0).getFirst().getEndKey());

    // Now trigger a split and stop when the split is in progress
    LOG.info("Splitting table");
    TEST_UTIL.getHBaseAdmin().split(TABLENAME.getName());
    LOG.info("Waiting for split result to be about to open");
    RegionStates regionStates = m.assignmentManager.getRegionStates();
    while (regionStates.getRegionsOfTable(TABLENAME).size() <= 1) {
      Thread.sleep(100);
    }
    LOG.info("Making sure we can call getTableRegions while opening");
    tableRegions = MetaReader.getTableRegionsAndLocations(m.getCatalogTracker(),
        TABLENAME, false);

    LOG.info("Regions: " + Joiner.on(',').join(tableRegions));
    // We have three regions because one is split-in-progress
    assertEquals(3, tableRegions.size());
    LOG.info("Making sure we can call getTableRegionClosest while opening");
    Pair<HRegionInfo, ServerName> pair =
        m.getTableRegionForRow(TABLENAME, Bytes.toBytes("cde"));
    LOG.info("Result is: " + pair);
    Pair<HRegionInfo, ServerName> tableRegionFromName =
        MetaReader.getRegion(m.getCatalogTracker(),
            pair.getFirst().getRegionName());
    assertEquals(tableRegionFromName.getFirst(), pair.getFirst());
  }

  @Test
  public void testMoveRegionWhenNotInitialized() {
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    HMaster m = cluster.getMaster();
    try {
      m.initialized = false; // fake it, set back later
      HRegionInfo meta = HRegionInfo.FIRST_META_REGIONINFO;
      m.move(meta.getEncodedNameAsBytes(), null);
      fail("Region should not be moved since master is not initialized");
    } catch (IOException ioe) {
      assertTrue(ioe instanceof PleaseHoldException);
    } finally {
      m.initialized = true;
    }
  }

  @Test
  public void testBalancerThrottling() throws Exception {
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    final HMaster m = cluster.getMaster();
    byte[] startKey = new byte[] {0x00};
    byte[] stopKey = new byte[] {0x7f};
    int rsCount = TEST_UTIL.getMiniHBaseCluster().getRegionServerThreads().size();
    TableName tableName = TableName.valueOf("testBalancerThrottling");
    HTable ht = TEST_UTIL.createTable(tableName, new byte[][]{FAMILYNAME}, 1,
        startKey, stopKey, 5 * rsCount);
    // test limit on max regions in transition
    unbalance(m, ht, startKey, stopKey);
    m.getConfiguration().setInt("hbase.balancer.max.balancing.regions", 1);
    final AtomicInteger maxCount = new AtomicInteger(0);
    final AtomicBoolean stop = new AtomicBoolean(false);
    Runnable checker = new Runnable() {
      @Override public void run() {
        while (!stop.get()) {
          maxCount.set(Math.max(maxCount.get(), m.assignmentManager.getRegionStates().getRegionsInTransitionCount()));
          try {
            Thread.sleep(10);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
      }
    };
    Thread thread = new Thread(checker);
    thread.start();
    m.balance();
    stop.set(true);
    thread.interrupt();
    thread.join();
    assertTrue("max regions in transition: " + maxCount.get(), maxCount.get() <= 1);
    // test min region moving interval
    unbalance(m, ht, startKey, stopKey);
    m.getConfiguration().unset("hbase.balancer.max.balancing.regions");
    m.getConfiguration().setInt("hbase.balancer.min.balancing.interval", 1000);
    boolean bytable = m.getConfiguration().getBoolean("hbase.master.loadbalance.bytable", false);
    m.getConfiguration().setBoolean("hbase.master.loadbalance.bytable", true);
    int regionsToBalance = m
        .getBalancer()
        .balanceCluster(
          m.assignmentManager.getRegionStates().getAssignmentsByTable().get(tableName)).size();
    long startTime = System.currentTimeMillis();
    m.balance();
    long elapsed = System.currentTimeMillis() - startTime;
    assertTrue("balance time: " + elapsed, elapsed >= 1000 * (regionsToBalance - 1));
    m.getConfiguration().setBoolean("hbase.master.loadbalance.bytable", bytable);
  }

  private void unbalance(HMaster master, HTable ht, byte[] start, byte[] stop) throws Exception {
    while (master.assignmentManager.getRegionStates().getRegionsInTransitionCount() > 0) {
      Thread.sleep(100);
    }
    HRegionServer biasedServer = TEST_UTIL.getMiniHBaseCluster().getRegionServer(0);
    for (HRegionLocation hrl : ht.getRegionsInRange(start, stop)) {
      master.move(hrl.getRegionInfo().getEncodedNameAsBytes(),
          Bytes.toBytes(biasedServer.getServerName().getServerName()));
    }
    while (master.assignmentManager.getRegionStates().getRegionsInTransitionCount() > 0) {
      Thread.sleep(100);
    }
  }
  
  @Test
  public void testMoveThrowsUnknownRegionException() throws IOException {
    TableName tableName =
        TableName.valueOf("testMoveThrowsUnknownRegionException");
    HTableDescriptor htd = new HTableDescriptor(tableName);
    HColumnDescriptor hcd = new HColumnDescriptor("value");
    htd.addFamily(hcd);

    admin.createTable(htd, null);
    try {
      HRegionInfo hri = new HRegionInfo(
        tableName, Bytes.toBytes("A"), Bytes.toBytes("Z"));
      admin.move(hri.getEncodedNameAsBytes(), null);
      fail("Region should not be moved since it is fake");
    } catch (IOException ioe) {
      assertTrue(ioe instanceof UnknownRegionException);
    } finally {
      TEST_UTIL.deleteTable(tableName);
    }
  }

  @Test
  public void testMoveThrowsPleaseHoldException() throws IOException {
    byte[] tableName = Bytes.toBytes("testMoveThrowsPleaseHoldException");
    HMaster master = TEST_UTIL.getMiniHBaseCluster().getMaster();
    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));
    HColumnDescriptor hcd = new HColumnDescriptor("value");
    htd.addFamily(hcd);

    admin.createTable(htd, null);
    try {
      List<HRegionInfo> tableRegions = admin.getTableRegions(tableName);

      master.initialized = false; // fake it, set back later
      admin.move(tableRegions.get(0).getEncodedNameAsBytes(), null);
      fail("Region should not be moved since master is not initialized");
    } catch (IOException ioe) {
      assertTrue(ioe instanceof PleaseHoldException);
    } finally {
      master.initialized = true;
      TEST_UTIL.deleteTable(tableName);
    }
  }
}

