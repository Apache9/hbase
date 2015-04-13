/**
 * Copyright 2010 The Apache Software Foundation
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
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.executor.EventHandler;
import org.apache.hadoop.hbase.executor.EventHandler.EventHandlerListener;
import org.apache.hadoop.hbase.executor.EventHandler.EventType;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
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
  private static final byte[] TABLENAME = Bytes.toBytes("TestMaster");
  private static final byte[] FAMILYNAME = Bytes.toBytes("fam");

  @BeforeClass
  public static void beforeAllTests() throws Exception {
    // Start a cluster of two regionservers.
    TEST_UTIL.startMiniCluster(2);
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
    assertTrue(m.assignmentManager.getZKTable().isEnabledTable
        (Bytes.toString(TABLENAME)));
    TEST_UTIL.loadTable(ht, FAMILYNAME);
    ht.close();

    List<Pair<HRegionInfo, ServerName>> tableRegions =
      MetaReader.getTableRegionsAndLocations(m.getCatalogTracker(),
          Bytes.toString(TABLENAME));
    LOG.info("Regions after load: " + Joiner.on(',').join(tableRegions));
    assertEquals(1, tableRegions.size());
    assertArrayEquals(HConstants.EMPTY_START_ROW,
        tableRegions.get(0).getFirst().getStartKey());
    assertArrayEquals(HConstants.EMPTY_END_ROW,
        tableRegions.get(0).getFirst().getEndKey());

    // Now trigger a split and stop when the split is in progress
    CountDownLatch split = new CountDownLatch(1);
    CountDownLatch proceed = new CountDownLatch(1);
    RegionSplitListener list = new RegionSplitListener(split, proceed);
    cluster.getMaster().executorService.
      registerListener(EventType.RS_ZK_REGION_SPLIT, list);

    LOG.info("Splitting table");
    TEST_UTIL.getHBaseAdmin().split(TABLENAME);
    LOG.info("Waiting for split result to be about to open");
    split.await(60, TimeUnit.SECONDS);
    try {
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
    } finally {
      proceed.countDown();
    }
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
      assertTrue(ioe.getCause() instanceof PleaseHoldException);
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
    // avoid using the same table created by other test cases
    byte[] testTableName = Bytes.toBytes(Bytes.toString(TABLENAME) + "_balance_throttling_test");
    HTable ht = TEST_UTIL.createTable(testTableName, new byte[][]{FAMILYNAME}, 1,
        startKey, stopKey, 5 * rsCount);
    // test limit on max regions in transition
    unbalance(m, ht, startKey, stopKey);
    m.getConfiguration().setInt("hbase.balancer.max.balancing.regions", 1);
    final AtomicInteger maxCount = new AtomicInteger(0);
    final AtomicBoolean stop = new AtomicBoolean(false);
    Runnable checker = new Runnable() {
      @Override public void run() {
        while (!stop.get()) {
          maxCount.set(Math.max(maxCount.get(), m.assignmentManager.getRegionsInTransitionCount()));
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
    int regionsToBalance = m.getBalancer().balanceCluster(
            m.assignmentManager.getAssignmentsByTable().get(Bytes.toString(testTableName))).size();
    long startTime = System.currentTimeMillis();
    m.balance();
    long elapsed = System.currentTimeMillis() - startTime;
    assertTrue("balance time: " + elapsed, elapsed >= 1000 * (regionsToBalance - 1));
  }

  private void unbalance(HMaster master, HTable ht, byte[] start, byte[] stop) throws Exception {
    while(master.assignmentManager.getRegionsInTransitionCount() > 0) {Thread.sleep(100);}
    HRegionServer biasedServer = TEST_UTIL.getMiniHBaseCluster().getRegionServer(0);
    for (HRegionLocation hrl : ht.getRegionsInRange(start, stop)) {
      master.move(hrl.getRegionInfo().getEncodedNameAsBytes(),
          Bytes.toBytes(biasedServer.getServerName().getServerName()));
    }
    while(master.assignmentManager.getRegionsInTransitionCount() > 0) {Thread.sleep(100);}
  }

  static class RegionSplitListener implements EventHandlerListener {
    CountDownLatch split, proceed;

    public RegionSplitListener(CountDownLatch split, CountDownLatch proceed) {
      this.split = split;
      this.proceed = proceed;
    }

    @Override
    public void afterProcess(EventHandler event) {
      if (event.getEventType() != EventType.RS_ZK_REGION_SPLIT) {
        return;
      }
      try {
        split.countDown();
        proceed.await(60, TimeUnit.SECONDS);
      } catch (InterruptedException ie) {
        throw new RuntimeException(ie);
      }
      return;
    }

    @Override
    public void beforeProcess(EventHandler event) {
    }
  }

  @org.junit.Rule
  public org.apache.hadoop.hbase.ResourceCheckerJUnitRule cu =
    new org.apache.hadoop.hbase.ResourceCheckerJUnitRule();
}
