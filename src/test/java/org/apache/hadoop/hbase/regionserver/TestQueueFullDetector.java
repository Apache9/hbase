/**
 * Copyright 2010 The Apache Software Foundation Licensed to the Apache Software Foundation (ASF)
 * under one or more contributor license agreements. See the NOTICE file distributed with this work
 * for additional information regarding copyright ownership. The ASF licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law or agreed to in
 * writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */
package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertTrue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.ipc.HBaseServer;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.ManualEnvironmentEdge;
import org.apache.hadoop.hbase.util.QueueCounter;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestQueueFullDetector /* extends HBaseTestCase */{
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static ManualEnvironmentEdge envEdge;
  private final Log LOG = LogFactory.getLog(this.getClass());
  private static MiniHBaseCluster cluster;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setBoolean("hbase.regionserver.queuefull.detector.enable", true);
    TEST_UTIL.getConfiguration().setInt("hbase.regionserver.queuefull.detector.denseperiod", 10);
    TEST_UTIL.getConfiguration().setInt("hbase.regionserver.queuefull.detector.densechecknum", 120);
    TEST_UTIL.getConfiguration().setInt("hbase.regionserver.queuefull.detector.sparseperiod", 50);

    cluster = TEST_UTIL.startMiniCluster();
    envEdge = new ManualEnvironmentEdge();
    envEdge.setValue(EnvironmentEdgeManager.currentTimeMillis());
    EnvironmentEdgeManager.injectEdge(envEdge);
  }

  @AfterClass
  public static void teardown() throws Exception {
    EnvironmentEdgeManager.reset();
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testOccasionalQueueFull() throws Exception {
    /*
     * MiniHBaseCluster ms = TEST_UTIL.startMiniCluster(); System.out.println("Ms is " + ms);
     * regionServer = ms.getRegionServer(0);
     */
    HRegionServer regionServer = cluster.getRegionServer(0);
    final QueueCounter queueCounter = ((HBaseServer) regionServer.getRpcServer()).getQueueCounter();
    class ChangeRunnable implements Runnable {
      private volatile boolean shouldStop = false;

      public void stopChanger() {
        shouldStop = true;
      }

      public void run() {
        long idx = 0;
        while (!shouldStop) {
          queueCounter.incIncomeReadCount();
          queueCounter.incIncomeWriteCount();
          // Simulate 90% failure due to queue full
          if (idx % 10 != 0) {
            queueCounter.setReadQueueFull(true);
            queueCounter.setWriteQueueFull(true);
            queueCounter.incRejectedReadCount();
            queueCounter.incRejectedWriteCount();
          } else {
            queueCounter.setReadQueueFull(false);
            queueCounter.setWriteQueueFull(false);
          }
          Thread.yield();
          idx++;
        }
      }
    }

    ChangeRunnable changerRunnable = new ChangeRunnable();
    Thread qcChanger = new Thread(changerRunnable);
    qcChanger.start();
    Thread.sleep(5000);
    changerRunnable.stopChanger();
    qcChanger.join();
    assertTrue(!regionServer.isStopping() && !regionServer.isStopped());
  }

  @Test
  public void testFrequentQueueFull() throws Exception {
    HRegionServer regionServer = cluster.getRegionServer(0);
    final QueueCounter queueCounter = ((HBaseServer) regionServer.getRpcServer()).getQueueCounter();
    class ChangeRunnable implements Runnable {
      private volatile boolean shouldStop = false;

      public void stopChanger() {
        shouldStop = true;
      }

      public void run() {
        long idx = 0;
        while (!shouldStop) {
          queueCounter.incIncomeReadCount();
          queueCounter.incIncomeWriteCount();
          if (idx % 40 == 0) {
            queueCounter.setReadQueueFull(false);
            queueCounter.setWriteQueueFull(false);
          } else {
            queueCounter.setReadQueueFull(true);
            queueCounter.setWriteQueueFull(true);
            queueCounter.incRejectedReadCount();
            queueCounter.incRejectedWriteCount();
          }
          Thread.yield();
          idx++;
        }
      }
    }

    ChangeRunnable changerRunnable = new ChangeRunnable();
    Thread qcChanger = new Thread(changerRunnable);
    qcChanger.start();
    Thread.sleep(5000);
    changerRunnable.stopChanger();
    qcChanger.join();
    assertTrue(regionServer.isStopping() || regionServer.isStopped());
  }
}
