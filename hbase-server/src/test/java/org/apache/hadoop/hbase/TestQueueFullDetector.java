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
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase;

import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.ManualEnvironmentEdge;
import org.apache.hadoop.hbase.util.QueueCounter;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestQueueFullDetector {
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static ManualEnvironmentEdge envEdge;
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
    HRegionServer regionServer = cluster.getRegionServer(0);
    final QueueCounter queueCounter = regionServer.getRpcServer().getScheduler().getQueueCounter();
    class ChangeRunnable implements Runnable {
      private volatile boolean shouldStop = false;

      public void stopChanger() {
        shouldStop = true;
      }

      public void run() {
        long idx = 0;
        while (!shouldStop) {
          queueCounter.incIncomeRequestCount();
          // Simulate 90% failure due to queue full
          if (idx % 10 != 0) {
            queueCounter.setQueueFull(true);
            queueCounter.incRejectedRequestCount();
          } else {
            queueCounter.setQueueFull(false);
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
    final QueueCounter queueCounter = regionServer.getRpcServer().getScheduler().getQueueCounter();
    class ChangeRunnable implements Runnable {
      private volatile boolean shouldStop = false;

      public void stopChanger() {
        shouldStop = true;
      }

      public void run() {
        long idx = 0;
        while (!shouldStop) {
          queueCounter.incIncomeRequestCount();
          if (idx % 40 == 0) {
            queueCounter.setQueueFull(false);
          } else {
            queueCounter.setQueueFull(true);
            queueCounter.incRejectedRequestCount();
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
