/**
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
package org.apache.hadoop.hbase.ipc;

import static org.junit.Assert.assertTrue;

import java.util.List;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.ManualEnvironmentEdge;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({RegionServerTests.class, MediumTests.class})
public class TestQueueFullDetector {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestQueueFullDetector.class);

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static ManualEnvironmentEdge envEdge;
  private static MiniHBaseCluster cluster;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setBoolean(QueueFullDetector.QUEUE_FULL_DETECTOR_ENABLED,
      QueueFullDetector.QUEUE_FULL_DETECTOR_ENABLED_DEFAULT);
    TEST_UTIL.getConfiguration().setInt(QueueFullDetector.QUEUE_FULL_DETECTOR_DENSE_PERIOD, 10);
    TEST_UTIL.getConfiguration().setInt(QueueFullDetector.QUEUE_FULL_DETECTOR_DENSE_CHECKNUM, 120);
    TEST_UTIL.getConfiguration().setInt(QueueFullDetector.QUEUE_FULL_DETECTOR_SPARSE_PERIOD, 50);

    cluster = TEST_UTIL.startMiniCluster();
    envEdge = new ManualEnvironmentEdge();
    envEdge.setValue(System.currentTimeMillis());
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
    List<QueueCounter> queueCounters =
        regionServer.getRpcServer().getScheduler().getQueueCounters();
    QueueCounter queueCounter = queueCounters.get(0);
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
    List<QueueCounter> queueCounters =
        regionServer.getRpcServer().getScheduler().getQueueCounters();
    QueueCounter queueCounter = queueCounters.get(0);
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
