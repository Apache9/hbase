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

import static org.junit.Assert.assertTrue;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestBalanceThrottlingByMaxRitPercent extends TestBalanceThrottlingBase {
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  @BeforeClass
  public static void beforeAllTests() throws Exception {
    // Set max balancing time to 500 ms and max percent of regions in transition to 0.05
    TEST_UTIL.getConfiguration().setInt(HConstants.HBASE_BALANCER_MAX_BALANCING, 500);
    TEST_UTIL.getConfiguration().setFloat(HConstants.HBASE_MASTER_BALANCER_MAX_RIT_PERCENT, 0.05f);
    TEST_UTIL.startMiniCluster(2);
  }

  @AfterClass
  public static void afterAllTests() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test(timeout = 300000)
  public void testThrottlingByMaxRitPercent() throws Exception {
    byte[] tableName = Bytes.toBytes("testThrottlingByMaxRitPercent");
    createTable(TEST_UTIL, tableName);
    final HMaster master = TEST_UTIL.getHBaseCluster().getMaster();

    unbalance(TEST_UTIL, master, tableName);
    AtomicInteger maxCount = new AtomicInteger(0);
    AtomicBoolean stop = new AtomicBoolean(false);
    Thread checker = startBalancerChecker(master, maxCount, stop);
    master.balance();
    stop.set(true);
    checker.interrupt();
    checker.join();
    // The max number of regions in transition is 100 * 0.05 = 5
    assertTrue("max regions in transition: " + maxCount.get(), maxCount.get() == 5);

    TEST_UTIL.deleteTable(tableName);
  }
}
