/*
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

package org.apache.hadoop.hbase.throughput;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test the reading and writing of throughput quota to and from zookeeper.
 */
@Category(LargeTests.class)
public class TestZKThroughputQuotaWatcher {
  private static final Log LOG = LogFactory.getLog(TestZKThroughputQuotaWatcher.class);
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static ThroughputManager TMGR_A;
  private static ThroughputManager TMGR_B;
  private final static Abortable ABORTABLE = new Abortable() {
    private final AtomicBoolean abort = new AtomicBoolean(false);

    @Override
    public void abort(String why, Throwable e) {
      LOG.info(why, e);
      abort.set(true);
    }

    @Override
    public boolean isAborted() {
      return abort.get();
    }
  };

  private static String TEST_TABLE_STR = "throughput_quota_test";
  private static byte[] TEST_TABLE = Bytes.toBytes(TEST_TABLE_STR);

  @BeforeClass
  public static void beforeClass() throws Exception {
    // setup configuration
    Configuration conf = TEST_UTIL.getConfiguration();
    ThroughputQuotaTestUtil.enableCoprocessor(conf);

    // start minicluster
    TEST_UTIL.startMiniCluster();
    TMGR_A = ThroughputManager.get(new ZooKeeperWatcher(conf,
      "TestZKThroughputQuotaWatcher_1", ABORTABLE), conf);
    TMGR_B = ThroughputManager.get(new ZooKeeperWatcher(conf,
      "TestZKThroughputQuotaWatcher_2", ABORTABLE), conf);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testZKWatcher() throws Exception {
    final String user1 = "alice";
    final String user2 = "bob";
    
    // check limiters
    assertNull(TMGR_A.getThroughputLimiter(user1, TEST_TABLE, RequestType.READ));
    assertNull(TMGR_A.getThroughputLimiter(user1, TEST_TABLE, RequestType.WRITE));
    assertNull(TMGR_B.getThroughputLimiter(user1, TEST_TABLE, RequestType.READ));
    assertNull(TMGR_B.getThroughputLimiter(user1, TEST_TABLE, RequestType.WRITE));
    assertNull(TMGR_A.getThroughputLimiter(user2, TEST_TABLE, RequestType.READ));
    assertNull(TMGR_A.getThroughputLimiter(user2, TEST_TABLE, RequestType.WRITE));
    assertNull(TMGR_B.getThroughputLimiter(user2, TEST_TABLE, RequestType.READ));
    assertNull(TMGR_B.getThroughputLimiter(user2, TEST_TABLE, RequestType.WRITE));

    // set read quota for user1
    Map<String, EnumMap<RequestType, Double>> userQuota = new HashMap<String, EnumMap<RequestType, Double>>();
    EnumMap<RequestType, Double> reqQuota = new EnumMap<RequestType, Double>(RequestType.class);
    reqQuota.put(RequestType.READ, 100.0);
    userQuota.put(user1, reqQuota);
    TMGR_A.writeToZookeeper(new ThroughputQuota(TEST_TABLE_STR, userQuota));
    Thread.sleep(100);

    // check limiters
    assertNotNull(TMGR_A.getThroughputLimiter(user1, TEST_TABLE, RequestType.READ));
    assertNull(TMGR_A.getThroughputLimiter(user1, TEST_TABLE, RequestType.WRITE));
    assertNotNull(TMGR_B.getThroughputLimiter(user1, TEST_TABLE, RequestType.READ));
    assertNull(TMGR_B.getThroughputLimiter(user1, TEST_TABLE, RequestType.WRITE));
    assertNull(TMGR_A.getThroughputLimiter(user2, TEST_TABLE, RequestType.READ));
    assertNull(TMGR_A.getThroughputLimiter(user2, TEST_TABLE, RequestType.WRITE));
    assertNull(TMGR_B.getThroughputLimiter(user2, TEST_TABLE, RequestType.READ));
    assertNull(TMGR_B.getThroughputLimiter(user2, TEST_TABLE, RequestType.WRITE));

    // clear quota for user1 and set write quota for user2
    userQuota.clear();
    userQuota.put(user1, new EnumMap<RequestType, Double>(RequestType.class));
    reqQuota.clear();
    reqQuota.put(RequestType.WRITE, 100.0);
    userQuota.put(user2, reqQuota);
    TMGR_B.writeToZookeeper(new ThroughputQuota(TEST_TABLE_STR, userQuota));
    Thread.sleep(100);

    // check limiters
    assertNull(TMGR_A.getThroughputLimiter(user1, TEST_TABLE, RequestType.READ));
    assertNull(TMGR_A.getThroughputLimiter(user1, TEST_TABLE, RequestType.WRITE));
    assertNull(TMGR_B.getThroughputLimiter(user1, TEST_TABLE, RequestType.READ));
    assertNull(TMGR_B.getThroughputLimiter(user1, TEST_TABLE, RequestType.WRITE));
    assertNull(TMGR_A.getThroughputLimiter(user2, TEST_TABLE, RequestType.READ));
    assertNotNull(TMGR_A.getThroughputLimiter(user2, TEST_TABLE, RequestType.WRITE));
    assertNull(TMGR_B.getThroughputLimiter(user2, TEST_TABLE, RequestType.READ));
    assertNotNull(TMGR_B.getThroughputLimiter(user2, TEST_TABLE, RequestType.WRITE));
    
    // remove all quota settings of test table
    TMGR_A.removeFromZookeeper(TEST_TABLE_STR);
    Thread.sleep(100);
    
    // check limiters
    assertNull(TMGR_A.getThroughputLimiter(user1, TEST_TABLE, RequestType.READ));
    assertNull(TMGR_A.getThroughputLimiter(user1, TEST_TABLE, RequestType.WRITE));
    assertNull(TMGR_B.getThroughputLimiter(user1, TEST_TABLE, RequestType.READ));
    assertNull(TMGR_B.getThroughputLimiter(user1, TEST_TABLE, RequestType.WRITE));
    assertNull(TMGR_A.getThroughputLimiter(user2, TEST_TABLE, RequestType.READ));
    assertNull(TMGR_A.getThroughputLimiter(user2, TEST_TABLE, RequestType.WRITE));
    assertNull(TMGR_B.getThroughputLimiter(user2, TEST_TABLE, RequestType.READ));
    assertNull(TMGR_B.getThroughputLimiter(user2, TEST_TABLE, RequestType.WRITE));
  }
}
