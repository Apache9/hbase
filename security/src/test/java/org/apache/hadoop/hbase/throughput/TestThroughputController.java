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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.LargeTests;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test throughput quota coprocessor workflow.
 */
@Category(LargeTests.class)
public class TestThroughputController {
  private static final Log LOG = LogFactory.getLog(TestThroughputController.class);
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static String TEST_TABLE_STR = "throughput_quota_test";
  private static byte[] TEST_TABLE = Bytes.toBytes(TEST_TABLE_STR);
  private static final byte[] TEST_FAMILY = Bytes.toBytes("cf");

  private static final byte[] row = Bytes.toBytes("row");
  private static final byte[] qualifier = Bytes.toBytes("qualifier");
  private static final byte[] value = Bytes.toBytes("value");

  @BeforeClass
  public static void beforeClass() throws Exception {
    // setup configuration
    Configuration conf = TEST_UTIL.getConfiguration();
    ThroughputQuotaTestUtil.enableCoprocessor(conf);

    // start minicluster
    TEST_UTIL.startMiniCluster();

    HBaseAdmin admin = TEST_UTIL.getHBaseAdmin();
    HTableDescriptor htd = new HTableDescriptor(TEST_TABLE);
    htd.addFamily(new HColumnDescriptor(TEST_FAMILY));
    admin.createTable(htd);

    // Wait for tables to become available
    TEST_UTIL.waitTableAvailable(ThroughputQuotaTable.THROUGHPUT_QUOTA_TABLE_NAME, 5000);
    TEST_UTIL.waitTableAvailable(TEST_TABLE, 5000);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  void setTestTableQuota(Double readQuota, Double writeQuota) throws IOException {
    // Use the current user
    String userName = User.getCurrent().getName();
    Map<String, EnumMap<RequestType, Double>> userQuota =
        new HashMap<String, EnumMap<RequestType, Double>>();
    EnumMap<RequestType, Double> reqQuota = new EnumMap<RequestType, Double>(RequestType.class);
    reqQuota.put(RequestType.READ, readQuota);
    reqQuota.put(RequestType.WRITE, writeQuota);
    userQuota.put(userName, reqQuota);
    ThroughputQuota quota = new ThroughputQuota(TEST_TABLE_STR, userQuota);
    ThroughputQuotaTable.saveThroughputQuota(TEST_UTIL.getConfiguration(), quota);
  }

  Tester getReadOpTester(final HTable ht, long testTimeMs) {
    return new Tester(testTimeMs) {
      @Override
      void op() throws IOException {
        Get get = new Get(row);
        get.addColumn(TEST_FAMILY, qualifier);
        ht.get(get);
      }
    };
  }

  Tester getWriteOpTester(final HTable ht, long testTimeMs) {
    return new Tester(testTimeMs) {
      @Override
      void op() throws IOException {
        Put put = new Put(row);
        put.add(TEST_FAMILY, qualifier, value);
        ht.put(put);
      }
    };
  }

  @Test
  public void testController() throws Exception {
    final double quota = 100.0;
    final long testTimeMs = 1000; // 1 sec
    final HTable ht = new HTable(TEST_UTIL.getConfiguration(), TEST_TABLE);

    // test without quota limits
    Tester tester0 = getReadOpTester(ht, testTimeMs);
    Tester tester1 = getWriteOpTester(ht, testTimeMs);

    tester0.start();
    tester1.start();
    tester0.join();
    tester1.join();

    // check there is no rejected requests
    assertTrue(tester0.getAllowed() > 0);
    assertEquals(0, tester0.getRejected());
    assertTrue(tester1.getAllowed() > 0);
    assertEquals(0, tester1.getRejected());

    // set write quota limits and test
    setTestTableQuota(null, quota);
    Thread.sleep(100);
    Tester tester2 = getWriteOpTester(ht, testTimeMs);
    Tester tester3 = getReadOpTester(ht, testTimeMs);

    tester2.start();
    tester3.start();
    tester2.join();
    tester3.join();

    // check there is rejected write requests
    assertTrue(tester2.getRejected() > 0);
    // check the allowed write requests number in 1 second is > 0 and < 2 * quota
    assertTrue(tester2.getAllowed() > 0 && tester2.getAllowed() < 2 * quota);
    // check there is no rejected read requests
    assertTrue(tester3.getAllowed() > 0);
    assertEquals(0, tester3.getRejected());

    // clear write quota limits, set read quota limits and test
    setTestTableQuota(quota, null);
    Thread.sleep(100);
    Tester tester4 = getWriteOpTester(ht, testTimeMs);
    Tester tester5 = getReadOpTester(ht, testTimeMs);

    tester4.start();
    tester5.start();
    tester4.join();
    tester5.join();

    // check there is no rejected write requests
    assertTrue(tester4.getAllowed() > 0);
    assertEquals(0, tester4.getRejected());
    // check there is rejected read requests
    assertTrue(tester5.getRejected() > 0);
    // check the allowed read requests number in 1 second is > 0 and < 2 * quota
    assertTrue(tester5.getAllowed() > 0 && tester5.getAllowed() < 2 * quota);

    // clear all quota limits and test
    setTestTableQuota(null, null);
    Thread.sleep(100);
    Tester tester6 = getWriteOpTester(ht, testTimeMs);
    Tester tester7 = getReadOpTester(ht, testTimeMs);

    tester6.start();
    tester7.start();
    tester6.join();
    tester7.join();

    // check there is no rejected requests
    assertTrue(tester6.getAllowed() > 0);
    assertEquals(0, tester6.getRejected());
    assertTrue(tester7.getAllowed() > 0);
    assertEquals(0, tester7.getRejected());

    ht.close();
  }

  static abstract class Tester extends Thread {
    long startTimeMs;
    long testTimeMs;

    long allowed;
    long rejected;

    public Tester(long testTimeMs) {
      this.testTimeMs = testTimeMs;
    }

    public long getAllowed() {
      return allowed;
    }

    public long getRejected() {
      return rejected;
    }

    @Override
    public void run() {
      allowed = 0;
      rejected = 0;

      long stopTimeMs = System.currentTimeMillis() + this.testTimeMs;

      while (System.currentTimeMillis() < stopTimeMs) {
        try {
          op();
          if (allowed != Long.MAX_VALUE) {
            ++allowed;
          }
        } catch (IOException e) {
          if (e.toString().contains("ThroughputExceededException")) {
            if (rejected != Long.MAX_VALUE) {
              ++rejected;
            }
          } else {
            LOG.error("operation failed", e);
          }
        }
      }
    }

    abstract void op() throws IOException;
  }
}
