/**
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
package org.apache.hadoop.hbase.client;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.ipc.CallTimeoutException;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(LargeTests.class)
public class TestClientTimeoutWhenLocatingRegion {

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static final TableName TABLE_NAME =
      TableName.valueOf(TestClientTimeoutWhenLocatingRegion.class.getSimpleName());

  private static final byte[] CF = Bytes.toBytes("cf");

  private static final byte[] CQ = Bytes.toBytes("cq");

  private static final byte[] ROW = Bytes.toBytes("row");

  private static final byte[] VALUE = Bytes.toBytes("value");

  private static HConnection CONN;

  public static final class SleepObserver extends BaseRegionObserver {

    @Override
    public RegionScanner postScannerOpen(ObserverContext<RegionCoprocessorEnvironment> e, Scan scan,
        RegionScanner s) throws IOException {
      if (s.getRegionInfo().isMetaTable()) {
        Threads.sleepWithoutInterrupt(1000);
      }
      return super.postScannerOpen(e, scan, s);
    }
  }

  @BeforeClass
  public static void setUp() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setStrings(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY, SleepObserver.class.getName());
    TEST_UTIL.startMiniCluster(1);
    TEST_UTIL.createTable(TABLE_NAME, CF);
    TEST_UTIL.waitTableAvailable(TABLE_NAME.getName());
    CONN = HConnectionManager.createConnection(TEST_UTIL.getConfiguration());
  }

  @AfterClass
  public static void tearDown() throws Exception {
    if (CONN != null) {
      CONN.close();
    }
    TEST_UTIL.shutdownMiniCluster();
  }

  private void test(HTable table) throws InterruptedException, IOException {
    int timeoutMs = 200;
    // set a small timeout so we will timeout when locating region.
    table.setOperationTimeout(timeoutMs);
    long startNs = System.nanoTime();
    try {
      table.put(new Put(ROW).add(CF, CQ, VALUE));
      fail("Should have timeout!");
    } catch (CallTimeoutException e) {
      // expected
    }
    long costMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNs);
    // assert that we do not spend too much time
    assertTrue(costMs < 2 * timeoutMs);

    // wait a while as the locating task is still running in background. It will put the location in
    // cache at last.
    Thread.sleep(2000);

    // now we have location in cache
    table.put(new Put(ROW).add(CF, CQ, VALUE));
    assertArrayEquals(VALUE, table.get(new Get(ROW)).getValue(CF, CQ));
  }

  @Test
  public void test() throws IOException, InterruptedException {
    HTable table = (HTable) CONN.getTable(TABLE_NAME);
    try {
      test(table);
    } finally {
      table.close();
    }
  }
}
