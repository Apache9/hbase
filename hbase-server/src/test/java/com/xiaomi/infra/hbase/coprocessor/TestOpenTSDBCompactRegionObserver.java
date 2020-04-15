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
package com.xiaomi.infra.hbase.coprocessor;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@Category(LargeTests.class)
public class TestOpenTSDBCompactRegionObserver {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestOpenTSDBCompactRegionObserver.class);
  private static final Logger LOG = LoggerFactory.getLogger(TestOpenTSDBCompactRegionObserver.class);

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static TableName TSDB = TableName.valueOf("tsdb");
  private static Connection conn;
  private static byte[] CF = Bytes.toBytes("t");

  // rowkey 1581483600 two datapoint
  private static byte[] ROWKEY_1 = Bytes.fromHex("000000015e4386500000000100000001");
  private static byte[] CQ_1 = Bytes.fromHex("0003");
  private static byte[] VALUE_1 = Bytes.fromHex("5e438650");
  private static byte[] CQ_2 = Bytes.fromHex("0013");
  private static byte[] VALUE_2 = Bytes.fromHex("5e438651");

  // rowkey 1581487200 one datapoint
  private static byte[] ROWKEY_2 = Bytes.fromHex("000000015e4394600000000100000001");
  private static byte[] CQ_3 = Bytes.fromHex("0013");
  private static byte[] VALUE_3 = Bytes.fromHex("5e439461");

  long now = System.currentTimeMillis();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setInt("hbase.hstore.compactionThreshold", 1);
    TEST_UTIL.startMiniCluster(1);
    HTableDescriptor htd = new HTableDescriptor(TSDB);
    htd.addFamily(new HColumnDescriptor(CF));
    htd.addCoprocessor(OpenTSDBCompactRegionObserver.class.getName());
    TEST_UTIL.createTable(htd, null);
    TEST_UTIL.waitTableAvailable(TSDB);
    conn = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration());
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    try (Admin admin = TEST_UTIL.getAdmin()) {
      admin.disableTable(TSDB);
      admin.deleteTable(TSDB);
    }
    TEST_UTIL.shutdownMiniCluster();
    conn.close();
  }

  @Test
  public void test() throws Exception {
    loadData(TSDB);
    try (Table table = conn.getTable(TSDB);) {
      assertAllExists(table);
      Result result1 = table.get(new Get(ROWKEY_1));
      LOG.info("rowkey1 has 2 cells "+result1.size());
      Result result2 = table.get(new Get(ROWKEY_2));
      LOG.info("rowkey2 has 1 cells "+result2.size());
    }

    //TEST_UTIL.getHBaseAdmin().flush(TSDB);
    TEST_UTIL.getAdmin().flush(TSDB);
    Thread.sleep(10000); // Sleep 1secs wait for flush
    //TEST_UTIL.getHBaseAdmin().compact(TSDB);
    TEST_UTIL.getAdmin().majorCompact(TSDB);
    Thread.sleep(10000); // Sleep 10secs wait for compact

    try (Table table = conn.getTable(TSDB)) {
      assertAllExists(table);
      // rowkey1 and rowkey2 should have only one cell
      Result result1 = table.get(new Get(ROWKEY_1));
      assertTrue("Compacted only one cell", (result1.size() == 1));
      Result result2 = table.get(new Get(ROWKEY_2));
      assertTrue("Compacted only one cell", (result2.size() == 1));
    }
  }

  private void assertAllExists(Table table) throws Exception {
    assertExists(table, ROWKEY_1);
    assertExists(table, ROWKEY_2);
  }

  private void assertExists(Table table, byte[] row) throws Exception {
    Result result = table.get(new Get(row));
    assertNotNull(result);
    assertFalse(result.isEmpty());
  }

  private void loadData(TableName tableName) throws Exception {
    try (Table table = conn.getTable(tableName)) {
      table.put(new Put(ROWKEY_1).addColumn(CF, CQ_1, now, VALUE_1));
      //TEST_UTIL.getHBaseAdmin().flush(tableName);
      TEST_UTIL.getAdmin().flush(tableName);
      Thread.sleep(10000); // Sleep 1secs wait for flush
      table.put(new Put(ROWKEY_1).addColumn(CF, CQ_2, now, VALUE_2));
      // one datapoint
      table.put(new Put(ROWKEY_2).addColumn(CF, CQ_3, now, VALUE_3));
    }
  }
}
