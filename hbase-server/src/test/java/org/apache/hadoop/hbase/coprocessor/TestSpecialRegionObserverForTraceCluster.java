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
package org.apache.hadoop.hbase.coprocessor;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(LargeTests.class)
public class TestSpecialRegionObserverForTraceCluster {
  private static final Log LOG = LogFactory.getLog(TestSpecialRegionObserverForTraceCluster.class);

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private final static Random RANDOM = new Random();
  private final static String HOST_NAME_HASH = "258";
  private final static String STAGE_ID = "123";

  private static TableName NORMAL_TABLE = TableName.valueOf("tsdb");
  private static TableName SPECIAL_TABLE = TableName.valueOf("tsdb_special");
  private static byte[] CF = Bytes.toBytes("cf");
  private static byte[] CQ = Bytes.toBytes("cq");
  private static byte[] VALUE = Bytes.toBytes("value");

  private static final byte[] NORMAL_ROW_1 = Bytes.toBytes("NORMAL_ROW_1");
  private static final byte[] NORMAL_ROW_2 = Bytes.toBytes("NORMAL_ROW_2");
  private static final byte[] NORMAL_ROW_3 = Bytes.toBytes("NORMAL_ROW_3");
  private static final byte[] EXPIRED_ROW_1 = Bytes.toBytes("EXPIRED_ROW_1");
  private static final byte[] EXPIRED_ROW_2 = Bytes.toBytes("EXPIRED_ROW_2");
  private static final byte[] EXPIRED_ROW_3 = Bytes.toBytes("EXPIRED_ROW_3");

  LocalDateTime now = LocalDateTime.now();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setInt("hbase.hstore.compactionThreshold", 1);
    TEST_UTIL.startMiniCluster(1);
    HTableDescriptor htd = new HTableDescriptor(NORMAL_TABLE);
    htd.addFamily(new HColumnDescriptor(CF).setTimeToLive(2592000));
    TEST_UTIL.createTable(htd, null);
    TEST_UTIL.waitTableAvailable(NORMAL_TABLE.getName());
    htd = new HTableDescriptor(SPECIAL_TABLE);
    htd.addFamily(new HColumnDescriptor(CF).setTimeToLive(2592000));
    htd.addCoprocessor(SpecialRegionObserverForTraceCluster.class.getName());
    TEST_UTIL.createTable(htd, null);
    TEST_UTIL.waitTableAvailable(SPECIAL_TABLE.getName());
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    try (HBaseAdmin admin = TEST_UTIL.getHBaseAdmin()) {
      admin.disableTable(NORMAL_TABLE);
      admin.deleteTable(NORMAL_TABLE);
      admin.disableTable(SPECIAL_TABLE);
      admin.deleteTable(SPECIAL_TABLE);
    }
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void test() throws Exception {
    loadData(NORMAL_TABLE);
    loadData(SPECIAL_TABLE);
    try (HTable table = new HTable(TEST_UTIL.getConfiguration(), NORMAL_TABLE)) {
      assertAllExists(table);
    }
    try (HTable table = new HTable(TEST_UTIL.getConfiguration(), SPECIAL_TABLE)) {
      assertAllExists(table);
    }

    TEST_UTIL.getHBaseAdmin().flush(NORMAL_TABLE.getNameAsString());
    TEST_UTIL.getHBaseAdmin().flush(SPECIAL_TABLE.getNameAsString());
    Thread.sleep(10000); // Sleep 1secs wait for flush
    TEST_UTIL.getHBaseAdmin().compact(NORMAL_TABLE.getNameAsString());
    TEST_UTIL.getHBaseAdmin().compact(SPECIAL_TABLE.getNameAsString());
    Thread.sleep(10000); // Sleep 10secs wait for compact

    try (HTable table = new HTable(TEST_UTIL.getConfiguration(), NORMAL_TABLE)) {
      assertAllExists(table);
    }
    try (HTable table = new HTable(TEST_UTIL.getConfiguration(), SPECIAL_TABLE)) {
      assertExists(table, NORMAL_ROW_1);
      assertExists(table, NORMAL_ROW_2);
      assertExists(table, NORMAL_ROW_3);
      assertNotExists(table, EXPIRED_ROW_1);
      assertNotExists(table, EXPIRED_ROW_2);
      assertNotExists(table, EXPIRED_ROW_3);
    }
  }

  private void assertAllExists(HTable table) throws Exception {
    assertExists(table, NORMAL_ROW_1);
    assertExists(table, NORMAL_ROW_2);
    assertExists(table, NORMAL_ROW_3);
    assertExists(table, EXPIRED_ROW_1);
    assertExists(table, EXPIRED_ROW_2);
    assertExists(table, EXPIRED_ROW_3);
  }

  private void assertExists(HTable table, byte[] row) throws Exception {
    Result result = table.get(new Get(row));
    assertNotNull(result);
    assertFalse(result.isEmpty());
    assertNotNull(result.getValue(CF, CQ));
  }

  private void assertNotExists(HTable table, byte[] row) throws Exception {
    Result result = table.get(new Get(row));
    assertNotNull(result);
    assertTrue(result.isEmpty());
  }

  private void loadData(TableName tableName) throws Exception {
    try (HTable table = new HTable(TEST_UTIL.getConfiguration(), tableName)) {
      table.put(new Put(NORMAL_ROW_1).add(CF, CQ,
          toNewTraceTimestamp(now.minusHours(1).toInstant(ZoneOffset.of("+8")).getEpochSecond()),
          VALUE));
      table.put(new Put(NORMAL_ROW_2).add(CF, CQ,
          toNewTraceTimestamp(now.minusDays(1).toInstant(ZoneOffset.of("+8")).getEpochSecond()),
          VALUE));
      table.put(new Put(NORMAL_ROW_3).add(CF, CQ,
          toNewTraceTimestamp(now.minusMonths(1).toInstant(ZoneOffset.of("+8")).getEpochSecond()),
          VALUE));
      TEST_UTIL.getHBaseAdmin().flush(tableName.getNameAsString());
      Thread.sleep(10000); // Sleep 1secs wait for flush
      table.put(new Put(EXPIRED_ROW_1).add(CF, CQ,
          toOldTraceTimestamp(now.minusMonths(3).toInstant(ZoneOffset.of("+8")).getEpochSecond()),
          VALUE));
      table.put(new Put(EXPIRED_ROW_2).add(CF, CQ,
          toOldTraceTimestamp(now.minusMonths(6).toInstant(ZoneOffset.of("+8")).getEpochSecond()),
          VALUE));
      table.put(new Put(EXPIRED_ROW_3).add(CF, CQ,
          toOldTraceTimestamp(now.minusYears(1).toInstant(ZoneOffset.of("+8")).getEpochSecond()),
          VALUE));
    }
  }

  private long toOldTraceTimestamp(long epochSecond) {
    long ts = epochSecond * 1000L;
    return Long.valueOf(ts + HOST_NAME_HASH + String.format("%03d", RANDOM.nextInt(1000)));
  }

  private long toNewTraceTimestamp(long epochSecond) {
    long ts = epochSecond * 1000L;
    return Long.valueOf(String.valueOf(ts / 10000000L) + String.valueOf(ts / 300000L) + STAGE_ID);
  }
}
