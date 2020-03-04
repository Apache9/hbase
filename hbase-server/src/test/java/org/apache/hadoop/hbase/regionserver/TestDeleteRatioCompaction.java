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

package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
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

@Category(LargeTests.class)
public class TestDeleteRatioCompaction {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestDeleteRatioCompaction.class);
  private static final Logger LOG = LoggerFactory.getLogger(TestDeleteRatioCompaction.class);
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private volatile boolean stop = false;
  private volatile boolean exception = false;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setBoolean(HConstants.DELETE_RATIO_COMPACTION_ENABLE, true);
    conf.setDouble(HConstants.DELETE_RATIO_THRESHOLD_KEY, 0.3);
    conf.setLong(HConstants.THREAD_WAKE_FREQUENCY, 100);
    conf.setLong(HStore.COMPACTCHECKER_INTERVAL_MULTIPLIER_KEY, 5);
    TEST_UTIL.startMiniCluster();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * Load data to a table, flush it to disk, compaction checker run major compaction and wait till
   * it is done.
   */
  @Test
  public void testCompaction() throws Exception {
    compaction("testCompaction", false);
  }

  @Test
  public void testCompactionWithZeroRowCount() throws Exception {
    compaction("testCompactionWithZeroRowCount", true);
  }

  public void compaction(String tableNameStr, boolean onlyContainsDeleteKV) throws Exception {
    // Create table
    TableName tableName = TableName.valueOf(tableNameStr);
    byte[] family = Bytes.toBytes("family");
    byte[][] families =
        { family, Bytes.add(family, Bytes.toBytes("2")), Bytes.add(family, Bytes.toBytes("3")) };
    try (Table table = TEST_UTIL.createTable(tableName, families)) {
      // disable compaction
      TEST_UTIL.getAdmin().compactionSwitch(false, new ArrayList<>(0));
      // load some data
      int rows = 100;
      int flushes = 8;
      if (!onlyContainsDeleteKV) {
        loadData(table, families, rows, flushes);
      }
      deleteData(table, families, rows / 2, flushes);
      // start a thread to read data
      new Thread(() -> getData(table, families, rows, flushes)).start();
      // check data is loaded
      HRegionServer rs = TEST_UTIL.getMiniHBaseCluster().getRegionServer(0);
      List<HRegion> regions = rs.getRegions(tableName);
      int countBefore = countStoreFilesInFamilies(regions, families);
      assertTrue(countBefore > 0);
      // compaction checker will run major compaction
      TEST_UTIL.getAdmin().compactionSwitch(true, new ArrayList<>(0));
      TEST_UTIL.waitFor(50000, () -> {
        int countAfter = countStoreFilesInFamilies(regions, families);
        return families.length == countAfter;
      });
      // check file count after compaction
      int countAfter = countStoreFilesInFamilies(regions, families);
      assertTrue(countAfter < countBefore);
      assertTrue(families.length == countAfter);
      assertFalse(exception);
    } finally {
      stop = true;
      Thread.sleep(200);
    }
  }

  private int countStoreFilesInFamilies(List<HRegion> regions, final byte[][] families) {
    int count = 0;
    for (HRegion region : regions) {
      count += region.getStoreFileList(families).size();
    }
    return count;
  }

  private void loadData(final Table table, final byte[][] families, final int rows,
      final int flushes) throws IOException {
    List<Put> puts = new ArrayList<>(rows);
    byte[] qualifier = Bytes.toBytes("val");
    // total row count is rows * flushes, total kv count is rows * flushes * families
    for (int i = 0; i < flushes; i++) {
      for (int k = 0; k < rows; k++) {
        byte[] row = Bytes.toBytes(i * rows + k);
        Put put = new Put(row);
        for (int j = 0; j < families.length; ++j) {
          put.addColumn(families[j], qualifier, row);
        }
        puts.add(put);
      }
      table.put(puts);
      TEST_UTIL.getAdmin().flush(table.getName());
      TEST_UTIL.flush();
      puts.clear();
    }
  }

  private void deleteData(final Table table, final byte[][] families, final int rows,
      final int flushes) throws IOException {
    List<Delete> deletes = new ArrayList<>(rows);
    byte[] qualifier = Bytes.toBytes("val");
    for (int i = 0; i < flushes; i++) {
      for (int k = 0; k < rows; k++) {
        byte[] row = Bytes.toBytes(i * rows + k);
        Delete d = new Delete(row);
        for (int j = 0; j < families.length; ++j) {
          d.addColumn(families[j], qualifier);
        }
        deletes.add(d);
      }
      table.delete(deletes);
      TEST_UTIL.getAdmin().flush(table.getName());
      TEST_UTIL.flush();
      deletes.clear();
    }
  }

  private void getData(final Table table, final byte[][] families, final int rows,
      final int flushes) {
    byte[] qualifier = Bytes.toBytes("val");
    for (int i = 0; i < flushes; i++) {
      if (stop) break;
      for (int k = 0; k < rows; k++) {
        if (stop) break;
        byte[] row = Bytes.toBytes(i * rows + k);
        Get get = new Get(row);
        for (int j = 0; j < families.length; ++j) {
          get.addColumn(families[j], qualifier);
          try {
            if (stop) break;
            table.get(get);
          } catch (Throwable e) {
            LOG.error("error when get data: ", e);
            if (e != null && e.toString().contains(NullPointerException.class.getName())) {
              exception = true;
            }
          }
        }
      }
    }
  }
}
