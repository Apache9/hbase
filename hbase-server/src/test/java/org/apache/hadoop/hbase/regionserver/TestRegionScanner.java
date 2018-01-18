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
package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.regionserver.ScannerContext.LimitScope;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestRegionScanner {

  private static final Log LOG = LogFactory.getLog(TestRegionScanner.class);

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static final TableName TABLE = TableName.valueOf("testTable");

  private static final byte[] FAMILY = Bytes.toBytes("testFamily");

  private static final byte[] COLUMN = Bytes.toBytes("testColumn");

  private static HBaseAdmin admin;

  private final long TIME_LIMIT = 1000;

  private final long ROWS_NUM = 10;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(1);
    admin = TEST_UTIL.getHBaseAdmin();
  }

  @Before
  public void setupTable() throws IOException {
    HTableDescriptor desc = new HTableDescriptor(TABLE);
    desc.addFamily(new HColumnDescriptor(FAMILY));
    admin.createTable(desc);
    HTable table = new HTable(TEST_UTIL.getConfiguration(), TABLE);
    for (int i = 0; i < ROWS_NUM; i++) {
      Put put = new Put(Bytes.toBytes("row" + i));
      put.add(FAMILY, COLUMN, Bytes.toBytes("row" + i));
      table.put(put);
    }
    table.flushCommits();
    table.close();
  }

  @Test
  public void testTimeLimit() throws Exception {
    List<HRegionInfo> regions = admin.getTableRegions(TABLE);
    assertEquals(1, regions.size());

    HRegion region = TEST_UTIL.getRSForFirstRegionInTable(TABLE).getOnlineRegion(regions.get(0).getRegionName());
    assertNotNull(region);

    Scan scan = new Scan();
    // Only need the last row
    scan.setFilter(new SleepFirstPrefixFilter(Bytes.toBytes("row9")));

    long startTime = System.currentTimeMillis();
    RegionScanner scanner = region.getScanner(scan);
    ScannerContext.Builder contextBuilder = ScannerContext.newBuilder(true);
    contextBuilder.setTimeLimit(LimitScope.BETWEEN_CELLS, startTime + TIME_LIMIT);
    ScannerContext scannerContext = contextBuilder.build();
    List<Cell> values = new ArrayList<Cell>();
    while (scanner.next(values, scannerContext)) {
      long currentTime = System.currentTimeMillis();
      assertTrue((currentTime - startTime) < 2 * TIME_LIMIT);
      startTime = currentTime;
      contextBuilder.setTimeLimit(LimitScope.BETWEEN_CELLS, startTime + TIME_LIMIT);
      scannerContext = contextBuilder.build();
    }
    long currentTime = System.currentTimeMillis();
    assertTrue((currentTime - startTime) < 2 * TIME_LIMIT);
  }

  private class SleepFirstPrefixFilter extends PrefixFilter {

    public SleepFirstPrefixFilter(byte[] prefix) {
      super(prefix);
    }

    @Override
    public ReturnCode filterKeyValue(Cell v) {
      try {
        Thread.sleep(TIME_LIMIT / 2);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      return super.filterKeyValue(v);
    }
  }
}
