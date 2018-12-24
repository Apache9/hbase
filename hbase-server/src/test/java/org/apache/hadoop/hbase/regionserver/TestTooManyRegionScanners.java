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
package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test limit of opened region scanners
 */
@Category(SmallTests.class)
public class TestTooManyRegionScanners {

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final int LIMITED_OPENED_REGION_SCANNERS = 10;

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestTooManyRegionScanners.class);

  @BeforeClass
  public static void beforeClass() throws Exception {
    //set limit to 10
    TEST_UTIL.getConfiguration().setInt("hbase.regionserver.maximum.opened.region.scanner.limit",
        LIMITED_OPENED_REGION_SCANNERS);
    TEST_UTIL.getConfiguration().setInt("hbase.client.scanner.caching", 2);
    TEST_UTIL.getConfiguration().setInt("hbase.client.retries.number", 1);
    TEST_UTIL.getConfiguration().setInt("hbase.rpc.timeout", 1000);
    TEST_UTIL.startMiniCluster();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testLimitedOpenedRegionScanners() throws Exception {
    //create table
    byte[] cf = Bytes.toBytes("cf");
    byte[] tableName = Bytes.toBytes(this.getClass().getSimpleName());
    HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));
    HColumnDescriptor hcd = new HColumnDescriptor(cf);
    desc.addFamily(hcd);
    Table table = TEST_UTIL.createTable(desc, null);

    //put 10 rows
    for (int i = 0; i < 100; i++) {
      byte[] rowkey = Bytes.toBytes("row_" + i);
      Put put = new Put(rowkey);
      put.addColumn(cf, Bytes.toBytes("col"), Bytes.toBytes("val_" + i));
      table.put(put);
    }

    //open 11 result scanners
    int opened = 0;
    for (int i = 0; i < LIMITED_OPENED_REGION_SCANNERS + 1; i++) {
      try {
        Scan scan = new Scan();
        scan.addFamily(cf);
        scan.setMaxResultSize(2);
        ResultScanner result_scanner = table.getScanner(scan);
        result_scanner.next();
        opened++;
      } catch (Exception e) {
        assertEquals(i, LIMITED_OPENED_REGION_SCANNERS);
        assertTrue(e.getMessage().contains("TooManyRegionScannersException"));
      }
    }
    assertEquals(opened, LIMITED_OPENED_REGION_SCANNERS);
  }

}
