/*
 * Copyright The Apache Software Foundation
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
package org.apache.hadoop.hbase.replication;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@Category(LargeTests.class)
public class TestSerialReplicationRegionMerge extends TestSerialReplicationBase {
  private static final Log LOG = LogFactory.getLog(TestSerialReplicationRegionMerge.class);

  @Test
  public void testRegionMerge() throws Exception {
    TableName tableName = TableName.valueOf("testRegionMerge");
    createTable(tableName);

    utility1.getHBaseAdmin().split(tableName.getName(), ROWS[50]);
    Thread.sleep(5000L);

    try (HTable t1 = new HTable(conf1, tableName); HTable t2 = new HTable(conf2, tableName)) {
      writeData(t1, 10, 100, 10, false);
      List<Pair<HRegionInfo, ServerName>> regions = utility1.getMiniHBaseCluster().getMaster()
          .getCatalogTracker().getTableRegionsAndLocations(tableName);
      assertEquals(2, regions.size());
      utility1.getHBaseAdmin().mergeRegions(regions.get(0).getFirst().getEncodedNameAsBytes(),
          regions.get(1).getFirst().getEncodedNameAsBytes(), true);
      writeData(t1, 11, 100, 10, false);

      long start = System.currentTimeMillis();
      while (System.currentTimeMillis() - start < TIMEOUT) {
        Scan scan = new Scan();
        scan.setCaching(100);
        List<Cell> list = new ArrayList<>();
        try (ResultScanner results = t2.getScanner(scan)) {
          for (Result result : results) {
            assertEquals(1, result.rawCells().length);
            list.add(result.rawCells()[0]);
          }
        }

        List<Integer> rows = getRowNumbers(list);
        List<Integer> rowsList0 = new ArrayList<Integer>();
        List<Integer> rowsList1 = new ArrayList<Integer>();
        for (int num : rows) {
          if (num % 10 == 0) {
            rowsList0.add(num);
          } else {
            rowsList1.add(num);
          }
        }

        LOG.info("Rows rowsList0 : " + Arrays.toString(rowsList0.toArray()));
        LOG.info("Rows rowsList1 : " + Arrays.toString(rowsList1.toArray()));
        assertListSerial(rowsList0, 10, 10);
        assertListSerial(rowsList1, 11, 10);
        if (!rowsList1.isEmpty()) {
          assertEquals(9, rowsList0.size());
        }

        if (rows.size() == 18) {
          return;
        }
        LOG.info("Waiting all logs pushed to slave. Expected 18 , actual " + rows.size());
        Thread.sleep(SLEEP_TIME);
      }
      fail("Wait too much for all logs been pushed");
    }
  }
}