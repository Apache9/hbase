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
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@Category(LargeTests.class)
public class TestSerialReplicationRegionSplit extends TestSerialReplicationBase {
  private static final Log LOG = LogFactory.getLog(TestSerialReplicationRegionSplit.class);

  @Test
  public void testRegionSplit() throws Exception {
    TableName tableName = TableName.valueOf("testRegionSplit");
    createTable(tableName);

    try (HTable t1 = new HTable(conf1, tableName); HTable t2 = new HTable(conf2, tableName)) {
      writeData(t1, 10, 100, 10, false);
      utility1.getHBaseAdmin().split(tableName.getName(), ROWS[50]);
      Thread.sleep(5000L);
      writeData(t1, 11, 100, 10, false);
      balanceTwoRegions(t1);
      writeData(t1, 12, 100, 10, false);

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
        List<Integer> rowsList21 = new ArrayList<Integer>();
        List<Integer> rowsList22 = new ArrayList<Integer>();
        for (int num : rows) {
          if (num % 10 == 0) {
            rowsList0.add(num);
          } else if (num % 10 == 1) {
            rowsList1.add(num);
          } else if (num < 50) { //num%10==2
            rowsList21.add(num);
          } else { // num%10==1&&num>50
            rowsList22.add(num);
          }
        }

        LOG.info("Rows rowsList0 : " + Arrays.toString(rowsList0.toArray()));
        LOG.info("Rows rowsList1 : " + Arrays.toString(rowsList1.toArray()));
        LOG.info("Rows rowsList21 : " + Arrays.toString(rowsList21.toArray()));
        LOG.info("Rows rowsList22 : " + Arrays.toString(rowsList22.toArray()));
        assertListSerial(rowsList0, 10, 10);
        assertListSerial(rowsList1, 11, 10);
        assertListSerial(rowsList21, 12, 10);
        assertListSerial(rowsList22, 52, 10);
        if (!rowsList1.isEmpty()) {
          assertEquals(9, rowsList0.size());
        }
        if (!rowsList21.isEmpty() || !rowsList22.isEmpty()) {
          assertEquals(9, rowsList1.size());
        }

        if (list.size() == 27) {
          return;
        }
        LOG.info("Waiting all logs pushed to slave. Expected 27 , actual " + list.size());
        Thread.sleep(SLEEP_TIME);
      }
      fail("Wait too much for all logs been pushed");
    }
  }

  private void balanceTwoRegions(HTable table) throws Exception {
    List<Pair<HRegionInfo, ServerName>> regions =
        utility1.getMiniHBaseCluster().getMaster().getCatalogTracker()
            .getTableRegionsAndLocations(table.getName());
    assertEquals(2, regions.size());
    HRegionInfo regionInfo1 = regions.get(0).getFirst();
    ServerName name1 = utility1.getHBaseCluster().getRegionServer(0).getServerName();
    HRegionInfo regionInfo2 = regions.get(1).getFirst();
    ServerName name2 = utility1.getHBaseCluster().getRegionServer(1).getServerName();
    utility1.getHBaseAdmin()
        .move(regionInfo1.getEncodedNameAsBytes(), Bytes.toBytes(name1.getServerName()));
    Thread.sleep(5000L);
    utility1.getHBaseAdmin()
        .move(regionInfo2.getEncodedNameAsBytes(), Bytes.toBytes(name2.getServerName()));
    Thread.sleep(5000L);
  }
}
