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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.catalog.MetaEditor;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(LargeTests.class)
public class TestSerialReplicationBasic extends TestSerialReplicationBase {
  private static final Log LOG = LogFactory.getLog(TestSerialReplicationBasic.class);

  @Test
  public void testRegionMoveAndFailover() throws Exception {
    TableName tableName = TableName.valueOf("testRegionMoveAndFailover");
    createTable(tableName);

    try (HTable t1 = new HTable(conf1, tableName); HTable t2 = new HTable(conf2, tableName)) {
      moveRegion(tableName, 1);
      moveRegion(tableName, 0);
      writeData(t1, 10, 20,1, false);
      moveRegion(tableName, 2);
      writeData(t1, 20, 30, 1, false);

      utility1.getHBaseCluster().abortRegionServer(2);
      Thread.sleep(SLEEP_TIME);
      utility1.waitTableAvailable(tableName.getName());
      writeData(t1, 30, 40, 1, false);

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
        LOG.info("Rows=" + Arrays.toString(rows.toArray()));
        assertListSerial(rows, 10, 1);

        if (rows.size() == 30) {
          return;
        }
        LOG.info("Waiting all logs pushed to slave. Expected 30 , actual " + list.size());
        Thread.sleep(SLEEP_TIME);
      }
      fail("Wait too much for all logs been pushed");
    }
  }

  @Test
  public void testUpdateReplicationPositions() throws Exception {
    HConnection connection = HConnectionManager.createConnection(utility1.getConfiguration());
    Map<String, Long> positions = new HashMap<>();
    positions.put("TestRegion1", 10L);
    positions.put("TestRegion2", 20L);
    MetaEditor.updateReplicationPositions(connection, PEER_ID, positions);
    assertEquals(10, MetaEditor
        .getReplicationPositionForOnePeer(connection, Bytes.toBytes("TestRegion1"), PEER_ID));
    assertEquals(20, MetaEditor
        .getReplicationPositionForOnePeer(connection, Bytes.toBytes("TestRegion2"), PEER_ID));

    positions.clear();
    positions.put("TestRegion1", 100L);
    positions.put("TestRegion2", 200L);
    MetaEditor.updateReplicationPositions(connection, PEER_ID, positions);
    assertEquals(100, MetaEditor
        .getReplicationPositionForOnePeer(connection, Bytes.toBytes("TestRegion1"), PEER_ID));
    assertEquals(200, MetaEditor
        .getReplicationPositionForOnePeer(connection, Bytes.toBytes("TestRegion2"), PEER_ID));

    positions.clear();
    positions.put("TestRegion1", 30L);
    positions.put("TestRegion2", 60L);
    MetaEditor.updateReplicationPositions(connection, PEER_ID, positions);
    assertEquals(100, MetaEditor
        .getReplicationPositionForOnePeer(connection, Bytes.toBytes("TestRegion1"), PEER_ID));
    assertEquals(200, MetaEditor
        .getReplicationPositionForOnePeer(connection, Bytes.toBytes("TestRegion2"), PEER_ID));
  }

}