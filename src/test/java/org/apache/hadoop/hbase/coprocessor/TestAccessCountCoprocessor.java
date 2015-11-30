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
package org.apache.hadoop.hbase.coprocessor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.text.SimpleDateFormat;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.AccessCounter;
import org.apache.hadoop.hbase.regionserver.TestAccessCounter;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.ManualEnvironmentEdge;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestAccessCountCoprocessor {
  private static final Log LOG = LogFactory.getLog(TestAccessCounter.class);
  private static ManualEnvironmentEdge envEdge;
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static MiniHBaseCluster cluster;
  private static AccessCounter counter;
  
  private final static byte[] tableName = Bytes.toBytes("testTable");
  private final static byte[] FAMILY = Bytes.toBytes("cf");
  private final static byte[][] QUALIFIER = new byte[][] { Bytes.toBytes("q1"),
      Bytes.toBytes("q2"), Bytes.toBytes("q3") };
  private final static int rowNum = 100;
  private final static int threadNum = 5;
  private int[] opNum = new int[QUALIFIER.length];
  
  private static HTable accountTable;
  private static HTable table;
  
  private static int period = 1000;
  
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setInt(AccessCounter.FLUSH_ACCESS_COUNTER_PERIOD, period);
    cluster = TEST_UTIL.startMiniCluster(1);
    
    accountTable = TEST_UTIL.createTable(AccessCounter.ACCOUNT_TABLE_NAME, new byte[][] {
        AccessCounter.READ_FAMILY, AccessCounter.WRITE_FAMILY });
    TEST_UTIL.waitTableAvailable(AccessCounter.ACCOUNT_TABLE_NAME, 1000);
    
    envEdge = new ManualEnvironmentEdge();
    envEdge.setValue(EnvironmentEdgeManager.currentTimeMillis());
    EnvironmentEdgeManager.injectEdge(envEdge);
    
    HTableDescriptor tableDesc = new HTableDescriptor(tableName);
    tableDesc.addFamily(new HColumnDescriptor(FAMILY));
    tableDesc.addCoprocessor(AccessCountCoprocessor.class.getName());
    TEST_UTIL.getHBaseAdmin().createTable(tableDesc);
    TEST_UTIL.waitTableAvailable(tableName, 1000);
    
    table = new HTable(TEST_UTIL.getConfiguration(), tableName);
    table.setAutoFlush(true);
    for (int i = 0; i < QUALIFIER.length; i++) {
      for (int j = 0; j < rowNum; j++) {
        Put put = new Put(Bytes.toBytes("row-" + j));
        put.add(FAMILY, QUALIFIER[i], Bytes.toBytes("data-" + i));
        table.put(put);
      }
    }

    Thread.sleep(period);
    counter = cluster.getRegionServer(0).getAccessCounter();
    assertTrue(counter.isCounterEnabled());
  }
  
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    EnvironmentEdgeManager.reset();
    accountTable.close();
    TEST_UTIL.deleteTable(AccessCounter.ACCOUNT_TABLE_NAME);
    TEST_UTIL.shutdownMiniCluster();
  }
  
  @Test
  public void testPostGet() throws Exception {
    envEdge.incValue(2 * period);
    counter.setTimestamp(EnvironmentEdgeManager.currentTimeMillis());
    for (int i = 0; i < QUALIFIER.length; i++) {
      opNum[i] = (int) (Math.random() * rowNum) + 1;
    }
    Thread[] threads = new Thread[threadNum];
    for (int k = 0; k < threadNum; k++) {
      threads[k] = new Thread() {
        @Override
        public void run() {
          for (int i = 0; i < QUALIFIER.length; i++) {
            for (int j = 0; j < opNum[i]; j++) {
              Get get = new Get(Bytes.toBytes("row-" + j));
              get.addColumn(FAMILY, QUALIFIER[i]);
              try {
                table.get(get);
              } catch (IOException e) {
              }
            }
          }
        }
      };
      threads[k].start();
    }
    for (int k = 0; k < threadNum; k++) {
      threads[k].join();
    }
    Thread.sleep(period);

    for (int i = 0; i < QUALIFIER.length; i++) {
      assertValue(AccessCounter.READ_FAMILY,
        Bytes.add(FAMILY, AccessCounter.COLUMN_SEPARATOR, QUALIFIER[i]), threadNum * opNum[i]);
    }
  }
  
  @Test
  public void testPostScan() throws Exception {
    envEdge.incValue(2 * period);
    counter.setTimestamp(EnvironmentEdgeManager.currentTimeMillis());
    Thread[] threads = new Thread[threadNum];
    for (int k = 0; k < threadNum; k++) {
      threads[k] = new Thread() {
        @Override
        public void run() {
          Scan scan = new Scan();
          ResultScanner scanner;
          try {
            scanner = table.getScanner(scan);
            while (scanner.next() != null) {
              ;
            }
          } catch (IOException e) {
          }
        }
      };
      threads[k].start();
    }
    for (int k = 0; k < threadNum; k++) {
      threads[k].join();
    }
    Thread.sleep(period);
    
    for (int i = 0; i < QUALIFIER.length; i++) {
      assertValue(AccessCounter.READ_FAMILY,
        Bytes.add(FAMILY, AccessCounter.COLUMN_SEPARATOR, QUALIFIER[i]), rowNum * threadNum);
    }
  }

  @Test
  public void testPostPut() throws Exception {
    envEdge.incValue(2 * period);
    counter.setTimestamp(EnvironmentEdgeManager.currentTimeMillis());
    for (int i = 0; i < QUALIFIER.length; i++) {
      opNum[i] = (int) (Math.random() * rowNum) + 1;
    }
    for (int i = 0; i < QUALIFIER.length; i++) {
      for (int j = rowNum; j < (rowNum + opNum[i]); j++) {
        Put put = new Put(Bytes.toBytes("row-" + j));
        put.add(FAMILY, QUALIFIER[i], Bytes.toBytes("data-" + j));
        try {
          table.put(put);
        } catch (IOException e) {
        }
      }
    }
    Thread.sleep(period);

    for (int i = 0; i < QUALIFIER.length; i++) {
      assertValue(AccessCounter.WRITE_FAMILY,
        Bytes.add(FAMILY, AccessCounter.COLUMN_SEPARATOR, QUALIFIER[i]), opNum[i]);
    }
  }
  
  public void assertValue(byte[] family, byte[] qualifier, int exceptedNum) throws Exception {
    // use ManualEnvironmentEdge to control timestamp
    String key = User.getCurrent().getShortName()
        + AccessCounter.KEY_DELIMITER
        + Bytes.toString(tableName)
        + AccessCounter.KEY_DELIMITER
        + new SimpleDateFormat("yyyy-MM-dd,HH:mm:ss").format(EnvironmentEdgeManager
            .currentTimeMillis());
    Get get = new Get(Bytes.toBytes(key));
    get.addColumn(family, qualifier);
    Result result = accountTable.get(get);
    assertEquals(exceptedNum, Bytes.toLong(result.getValue(family, qualifier)));
  }
  
}
