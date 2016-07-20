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

import java.text.SimpleDateFormat;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.ManualEnvironmentEdge;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestAccessCounter {
  private static final Log LOG = LogFactory.getLog(TestAccessCounter.class);
  private static ManualEnvironmentEdge envEdge;
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static MiniHBaseCluster cluster;
  private static AccessCounter counter;
  
  private final static byte[] tableName = Bytes.toBytes("testTable");
  private final static byte[] FAMILY = Bytes.toBytes("cf");
  private final static byte[][] QUALIFIER = new byte[][] { Bytes.toBytes("q1"),
      Bytes.toBytes("q2"), Bytes.toBytes("q3") };
  
  private static HTable accountTable;
  
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    cluster = TEST_UTIL.startMiniCluster(1);
    
    accountTable = TEST_UTIL.createTable(AccessCounter.ACCOUNT_TABLE_NAME, new byte[][] {
        AccessCounter.READ_FAMILY, AccessCounter.WRITE_FAMILY });
    TEST_UTIL.waitTableAvailable(AccessCounter.ACCOUNT_TABLE_NAME, 1000);
    
    counter = cluster.getRegionServer(0).getAccessCounter();
    counter.chore();
    assertTrue(counter.isCounterEnabled());
    
    envEdge = new ManualEnvironmentEdge();
    envEdge.setValue(EnvironmentEdgeManager.currentTimeMillis());
    EnvironmentEdgeManager.injectEdge(envEdge);
    counter.setTimestamp(counter.normalizeTimestamp());
  }
  
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    EnvironmentEdgeManager.reset();
    accountTable.close();
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testIncrmentReadCount() throws Exception {
    int[] readNum = new int[QUALIFIER.length];
    for (int i = 0; i < QUALIFIER.length; i++) {
      readNum[i] = (int)(Math.random() * 1000) + 1;
      for (int j = 0; j < readNum[i]; j++) {
        counter.incrementReadCount(User.getCurrent(), tableName, FAMILY, QUALIFIER[i]);
      }
    }
    // flush counter map to _access_account_ table
    counter.chore();
    String key = User.getCurrent().getShortName() + AccessCounter.KEY_DELIMITER
        + Bytes.toString(tableName) + AccessCounter.KEY_DELIMITER
        + new SimpleDateFormat("yyyy-MM-dd,HH:mm:ss").format(counter.normalizeTimestamp());
    for (int i = 0; i < QUALIFIER.length; i++) {
      Get get = new Get(Bytes.toBytes(key));
      get.addColumn(AccessCounter.READ_FAMILY,
        Bytes.add(FAMILY, AccessCounter.COLUMN_SEPARATOR, QUALIFIER[i]));
      Result result = accountTable.get(get);
      assertEquals(readNum[i], Bytes.toLong(result.getValue(AccessCounter.READ_FAMILY,
        Bytes.add(FAMILY, AccessCounter.COLUMN_SEPARATOR, QUALIFIER[i]))));
    }
  }
  
  @Test
  public void testIncrementWriteCount() throws Exception {
    int[] readNum = new int[QUALIFIER.length];
    for (int i = 0; i < QUALIFIER.length; i++) {
      readNum[i] = (int)(Math.random() * 1000) + 1;
      for (int j = 0; j < readNum[i]; j++) {
        counter.incrementWriteCount(User.getCurrent(), tableName, FAMILY, QUALIFIER[i]);
      }
    }
    // flush counter map to _access_account_ table
    counter.chore();
    String key = User.getCurrent().getShortName() + AccessCounter.KEY_DELIMITER
        + Bytes.toString(tableName) + AccessCounter.KEY_DELIMITER
        + new SimpleDateFormat("yyyy-MM-dd,HH:mm:ss").format(counter.normalizeTimestamp());
    for (int i = 0; i < QUALIFIER.length; i++) {
      Get get = new Get(Bytes.toBytes(key));
      get.addColumn(AccessCounter.WRITE_FAMILY,
        Bytes.add(FAMILY, AccessCounter.COLUMN_SEPARATOR, QUALIFIER[i]));
      Result result = accountTable.get(get);
      assertEquals(readNum[i], Bytes.toLong(result.getValue(AccessCounter.WRITE_FAMILY,
        Bytes.add(FAMILY, AccessCounter.COLUMN_SEPARATOR, QUALIFIER[i]))));
    }
  }
}
