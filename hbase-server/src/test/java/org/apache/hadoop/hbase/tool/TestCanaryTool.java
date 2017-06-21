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

package org.apache.hadoop.hbase.tool;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.ToolRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Category({ MediumTests.class })
public class TestCanaryTool {

  private HBaseTestingUtility testingUtility;
  private static final byte[] FAMILY = Bytes.toBytes("f");
  private static final byte[] COLUMN = Bytes.toBytes("col");
  private static byte[][] SPLIT_KEYS;

  @Rule
  public TestName name = new TestName();

  @Before
  public void setUp() throws Exception {
    testingUtility = new HBaseTestingUtility();
    testingUtility.startMiniCluster();
    SPLIT_KEYS = new byte[99][];
    for (int i = 1; i < 100; i++) {
      SPLIT_KEYS[i - 1] = Bytes.toBytes(String.format("%02d", i));
    }
    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(name.getMethodName()));
    htd.addFamily(new HColumnDescriptor(FAMILY));
    testingUtility.getHBaseAdmin().createTable(htd, SPLIT_KEYS);
    try (HConnection conn = HConnectionManager.createConnection(testingUtility.getConfiguration());
        HTableInterface table = conn.getTable(htd.getTableName())) {
      for (int i = 0; i < 1000; i++) {
        byte[] row = Bytes.toBytes(String.format("%03d", i));
        Put p = new Put(row);
        p.add(FAMILY, COLUMN, row);
        table.put(p);
      }
    }
    testingUtility.getConfiguration().setInt("hbase.canary.concurrency.max", 10);
  }

  @After
  public void tearDown() throws Exception {
    testingUtility.shutdownMiniCluster();
  }

  @Test
  public void testBasicCanaryWorks() throws Exception {
    Canary.StdOutSink sink = spy(new Canary.StdOutSink());
    Canary canary = new Canary(sink);
    String[] args = { name.getMethodName() };
    assertEquals(0, ToolRunner.run(testingUtility.getConfiguration(), canary, args));
    verify(sink, atLeast(SPLIT_KEYS.length + 1)).publishReadTiming(isA(HRegionInfo.class),
      isA(HColumnDescriptor.class), anyLong());
    verify(sink, never()).publishReadFailure(isA(HRegionInfo.class), isA(Throwable.class));
    verify(sink, never()).publishReadFailure(isA(HRegionInfo.class), isA(HColumnDescriptor.class),
      isA(Throwable.class));
  }
}
