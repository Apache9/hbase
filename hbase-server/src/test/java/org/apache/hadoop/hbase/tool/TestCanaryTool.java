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
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
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

  @Rule
  public TestName name = new TestName();

  @Before
  public void setUp() throws Exception {
    testingUtility = new HBaseTestingUtility();
    testingUtility.startMiniCluster();
  }

  @After
  public void tearDown() throws Exception {
    testingUtility.shutdownMiniCluster();
  }

  @Test
  public void testBasicCanaryWorks() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    HTable table = testingUtility.createTable(tableName, new byte[][] { FAMILY });
    // insert some test rows
    for (int i = 0; i < 1000; i++) {
      byte[] iBytes = Bytes.toBytes(i);
      Put p = new Put(iBytes);
      p.add(FAMILY, COLUMN, iBytes);
      table.put(p);
    }
    Canary.StdOutSink sink = spy(new Canary.StdOutSink());
    Canary canary = new Canary(sink);
    String[] args = { name.getMethodName() };
    assertEquals(0, ToolRunner.run(testingUtility.getConfiguration(), canary, args));
    verify(sink, atLeastOnce()).publishReadTiming(isA(HRegionInfo.class),
      isA(HColumnDescriptor.class), anyLong());
    verify(sink, never()).publishReadFailure(isA(HRegionInfo.class), isA(Throwable.class));
    verify(sink, never()).publishReadFailure(isA(HRegionInfo.class), isA(HColumnDescriptor.class),
      isA(Throwable.class));
  }
}
