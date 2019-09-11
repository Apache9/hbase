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
package org.apache.hadoop.hbase.client;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Category({ MediumTests.class })
public class TestSplitOrMergeStatus {

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static byte[] FAMILY = Bytes.toBytes("testFamily");

  @Rule
  public TestName name = new TestName();

  @Before
  public void setUp() throws Exception {
    TEST_UTIL.startMiniCluster(2);
  }

  @After
  public void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testMultiSwitches() throws IOException {
    try (HBaseAdmin admin = TEST_UTIL.getHBaseAdmin()) {
      assertTrue(admin.splitSwitch(false, false));
      assertTrue(admin.mergeSwitch(false, false));

      assertFalse(admin.isSplitEnabled());
      assertFalse(admin.isMergeEnabled());
    }
  }

  @Test
  public void testSplitSwitch() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    HTable t = TEST_UTIL.createTable(tableName, FAMILY);
    TEST_UTIL.loadTable(t, FAMILY, false);

    try (HBaseAdmin admin = TEST_UTIL.getHBaseAdmin()) {
      int originalCount = admin.getTableRegions(tableName).size();
      initSwitchStatus(admin);

      // split switch is off
      boolean result = admin.splitSwitch(false, false);
      assertTrue(result);
      admin.split(t.getName().getNameAsString());
      Thread.sleep(1000);
      int count = admin.getTableRegions(tableName).size();
      assertTrue(originalCount == count);

      // split switch is on
      result = admin.splitSwitch(true, false);
      assertFalse(result);
      admin.split(t.getName().getNameAsString());
      while ((count = admin.getTableRegions(tableName).size()) == originalCount) {
        Threads.sleep(1);
      }
      count = admin.getTableRegions(tableName).size();
      assertTrue(originalCount < count);
    }
  }

  @Test
  public void testMergeSwitch() throws Exception {
    final TableName tableName = TableName.valueOf(name.getMethodName());
    HTable t = TEST_UTIL.createMultiRegionTable(tableName, FAMILY, 6);
    TEST_UTIL.loadTable(t, FAMILY, false);

    try (HBaseAdmin admin = TEST_UTIL.getHBaseAdmin()) {
      int originalCount = admin.getTableRegions(tableName).size();

      // Merge switch is off so merge should NOT succeed.
      boolean result = admin.mergeSwitch(false, false);
      assertTrue(result);
      List<HRegionInfo> regions = admin.getTableRegions(t.getName());
      assertTrue(regions.size() > 1);
      admin.mergeRegions(regions.get(0).getEncodedNameAsBytes(),
        regions.get(1).getEncodedNameAsBytes(), true);
      Thread.sleep(1000);
      int count = admin.getTableRegions(tableName).size();
      assertTrue("newCount=" + count + ", oldCount=" + originalCount, originalCount == count);

      // merge switch is on
      result = admin.mergeSwitch(true, false);
      regions = admin.getTableRegions(t.getName());
      assertFalse(result);
      admin.mergeRegions(regions.get(0).getEncodedNameAsBytes(),
        regions.get(1).getEncodedNameAsBytes(), true);
      Thread.sleep(10000);
      count = admin.getTableRegions(tableName).size();
      assertTrue(count < originalCount);
    }
  }

  private void initSwitchStatus(HBaseAdmin admin) throws IOException {
    if (!admin.isSplitEnabled()) {
      admin.splitSwitch(true, false);
    }
    if (!admin.isMergeEnabled()) {
      admin.mergeSwitch(true, false);
    }
    assertTrue(admin.isSplitEnabled());
    assertTrue(admin.isMergeEnabled());
  }
}
