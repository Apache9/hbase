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

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ LargeTests.class, ClientTests.class })
public class TestCreateTableIgnoreSplits {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestCreateTableIgnoreSplits.class);

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setBoolean(HConstants.IGNORE_SPLITS_WHEN_CREATE_TABLE, true);
    TEST_UTIL.startMiniCluster();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testCreateTableIgnoreSplits() throws Exception {
    TableName tableName = TableName.valueOf("testCreateTable_IgnoreSplits");
    TableDescriptor desc = TableDescriptorBuilder.newBuilder(tableName)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("C")).build())
        .build();
    // test normal table ignore splits
    byte[][] splits = new byte[][] { Bytes.toBytes("aa"), Bytes.toBytes("bb") };
    try (Table table = TEST_UTIL.createTable(desc, splits)) {
      assertEquals(new Bytes(Bytes.toBytes("true")),
        table.getDescriptor().getValue(TableDescriptorBuilder.IGNORE_SPLITS_WHEN_CREATING_KEY));
      assertEquals(1,
        TEST_UTIL.getConnection().getRegionLocator(tableName).getAllRegionLocations().size());
    }

    // test salted table ingore splits
    tableName = TableName.valueOf("testCreateSaltedTable_IgnoreSplits");
    desc = TableDescriptorBuilder.newBuilder(tableName)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("C")).build())
        .setSlotsCount(256).build();
    try (Table table = TEST_UTIL.createTable(desc, null)) {
      assertEquals(new Bytes(Bytes.toBytes("true")),
        table.getDescriptor().getValue(TableDescriptorBuilder.IGNORE_SPLITS_WHEN_CREATING_KEY));
      assertEquals(1,
        TEST_UTIL.getConnection().getRegionLocator(tableName).getAllRegionLocations().size());
      assertEquals(1, table.getDescriptor().getSlotsCount().intValue());
    }
  }
}
