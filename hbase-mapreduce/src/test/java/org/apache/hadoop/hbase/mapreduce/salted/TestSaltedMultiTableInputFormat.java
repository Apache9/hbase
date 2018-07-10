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
package org.apache.hadoop.hbase.mapreduce.salted;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.mapreduce.MultiTableInputFormat;
import org.apache.hadoop.hbase.mapreduce.MultiTableInputFormatBase;
import org.apache.hadoop.hbase.mapreduce.TestMultiTableInputFormat;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.VerySlowMapReduceTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.experimental.categories.Category;

@Category({VerySlowMapReduceTests.class, LargeTests.class})
public class TestSaltedMultiTableInputFormat extends TestMultiTableInputFormat {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestSaltedMultiTableInputFormat.class);

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // switch TIF to log at DEBUG level
    TEST_UTIL.enableDebug(MultiTableInputFormat.class);
    TEST_UTIL.enableDebug(MultiTableInputFormatBase.class);

    // start mini hbase cluster
    TEST_UTIL.startMiniCluster(3);
    // create and fill table
    for (int i = 0; i < 3; i++) {
      if (i == 2) {
        // create and load salted table
        Table table = TEST_UTIL.createSaltedTable(
            TableName.valueOf(Bytes.toBytes(TABLE_NAME + i)), INPUT_FAMILY, 4);
        TEST_UTIL.loadTable(table, INPUT_FAMILY, false);
      } else {
        Table table = TEST_UTIL.createMultiRegionTable(
            TableName.valueOf(TABLE_NAME + String.valueOf(i)), INPUT_FAMILY, 4);
        TEST_UTIL.loadTable(table, INPUT_FAMILY, false);
      }
    }
    // start MR cluster
    TEST_UTIL.startMiniMapReduceCluster();
  }
}
