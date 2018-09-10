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
package org.apache.hadoop.hbase.snapshot;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MapReduceTests;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({MapReduceTests.class, LargeTests.class})
public class TestExportSnapshotRetry extends TestExportSnapshotBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestExportSnapshotRetry.class);

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    setUpBaseConf(TEST_UTIL.getConfiguration());
    TEST_UTIL.startMiniCluster(1, 3);
    TEST_UTIL.startMiniMapReduceCluster();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniMapReduceCluster();
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * Check that ExportSnapshot will succeed if something fails but the retry succeed.
   */
  @Test
  public void testExportRetry() throws Exception {
    Path copyDir = getLocalDestinationDir();
    FileSystem fs = FileSystem.get(copyDir.toUri(), new Configuration());
    copyDir = copyDir.makeQualified(fs);
    Configuration conf = new Configuration(TEST_UTIL.getConfiguration());
    conf.setBoolean(ExportSnapshot.Testing.CONF_TEST_FAILURE, true);
    conf.setInt(ExportSnapshot.Testing.CONF_TEST_FAILURE_COUNT, 2);
    conf.setInt("mapreduce.map.maxattempts", 3);
    testExportFileSystemState(conf, tableName, snapshotName, snapshotName, tableNumFiles,
        TEST_UTIL.getDefaultRootDirPath(), copyDir, true, getBypassRegionPredicate(), true);
  }
}
