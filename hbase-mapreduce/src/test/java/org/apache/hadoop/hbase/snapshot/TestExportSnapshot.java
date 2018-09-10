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

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.MapReduceTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test Export Snapshot Tool
 */
@Category({MapReduceTests.class, LargeTests.class})
public class TestExportSnapshot extends TestExportSnapshotBase {

  private static final Logger LOG = LoggerFactory.getLogger(TestExportSnapshot.class);

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestExportSnapshot.class);

  /**
   * Verify if exported snapshot and copied files matches the original one.
   */
  @Test
  public void testExportFileSystemState() throws Exception {
    testExportFileSystemState(tableName, snapshotName, snapshotName, tableNumFiles);
  }

  @Test
  public void testExportFileSystemStateWithSkipTmp() throws Exception {
    TEST_UTIL.getConfiguration().setBoolean(ExportSnapshot.CONF_SKIP_TMP, true);
    try {
      testExportFileSystemState(tableName, snapshotName, snapshotName, tableNumFiles);
    } finally {
      TEST_UTIL.getConfiguration().setBoolean(ExportSnapshot.CONF_SKIP_TMP, false);
    }
  }

  @Test
  public void testEmptyExportFileSystemState() throws Exception {
    testExportFileSystemState(tableName, emptySnapshotName, emptySnapshotName, 0);
  }

  @Test
  public void testConsecutiveExports() throws Exception {
    Path copyDir = getLocalDestinationDir();
    testExportFileSystemState(tableName, snapshotName, snapshotName, tableNumFiles, copyDir, false);
    testExportFileSystemState(tableName, snapshotName, snapshotName, tableNumFiles, copyDir, true);
    removeExportDir(copyDir);
  }

  @Test
  public void testExportWithTargetName() throws Exception {
    final byte[] targetName = Bytes.toBytes("testExportWithTargetName");
    testExportFileSystemState(tableName, snapshotName, targetName, tableNumFiles);
  }

  private void testExportFileSystemState(final TableName tableName, final byte[] snapshotName,
      final byte[] targetName, int filesExpected) throws Exception {
    testExportFileSystemState(tableName, snapshotName, targetName,
      filesExpected, getHdfsDestinationDir(), false);
  }

  protected void testExportFileSystemState(final TableName tableName,
      final byte[] snapshotName, final byte[] targetName, int filesExpected,
      Path copyDir, boolean overwrite) throws Exception {
    testExportFileSystemState(TEST_UTIL.getConfiguration(), tableName, snapshotName, targetName,
      filesExpected, TEST_UTIL.getDefaultRootDirPath(), copyDir,
      overwrite, getBypassRegionPredicate(), true);
  }

  /**
   * Check that ExportSnapshot will fail if we inject failure more times than MR will retry.
   */
  @Test
  public void testExportFailure() throws Exception {
    Path copyDir = getLocalDestinationDir();
    FileSystem fs = FileSystem.get(copyDir.toUri(), new Configuration());
    copyDir = copyDir.makeQualified(fs);
    Configuration conf = new Configuration(TEST_UTIL.getConfiguration());
    conf.setBoolean(ExportSnapshot.Testing.CONF_TEST_FAILURE, true);
    conf.setInt(ExportSnapshot.Testing.CONF_TEST_FAILURE_COUNT, 4);
    conf.setInt("mapreduce.map.maxattempts", 3);
    testExportFileSystemState(conf, tableName, snapshotName, snapshotName, tableNumFiles,
        TEST_UTIL.getDefaultRootDirPath(), copyDir, true, getBypassRegionPredicate(), false);
  }

  private Path getHdfsDestinationDir() {
    Path rootDir = TEST_UTIL.getHBaseCluster().getMaster().getMasterFileSystem().getRootDir();
    Path path = new Path(new Path(rootDir, "export-test"), "export-" + System.currentTimeMillis());
    LOG.info("HDFS export destination path: " + path);
    return path;
  }

  private static void removeExportDir(final Path path) throws IOException {
    FileSystem fs = FileSystem.get(path.toUri(), new Configuration());
    fs.delete(path, true);
  }
}
