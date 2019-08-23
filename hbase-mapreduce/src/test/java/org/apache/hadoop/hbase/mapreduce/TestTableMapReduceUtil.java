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
package org.apache.hadoop.hbase.mapreduce;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.security.access.SnapshotScannerHDFSAclHelper;
import org.apache.hadoop.hbase.testclassification.MapReduceTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;

/**
 * Test different variants of initTableMapperJob method
 */
@Category({MapReduceTests.class, MediumTests.class})
public class TestTableMapReduceUtil {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestTableMapReduceUtil.class);

  private final static HBaseTestingUtility UTIL = new HBaseTestingUtility();

  @BeforeClass
  public static void beforeClass() throws Exception {
    UTIL.startMiniCluster();
    UTIL.createTable(TableName.valueOf("Table"), new String[]{"C"});
  }

  @AfterClass
  public static void afterClass() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  /*
   * initTableSnapshotMapperJob is tested in {@link TestTableSnapshotInputFormat} because
   * the method depends on an online cluster.
   */

  @Test
  public void testInitTableMapperJob1() throws Exception {
    Job job = new Job(UTIL.getConfiguration(), "tableName");
    // test
    TableMapReduceUtil.initTableMapperJob("Table", new Scan(), Import.Importer.class, Text.class,
        Text.class, job, false, WALInputFormat.class);
    assertEquals(WALInputFormat.class, job.getInputFormatClass());
    assertEquals(Import.Importer.class, job.getMapperClass());
    assertEquals(LongWritable.class, job.getOutputKeyClass());
    assertEquals(Text.class, job.getOutputValueClass());
    assertNull(job.getCombinerClass());
    assertEquals("Table", job.getConfiguration().get(TableInputFormat.INPUT_TABLE));
  }

  @Test
  public void testInitTableMapperJob2() throws Exception {
    Job job = new Job(UTIL.getConfiguration(), "tableName");
    TableMapReduceUtil.initTableMapperJob(Bytes.toBytes("Table"), new Scan(),
        Import.Importer.class, Text.class, Text.class, job, false, WALInputFormat.class);
    assertEquals(WALInputFormat.class, job.getInputFormatClass());
    assertEquals(Import.Importer.class, job.getMapperClass());
    assertEquals(LongWritable.class, job.getOutputKeyClass());
    assertEquals(Text.class, job.getOutputValueClass());
    assertNull(job.getCombinerClass());
    assertEquals("Table", job.getConfiguration().get(TableInputFormat.INPUT_TABLE));
  }

  @Test
  public void testInitTableMapperJob3() throws Exception {
    Job job = new Job(UTIL.getConfiguration(), "tableName");
    TableMapReduceUtil.initTableMapperJob(Bytes.toBytes("Table"), new Scan(),
        Import.Importer.class, Text.class, Text.class, job);
    assertEquals(TableInputFormat.class, job.getInputFormatClass());
    assertEquals(Import.Importer.class, job.getMapperClass());
    assertEquals(LongWritable.class, job.getOutputKeyClass());
    assertEquals(Text.class, job.getOutputValueClass());
    assertNull(job.getCombinerClass());
    assertEquals("Table", job.getConfiguration().get(TableInputFormat.INPUT_TABLE));
  }

  @Test
  public void testInitTableMapperJob4() throws Exception {
    Job job = new Job(UTIL.getConfiguration(), "tableName");
    TableMapReduceUtil.initTableMapperJob(Bytes.toBytes("Table"), new Scan(),
        Import.Importer.class, Text.class, Text.class, job, false);
    assertEquals(TableInputFormat.class, job.getInputFormatClass());
    assertEquals(Import.Importer.class, job.getMapperClass());
    assertEquals(LongWritable.class, job.getOutputKeyClass());
    assertEquals(Text.class, job.getOutputValueClass());
    assertNull(job.getCombinerClass());
    assertEquals("Table", job.getConfiguration().get(TableInputFormat.INPUT_TABLE));
  }

  @Test
  public void testGetAndCheckTmpRestoreDir() throws IOException {
    Configuration conf = UTIL.getConfiguration();
    Path rootDir = FSUtils.getRootDir(conf);
    FileSystem fs = rootDir.getFileSystem(conf);
    Path tmpRestoreDir = new Path(conf.get(SnapshotScannerHDFSAclHelper.SNAPSHOT_RESTORE_TMP_DIR,
      SnapshotScannerHDFSAclHelper.SNAPSHOT_RESTORE_TMP_DIR_DEFAULT));
    if (!fs.exists(tmpRestoreDir)) {
      fs.mkdirs(tmpRestoreDir);
    }
    Path path = TableMapReduceUtil.getAndCheckTmpRestoreDir(conf);
    assertEquals(path.getFileSystem(conf).getUri(), rootDir.getFileSystem(conf).getUri());
  }
}