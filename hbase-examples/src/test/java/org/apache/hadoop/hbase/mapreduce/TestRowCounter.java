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

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.protobuf.generated.SnapshotProtos.SnapshotDescription;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(LargeTests.class)
public class TestRowCounter {
  private static final String NAMESPACE = "test_ns";
  private static final TableName TABLE_NAME = TableName.valueOf(NAMESPACE, "testTable");
  private static final byte[] FAMILY = Bytes.toBytes("f");
  private static final byte[] QUALIFLY = Bytes.toBytes("q");
  private static final byte[] ROW = Bytes.toBytes("row");
  private static final long MAX_ROWS = 10000;

  private static HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static Configuration CONF;
  private static HTable TABLE;

  @BeforeClass
  public static void beforeClass() throws Exception {
    CONF = UTIL.getConfiguration();
    CONF.set(MRJobConfig.MR_AM_STAGING_DIR, UTIL.getDataTestDir("staging").toString());
    CONF.set("fs.permissions.umask-mode", "007");

    UTIL.startMiniZKCluster();
    UTIL.startMiniMapReduceCluster();
    UTIL.startMiniCluster();

    try (HBaseAdmin admin = new HBaseAdmin(CONF)) {
      NamespaceDescriptor nd = NamespaceDescriptor.create(NAMESPACE).build();
      admin.createNamespace(nd);
    }

    TABLE = UTIL.createTable(TABLE_NAME.getName(), FAMILY);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  private void loadData() throws IOException {
    for (int i = 0; i < MAX_ROWS; i++) {
      Put put = new Put(Bytes.add(ROW, Bytes.toBytes(i)));
      put.add(FAMILY, QUALIFLY, Bytes.toBytes(i));
      TABLE.put(put);
    }
  }

  @Test
  public void test() throws Exception {
    // Load data into mini cluster.
    loadData();

    // creat a temp directory to restore snapshot.
    FileSystem fs = FileSystem.get(CONF);
    Path tmpRestoreDir = new Path(HConstants.SNAPSHOT_RESTORE_TMP_DIR_DEFAULT);
    fs.mkdirs(tmpRestoreDir);

    RowCounter rowCounter = new RowCounter(CONF, TABLE_NAME);
    // check the row size scanned by map-reduce job.
    Assert.assertEquals(MAX_ROWS, rowCounter.runJob());

    // check the existence of snapshot
    try (HBaseAdmin admin = new HBaseAdmin(CONF)) {
      List<SnapshotDescription> snapshots = admin.listSnapshots();
      Assert.assertEquals(0, snapshots.size());
    }

    // Run the map-reduce job again.
    Assert.assertEquals(MAX_ROWS, rowCounter.runJob());
    try (HBaseAdmin admin = new HBaseAdmin(CONF)) {
      List<SnapshotDescription> snapshots = admin.listSnapshots();
      Assert.assertEquals(0, snapshots.size());
    }
  }
}
