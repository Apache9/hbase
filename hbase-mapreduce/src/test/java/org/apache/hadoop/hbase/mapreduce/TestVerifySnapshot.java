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
package org.apache.hadoop.hbase.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.snapshot.SnapshotTestingUtils;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hbase.mapreduce.VerifySnapshot.COMPARE_SNAPSHOT_NAME;
import static org.apache.hadoop.hbase.mapreduce.VerifySnapshot.SNAPSHOT_NAME;
import static org.apache.hadoop.hbase.mapreduce.VerifySnapshot.UT_FLAG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@Category(LargeTests.class)
public class TestVerifySnapshot {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestVerifySnapshot.class);
  private static final Logger LOG = LoggerFactory.getLogger(TestVerifySnapshot.class);
  private static final byte[] tableName = Bytes.toBytes("test");
  private static final byte[] tableName2 = Bytes.toBytes("test2");

  private static final byte[] famName = Bytes.toBytes("f");
  private static final byte[] row = Bytes.toBytes("row");

  private static HBaseTestingUtility utility1;

  private static Table htable1;
  private static Table htable2;
  private static Connection connection;

  private static final int NB_ROWS_IN_BATCH = 10;
  private static Configuration conf1 = null;

  @BeforeClass
  public static void beforeClass() throws Exception {
    // start two hbase mini clusters
    conf1 = HBaseConfiguration.create();
    conf1.setInt("jvm.threadmonitor.runnable-threshold-max", 500);
    conf1.setInt("jvm.threadmonitor.blocked-threshold-max", 500);
    conf1.setInt("jvm.threadmonitor.waiting-threshold-min", 5);
    conf1.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/1");

    conf1.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/1");
    conf1.setInt(HConstants.MASTER_INFO_PORT, 60011);

    utility1 = new HBaseTestingUtility(conf1);
    conf1.set(MRJobConfig.MR_AM_STAGING_DIR, utility1.getDataTestDir("staging").toString());
    utility1.startMiniZKCluster();
    utility1.startMiniMapReduceCluster();
    utility1.startMiniCluster(2);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    utility1.shutdownMiniMapReduceCluster();
    utility1.shutdownMiniCluster();
  }

  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception {
    // create tables and add peer
    TableDescriptorBuilder table1 = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName));
    HColumnDescriptor fam = new HColumnDescriptor(famName);
    table1.setColumnFamily(fam);

    TableDescriptorBuilder table2 = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName2));
    table2.setColumnFamily(fam);

    utility1.getHBaseAdmin().createTable(table1.build());
    utility1.getHBaseAdmin().createTable(table2.build());

    connection = ConnectionFactory.createConnection(utility1.getConfiguration());
    htable1 = connection.getTable(TableName.valueOf(tableName));
    htable2 = connection.getTable(TableName.valueOf(tableName2));
  }


  /**
   * @throws java.lang.Exception
   */
  @After
  public void tearDown() throws Exception {
    htable1.close();
    htable2.close();
    connection.close();
    utility1.deleteTable(TableName.valueOf(tableName));
    utility1.deleteTable(TableName.valueOf(tableName2));
  }
  private void testSmallBatch() throws Exception {
    LOG.info("testSmallBatch");
    Put put;
    // normal Batch tests
    for (int i = 0; i < NB_ROWS_IN_BATCH; i++) {
      put = new Put(Bytes.toBytes(i));
      put.addColumn(famName, row, System.currentTimeMillis(), row);
      htable1.put(put);
      htable2.put(put);
    }

    Scan scan = new Scan();

    ResultScanner scanner1 = htable1.getScanner(scan);
    Result[] res1 = scanner1.next(NB_ROWS_IN_BATCH);
    scanner1.close();
    assertEquals(NB_ROWS_IN_BATCH, res1.length);


    ResultScanner scanner2 = htable2.getScanner(scan);
    Result[] res2 = scanner2.next(NB_ROWS_IN_BATCH);
    scanner2.close();
    assertEquals(NB_ROWS_IN_BATCH, res2.length);
  }

  @Test
  public void testVerifySnapshot() throws Exception {
    // Populate the tables, at the same time it guarantees that the tables are
    // identical since it does the check
    testSmallBatch();

    // Take source and target tables snapshot
    Path rootDir = FSUtils.getRootDir(conf1);
    FileSystem fs = rootDir.getFileSystem(conf1);
    String sourceSnapshotName = "sourceSnapshot-" + System.currentTimeMillis();
    SnapshotTestingUtils.createSnapshotAndValidate(utility1.getHBaseAdmin(),
        TableName.valueOf(tableName), new String(famName), sourceSnapshotName, rootDir, fs, true);

    // Take target snapshot
    Path peerRootDir = FSUtils.getRootDir(conf1);
    FileSystem peerFs = peerRootDir.getFileSystem(conf1);
    String compareSnapshotName = "compareSnapshot-" + System.currentTimeMillis();
    SnapshotTestingUtils.createSnapshotAndValidate(utility1.getHBaseAdmin(),
        TableName.valueOf(tableName2), new String(famName), compareSnapshotName, peerRootDir, peerFs,
        true);

    String peerFSAddress = peerFs.getUri().toString();
    String temPath1 = utility1.getDataTestDir().toString();

    String[] args = new String[] { "--snapshot-URI", sourceSnapshotName, "--compare-snapshot-URI",
        compareSnapshotName, "--endTime", String.valueOf(Long.MAX_VALUE), "--restoreDir", temPath1,
        "--compare-restoreDir", temPath1, "--cluster-hdfs", fs.getUri().toString(),
        "--compare-cluster-hdfs", peerFSAddress, "--cluster-hbase-root",
        FSUtils.getRootDir(conf1).toString(), "--compare-cluster-hbase-root",
        FSUtils.getRootDir(conf1).toString() };
    conf1.setBoolean(UT_FLAG, true);
    conf1.set(SNAPSHOT_NAME, sourceSnapshotName);
    conf1.set(COMPARE_SNAPSHOT_NAME, compareSnapshotName);

    Job job = new VerifySnapshot().createSubmittableJob(conf1, args);
    if (job == null) {
      fail("Job wasn't created, see the log");
    }
    if (!job.waitForCompletion(true)) {
      fail("Job failed, see the log");
    }
    assertEquals(NB_ROWS_IN_BATCH,
        job.getCounters().findCounter(VerifySnapshot.SnapshotVerifier.Counters.GOOD_ROWS).getValue());
    assertEquals(0, job.getCounters()
        .findCounter(VerifySnapshot.SnapshotVerifier.Counters.ONLY_IN_COMPARISON).getValue());
    assertEquals(0, job.getCounters()
        .findCounter(VerifySnapshot.SnapshotVerifier.Counters.ONLY_IN_SOURCE).getValue());
    assertEquals(0, job.getCounters()
        .findCounter(VerifySnapshot.SnapshotVerifier.Counters.CONTENT_DIFFERENT_ROWS).getValue());

    Scan scan = new Scan();
    ResultScanner rs = htable2.getScanner(scan);
    Put put = null;
    for (Result result : rs) {
      put = new Put(result.getRow());
      Cell firstVal = result.rawCells()[0];
      put.addColumn(CellUtil.cloneFamily(firstVal), CellUtil.cloneQualifier(firstVal),
          Bytes.toBytes("diff data"));
      htable2.put(put);
    }
    Delete delete = new Delete(put.getRow());
    htable2.delete(delete);

    sourceSnapshotName = "sourceSnapshot-" + System.currentTimeMillis();
    SnapshotTestingUtils.createSnapshotAndValidate(utility1.getHBaseAdmin(),
        TableName.valueOf(tableName), new String(famName), sourceSnapshotName, rootDir, fs, true);

    compareSnapshotName = "compareSnapshot-" + System.currentTimeMillis();
    SnapshotTestingUtils.createSnapshotAndValidate(utility1.getHBaseAdmin(),
        TableName.valueOf(tableName2), new String(famName), compareSnapshotName, peerRootDir, peerFs,
        true);

    args = new String[] { "--snapshot-URI", sourceSnapshotName, "--compare-snapshot-URI",
        compareSnapshotName, "--endTime", String.valueOf(Long.MAX_VALUE), "--restoreDir", temPath1,
        "--compare-restoreDir", temPath1, "--cluster-hdfs", fs.getUri().toString(),
        "--compare-cluster-hdfs", peerFSAddress, "--cluster-hbase-root",
        FSUtils.getRootDir(conf1).toString(), "--compare-cluster-hbase-root",
        FSUtils.getRootDir(conf1).toString() };

    conf1.set(SNAPSHOT_NAME, sourceSnapshotName);
    conf1.set(COMPARE_SNAPSHOT_NAME, compareSnapshotName);
    job = new VerifySnapshot().createSubmittableJob(conf1, args);
    if (job == null) {
      fail("Job wasn't created, see the log");
    }
    if (!job.waitForCompletion(true)) {
      fail("Job failed, see the log");
    }
    assertEquals(0,
        job.getCounters().findCounter(VerifySnapshot.SnapshotVerifier.Counters.GOOD_ROWS).getValue());
    assertEquals(1, job.getCounters()
        .findCounter(VerifySnapshot.SnapshotVerifier.Counters.ONLY_IN_SOURCE).getValue());
    assertEquals(9, job.getCounters()
        .findCounter(VerifySnapshot.SnapshotVerifier.Counters.CONTENT_DIFFERENT_ROWS).getValue());
  }
}
