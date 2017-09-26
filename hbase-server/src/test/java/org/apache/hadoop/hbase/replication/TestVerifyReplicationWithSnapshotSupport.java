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

package org.apache.hadoop.hbase.replication;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.replication.ReplicationAdmin;
import org.apache.hadoop.hbase.mapreduce.replication.VerifyReplication;
import org.apache.hadoop.hbase.mapreduce.replication.VerifyReplication.Verifier.Counters;
import org.apache.hadoop.hbase.snapshot.SnapshotTestingUtils;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category(MediumTests.class)
public class TestVerifyReplicationWithSnapshotSupport {

  private static final Logger LOG =
      Logger.getLogger(TestVerifyReplicationWithSnapshotSupport.class);
  private static final byte[] tableName = Bytes.toBytes("test_verify");
  private static final byte[] famName = Bytes.toBytes("f");
  protected static final byte[] row = Bytes.toBytes("row");

  private static HBaseTestingUtility utility1;
  private static HBaseTestingUtility utility2;
  private static ReplicationAdmin admin;

  private static HTable htable1;
  private static HTable htable2;

  private static final int NB_RETRIES = 10;
  private static final int NB_ROWS_IN_BATCH = 1000;
  protected static final long SLEEP_TIME = 500;

  private static Configuration conf1 = null;
  private static Configuration conf2 = null;

  @BeforeClass
  public static void beforeClass() throws Exception {
    // start two hbase mini clusters
    conf1 = HBaseConfiguration.create();
    conf1.setInt("jvm.threadmonitor.runnable-threshold-max", 500);
    conf1.setInt("jvm.threadmonitor.blocked-threshold-max", 500);
    conf1.setInt("jvm.threadmonitor.waiting-threshold-min", 5);

    conf1.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/1");
    conf1.setBoolean(HConstants.REPLICATION_ENABLE_KEY, true);
    conf1.setInt(HConstants.MASTER_INFO_PORT, 60011);

    utility1 = new HBaseTestingUtility(conf1);
    conf1.set(MRJobConfig.MR_AM_STAGING_DIR, utility1.getDataTestDir("staging").toString());
    utility1.startMiniZKCluster();

    MiniZooKeeperCluster miniZK = utility1.getZkCluster();
    utility1.startMiniMapReduceCluster();

    // Base conf2 on conf1 so it gets the right zk cluster.
    conf2 = HBaseConfiguration.create(conf1);
    conf2.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/2");
    conf2.setBoolean(HConstants.REPLICATION_ENABLE_KEY, true);
    conf2.setInt(HConstants.MASTER_INFO_PORT, 60012);

    utility2 = new HBaseTestingUtility(conf2);
    utility2.setZkCluster(miniZK);

    LOG.info("Setup second Zk");

    utility1.startMiniCluster(2);
    utility2.startMiniCluster(2);

    admin = new ReplicationAdmin(conf1);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    utility1.shutdownMiniMapReduceCluster();
    utility2.shutdownMiniCluster();
    utility1.shutdownMiniCluster();
  }

  public static void createTable(final HBaseTestingUtility util, final TableName tableName,
      final byte[]... families) throws IOException, InterruptedException {
    HTableDescriptor htd = new HTableDescriptor(tableName);
    for (byte[] family : families) {
      HColumnDescriptor hcd = new HColumnDescriptor(family);
      hcd.setScope(1);
      htd.addFamily(hcd);
    }
    util.createTable(htd, null);
  }

  @Before
  public void setUp() throws Exception {
    // create tables and add peer
    createTable(utility1, TableName.valueOf(tableName), famName);
    createTable(utility2, TableName.valueOf(tableName), famName);

    htable1 = new HTable(utility1.getConfiguration(), tableName);
    htable2 = new HTable(utility2.getConfiguration(), tableName);

    ReplicationPeerConfig pc = new ReplicationPeerConfig();
    pc.setClusterKey(utility2.getClusterKey());
    admin.addPeer("1", pc, null);
    assertEquals(admin.listPeerConfigs().size(), 1);
  }

  private void testSmallBatch() throws Exception {
    LOG.info("testSmallBatch");
    Put put;
    // normal Batch tests
    htable1.setAutoFlush(false, true);
    for (int i = 0; i < NB_ROWS_IN_BATCH; i++) {
      put = new Put(Bytes.toBytes(i));
      put.add(famName, row, row);
      htable1.put(put);
    }
    htable1.flushCommits();

    Scan scan = new Scan();

    ResultScanner scanner1 = htable1.getScanner(scan);
    Result[] res1 = scanner1.next(NB_ROWS_IN_BATCH);
    scanner1.close();
    assertEquals(NB_ROWS_IN_BATCH, res1.length);

    for (int i = 0; i < NB_RETRIES; i++) {
      scan = new Scan();
      if (i == NB_RETRIES - 1) {
        fail("Waited too much time for normal batch replication");
      }
      ResultScanner scanner = htable2.getScanner(scan);
      Result[] res = scanner.next(NB_ROWS_IN_BATCH);
      scanner.close();
      if (res.length != NB_ROWS_IN_BATCH) {
        LOG.info("Only got " + res.length + " rows");
        Thread.sleep(SLEEP_TIME);
      } else {
        break;
      }
    }
  }

  @After
  public void tearDown() throws Exception {
    admin.removePeer("1");
    htable1.close();
    htable2.close();
    utility1.deleteTable(tableName);
    utility2.deleteTable(tableName);
  }

  @Test
  public void testVerifyReplicationSnapshotArguments() throws InterruptedException {
    // sleep 5 seconds to wait peer added. otherwise if we remove a peer
    // before peer finished adding, the rs will be stopped.
    Thread.sleep(5 * 1000L);
    String[] args =
        new String[] { "--sourceSnapshotName=snapshot1", "2", Bytes.toString(tableName) };
    assertFalse(Lists.newArrayList(args).toString(), new VerifyReplication().doCommandLine(args));

    args = new String[] { "--sourceSnapshotTmpDir=tmp", "2", Bytes.toString(tableName) };
    assertFalse(Lists.newArrayList(args).toString(), new VerifyReplication().doCommandLine(args));

    args = new String[] { "--sourceSnapshotName=snapshot1", "--sourceSnapshotTmpDir=tmp", "2",
        Bytes.toString(tableName) };
    assertTrue(Lists.newArrayList(args).toString(), new VerifyReplication().doCommandLine(args));

    args = new String[] { "--peerSnapshotName=snapshot1", "2", Bytes.toString(tableName) };
    assertFalse(Lists.newArrayList(args).toString(), new VerifyReplication().doCommandLine(args));

    args = new String[] { "--peerSnapshotTmpDir=/tmp/", "2", Bytes.toString(tableName) };
    assertFalse(Lists.newArrayList(args).toString(), new VerifyReplication().doCommandLine(args));

    args = new String[] { "--peerSnapshotName=snapshot1", "--peerSnapshotTmpDir=/tmp/",
        "--peerFSAddress=tempfs", "--peerHBaseRootAddress=hdfs://tempfs:50070/hbase/", "2",
        Bytes.toString(tableName) };
    assertTrue(Lists.newArrayList(args).toString(), new VerifyReplication().doCommandLine(args));

    args = new String[] { "--sourceSnapshotName=snapshot1", "--sourceSnapshotTmpDir=/tmp/",
        "--peerSnapshotName=snapshot2", "--peerSnapshotTmpDir=/tmp/", "--peerFSAddress=tempfs",
        "--peerHBaseRootAddress=hdfs://tempfs:50070/hbase/", "2", Bytes.toString(tableName) };

    assertTrue(Lists.newArrayList(args).toString(), new VerifyReplication().doCommandLine(args));
  }

  private boolean isUUIDString(String uuid) {
    try {
      UUID.fromString(uuid);
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  private void checkRestoreTmpDir(Configuration conf, String restoreTmpDir, int expectedCount)
      throws IOException {
    FileSystem fs = FileSystem.get(conf);
    FileStatus[] subDirectories = fs.listStatus(new Path(restoreTmpDir));
    assertNotNull(subDirectories);
    int count = 0;
    for (int i = 0; i < expectedCount; i++) {
      if (isUUIDString(subDirectories[i].getPath().getName())) {
        assertTrue(subDirectories[i].isDirectory());
        count++;
      }
    }
    assertEquals(expectedCount, count);
  }

  @Test
  public void testVerifyReplicationWithSnapshotSupport() throws Exception {
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
    Path peerRootDir = FSUtils.getRootDir(conf2);
    FileSystem peerFs = peerRootDir.getFileSystem(conf2);
    String peerSnapshotName = "peerSnapshot-" + System.currentTimeMillis();
    SnapshotTestingUtils.createSnapshotAndValidate(utility2.getHBaseAdmin(),
      TableName.valueOf(tableName), new String(famName), peerSnapshotName, peerRootDir, peerFs,
      true);

    String peerFSAddress = peerFs.getUri().toString();
    String temPath1 = utility1.getDataTestDir().toString();
    String temPath2 = "/tmp2";

    String[] args = new String[] { "--sourceSnapshotName=" + sourceSnapshotName,
        "--sourceSnapshotTmpDir=" + temPath1, "--peerSnapshotName=" + peerSnapshotName,
        "--peerSnapshotTmpDir=" + temPath2, "--peerFSAddress=" + peerFSAddress,
        "--peerHBaseRootAddress=" + FSUtils.getRootDir(conf2), "1", Bytes.toString(tableName) };

    Job job = new VerifyReplication().createSubmittableJob(conf1, args);
    if (job == null) {
      fail("Job wasn't created, see the log");
    }
    if (!job.waitForCompletion(true)) {
      fail("Job failed, see the log");
    }
    assertEquals(NB_ROWS_IN_BATCH,
      job.getCounters().findCounter(VerifyReplication.Verifier.Counters.GOODROWS).getValue());
    assertEquals(0,
      job.getCounters().findCounter(VerifyReplication.Verifier.Counters.BADROWS).getValue());
    checkRestoreTmpDir(conf1, temPath1, 1);
    checkRestoreTmpDir(conf2, temPath2, 1);

    Scan scan = new Scan();
    ResultScanner rs = htable2.getScanner(scan);
    Put put = null;
    for (Result result : rs) {
      put = new Put(result.getRow());
      Cell firstVal = result.rawCells()[0];
      put.add(CellUtil.cloneFamily(firstVal), CellUtil.cloneQualifier(firstVal),
        Bytes.toBytes("diff data"));
      htable2.put(put);
    }
    Delete delete = new Delete(put.getRow());
    htable2.delete(delete);

    sourceSnapshotName = "sourceSnapshot-" + System.currentTimeMillis();
    SnapshotTestingUtils.createSnapshotAndValidate(utility1.getHBaseAdmin(),
      TableName.valueOf(tableName), new String(famName), sourceSnapshotName, rootDir, fs, true);

    peerSnapshotName = "peerSnapshot-" + System.currentTimeMillis();
    SnapshotTestingUtils.createSnapshotAndValidate(utility2.getHBaseAdmin(),
      TableName.valueOf(tableName), new String(famName), peerSnapshotName, peerRootDir, peerFs,
      true);

    args = new String[] { "--sourceSnapshotName=" + sourceSnapshotName,
        "--sourceSnapshotTmpDir=" + temPath1, "--peerSnapshotName=" + peerSnapshotName,
        "--peerSnapshotTmpDir=" + temPath2, "--peerFSAddress=" + peerFSAddress,
        "--peerHBaseRootAddress=" + FSUtils.getRootDir(conf2), "1", Bytes.toString(tableName) };

    job = new VerifyReplication().createSubmittableJob(conf1, args);
    if (job == null) {
      fail("Job wasn't created, see the log");
    }
    if (!job.waitForCompletion(true)) {
      fail("Job failed, see the log");
    }
    assertEquals(0,
      job.getCounters().findCounter(VerifyReplication.Verifier.Counters.GOODROWS).getValue());
    assertEquals(NB_ROWS_IN_BATCH,
      job.getCounters().findCounter(VerifyReplication.Verifier.Counters.BADROWS).getValue());

    checkRestoreTmpDir(conf1, temPath1, 2);
    checkRestoreTmpDir(conf2, temPath2, 2);
  }

  @Test
  public void testRepair() throws Exception {
    testSmallBatch();
    // Take source and target tables snapshot
    Path rootDir = FSUtils.getRootDir(conf1);
    FileSystem fs = rootDir.getFileSystem(conf1);
    String sourceSnapshotName = "sourceSnapshot-" + System.currentTimeMillis();
    SnapshotTestingUtils.createSnapshotAndValidate(utility1.getHBaseAdmin(),
      TableName.valueOf(tableName), new String(famName), sourceSnapshotName, rootDir, fs, true);
    // Take target snapshot
    Path peerRootDir = FSUtils.getRootDir(conf2);
    FileSystem peerFs = peerRootDir.getFileSystem(conf2);
    String peerSnapshotName = "peerSnapshot-" + System.currentTimeMillis();
    SnapshotTestingUtils.createSnapshotAndValidate(utility2.getHBaseAdmin(),
      TableName.valueOf(tableName), new String(famName), peerSnapshotName, peerRootDir, peerFs,
      true);

    String peerFSAddress = peerFs.getUri().toString();
    String temPath1 = utility1.getDataTestDir().toString();
    String temPath2 = "/tmp2";

    /*************************** Initial Verify **********************************************/
    String[] args = new String[] { "--sourceSnapshotName=" + sourceSnapshotName,
        "--sourceSnapshotTmpDir=" + temPath1, "--peerSnapshotName=" + peerSnapshotName,
        "--peerSnapshotTmpDir=" + temPath2, "--peerFSAddress=" + peerFSAddress,
        "--peerHBaseRootAddress=" + FSUtils.getRootDir(conf2), "1", Bytes.toString(tableName) };

    Job job = new VerifyReplication().createSubmittableJob(conf1, args);
    if (job == null) {
      fail("Job wasn't created, see the log");
    }
    if (!job.waitForCompletion(true)) {
      fail("Job failed, see the log");
    }
    assertEquals(NB_ROWS_IN_BATCH,
      job.getCounters().findCounter(VerifyReplication.Verifier.Counters.GOODROWS).getValue());
    assertEquals(0,
      job.getCounters().findCounter(VerifyReplication.Verifier.Counters.BADROWS).getValue());

    /************************* Delete rows and flush & major compaction ***********************/
    ResultScanner rs = htable2.getScanner(new Scan());
    for (Result r : rs) {
      htable2.delete(new Delete(r.getRow()));
    }
    utility2.getHBaseAdmin().flush(tableName);
    utility2.getHBaseAdmin().majorCompact(Bytes.toString(tableName));
    Thread.sleep(30000);

    /************************** Verify After deleting peer table ****************************/
    // Create new snapshot for the deletes.
    sourceSnapshotName = "sourceSnapshot-" + System.currentTimeMillis();
    SnapshotTestingUtils.createSnapshotAndValidate(utility1.getHBaseAdmin(),
      TableName.valueOf(tableName), new String(famName), sourceSnapshotName, rootDir, fs, true);

    peerSnapshotName = "peerSnapshot-" + System.currentTimeMillis();
    SnapshotTestingUtils.createSnapshotAndValidate(utility2.getHBaseAdmin(),
      TableName.valueOf(tableName), new String(famName), peerSnapshotName, peerRootDir, peerFs,
      true);

    args = new String[] { "--sourceSnapshotName=" + sourceSnapshotName,
        "--sourceSnapshotTmpDir=" + temPath1, "--peerSnapshotName=" + peerSnapshotName,
        "--peerSnapshotTmpDir=" + temPath2, "--peerFSAddress=" + peerFSAddress,
        "--peerHBaseRootAddress=" + FSUtils.getRootDir(conf2), "--repairPeer", "1",
        Bytes.toString(tableName) };
    job = new VerifyReplication().createSubmittableJob(conf1, args);
    if (job == null) {
      fail("Job wasn't created, see the log");
    }
    if (!job.waitForCompletion(true)) {
      fail("Job failed, see the log");
    }
    assertEquals(0, job.getCounters().findCounter(Counters.GOODROWS).getValue());
    assertEquals(NB_ROWS_IN_BATCH, job.getCounters().findCounter(Counters.BADROWS).getValue());
    assertEquals(NB_ROWS_IN_BATCH,
      job.getCounters().findCounter(Counters.ONLY_IN_SOURCE_TABLE_ROWS).getValue());
    assertEquals(NB_ROWS_IN_BATCH,
      job.getCounters().findCounter(Counters.REPAIR_PEER_ROWS).getValue());

    /***************************** Verify again after repair *********************************/
    // Create new snapshot for the repair.
    sourceSnapshotName = "sourceSnapshot-" + System.currentTimeMillis();
    SnapshotTestingUtils.createSnapshotAndValidate(utility1.getHBaseAdmin(),
      TableName.valueOf(tableName), new String(famName), sourceSnapshotName, rootDir, fs, true);

    peerSnapshotName = "peerSnapshot-" + System.currentTimeMillis();
    SnapshotTestingUtils.createSnapshotAndValidate(utility2.getHBaseAdmin(),
      TableName.valueOf(tableName), new String(famName), peerSnapshotName, peerRootDir, peerFs,
      true);

    args = new String[] { "--sourceSnapshotName=" + sourceSnapshotName,
        "--sourceSnapshotTmpDir=" + temPath1, "--peerSnapshotName=" + peerSnapshotName,
        "--peerSnapshotTmpDir=" + temPath2, "--peerFSAddress=" + peerFSAddress,
        "--peerHBaseRootAddress=" + FSUtils.getRootDir(conf2), "1", Bytes.toString(tableName) };

    // verify again.
    job = new VerifyReplication().createSubmittableJob(conf1, args);
    if (job == null) {
      fail("Job wasn't created, see the log");
    }
    if (!job.waitForCompletion(true)) {
      fail("Job failed, see the log");
    }
    assertEquals(NB_ROWS_IN_BATCH, job.getCounters().findCounter(Counters.GOODROWS).getValue());
    assertEquals(0, job.getCounters().findCounter(Counters.BADROWS).getValue());
  }
}
