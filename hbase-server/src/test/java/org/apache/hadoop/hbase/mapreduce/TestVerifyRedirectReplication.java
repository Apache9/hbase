/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */
package org.apache.hadoop.hbase.mapreduce;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.replication.VerifyRedirectReplication;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.regionserver.RedirectingInterClusterReplicationEndpoint;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(LargeTests.class)
public class TestVerifyRedirectReplication {
  private static final Log LOG = LogFactory.getLog(TestVerifyRedirectReplication.class);
  private static final String tableName = "testSource";
  private static final String peerTableName = "testPeer";
  private static final byte[] famName = Bytes.toBytes("f");
  private static final byte[] row = Bytes.toBytes("row");

  private static HBaseTestingUtility utility1;
  private static HBaseTestingUtility utility2;

  private static HTable htable1;
  private static HTable htable2;


  private static final int NB_RETRIES = 10;
  private static final int NB_ROWS_IN_BATCH = 10;

  @BeforeClass
  public static void beforeClass() throws Exception {
    // start two hbase mini clusters
    Configuration conf1 = HBaseConfiguration.create();
    conf1.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/1");
    conf1.setBoolean(HConstants.REPLICATION_ENABLE_KEY, true);

    utility1 = new HBaseTestingUtility(conf1);
    conf1.set(MRJobConfig.MR_AM_STAGING_DIR, utility1.getDataTestDir("staging").toString());
    utility1.startMiniZKCluster();

    MiniZooKeeperCluster miniZK = utility1.getZkCluster();
    utility1.startMiniMapReduceCluster();

    // Base conf2 on conf1 so it gets the right zk cluster.
    Configuration conf2 = HBaseConfiguration.create(conf1);
    conf2.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/2");
    conf2.setBoolean(HConstants.REPLICATION_ENABLE_KEY, true);

    utility2 = new HBaseTestingUtility(conf2);
    utility2.setZkCluster(miniZK);

    LOG.info("Setup second Zk");

    utility1.startMiniCluster(2);
    utility2.startMiniCluster(2);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    utility1.shutdownMiniMapReduceCluster();
    utility2.shutdownMiniCluster();
    utility1.shutdownMiniCluster();
  }

  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception {
    // create tables and add peer
    HTableDescriptor table = new HTableDescriptor(TableName.valueOf(tableName));
    HColumnDescriptor fam = new HColumnDescriptor(famName);
    fam.setScope(HConstants.REPLICATION_SCOPE_GLOBAL);
    table.addFamily(fam);

    HTableDescriptor peerTable = new HTableDescriptor(TableName.valueOf(peerTableName));
    peerTable.addFamily(fam);

    utility1.getHBaseAdmin().createTable(table);
    utility2.getHBaseAdmin().createTable(peerTable);

    htable1 = new HTable(utility1.getConfiguration(), tableName);
    htable2 = new HTable(utility2.getConfiguration(), peerTableName);

    ReplicationPeerConfig rpc = new ReplicationPeerConfig();
    rpc.setClusterKey(utility2.getClusterKey());
    rpc.setReplicationEndpointImpl(RedirectingInterClusterReplicationEndpoint.class.getName());
    rpc.setReplicateAllUserTables(true);
    rpc.getConfiguration().put(tableName, peerTableName);
    utility1.getHBaseAdmin().addReplicationPeer("1", rpc);
  }

  /**
   * @throws java.lang.Exception
   */
  @After
  public void tearDown() throws Exception {
    utility1.getHBaseAdmin().removeReplicationPeer("1");
    htable1.close();
    htable2.close();
    utility1.deleteTable(tableName);
    utility2.deleteTable(peerTableName);
  }

  /**
   * Add some data in table1
   * @throws Exception
   */
  public void insertTestData() throws Exception {
    Put put;
    htable1.setAutoFlush(false);
    for (int i = 0; i < NB_ROWS_IN_BATCH; i++) {
      put = new Put(Bytes.toBytes("row-" + i));
      put.add(famName, row, row);
      htable1.put(put);
    }
    htable1.flushCommits();

    Scan scan = new Scan();

    ResultScanner scanner1 = htable1.getScanner(scan);
    Result[] res1 = scanner1.next(NB_ROWS_IN_BATCH);
    scanner1.close();
    assertEquals(NB_ROWS_IN_BATCH, res1.length);

    // make sure all rows are replicated to table2
    for (int i = 0; i < NB_RETRIES; i++) {
      if (i==NB_RETRIES-1) {
        fail("Waited too much time for normal batch replication");
      }
      ResultScanner scanner = htable2.getScanner(scan);
      Result[] res = scanner.next(NB_ROWS_IN_BATCH);
      scanner.close();
      if (res.length != NB_ROWS_IN_BATCH) {
        LOG.info("Only got " + res.length + " rows");
        Thread.sleep(10000);
      } else {
        break;
      }
    }
  }

  /**
   * Test the startrow/stoprow options in VerifyRedirectReplication
   * @throws Exception
   */
  @Test
  public void testStartStopRow() throws Exception {
    insertTestData();
    String[] args =
        new String[] { "--startrow=row-3", "--stoprow=row-7", "--peerTable=" + peerTableName, "1",
            tableName };
    Job job =
        new VerifyRedirectReplication().createSubmittableJob(utility1.getConfiguration(), args);

    if (job == null) {
      fail("Job wasn't created, see the log");
    }
    if (!job.waitForCompletion(true)) {
      fail("Job failed, see the log");
    }
    assertEquals(4, job.getCounters().
        findCounter(VerifyRedirectReplication.Verifier.Counters.GOODROWS).getValue());
    assertEquals(0, job.getCounters().
        findCounter(VerifyRedirectReplication.Verifier.Counters.BADROWS).getValue());

    // delete row-1, GOODROWS will not change
    Delete delete = new Delete(Bytes.toBytes("row-1"));
    htable2.delete(delete);
    job = new VerifyRedirectReplication().createSubmittableJob(utility1.getConfiguration(), args);
    if (job == null) {
      fail("Job wasn't created, see the log");
    }
    if (!job.waitForCompletion(true)) {
      fail("Job failed, see the log");
    }
    assertEquals(4, job.getCounters().
        findCounter(VerifyRedirectReplication.Verifier.Counters.GOODROWS).getValue());
    assertEquals(0, job.getCounters().
        findCounter(VerifyRedirectReplication.Verifier.Counters.BADROWS).getValue());

    // delete row-5, GOODROWS will change
    delete = new Delete(Bytes.toBytes("row-5"));
    htable2.delete(delete);

    job = new VerifyRedirectReplication().createSubmittableJob(utility1.getConfiguration(), args);
    if (job == null) {
      fail("Job wasn't created, see the log");
    }
    if (!job.waitForCompletion(true)) {
      fail("Job failed, see the log");
    }
    assertEquals(3, job.getCounters().
        findCounter(VerifyRedirectReplication.Verifier.Counters.GOODROWS).getValue());
    assertEquals(1, job.getCounters().
        findCounter(VerifyRedirectReplication.Verifier.Counters.BADROWS).getValue());
  }

  /**
   * Test the target options in VerifyRedirectReplication.
   * The target option can be used to control the rate of replication verify
   * @throws Exception
   */
  @Test
  public void testScanLimit() throws Exception {
    insertTestData();
    String[] args1 = new String[] { "--peerTable=" + peerTableName, "1", tableName };
    String[] args2 =
        new String[] { "--scanrate=100", "--peerTable=" + peerTableName, "1", tableName };
    Job job1 =
        new VerifyRedirectReplication().createSubmittableJob(utility1.getConfiguration(), args1);
    Job job2 =
        new VerifyRedirectReplication().createSubmittableJob(utility1.getConfiguration(), args2);

    assertNotNull(job1);
    assertEquals(-1, job1.getConfiguration().getInt(TableMapper.SCAN_RATE_LIMIT, -1));
    if (!job1.waitForCompletion(true)) {
      fail("Job failed, see the log");
    }
    assertEquals(NB_ROWS_IN_BATCH, job1.getCounters().
        findCounter(VerifyRedirectReplication.Verifier.Counters.GOODROWS).getValue());
    assertEquals(0, job1.getCounters().
        findCounter(VerifyRedirectReplication.Verifier.Counters.BADROWS).getValue());

    assertNotNull(job2);
    assertEquals(100, job2.getConfiguration().getInt(TableMapper.SCAN_RATE_LIMIT, -1));
    if (!job2.waitForCompletion(true)) {
      fail("Job failed, see the log");
    }
    assertEquals(NB_ROWS_IN_BATCH, job2.getCounters().
        findCounter(VerifyRedirectReplication.Verifier.Counters.GOODROWS).getValue());
    assertEquals(0, job2.getCounters().
        findCounter(VerifyRedirectReplication.Verifier.Counters.BADROWS).getValue());
  }

  @Test
  public void testLogTable() throws Exception {
    insertTestData();
    String logTableName = "LogTable";
    TableName logTable = TableName.valueOf(logTableName);

    // create log table
    HBaseAdmin admin1 = utility1.getHBaseAdmin();
    HTableDescriptor htd = new HTableDescriptor(logTable);
    HColumnDescriptor hcd = new HColumnDescriptor(Bytes.toBytes("A"));
    htd.addFamily(hcd);
    admin1.createTable(htd);

    String[] args =
        new String[] { "--logtable=" + logTableName, "--peerTable=" + peerTableName, "1",
            tableName };
    Configuration conf = HBaseConfiguration.create(utility1.getConfiguration());
    Job job = new VerifyRedirectReplication().createSubmittableJob(conf, args);

    if (job == null) {
      fail("Job wasn't created, see the log");
    }
    if (!job.waitForCompletion(true)) {
      fail("Job failed, see the log");
    }
    assertEquals(10,
        job.getCounters().findCounter(VerifyRedirectReplication.Verifier.Counters.GOODROWS)
            .getValue());
    assertEquals(0,
        job.getCounters().findCounter(VerifyRedirectReplication.Verifier.Counters.BADROWS)
            .getValue());

    // delete row-1, GOODROWS will change
    Delete delete = new Delete(Bytes.toBytes("row-1"));
    htable2.delete(delete);
    job = new VerifyRedirectReplication().createSubmittableJob(conf, args);
    if (job == null) {
      fail("Job wasn't created, see the log");
    }
    if (!job.waitForCompletion(true)) {
      fail("Job failed, see the log");
    }
    assertEquals(9,
        job.getCounters().findCounter(VerifyRedirectReplication.Verifier.Counters.GOODROWS)
            .getValue());
    assertEquals(1,
        job.getCounters().findCounter(VerifyRedirectReplication.Verifier.Counters.BADROWS)
            .getValue());

    Result[] rs = null;
    try (HTable ht = new HTable(conf, logTable)) {
      try (ResultScanner scanner = ht.getScanner(new Scan())) {
        rs = scanner.next(10);
      }
    }
    Assert.assertEquals(rs.length, 1);

    admin1.disableTable(logTable);
    admin1.deleteTable(logTable);
  }
}