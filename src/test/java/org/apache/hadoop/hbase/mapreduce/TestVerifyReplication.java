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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.LargeTests;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.replication.ReplicationAdmin;
import org.apache.hadoop.hbase.mapreduce.replication.VerifyReplication;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.apache.hadoop.mapreduce.Job;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(LargeTests.class)
public class TestVerifyReplication {
  private static final Log LOG = LogFactory.getLog(TestVerifyReplication.class);
  private static final byte[] tableName = Bytes.toBytes("test");
  private static final byte[] famName = Bytes.toBytes("f");
  private static final byte[] row = Bytes.toBytes("row");

  private static HBaseTestingUtility utility1;
  private static HBaseTestingUtility utility2;
  private static ReplicationAdmin admin;
  
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
   
    admin = new ReplicationAdmin(conf1);
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
    HTableDescriptor table = new HTableDescriptor(tableName);
    HColumnDescriptor fam = new HColumnDescriptor(famName);
    fam.setScope(HConstants.REPLICATION_SCOPE_GLOBAL);
    table.addFamily(fam);
    
    utility1.getHBaseAdmin().createTable(table);
    utility2.getHBaseAdmin().createTable(table);

    htable1 = new HTable(utility1.getConfiguration(), tableName);
    htable2 = new HTable(utility2.getConfiguration(), tableName);
    
    admin.addPeer("1", utility2.getClusterKey());
  }

  
  /**
   * @throws java.lang.Exception
   */
  @After
  public void tearDown() throws Exception {
    admin.removePeer("1");
    htable1.close();
    htable2.close();
    utility1.deleteTable(tableName);
    utility2.deleteTable(tableName);
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
   * Test the startrow/stoprow options in VerifyReplication
   * @throws Exception
   */
  @Test(timeout = 300000)
  public void testStartStopRow() throws Exception {
    insertTestData();
    String[] args = new String[] {"--startrow=row-3", "--stoprow=row-7", "1", Bytes.toString(tableName)};
    Job job = new VerifyReplication(utility1.getConfiguration()).createSubmittableJob(args);
    
    if (job == null) {
      fail("Job wasn't created, see the log");
    }
    if (!job.waitForCompletion(true)) {
      fail("Job failed, see the log");
    }
    assertEquals(4, job.getCounters().
        findCounter(VerifyReplication.Verifier.Counters.GOODROWS).getValue());
    assertEquals(0, job.getCounters().
        findCounter(VerifyReplication.Verifier.Counters.BADROWS).getValue());
    
    // delete row-1, GOODROWS will not change
    Delete delete = new Delete(Bytes.toBytes("row-1"));
    htable2.delete(delete);
    job = new VerifyReplication(utility1.getConfiguration()).createSubmittableJob(args);
    if (job == null) {
      fail("Job wasn't created, see the log");
    }
    if (!job.waitForCompletion(true)) {
      fail("Job failed, see the log");
    }
    assertEquals(4, job.getCounters().
        findCounter(VerifyReplication.Verifier.Counters.GOODROWS).getValue());
    assertEquals(0, job.getCounters().
        findCounter(VerifyReplication.Verifier.Counters.BADROWS).getValue());
    
    // delete row-5, GOODROWS will change
    delete = new Delete(Bytes.toBytes("row-5"));
    htable2.delete(delete);
    
    job = new VerifyReplication(utility1.getConfiguration()).createSubmittableJob(args);
    if (job == null) {
      fail("Job wasn't created, see the log");
    }
    if (!job.waitForCompletion(true)) {
      fail("Job failed, see the log");
    }
    assertEquals(3, job.getCounters().
            findCounter(VerifyReplication.Verifier.Counters.GOODROWS).getValue());
        assertEquals(1, job.getCounters().
            findCounter(VerifyReplication.Verifier.Counters.BADROWS).getValue());
  }
  
  /**
   * Test the target options in VerifyReplication. The target option can be used to control the rate
   * of replication verify
   * @throws Exception
   */
  @Test(timeout = 300000)
  public void testScanLimit() throws Exception {
    insertTestData();
    VerifyReplication verifyRep = new VerifyReplication(utility1.getConfiguration());
    String[] args1 = new String[] {"1", Bytes.toString(tableName)};
    String[] args2 = new String[] {"--scanrate=1", "1", Bytes.toString(tableName)};
    Job job1 = verifyRep.createSubmittableJob(args1);
    Job job2 = verifyRep.createSubmittableJob(args2);
    
    assertNotNull(job1);
    long st = System.currentTimeMillis();
    if (!job1.waitForCompletion(true)) {
      fail("Job failed, see the log");
    }
    long cost1 = System.currentTimeMillis() -st;
    assertEquals(NB_ROWS_IN_BATCH, job1.getCounters().
        findCounter(VerifyReplication.Verifier.Counters.GOODROWS).getValue());
    assertEquals(0, job1.getCounters().
        findCounter(VerifyReplication.Verifier.Counters.BADROWS).getValue());
    
    assertNotNull(job2);
    st = System.currentTimeMillis();
    if (!job2.waitForCompletion(true)) {
      fail("Job failed, see the log");
    }
    long cost2 = System.currentTimeMillis() -st;
    assertEquals(NB_ROWS_IN_BATCH, job2.getCounters().
        findCounter(VerifyReplication.Verifier.Counters.GOODROWS).getValue());
    assertEquals(0, job2.getCounters().
        findCounter(VerifyReplication.Verifier.Counters.BADROWS).getValue());
    
    // it takes more time for job with scan rate
    assertTrue(cost2 > cost1);
  }
}
