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
package org.apache.hadoop.hbase.replication.thrift;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.replication.ReplicationAdmin;
import org.apache.hadoop.hbase.replication.TestReplicationSmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

import static org.apache.hadoop.hbase.replication.thrift.ReplicationTestUtils.*;
import static org.junit.Assert.*;

@Category(MediumTests.class)
public class TestThriftReplicationSmallTests extends TestThriftReplicationBase {

  private static final Log LOG = LogFactory.getLog(TestReplicationSmallTests.class);

  protected static final int NB_ROWS_IN_BATCH = 100;
  protected static final int NB_ROWS_IN_BIG_BATCH = NB_ROWS_IN_BATCH * 10;
  protected static final int NB_RETRIES_FOR_BIG_BATCH = 30;
  
  private final static HBaseTestingUtility clusterA = new HBaseTestingUtility();
  private final static HBaseTestingUtility clusterB = new HBaseTestingUtility();
  private static HTable tableA;
  private static HTable tableB;
  protected static final byte[] row = Bytes.toBytes("row");

  @BeforeClass
  public static void setUpClass() throws Exception {
    int clusterAServerPort = HBaseTestingUtility.randomFreePort();
    int clusterBServerPort = HBaseTestingUtility.randomFreePort();

    setupConfiguration(clusterA, clusterAServerPort);
    setupConfiguration(clusterB, clusterBServerPort);

    addPeerThriftPort(clusterA, "1", clusterBServerPort);
    addPeerThriftPort(clusterB, "1", clusterAServerPort);

    HTableDescriptor table = createTestTableWithVersion(100);
    table.addFamily(new HColumnDescriptor("NO_REP_FAMILY"));

    clusterA.startMiniCluster();
    clusterB.startMiniCluster();

    createTableOnCluster(clusterA, table);
    createTableOnCluster(clusterB, table);

    tableA = getTestTable(clusterA, table);
    tableB = getTestTable(clusterB, table);

    addReplicationPeer("1", clusterA, clusterB);
    addReplicationPeer("1", clusterB, clusterA);
  }

  @Before
  public void setUp() throws Exception {
    tableA.setAutoFlush(true);
    tableB.setAutoFlush(true);
    // Starting and stopping replication can make us miss new logs,
    // rolling like this makes sure the most recent one gets added to the queue
    for ( JVMClusterUtil.RegionServerThread r :
        clusterA.getHBaseCluster().getRegionServerThreads()) {
      r.getRegionServer().getWAL().rollWriter();
    }
    clusterA.truncateTable(tableA.getTableName());
    // truncating the table will send one Delete per row to the slave cluster
    // in an async fashion, which is why we cannot just call truncateTable on
    // utility2 since late writes could make it to the slave in some way.
    // Instead, we truncate the first table and wait for all the Deletes to
    // make it to the slave.
    Scan scan = new Scan();
    int lastCount = 0;
    for (int i = 0; i < NB_RETRIES; i++) {
      if (i==NB_RETRIES-1) {
        fail("Waited too much time for truncate");
      }
      ResultScanner scanner = tableB.getScanner(scan);
      Result[] res = scanner.next(NB_ROWS_IN_BIG_BATCH);
      scanner.close();
      if (res.length != 0) {
        if (res.length < lastCount) {
          i--; // Don't increment timeout if we make progress
        }
        lastCount = res.length;
        LOG.info("Still got " + res.length + " rows");
        Thread.sleep(SLEEP_TIME);
      } else {
        break;
      }
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    clusterA.shutdownMiniCluster();
    clusterB.shutdownMiniCluster();
  }

  /**
   * Verify that version and column delete marker types are replicated
   * correctly.
   * @throws Exception
   */
  @Test(timeout=300000)
  public void testDeleteTypes() throws Exception {
    LOG.info("testDeleteTypes");
    final byte[] v1 = Bytes.toBytes("v1");
    final byte[] v2 = Bytes.toBytes("v2");
    final byte[] v3 = Bytes.toBytes("v3");

    long t = EnvironmentEdgeManager.currentTimeMillis();
    // create three versions for "row"
    Put put = new Put(row);
    put.add(ReplicationTestUtils.DEFAULT_FAMILY, row, t, v1);
    tableA.put(put);

    put = new Put(row);
    put.add(ReplicationTestUtils.DEFAULT_FAMILY, row, t+1, v2);
    tableA.put(put);

    put = new Put(row);
    put.add(ReplicationTestUtils.DEFAULT_FAMILY, row, t+2, v3);
    tableA.put(put);

    Get get = new Get(row);
    get.setMaxVersions();
    for (int i = 0; i < NB_RETRIES; i++) {
      if (i==NB_RETRIES-1) {
        fail("Waited too much time for put replication");
      }
      Result res = tableB.get(get);
      if (res.size() < 3) {
        LOG.info("Rows not available");
        Thread.sleep(SLEEP_TIME);
      } else {
        assertArrayEquals(res.raw()[0].getValue(), v3);
        assertArrayEquals(res.raw()[1].getValue(), v2);
        assertArrayEquals(res.raw()[2].getValue(), v1);
        break;
      }
    }
    // place a version delete marker (delete last version)
    Delete d = new Delete(row);
    d.deleteColumn(ReplicationTestUtils.DEFAULT_FAMILY, row, t);
    tableA.delete(d);

    get = new Get(row);
    get.setMaxVersions();
    for (int i = 0; i < NB_RETRIES; i++) {
      if (i==NB_RETRIES-1) {
        fail("Waited too much time for put replication");
      }
      Result res = tableB.get(get);
      if (res.size() > 2) {
        LOG.info("Version not deleted");
        Thread.sleep(SLEEP_TIME);
      } else {
        assertArrayEquals(res.raw()[0].getValue(), v3);
        assertArrayEquals(res.raw()[1].getValue(), v2);
        break;
      }
    }

    // place a column delete marker
    d = new Delete(row);
    d.deleteColumns(DEFAULT_FAMILY, row, t+2);
    tableA.delete(d);

    // now *both* of the remaining version should be deleted
    // at the replica
    get = new Get(row);
    for (int i = 0; i < NB_RETRIES; i++) {
      if (i==NB_RETRIES-1) {
        fail("Waited too much time for del replication");
      }
      Result res = tableB.get(get);
      if (res.size() >= 1) {
        LOG.info("Rows not deleted");
        Thread.sleep(SLEEP_TIME);
      } else {
        break;
      }
    }
  }

  /**
   * Add a row, check it's replicated, delete it, check's gone
   * @throws Exception
   */
  @Test(timeout=300000)
  public void testSimplePutDelete() throws Exception {
    LOG.info("testSimplePutDelete");
    Put put = new Put(row);
    put.add(DEFAULT_FAMILY, row, row);

    tableA.put(put);

    Get get = new Get(row);
    for (int i = 0; i < NB_RETRIES; i++) {
      if (i==NB_RETRIES-1) {
        fail("Waited too much time for put replication");
      }
      Result res = tableB.get(get);
      if (res.size() == 0) {
        LOG.info("Row not available");
        Thread.sleep(SLEEP_TIME);
      } else {
        assertArrayEquals(res.value(), row);
        break;
      }
    }

    Delete del = new Delete(row);
    tableA.delete(del);

    get = new Get(row);
    for (int i = 0; i < NB_RETRIES; i++) {
      if (i==NB_RETRIES-1) {
        fail("Waited too much time for del replication");
      }
      Result res = tableB.get(get);
      if (res.size() >= 1) {
        LOG.info("Row not deleted");
        Thread.sleep(SLEEP_TIME);
      } else {
        break;
      }
    }
  }

  /**
   * Try a small batch upload using the write buffer, check it's replicated
   * @throws Exception
   */
  @Test(timeout=300000)
  public void testSmallBatch() throws Exception {
    LOG.info("testSmallBatch");
    Put put;
    // normal Batch tests
    tableA.setAutoFlush(false);
    for (int i = 0; i < NB_ROWS_IN_BATCH; i++) {
      put = new Put(Bytes.toBytes(i));
      put.add(DEFAULT_FAMILY, row, row);
      tableA.put(put);
    }
    tableA.flushCommits();

    Scan scan = new Scan();

    ResultScanner scanner1 = tableA.getScanner(scan);
    Result[] res1 = scanner1.next(NB_ROWS_IN_BATCH);
    scanner1.close();
    assertEquals(NB_ROWS_IN_BATCH, res1.length);

    for (int i = 0; i < NB_RETRIES; i++) {
      if (i==NB_RETRIES-1) {
        fail("Waited too much time for normal batch replication");
      }
      ResultScanner scanner = tableB.getScanner(scan);
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

  /**
   * Test stopping replication, trying to insert, make sure nothing's
   * replicated, enable it, try replicating and it should work
   * @throws Exception
   */
  @Test(timeout=300000)
  public void testStartStop() throws Exception {

    // Test stopping replication
    changeReplicationState(clusterA, false);

    Put put = new Put(Bytes.toBytes("stop start"));
    put.add(DEFAULT_FAMILY, row, row);
    tableA.put(put);

    Get get = new Get(Bytes.toBytes("stop start"));
    for (int i = 0; i < NB_RETRIES; i++) {
      if (i == NB_RETRIES - 1) {
        break;
      }
      Result res = tableB.get(get);
      if (res.size() >= 1) {
        fail("Replication wasn't stopped");

      } else {
        LOG.info("Row not replicated, let's wait a bit more...");
        Thread.sleep(SLEEP_TIME);
      }
    }

    // Test restart replication
    changeReplicationState(clusterA, true);

    tableA.put(put);

    for (int i = 0; i < NB_RETRIES; i++) {
      if (i == NB_RETRIES - 1) {
        fail("Waited too much time for put replication");
      }
      Result res = tableB.get(get);
      if (res.size() == 0) {
        LOG.info("Row not available");
        Thread.sleep(SLEEP_TIME);
      } else {
        assertArrayEquals(res.value(), row);
        break;
      }
    }

    put = new Put(Bytes.toBytes("do not rep"));
    put.add("NO_REP_FAMILY".getBytes(), row, row);
    tableA.put(put);

    get = new Get(Bytes.toBytes("do not rep"));
    for (int i = 0; i < NB_RETRIES; i++) {
      if (i == NB_RETRIES - 1) {
        break;
      }
      Result res = tableB.get(get);
      if (res.size() >= 1) {
        fail("Not supposed to be replicated");
      } else {
        LOG.info("Row not replicated, let's wait a bit more...");
        Thread.sleep(SLEEP_TIME);
      }
    }

  }


  /**
   * Test disable/enable replication, trying to insert, make sure nothing's
   * replicated, enable it, the insert should be replicated
   *
   * @throws Exception
   */
  @Test(timeout = 300000)
  public void testDisableEnable() throws Exception {

    // Test disabling replication
    ReplicationAdmin clusterAAdmin = getReplicationAdmin(clusterA);
    clusterAAdmin.disablePeer("1");

    byte[] rowkey = Bytes.toBytes("disable enable");
    Put put = new Put(rowkey);
    put.add(DEFAULT_FAMILY, row, row);
    tableA.put(put);

    Get get = new Get(rowkey);
    for (int i = 0; i < NB_RETRIES; i++) {
      Result res = tableB.get(get);
      if (res.size() >= 1) {
        fail("Replication wasn't disabled");
      } else {
        LOG.info("Row not replicated, let's wait a bit more...");
        Thread.sleep(SLEEP_TIME);
      }
    }

    // Test enable replication
    clusterAAdmin.enablePeer("1");

    for (int i = 0; i < NB_RETRIES; i++) {
      Result res = tableB.get(get);
      if (res.size() == 0) {
        LOG.info("Row not available");
        Thread.sleep(SLEEP_TIME);
      } else {
        assertArrayEquals(res.value(), row);
        return;
      }
    }
    fail("Waited too much time for put replication");
  }

  /**
   * Integration test for TestReplicationAdmin, removes and re-add a peer
   * cluster
   *
   * @throws Exception
   */
  @Test(timeout=300000)
  public void testAddAndRemoveClusters() throws Exception {
    LOG.info("testAddAndRemoveClusters");
    ReplicationAdmin clusterAAdmin = getReplicationAdmin(clusterA);
    clusterAAdmin.removePeer("1");
    Thread.sleep(SLEEP_TIME * 3);
    byte[] rowKey = Bytes.toBytes("Won't be replicated");
    Put put = new Put(rowKey);
    put.add(DEFAULT_FAMILY, row, row);
    tableA.put(put);

    Get get = new Get(rowKey);
    for (int i = 0; i < NB_RETRIES; i++) {
      if (i == NB_RETRIES-1) {
        break;
      }
      Result res = tableB.get(get);
      if (res.size() >= 1) {
        fail("Not supposed to be replicated");
      } else {
        LOG.info("Row not replicated, let's wait a bit more...");
        Thread.sleep(SLEEP_TIME);
      }
    }

    ReplicationTestUtils.addReplicationPeer("1", clusterA, clusterB);
    Thread.sleep(SLEEP_TIME*3);
    rowKey = Bytes.toBytes("do rep");
    put = new Put(rowKey);
    put.add(DEFAULT_FAMILY, row, row);
    LOG.info("Adding new row");
    tableA.put(put);

    get = new Get(rowKey);
    for (int i = 0; i < NB_RETRIES/2; i++) {
      if (i==NB_RETRIES-1) {
        fail("Waited too much time for put replication");
      }
      Result res = tableB.get(get);
      if (res.size() == 0) {
        LOG.info("Row not available");
        Thread.sleep(SLEEP_TIME*i);
      } else {
        assertArrayEquals(res.value(), row);
        break;
      }
    }
  }


  /**
   * Do a more intense version testSmallBatch, one  that will trigger
   * hlog rolling and other non-trivial code paths
   * @throws Exception
   */
  @Test(timeout=300000)
  public void loadTesting() throws Exception {
    tableA.setWriteBufferSize(1024);
    tableA.setAutoFlush(false);
    for (int i = 0; i < NB_ROWS_IN_BIG_BATCH; i++) {
      Put put = new Put(Bytes.toBytes(i));
      put.add(DEFAULT_FAMILY, row, row);
      tableA.put(put);
    }
    tableA.flushCommits();

    Scan scan = new Scan();

    ResultScanner scanner = tableA.getScanner(scan);
    Result[] res = scanner.next(NB_ROWS_IN_BIG_BATCH);
    scanner.close();

    assertEquals(NB_ROWS_IN_BIG_BATCH, res.length);

    scan = new Scan();

    for (int i = 0; i < NB_RETRIES_FOR_BIG_BATCH; i++) {

      scanner = tableB.getScanner(scan);
      res = scanner.next(NB_ROWS_IN_BIG_BATCH);
      scanner.close();
      if (res.length != NB_ROWS_IN_BIG_BATCH) {
        if (i == NB_RETRIES_FOR_BIG_BATCH-1) {
          int lastRow = -1;
          for (Result result : res) {
            int currentRow = Bytes.toInt(result.getRow());
            for (int row = lastRow+1; row < currentRow; row++) {
              LOG.error("Row missing: " + row);
            }
            lastRow = currentRow;
          }
          LOG.error("Last row: " + lastRow);
          fail("Waited too much time for normal batch replication, "
              + res.length + " instead of " + NB_ROWS_IN_BIG_BATCH);
        } else {
          LOG.info("Only got " + res.length + " rows");
          Thread.sleep(SLEEP_TIME);
        }
      } else {
        break;
      }
    }
  }

  /**
   * Test for HBASE-8663
   * Create two new Tables with colfamilies enabled for replication then run
   * ReplicationAdmin.listReplicated(). Finally verify the table:colfamilies. Note:
   * TestReplicationAdmin is a better place for this testing but it would need mocks.
   * @throws Exception
   */
  @Test(timeout = 300000)
  public void testVerifyListReplicatedTable() throws Exception {
    LOG.info("testVerifyListReplicatedTable");

    final String tName = "VerifyListReplicated_";
    final String colFam = "cf1";
    final int numOfTables = 3;

    HBaseAdmin hadmin = clusterA.getHBaseAdmin();

    // Create Tables
    for (int i = 0; i < numOfTables; i++) {
      HTableDescriptor ht = new HTableDescriptor(tName + i);
      HColumnDescriptor cfd = new HColumnDescriptor(colFam);
      cfd.setScope(HConstants.REPLICATION_SCOPE_GLOBAL);
      ht.addFamily(cfd);
      hadmin.createTable(ht);
    }

    // verify the result
    List<HashMap<String, String>> replicationColFams = getReplicationAdmin(clusterA).listReplicated();
    int[] match = new int[numOfTables]; // array of 3 with init value of zero

    for (int i = 0; i < replicationColFams.size(); i++) {
      HashMap<String, String> replicationEntry = replicationColFams.get(i);
      String tn = replicationEntry.get(ReplicationAdmin.TNAME);
      if ((tn.startsWith(tName)) && replicationEntry.get(ReplicationAdmin.CFNAME).equals(colFam)) {
        int m = Integer.parseInt(tn.substring(tn.length() - 1)); // get the last digit
        match[m]++; // should only increase once
      }
    }

    // check the matching result
    for (int i = 0; i < match.length; i++) {
      assertTrue("listReplicated() does not match table " + i, (match[i] == 1));
    }

    // drop tables
    for (int i = 0; i < numOfTables; i++) {
      String ht = tName + i;
      hadmin.disableTable(ht);
      hadmin.deleteTable(ht);
    }

    hadmin.close();
  }

  private void changeReplicationState(HBaseTestingUtility cluster, boolean newState)
      throws IOException, InterruptedException {
    new ReplicationAdmin(cluster.getConfiguration()).setReplicating(newState);
    Thread.sleep(SLEEP_TIME*3);
  }

  private ReplicationAdmin getReplicationAdmin(HBaseTestingUtility cluster) throws IOException {
    return new ReplicationAdmin(cluster.getConfiguration());
  }

  @org.junit.Rule
  public org.apache.hadoop.hbase.ResourceCheckerJUnitRule cu =
      new org.apache.hadoop.hbase.ResourceCheckerJUnitRule();


}
