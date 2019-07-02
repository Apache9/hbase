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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.fail;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ LargeTests.class })
public class TestReplicationEditsDroppedWithNoSuchColumnFamily {

  private static final Log LOG = LogFactory.getLog(TestReplicationEditsDroppedWithDroppedTable.class);

  private static Configuration sourceConf = HBaseConfiguration.create();
  private static Configuration sinkConf = HBaseConfiguration.create();

  protected static HBaseTestingUtility sourceUtility;
  protected static HBaseTestingUtility sinkUtility;

  private static HBaseAdmin sourceAdmin;
  private static HBaseAdmin sinkAdmin;

  private static final String namespace = "NS";
  private static final TableName TABLE = TableName.valueOf("t");
  private static final byte[] ROW = Bytes.toBytes("row");
  private static final byte[] FAMILY = Bytes.toBytes("f");
  private static final byte[] NO_SUCH_COLUMN_FAMILY = Bytes.toBytes("n");
  private static final byte[] QUALIFIER = Bytes.toBytes("q");
  private static final byte[] VALUE = Bytes.toBytes("value");

  private static final String PEER_ID = "1";
  private static final long SLEEP_TIME = 1000;
  private static final int NB_RETRIES = 10;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // Set true to filter replication edits for dropped table
    sourceConf
        .setBoolean(HBaseReplicationEndpoint.REPLICATION_DROP_ON_DELETED_COLUMN_FAMILY_KEY, true);
    sourceConf.setBoolean(HConstants.REPLICATION_SYNC_TABLE_SCHEMA, false);
    sourceConf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/1");
    sourceConf.setInt("replication.source.nb.capacity", 1);
    sourceUtility = new HBaseTestingUtility(sourceConf);
    sourceUtility.startMiniZKCluster();
    MiniZooKeeperCluster miniZK = sourceUtility.getZkCluster();
    sourceConf = sourceUtility.getConfiguration();

    sinkConf = HBaseConfiguration.create(sourceConf);
    sinkConf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/2");
    sinkUtility = new HBaseTestingUtility(sinkConf);
    sinkUtility.setZkCluster(miniZK);

    sourceUtility.startMiniCluster(1);
    sinkUtility.startMiniCluster(1);

    sourceAdmin = sourceUtility.getHBaseAdmin();
    sinkAdmin = sinkUtility.getHBaseAdmin();

    NamespaceDescriptor nsDesc = NamespaceDescriptor.create(namespace).build();
    sourceAdmin.createNamespace(nsDesc);
    sinkAdmin.createNamespace(nsDesc);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    sinkUtility.shutdownMiniCluster();
    sourceUtility.shutdownMiniCluster();
  }

  @Before
  public void setup() throws Exception {
    // Roll log
    for (JVMClusterUtil.RegionServerThread r : sourceUtility.getHBaseCluster()
        .getRegionServerThreads()) {
      sourceUtility.getHBaseAdmin().rollHLogWriter(r.getRegionServer().getServerName().getServerName());
    }
    // add peer
    ReplicationPeerConfig rpc = new ReplicationPeerConfig();
    rpc.setClusterKey(sinkUtility.getClusterKey()).setReplicateAllUserTables(true);
    sourceAdmin.addReplicationPeer(PEER_ID, rpc);
    // create table
    createTable(TABLE);
  }

  @After
  public void tearDown() throws Exception {
    sourceAdmin.removeReplicationPeer(PEER_ID);
    Thread.sleep(SLEEP_TIME);
    sourceUtility.getMiniHBaseCluster().getMaster().getReplicationZKNodeCleanerChore().choreForTesting();
    Thread.sleep(SLEEP_TIME);
    sourceAdmin.disableTable(TABLE);
    sourceAdmin.deleteTable(TABLE);
    sinkAdmin.disableTable(TABLE);
    sinkAdmin.deleteTable(TABLE);
  }

  private void createTable(TableName tableName) throws Exception {
    HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);

    HColumnDescriptor columnDescriptor = new HColumnDescriptor(FAMILY);
    columnDescriptor.setScope(HConstants.REPLICATION_SCOPE_GLOBAL);
    tableDescriptor.addFamily(columnDescriptor);

    HColumnDescriptor noSuchColumnDescriptor = new HColumnDescriptor(NO_SUCH_COLUMN_FAMILY);
    noSuchColumnDescriptor.setScope(HConstants.REPLICATION_SCOPE_GLOBAL);
    tableDescriptor.addFamily(noSuchColumnDescriptor);

    sourceAdmin.createTable(tableDescriptor);
    sinkAdmin.createTable(tableDescriptor);

    sourceUtility.waitUntilAllRegionsAssigned(tableName);
    sinkUtility.waitUntilAllRegionsAssigned(tableName);
  }

  @Test
  public void testEditsDroppedWithDeletedColumnFamily() throws Exception {
    sourceAdmin.disableReplicationPeer(PEER_ID);

    try (HConnection conn = HConnectionManager.createConnection(sourceConf);
        HTableInterface t = conn.getTable(TABLE)) {
      Put put = new Put(ROW);
      put.add(NO_SUCH_COLUMN_FAMILY, QUALIFIER, VALUE);
      t.put(put);
    }

    sourceAdmin.disableTable(TABLE);
    sourceAdmin.deleteColumn(TABLE, NO_SUCH_COLUMN_FAMILY);
    sourceAdmin.enableTable(TABLE);

    sinkAdmin.disableTable(TABLE);
    sinkAdmin.deleteColumn(TABLE, NO_SUCH_COLUMN_FAMILY);
    sinkAdmin.enableTable(TABLE);

    sourceAdmin.enableReplicationPeer(PEER_ID);

    verifyReplicationProceeded();
  }

  @Test
  public void testEditsBehindDeletedColumnFamilyTiming() throws Exception {
    sourceAdmin.disableReplicationPeer(PEER_ID);
    try (HConnection conn = HConnectionManager.createConnection(sourceConf);
        HTableInterface sourceTable = conn.getTable(TABLE)) {
      Put put = new Put(ROW);
      put.add(NO_SUCH_COLUMN_FAMILY, QUALIFIER, VALUE);
      sourceTable.put(put);
    }

    // Only delete table from peer cluster
    sinkAdmin.disableTable(TABLE);
    sinkAdmin.deleteColumn(TABLE, NO_SUCH_COLUMN_FAMILY);
    sinkAdmin.enableTable(TABLE);

    sourceAdmin.enableReplicationPeer(PEER_ID);

    // the source table still exists, replication should be stalled
    verifyReplicationStuck();
    sourceAdmin.disableTable(TABLE);
    // still stuck, source table still exists
    sourceAdmin.deleteColumn(TABLE, NO_SUCH_COLUMN_FAMILY);
    sourceAdmin.enableTable(TABLE);
    // now the source table is gone, replication should proceed, the
    // offending edits be dropped
    verifyReplicationProceeded();
  }

  private void verifyReplicationProceeded() throws Exception {
    try (HConnection conn = HConnectionManager.createConnection(sourceConf);
        HTableInterface sourceTable = conn.getTable(TABLE)) {
      Put put = new Put(ROW);
      put.add(FAMILY, QUALIFIER, VALUE);
      sourceTable.put(put);
    }
    try (HTable sinkTable = new HTable(sinkConf, TABLE)) {
      for (int i = 0; i < NB_RETRIES; i++) {
        if (i == NB_RETRIES - 1) {
          fail("Waited too much time for put replication");
        }
        Result result = sinkTable.get(new Get(ROW).addColumn(FAMILY, QUALIFIER));
        if (result == null || result.isEmpty()) {
          LOG.info("Row not available in peer cluster");
          Thread.sleep(SLEEP_TIME);
        } else {
          assertArrayEquals(VALUE, result.getValue(FAMILY, QUALIFIER));
          break;
        }
      }
    }
  }

  private void verifyReplicationStuck() throws Exception {
    try (HConnection conn = HConnectionManager.createConnection(sourceConf);
        HTableInterface normalTable = conn.getTable(TABLE)) {
      Put put = new Put(ROW);
      put.add(FAMILY, QUALIFIER, VALUE);
      normalTable.put(put);
    }
    try (HTable normalTable = new HTable(sinkConf, TABLE)) {
      for (int i = 0; i < NB_RETRIES; i++) {
        Result result = normalTable.get(new Get(ROW).addColumn(FAMILY, QUALIFIER));
        if (result != null && !result.isEmpty()) {
          fail("Edit should have been stuck behind deleted column families, but value is " + Bytes
              .toString(result.getValue(FAMILY, QUALIFIER)));
        } else {
          LOG.info("Row not replicated, let's wait a bit more...");
          Thread.sleep(SLEEP_TIME);
        }
      }
    }
  }
}

