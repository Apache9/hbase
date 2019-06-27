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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.ipc.RemoteWithExtrasException;
import org.apache.hadoop.hbase.replication.regionserver.HBaseInterClusterReplicationEndpoint;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.apache.hadoop.util.StringUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.fail;

import java.util.Arrays;

/**
 * Replication with dropped table will stuck as the default REPLICATION_DROP_ON_DELETED_TABLE_KEY
 * is false.
 */
@Category({ LargeTests.class })
public class TestReplicationStuckWithDroppedTable {
  private static final Log LOG = LogFactory.getLog(TestReplicationEditsDroppedWithDroppedTable.class);

  private static Configuration conf1 = HBaseConfiguration.create();
  private static Configuration conf2 = HBaseConfiguration.create();

  protected static HBaseTestingUtility utility1;
  protected static HBaseTestingUtility utility2;

  private static HBaseAdmin admin1;
  private static HBaseAdmin admin2;

  private static final TableName NORMAL_TABLE = TableName.valueOf("normal-table");
  private static final TableName DROPPED_TABLE = TableName.valueOf("dropped-table");
  private static final byte[] ROW = Bytes.toBytes("row");
  private static final byte[] FAMILY = Bytes.toBytes("f");
  private static final byte[] QUALIFIER = Bytes.toBytes("q");
  private static final byte[] VALUE = Bytes.toBytes("value");

  private static final String PEER_ID = "1";
  private static final long SLEEP_TIME = 1000;
  private static final int NB_RETRIES = 10;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    conf1.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/1");
    conf1.setInt("replication.source.nb.capacity", 1);
    utility1 = new HBaseTestingUtility(conf1);
    utility1.startMiniZKCluster();
    MiniZooKeeperCluster miniZK = utility1.getZkCluster();
    conf1 = utility1.getConfiguration();

    conf2 = HBaseConfiguration.create(conf1);
    conf2.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/2");
    utility2 = new HBaseTestingUtility(conf2);
    utility2.setZkCluster(miniZK);

    utility1.startMiniCluster(1);
    utility2.startMiniCluster(1);

    admin1 = utility1.getHBaseAdmin();
    admin2 = utility2.getHBaseAdmin();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    utility2.shutdownMiniCluster();
    utility1.shutdownMiniCluster();
  }

  private void createTable(TableName tableName) throws Exception {
    HTableDescriptor desc = new HTableDescriptor(tableName);
    HColumnDescriptor familyDesc = new HColumnDescriptor(FAMILY);
    familyDesc.setScope(HConstants.REPLICATION_SCOPE_GLOBAL);
    desc.addFamily(familyDesc);
    // as default REPLICATION_SYNC_TABLE_SCHEMA is true, only create it in source cluster
    admin1.createTable(desc);
    utility1.waitUntilAllRegionsAssigned(tableName);
    utility2.waitUntilAllRegionsAssigned(tableName);
  }

  @Test
  public void testBatchTableNotFoundException() throws Exception {
    TableName tb = TableName.valueOf("testBatchTableNotFoundException");
    utility1.createTable(tb, Bytes.toBytes("f"));
    Configuration conf = HBaseConfiguration.create(conf1);
    conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 3);
    try (HConnection conn = HConnectionManager.createConnection(conf1);
        HTableInterface table = conn.getTable(tb)) {
      // getTable before because we need to cache the table descriptor in cache for salting
      // checking, so that we won't throw the exception when getTable in the next conn.getTable.
      admin1.disableTable(tb);
      admin1.deleteTable(tb);

      HTableInterface newTable = conn.getTable(tb);

      Put put = new Put(ROW).add(FAMILY, QUALIFIER, VALUE);
      boolean caughtTableNotFoundException = false;
      try {
        newTable.batch(Arrays.asList(put));
      } catch (Exception e) {
        LOG.info("caught the exception when batching: ", e);
        caughtTableNotFoundException = true;
        Assert.assertTrue(e instanceof RetriesExhaustedWithDetailsException);
        Assert.assertTrue(e.getCause() instanceof TableNotFoundException);
        RemoteWithExtrasException re = new RemoteWithExtrasException(e.getClass().getName(),
            StringUtils.stringifyException(e), false);
        Assert.assertTrue(HBaseInterClusterReplicationEndpoint.isTableNotFoundException(re));
      }
      Assert.assertTrue(caughtTableNotFoundException);
    }
  }

  @Test
  public void testEditsStuckBehindDroppedTable() throws Exception {
    // add peer
    ReplicationPeerConfig rpc = new ReplicationPeerConfig();
    rpc.setClusterKey(utility2.getClusterKey()).setReplicateAllUserTables(true);
    admin1.addReplicationPeer(PEER_ID, rpc);

    // create table
    createTable(NORMAL_TABLE);
    createTable(DROPPED_TABLE);

    admin1.disableReplicationPeer(PEER_ID);

    try (HConnection conn = HConnectionManager.createConnection(conf1);
        HTableInterface droppedTable = conn.getTable(DROPPED_TABLE)) {
      Put put = new Put(ROW);
      put.add(FAMILY, QUALIFIER, VALUE);
      droppedTable.put(put);
    }

    admin1.disableTable(DROPPED_TABLE);
    admin1.deleteTable(DROPPED_TABLE);
    admin2.disableTable(DROPPED_TABLE);
    admin2.deleteTable(DROPPED_TABLE);

    admin1.enableReplicationPeer(PEER_ID);

    verifyReplicationStuck();

    // Remove peer
    admin1.removeReplicationPeer(PEER_ID);
    // Drop table
    admin1.disableTable(NORMAL_TABLE);
    admin1.deleteTable(NORMAL_TABLE);
    admin2.disableTable(NORMAL_TABLE);
    admin2.deleteTable(NORMAL_TABLE);
  }

  private void verifyReplicationStuck() throws Exception {
    try (HConnection conn = HConnectionManager.createConnection(conf1);
        HTableInterface normalTable = conn.getTable(NORMAL_TABLE)) {
      Put put = new Put(ROW);
      put.add(FAMILY, QUALIFIER, VALUE);
      normalTable.put(put);
    }
    try (HTable normalTable = new HTable(conf2, NORMAL_TABLE)) {
      for (int i = 0; i < NB_RETRIES; i++) {
        Result result = normalTable.get(new Get(ROW).addColumn(FAMILY, QUALIFIER));
        if (result != null && !result.isEmpty()) {
          fail("Edit should have been stuck behind dropped tables, but value is " + Bytes
              .toString(result.getValue(FAMILY, QUALIFIER)));
        } else {
          LOG.info("Row not replicated, let's wait a bit more...");
          Thread.sleep(SLEEP_TIME);
        }
      }
    }
  }
}
