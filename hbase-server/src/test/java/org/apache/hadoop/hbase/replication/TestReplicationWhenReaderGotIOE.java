/*
 * Copyright The Apache Software Foundation
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
package org.apache.hadoop.hbase.replication;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.HTestConst;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.replication.regionserver.ReplicationSource;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@Category(LargeTests.class)
public class TestReplicationWhenReaderGotIOE {
  private static final Log LOG = LogFactory.getLog(TestReplicationWhenReaderGotIOE.class);

  private static Configuration conf1;
  private static Configuration conf2;

  private static HBaseTestingUtility utility1;
  private static HBaseTestingUtility utility2;

  private static final int TIMEOUT = 180000;
  private static final int SLEEP_TIME = 1000;
  private static final int COUNT = 100;

  private static final String PEER_ID = "1";
  private static final byte[] FAMILY = Bytes.toBytes("f");
  private static final byte[] QUALIFIER = Bytes.toBytes("q");
  private static final byte[] VALUE = Bytes.toBytes("v");
  private static final byte[] ROW = Bytes.toBytes("r");
  private static final byte[][] ROWS = HTestConst.makeNAscii(ROW, COUNT);

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    conf1 = HBaseConfiguration.create();
    conf1.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/1");
    // Read 5 entries every time
    conf1.setLong("replication.source.nb.capacity", 5L);
    conf1.setLong("replication.source.sleepforretries", 2000L);
    conf1.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 5);
    conf1.setBoolean("hbase.assignment.usezk", false);

    utility1 = new HBaseTestingUtility(conf1);
    utility1.startMiniZKCluster();
    MiniZooKeeperCluster miniZK = utility1.getZkCluster();
    new ZooKeeperWatcher(conf1, "cluster1", null, true);

    conf2 = new Configuration(conf1);
    conf2.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/2");

    utility2 = new HBaseTestingUtility(conf2);
    utility2.setZkCluster(miniZK);
    new ZooKeeperWatcher(conf2, "cluster2", null, true);

    utility1.startMiniCluster(1, 1);
    utility2.startMiniCluster(1, 1);

    ReplicationPeerConfig rpc = new ReplicationPeerConfig();
    rpc.setClusterKey(utility2.getClusterKey());
    utility1.getHBaseAdmin().addReplicationPeer(PEER_ID, rpc);
  }

  @AfterClass
  public static void setUpAfterClass() throws Exception {
    utility2.shutdownMiniCluster();
    utility1.shutdownMiniCluster();
  }

  private void createTable(TableName tableName) throws IOException {
    HTableDescriptor table = new HTableDescriptor(tableName);
    HColumnDescriptor fam = new HColumnDescriptor(FAMILY);
    fam.setScope(HConstants.REPLICATION_SCOPE_GLOBAL);
    table.addFamily(fam);
    utility1.getHBaseAdmin().createTable(table);
  }

  @Test
  public void testCloseReaderThrowIOE() throws Exception {
    ReplicationSource.throwIOEWhenCloseReaderFirstTime = true;

    TableName tableName = TableName.valueOf("testCloseReaderThrowIOE");
    createTable(tableName);
    writeDataAndVeriyReplication(tableName);

    ReplicationSource.throwIOEWhenCloseReaderFirstTime = false;
  }

  @Test
  public void testReadWALEntryThrowIOE() throws Exception {
    ReplicationSource.throwIOEWhenReadWALEntryFirstTime = true;

    TableName tableName = TableName.valueOf("testReadWALEntryThrowIOE");
    createTable(tableName);
    writeDataAndVeriyReplication(tableName);

    ReplicationSource.throwIOEWhenReadWALEntryFirstTime = false;
  }

  private void writeDataAndVeriyReplication(TableName tableName) throws Exception {
    try (HTable t1 = new HTable(conf1, tableName); HTable t2 = new HTable(conf2, tableName)) {
      for (int i = 0; i < COUNT; i += 1) {
        Put put = new Put(ROWS[i]);
        put.add(FAMILY, QUALIFIER, VALUE);
        t1.put(put);
      }

      long start = System.currentTimeMillis();
      while (System.currentTimeMillis() - start < TIMEOUT) {
        int rowCount = 0;
        Scan scan = new Scan();
        scan.setCaching(100);
        StringBuilder sb = new StringBuilder();
        try (ResultScanner results = t2.getScanner(scan)) {
          for (Result result : results) {
            sb.append(Bytes.toString(result.getRow()) + ",");
            assertEquals(Bytes.toString(VALUE), Bytes.toString(result.getValue(FAMILY, QUALIFIER)));
            rowCount++;
          }
        }
        LOG.info("Pushed rows : " + sb.toString());
        if (rowCount == COUNT) {
          return;
        } else {
          LOG.info("Waiting all logs pushed to slave. Expected " + COUNT + " actual " + rowCount);
        }
        Thread.sleep(SLEEP_TIME);
      }
      fail("Wait too much for all logs been pushed");
    }
  }
}
