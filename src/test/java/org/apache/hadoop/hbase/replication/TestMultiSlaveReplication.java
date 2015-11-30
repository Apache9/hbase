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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.replication.ReplicationAdmin;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.replication.ReplicationZookeeper.PeerProtocol;
import org.apache.hadoop.hbase.replication.ReplicationZookeeper.PeerState;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(LargeTests.class)
public class TestMultiSlaveReplication {

  private static final Log LOG = LogFactory.getLog(TestReplicationBase.class);

  private static Configuration conf1;
  private static Configuration conf2;
  private static Configuration conf3;

  private static HBaseTestingUtility utility1;
  private static HBaseTestingUtility utility2;
  private static HBaseTestingUtility utility3;
  private static final long SLEEP_TIME = 500;
  private static final int NB_RETRIES = 10;

  private static final byte[] tableName = Bytes.toBytes("test");
  private static final byte[] tabAName = Bytes.toBytes("TA");
  private static final byte[] tabBName = Bytes.toBytes("TB");
  private static final byte[] tabCName = Bytes.toBytes("TC");
  private static final byte[] famName = Bytes.toBytes("f");
  private static final byte[] f1Name = Bytes.toBytes("f1");
  private static final byte[] f2Name = Bytes.toBytes("f2");
  private static final byte[] f3Name = Bytes.toBytes("f3");
  private static final byte[] row = Bytes.toBytes("row");
  private static final byte[] row1 = Bytes.toBytes("row1");
  private static final byte[] row2 = Bytes.toBytes("row2");
  private static final byte[] row3 = Bytes.toBytes("row3");
  private static final byte[] noRepfamName = Bytes.toBytes("norep");
  private static final byte[] val = Bytes.toBytes("myval");

  private static HTableDescriptor table;
  private static HTableDescriptor tabA;
  private static HTableDescriptor tabB;
  private static HTableDescriptor tabC;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    conf1 = HBaseConfiguration.create();
    conf1.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/1");
    // smaller block size and capacity to trigger more operations
    // and test them
    conf1.setInt("hbase.regionserver.hlog.blocksize", 1024*20);
    conf1.setInt("replication.source.size.capacity", 1024);
    conf1.setLong("replication.source.sleepforretries", 100);
    conf1.setInt("hbase.regionserver.maxlogs", 10);
    conf1.setLong("hbase.master.logcleaner.ttl", 10);
    conf1.setBoolean(HConstants.REPLICATION_ENABLE_KEY, true);
    conf1.setBoolean("dfs.support.append", true);
    conf1.setLong(HConstants.THREAD_WAKE_FREQUENCY, 100);
    conf1.setStrings(CoprocessorHost.USER_REGION_COPROCESSOR_CONF_KEY,
        "org.apache.hadoop.hbase.replication.TestMasterReplication$CoprocessorCounter");

    utility1 = new HBaseTestingUtility(conf1);
    utility1.startMiniZKCluster();
    MiniZooKeeperCluster miniZK = utility1.getZkCluster();
    // By setting the mini ZK cluster through this method, even though this is
    // already utility1's mini ZK cluster, we are telling utility1 not to shut
    // the mini ZK cluster when we shut down the HBase cluster.
    utility1.setZkCluster(miniZK);
    new ZooKeeperWatcher(conf1, "cluster1", null, true);

    conf2 = new Configuration(conf1);
    conf2.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/2");

    conf3 = new Configuration(conf1);
    conf3.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/3");

    utility2 = new HBaseTestingUtility(conf2);
    utility2.setZkCluster(miniZK);
    new ZooKeeperWatcher(conf2, "cluster3", null, true);

    utility3 = new HBaseTestingUtility(conf3);
    utility3.setZkCluster(miniZK);
    new ZooKeeperWatcher(conf3, "cluster3", null, true);

    table = new HTableDescriptor(tableName);
    HColumnDescriptor fam = new HColumnDescriptor(famName);
    fam.setScope(HConstants.REPLICATION_SCOPE_GLOBAL);
    table.addFamily(fam);
    fam = new HColumnDescriptor(noRepfamName);
    table.addFamily(fam);

    tabA = new HTableDescriptor(tabAName);
    fam = new HColumnDescriptor(f1Name);
    fam.setScope(HConstants.REPLICATION_SCOPE_GLOBAL);
    tabA.addFamily(fam);
    fam = new HColumnDescriptor(f2Name);
    fam.setScope(HConstants.REPLICATION_SCOPE_GLOBAL);
    tabA.addFamily(fam);
    fam = new HColumnDescriptor(f3Name);
    fam.setScope(HConstants.REPLICATION_SCOPE_GLOBAL);
    tabA.addFamily(fam);

    tabB = new HTableDescriptor(tabBName);
    fam = new HColumnDescriptor(f1Name);
    fam.setScope(HConstants.REPLICATION_SCOPE_GLOBAL);
    tabB.addFamily(fam);
    fam = new HColumnDescriptor(f2Name);
    fam.setScope(HConstants.REPLICATION_SCOPE_GLOBAL);
    tabB.addFamily(fam);
    fam = new HColumnDescriptor(f3Name);
    fam.setScope(HConstants.REPLICATION_SCOPE_GLOBAL);
    tabB.addFamily(fam);

    tabC = new HTableDescriptor(tabCName);
    fam = new HColumnDescriptor(f1Name);
    fam.setScope(HConstants.REPLICATION_SCOPE_GLOBAL);
    tabC.addFamily(fam);
    fam = new HColumnDescriptor(f2Name);
    fam.setScope(HConstants.REPLICATION_SCOPE_GLOBAL);
    tabC.addFamily(fam);
    fam = new HColumnDescriptor(f3Name);
    fam.setScope(HConstants.REPLICATION_SCOPE_GLOBAL);
    tabC.addFamily(fam);
  }

  @Test(timeout=300000)
  public void testMultiSlaveReplication() throws Exception {
    LOG.info("testCyclicReplication");
    MiniHBaseCluster master = utility1.startMiniCluster();
    utility2.startMiniCluster();
    utility3.startMiniCluster();
    ReplicationAdmin admin1 = new ReplicationAdmin(conf1);

    new HBaseAdmin(conf1).createTable(table);
    new HBaseAdmin(conf2).createTable(table);
    new HBaseAdmin(conf3).createTable(table);
    HTable htable1 = new HTable(conf1, tableName);
    htable1.setWriteBufferSize(1024);
    HTable htable2 = new HTable(conf2, tableName);
    htable2.setWriteBufferSize(1024);
    HTable htable3 = new HTable(conf3, tableName);
    htable3.setWriteBufferSize(1024);
    
    admin1.addPeer("1", utility2.getClusterKey(), PeerState.ENABLED.toString(), "", null,
      PeerProtocol.NATIVE.toString());

    // put "row" and wait 'til it got around, then delete
    putAndWait(row, famName, htable1, htable2);
    deleteAndWait(row, htable1, htable2);
    // check it wasn't replication to cluster 3
    checkRow(row,0,htable3);

    putAndWait(row2, famName, htable1, htable2);

    // now roll the region server's logs
    new HBaseAdmin(conf1).rollHLogWriter(master.getRegionServer(0).getServerName().toString());
    // after the log was rolled put a new row
    putAndWait(row3, famName, htable1, htable2);

    admin1.addPeer("2", utility3.getClusterKey(), PeerState.ENABLED.toString(), "", null,
      PeerProtocol.NATIVE.toString());

    // put a row, check it was replicated to all clusters
    putAndWait(row1, famName, htable1, htable2, htable3);
    // delete and verify
    deleteAndWait(row1, htable1, htable2, htable3);

    // make sure row2 did not get replicated after
    // cluster 3 was added
    checkRow(row2,0,htable3);

    // row3 will get replicated, because it was in the
    // latest log
    checkRow(row3,1,htable3);

    Put p = new Put(row);
    p.add(famName, row, row);
    htable1.put(p);
    // now roll the logs again
    new HBaseAdmin(conf1).rollHLogWriter(master.getRegionServer(0)
        .getServerName().toString());

    // cleanup "row2", also conveniently use this to wait replication
    // to finish
    deleteAndWait(row2, htable1, htable2, htable3);
    // Even if the log was rolled in the middle of the replication
    // "row" is still replication.
    checkRow(row, 1, htable2);
    // Replication thread of cluster 2 may be sleeping, and since row2 is not there in it, 
    // we should wait before checking.
    checkWithWait(row, 1, htable3);

    // cleanup the rest
    deleteAndWait(row, htable1, htable2, htable3);
    deleteAndWait(row3, htable1, htable2, htable3);

    admin1.removePeer("1");
    admin1.removePeer("2");

    utility3.shutdownMiniCluster();
    utility2.shutdownMiniCluster();
    utility1.shutdownMiniCluster();
  }

  @Test(timeout=300000)
  public void testPerTableCFsMultiSlaveReplication() throws Exception {
    LOG.info("testPerTableCFsMultiSlaveReplication");
    MiniHBaseCluster master = utility1.startMiniCluster();
    utility2.startMiniCluster();
    utility3.startMiniCluster();
    ReplicationAdmin admin1 = new ReplicationAdmin(conf1);

    new HBaseAdmin(conf1).createTable(tabA);
    new HBaseAdmin(conf1).createTable(tabB);
    new HBaseAdmin(conf1).createTable(tabC);
    new HBaseAdmin(conf2).createTable(tabA);
    new HBaseAdmin(conf2).createTable(tabB);
    new HBaseAdmin(conf2).createTable(tabC);
    new HBaseAdmin(conf3).createTable(tabA);
    new HBaseAdmin(conf3).createTable(tabB);
    new HBaseAdmin(conf3).createTable(tabC);

    HTable htab1A = new HTable(conf1, tabAName);
    htab1A.setWriteBufferSize(1024);
    HTable htab2A = new HTable(conf2, tabAName);
    htab2A.setWriteBufferSize(1024);
    HTable htab3A = new HTable(conf3, tabAName);
    htab3A.setWriteBufferSize(1024);

    HTable htab1B = new HTable(conf1, tabBName);
    htab1B.setWriteBufferSize(1024);
    HTable htab2B = new HTable(conf2, tabBName);
    htab2B.setWriteBufferSize(1024);
    HTable htab3B = new HTable(conf3, tabBName);
    htab3B.setWriteBufferSize(1024);

    HTable htab1C = new HTable(conf1, tabCName);
    htab1C.setWriteBufferSize(1024);
    HTable htab2C = new HTable(conf2, tabCName);
    htab2C.setWriteBufferSize(1024);
    HTable htab3C = new HTable(conf3, tabCName);
    htab3C.setWriteBufferSize(1024);

    // A. add cluster2/cluster3 as peers to cluster1
    admin1.addPeer("2", utility2.getClusterKey(), "ENABLED", "TC; TB:f1,f3", null,
      PeerProtocol.NATIVE.toString());
    admin1.addPeer("3", utility3.getClusterKey(), "ENABLED", "TA; TB:f1,f2", null,
      PeerProtocol.NATIVE.toString());

    // A1. tableA can only replicated to cluster3
    putAndWaitWithFamily(row1, f1Name, val, htab1A, htab3A);
    checkRowWithFamily(row1, f1Name, 0, htab2A);
    deleteAndWaitWithFamily(row1, f1Name, htab1A, htab3A);

    putAndWaitWithFamily(row1, f2Name, val, htab1A, htab3A);
    checkRowWithFamily(row1, f2Name, 0, htab2A);
    deleteAndWaitWithFamily(row1, f2Name, htab1A, htab3A);

    putAndWaitWithFamily(row1, f3Name, val, htab1A, htab3A);
    checkRowWithFamily(row1, f3Name, 0, htab2A);
    deleteAndWaitWithFamily(row1, f3Name, htab1A, htab3A);

    // A2. cf 'f1' of tableB can replicated to both cluster2 and cluster3
    putAndWaitWithFamily(row1, f1Name, val, htab1B, htab2B, htab3B);
    deleteAndWaitWithFamily(row1, f1Name, htab1B, htab2B, htab3B);

    //  cf 'f2' of tableB can only replicated to cluster3
    putAndWaitWithFamily(row1, f2Name, val, htab1B, htab3B);
    checkRowWithFamily(row1, f2Name, 0, htab2B);
    deleteAndWaitWithFamily(row1, f2Name, htab1B, htab3B);

    //  cf 'f3' of tableB can only replicated to cluster2
    putAndWaitWithFamily(row1, f3Name, val, htab1B, htab2B);
    checkRowWithFamily(row1, f3Name, 0, htab3B);
    deleteAndWaitWithFamily(row1, f3Name, htab1B, htab2B);

    // A3. tableC can only replicated to cluster2
    putAndWaitWithFamily(row1, f1Name, val, htab1C, htab2C);
    checkRowWithFamily(row1, f1Name, 0, htab3C);
    deleteAndWaitWithFamily(row1, f1Name, htab1C, htab2C);

    putAndWaitWithFamily(row1, f2Name, val, htab1C, htab2C);
    checkRowWithFamily(row1, f2Name, 0, htab3C);
    deleteAndWaitWithFamily(row1, f2Name, htab1C, htab2C);

    putAndWaitWithFamily(row1, f3Name, val, htab1C, htab2C);
    checkRowWithFamily(row1, f3Name, 0, htab3C);
    deleteAndWaitWithFamily(row1, f3Name, htab1C, htab2C);


    // B. change peers' replicable table-cf config
    admin1.setPeerTableCFs("2", "TA:f1,f2; TC:f2,f3");
    admin1.setPeerTableCFs("3", "TB; TC:f3");

    // B1. cf 'f1' of tableA can only replicated to cluster2
    putAndWaitWithFamily(row2, f1Name, val, htab1A, htab2A);
    checkRowWithFamily(row2, f1Name, 0, htab3A);
    deleteAndWaitWithFamily(row2, f1Name, htab1A, htab2A);
    //     cf 'f2' of tableA can only replicated to cluster2
    putAndWaitWithFamily(row2, f2Name, val, htab1A, htab2A);
    checkRowWithFamily(row2, f2Name, 0, htab3A);
    deleteAndWaitWithFamily(row2, f2Name, htab1A, htab2A);
    //     cf 'f3' of tableA isn't replicable to either cluster2 or cluster3
    putAndWaitWithFamily(row2, f3Name, val, htab1A);
    checkRowWithFamily(row2, f3Name, 0, htab2A, htab3A);
    deleteAndWaitWithFamily(row2, f3Name, htab1A);

    // B2. tableB can only replicated to cluster3
    putAndWaitWithFamily(row2, f1Name, val, htab1B, htab3B);
    checkRowWithFamily(row2, f1Name, 0, htab2B);
    deleteAndWaitWithFamily(row2, f1Name, htab1B, htab3B);

    putAndWaitWithFamily(row2, f2Name, val, htab1B, htab3B);
    checkRowWithFamily(row2, f2Name, 0, htab2B);
    deleteAndWaitWithFamily(row2, f2Name, htab1B, htab3B);

    putAndWaitWithFamily(row2, f3Name, val, htab1B, htab3B);
    checkRowWithFamily(row2, f3Name, 0, htab2B);
    deleteAndWaitWithFamily(row2, f3Name, htab1B, htab3B);

    // B3. cf 'f1' of tableC non-replicable to either cluster
    putAndWaitWithFamily(row2, f1Name, val, htab1C);
    checkRowWithFamily(row2, f1Name, 0, htab2C, htab3C);
    deleteAndWaitWithFamily(row2, f1Name, htab1C);
    //     cf 'f2' of tableC can only replicated to cluster2
    putAndWaitWithFamily(row2, f2Name, val, htab1C, htab2C);
    checkRowWithFamily(row2, f2Name, 0, htab3C);
    deleteAndWaitWithFamily(row2, f2Name, htab1C, htab2C);
    //     cf 'f3' of tableC can replicated to cluster2 and cluster3
    putAndWaitWithFamily(row2, f3Name, val, htab1C, htab2C, htab3C);
    deleteAndWaitWithFamily(row2, f3Name, htab1C, htab2C, htab3C);

    admin1.removePeer("2");
    admin1.removePeer("3");

    utility3.shutdownMiniCluster();
    utility2.shutdownMiniCluster();
    utility1.shutdownMiniCluster();
  }
 
  private void checkWithWait(byte[] row, int count, HTable table) throws Exception {
    Get get = new Get(row);
    for (int i = 0; i < NB_RETRIES; i++) {
      if (i == NB_RETRIES - 1) {
        fail("Waited too much time while getting the row.");
      }
      boolean rowReplicated = false;
      Result res = table.get(get);
      if (res.size() >= 1) {
        LOG.info("Row is replicated");
        rowReplicated = true;
        assertEquals(count, res.size());
        break;
      }
      if (rowReplicated) {
        break;
      } else {
        Thread.sleep(SLEEP_TIME);
      }
    }
  }

  private void checkRowWithFamily(byte[] row, byte[] fam, int count, HTable... tables) throws IOException {
    Get get = new Get(row);
    get.addFamily(fam);
    for (HTable table : tables) {
      Result res = table.get(get);
      assertEquals(count, res.size());
    }
  }

  private void checkRow(byte[] row, int count, HTable... tables) throws IOException {
    Get get = new Get(row);
    for (HTable table : tables) {
      Result res = table.get(get);
      assertEquals(count, res.size());
    }
  }

  private void deleteAndWaitWithFamily(byte[] row, byte[] fam,
      HTable source, HTable... targets)
  throws Exception {
    Delete del = new Delete(row);
    del.deleteFamily(fam);
    source.delete(del);

    Get get = new Get(row);
    get.addFamily(fam);
    for (int i = 0; i < NB_RETRIES; i++) {
      if (i==NB_RETRIES-1) {
        fail("Waited too much time for del replication");
      }
      boolean removedFromAll = true;
      for (HTable target : targets) {
        Result res = target.get(get);
        if (res.size() >= 1) {
          LOG.info("Row not deleted");
          removedFromAll = false;
          break;
        }
      }
      if (removedFromAll) {
        break;
      } else {
        Thread.sleep(SLEEP_TIME);
      }
    }
  }

  private void deleteAndWait(byte[] row, HTable source, HTable... targets)
  throws Exception {
    Delete del = new Delete(row);
    source.delete(del);

    Get get = new Get(row);
    for (int i = 0; i < NB_RETRIES; i++) {
      if (i==NB_RETRIES-1) {
        fail("Waited too much time for del replication");
      }
      boolean removedFromAll = true;
      for (HTable target : targets) {
        Result res = target.get(get);
        if (res.size() >= 1) {
          LOG.info("Row not deleted");
          removedFromAll = false;
          break;
        }
      }
      if (removedFromAll) {
        break;
      } else {
        Thread.sleep(SLEEP_TIME);        
      }
    }
  }

  private void putAndWaitWithFamily(byte[] row, byte[] fam, byte[] val,
      HTable source, HTable... targets)
  throws Exception {
    Put put = new Put(row);
    put.add(fam, row, val);
    source.put(put);

    Get get = new Get(row);
    get.addFamily(fam);
    for (int i = 0; i < NB_RETRIES; i++) {
      if (i==NB_RETRIES-1) {
        fail("Waited too much time for put replication");
      }
      boolean replicatedToAll = true;
      for (HTable target : targets) {
        Result res = target.get(get);
        if (res.size() == 0) {
          LOG.info("Row not available");
          replicatedToAll = false;
          break;
        } else {
          assertEquals(res.size(), 1);
          assertArrayEquals(res.value(), val);
        }
      }
      if (replicatedToAll) {
        break;
      } else {
        Thread.sleep(SLEEP_TIME);
      }
    }
  }

  private void putAndWait(byte[] row, byte[] fam, HTable source, HTable... targets)
  throws Exception {
    Put put = new Put(row);
    put.add(fam, row, row);
    source.put(put);

    Get get = new Get(row);
    for (int i = 0; i < NB_RETRIES; i++) {
      if (i==NB_RETRIES-1) {
        fail("Waited too much time for put replication");
      }
      boolean replicatedToAll = true;
      for (HTable target : targets) {
        Result res = target.get(get);
        if (res.size() == 0) {
          LOG.info("Row not available");
          replicatedToAll = false;
          break;
        } else {
          assertArrayEquals(res.value(), row);
        }
      }
      if (replicatedToAll) {
        break;
      } else {
        Thread.sleep(SLEEP_TIME);
      }
    }
  }

  @org.junit.Rule
  public org.apache.hadoop.hbase.ResourceCheckerJUnitRule cu =
    new org.apache.hadoop.hbase.ResourceCheckerJUnitRule();
}

