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
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.HTestConst;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@InterfaceAudience.Private
public class TestSerialReplicationBase {
  private static final Log LOG = LogFactory.getLog(TestSerialReplicationBase.class);

  protected static Configuration conf1;
  protected static Configuration conf2;

  protected static HBaseTestingUtility utility1;
  protected static HBaseTestingUtility utility2;

  protected final int TIMEOUT = 300000;
  protected final int SLEEP_TIME = 1000;

  protected static final String PEER_ID = "1";
  private static final byte[] famName = Bytes.toBytes("f");
  private static final byte[] VALUE = Bytes.toBytes("v");
  private static final byte[] ROW = Bytes.toBytes("r");
  protected static final byte[][] ROWS = HTestConst.makeNAscii(ROW, 100);

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    conf1 = HBaseConfiguration.create();
    conf1.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/1");
    // smaller block size and capacity to trigger more operations
    // and test them
    conf1.setInt("hbase.regionserver.hlog.blocksize", 1024 * 20);
    conf1.setInt("hbase.regionserver.maxlogs", 10);
    conf1.setLong("hbase.master.logcleaner.ttl", 10);
    conf1.setBoolean("dfs.support.append", true);
    conf1.setLong(HConstants.THREAD_WAKE_FREQUENCY, 100);
    conf1.setStrings(CoprocessorHost.USER_REGION_COPROCESSOR_CONF_KEY,
        "org.apache.hadoop.hbase.replication.TestMasterReplication$CoprocessorCounter");
    conf1.setLong("replication.source.per.peer.node.bandwidth", 100L);// Each WAL is 120 bytes
    conf1.setLong("replication.source.size.capacity", 1L);
    conf1.setLong("replication.source.sleepforretries", 2000L);
    conf1.setLong(HConstants.REPLICATION_SERIALLY_WAITING_KEY, 1000L);
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

    utility1.startMiniCluster(1, 3);
    utility2.startMiniCluster(1, 1);

    ReplicationPeerConfig rpc = new ReplicationPeerConfig();
    rpc.setClusterKey(utility2.getClusterKey()).setSerial(true);
    utility1.getHBaseAdmin().addReplicationPeer(PEER_ID, rpc);

    utility1.getHBaseAdmin().setBalancerRunning(false, true);
    // Move meta to first rs, as the test will kill rs 2
    moveRegion(TableName.META_TABLE_NAME, 0);
  }

  @AfterClass
  public static void setUpAfterClass() throws Exception {
    utility2.shutdownMiniCluster();
    utility1.shutdownMiniCluster();
  }

  protected void createTable(TableName tableName) throws IOException {
    HTableDescriptor table = new HTableDescriptor(tableName);
    HColumnDescriptor fam = new HColumnDescriptor(famName);
    fam.setScope(HConstants.REPLICATION_SCOPE_SERIAL);
    table.addFamily(fam);
    utility1.getHBaseAdmin().createTable(table);
  }

  protected void writeData(HTable table, int start, int end, int step, boolean skipWAL)
      throws IOException {
    for (int i = start; i < end; i += step) {
      Put put = new Put(ROWS[i]);
      put.add(famName, VALUE, VALUE);
      if (skipWAL) {
        put.setDurability(Durability.SKIP_WAL);
      }
      table.put(put);
    }
  }

  protected List<Integer> getRowNumbers(List<Cell> cells) {
    List<Integer> listOfRowNumbers = new ArrayList<>();
    for (Cell c : cells) {
      listOfRowNumbers.add(Integer.parseInt(Bytes
          .toString(c.getRowArray(), c.getRowOffset() + ROW.length,
              c.getRowLength() - ROW.length)));
    }
    return listOfRowNumbers;
  }

  protected static void moveRegion(TableName tableName, int index)
      throws IOException, InterruptedException {
    LOG.debug("Move table " + tableName + " region to RegionServer " + index);
    List<Pair<HRegionInfo, ServerName>> regions =
        utility1.getMiniHBaseCluster().getMaster().getCatalogTracker()
            .getTableRegionsAndLocations(tableName);
    assertEquals(1, regions.size());
    HRegionInfo regionInfo = regions.get(0).getFirst();
    ServerName name = utility1.getHBaseCluster().getRegionServer(index).getServerName();
    utility1.getHBaseAdmin()
        .move(regionInfo.getEncodedNameAsBytes(), Bytes.toBytes(name.getServerName()));
    try {
      Thread.sleep(5000L); // wait to complete
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  protected void assertListSerial(List<Integer> list, int start, int step) {
    for (int i = 0; i < list.size(); i++) {
      assertTrue(list.get(i) == start + step * i);
    }
  }
}
