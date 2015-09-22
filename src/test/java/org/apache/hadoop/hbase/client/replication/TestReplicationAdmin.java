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
package org.apache.hadoop.hbase.client.replication;

import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.replication.ReplicationZookeeper;
import org.apache.hadoop.hbase.replication.regionserver.ReplicationSourceManager;
import org.junit.BeforeClass;
import org.junit.AfterClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;

/**
 * Unit testing of ReplicationAdmin
 */
@Category(MediumTests.class)
public class TestReplicationAdmin {

  private static final Log LOG =
      LogFactory.getLog(TestReplicationAdmin.class);
  private final static HBaseTestingUtility TEST_UTIL =
      new HBaseTestingUtility();

  private final String ID_ONE = "1";
  private final String KEY_ONE = "127.0.0.1:2181:/hbase";
  private final String ID_SECOND = "2";
  private final String KEY_SECOND = "127.0.0.1:2181:/hbase2";

  private static ReplicationSourceManager manager;
  private static ReplicationAdmin admin;
  private static AtomicBoolean replicating = new AtomicBoolean(true);

  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniZKCluster();
    TEST_UTIL.startMiniCluster(1);
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setBoolean(HConstants.REPLICATION_ENABLE_KEY, true);
    admin = new ReplicationAdmin(conf);
    Path oldLogDir = new Path(TEST_UTIL.getDataTestDir(),
        HConstants.HREGION_OLDLOGDIR_NAME);
    Path logDir = new Path(TEST_UTIL.getDataTestDir(),
        HConstants.HREGION_LOGDIR_NAME);
    manager = new ReplicationSourceManager(admin.getReplicationZk(), conf,
        // The following stopper never stops so that we can respond
        // to zk notification
        new Stoppable() {
          @Override
          public void stop(String why) {}
          @Override
          public boolean isStopped() {return false;}
        }, FileSystem.get(conf), replicating, logDir, oldLogDir);
  }

  /**
   * @throws java.lang.Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * Simple testing of adding and removing peers, basically shows that
   * all interactions with ZK work
   * @throws Exception
   */
  @Test
  public void testAddRemovePeer() throws Exception {
    assertEquals(0, manager.getSources().size());
    // Add a valid peer
    admin.addPeer(ID_ONE, KEY_ONE);
    // try adding the same (fails)
    try {
      admin.addPeer(ID_ONE, KEY_ONE);
    } catch (IllegalArgumentException iae) {
      // OK!
    }
    assertEquals(1, admin.getPeersCount());
    // Try to remove an inexisting peer
    try {
      admin.removePeer(ID_SECOND);
      fail();
    } catch (IllegalArgumentException iae) {
      // OK!
    }
    assertEquals(1, admin.getPeersCount());
    // Add a second since multi-slave is supported
    try {
      admin.addPeer(ID_SECOND, KEY_SECOND);
    } catch (IllegalStateException iae) {
      fail();
      // OK!
    }
    assertEquals(2, admin.getPeersCount());
    // Remove the first peer we added
    admin.removePeer(ID_ONE);
    assertEquals(1, admin.getPeersCount());
  }

  @Test
  public void testRemovePeerTableCFs() throws Exception {
    assertEquals(0, manager.getSources().size());
    String table1 = "t1";
    HBaseAdmin hAdmin = new HBaseAdmin(TEST_UTIL.getConfiguration());

    if (!hAdmin.tableExists(table1)) {
      HTableDescriptor desc = new HTableDescriptor(table1);
      desc.addFamily(new HColumnDescriptor("cf"));
      hAdmin.createTable(desc);
    }
    // Add a valid peer without tables
    admin.addPeer(ID_ONE, KEY_ONE, "ENABLED");
    Assert.assertTrue(admin.getPeerTableCFs(ID_ONE).isEmpty());
    // remove from empty tableCFs
    admin.removePeerTableCFs(ID_ONE, "t1");
    Assert.assertTrue(admin.getPeerTableCFs(ID_ONE).isEmpty());
    
    admin.removePeer(ID_ONE);
    // Add a valid peer
    admin.addPeer(ID_ONE, KEY_ONE, "ENABLED", "t1");
    Assert.assertEquals("t1", admin.getPeerTableCFs(ID_ONE));
    // tableCFs not contained
    admin.removePeerTableCFs(ID_ONE, "t2");
    Assert.assertEquals("t1", admin.getPeerTableCFs(ID_ONE));
    // empty tableCFs after removed
    admin.removePeerTableCFs(ID_ONE, "t1");
    Assert.assertEquals("t1", admin.getPeerTableCFs(ID_ONE));

    String table2 = "t2";
    if (!hAdmin.tableExists(table2)) {
      HTableDescriptor desc = new HTableDescriptor(table2);
      desc.addFamily(new HColumnDescriptor("cf"));
      hAdmin.createTable(desc);
    }
    admin.appendPeerTableCFs(ID_ONE, table2);
    Assert.assertEquals("t1;t2", admin.getPeerTableCFs(ID_ONE));
    
    String table3 = "t3";
    if (!hAdmin.tableExists(table3)) {
      HTableDescriptor desc = new HTableDescriptor(table3);
      desc.addFamily(new HColumnDescriptor("cf"));
      hAdmin.createTable(desc);
    }
    admin.appendPeerTableCFs(ID_ONE, table3);
    Assert.assertEquals("t1;t2;t3", admin.getPeerTableCFs(ID_ONE));
    
    admin.removePeerTableCFs(ID_ONE, "t2");
    Assert.assertEquals("t1;;t3", admin.getPeerTableCFs(ID_ONE));
    
    admin.appendPeerTableCFs(ID_ONE, "t1");
    Assert.assertEquals("t1;;t3;t1", admin.getPeerTableCFs(ID_ONE));
    admin.removePeerTableCFs(ID_ONE, "t1");
    Assert.assertEquals(";;t3;", admin.getPeerTableCFs(ID_ONE));
    
    admin.removePeer(ID_ONE);
    hAdmin.close();
  }
  

  @Test
  public void testAppendPeerTableCFs() throws Exception {
    assertEquals(0, manager.getSources().size());
    String table1 = "t1";
    HBaseAdmin hAdmin = new HBaseAdmin(TEST_UTIL.getConfiguration());

    if (!hAdmin.tableExists(table1)) {
      HTableDescriptor desc = new HTableDescriptor(table1);
      desc.addFamily(new HColumnDescriptor("cf"));
      hAdmin.createTable(desc);
    }
    // Add a valid peer
    admin.addPeer(ID_ONE, KEY_ONE, "ENABLED", table1);

    String tableCFs = admin.getPeerTableCFs(ID_ONE);
    assertEquals("t1", tableCFs);

    String table2 = "t2";
    if (!hAdmin.tableExists(table2)) {
      HTableDescriptor desc = new HTableDescriptor(table2);
      desc.addFamily(new HColumnDescriptor("cf"));
      hAdmin.createTable(desc);
    }
    // append t2
    admin.appendPeerTableCFs(ID_ONE, table2);
    tableCFs = admin.getPeerTableCFs(ID_ONE);
    
    assertEquals("t1;t2", tableCFs);
    admin.removePeer(ID_ONE);

    // Add a valid peer without tables
    admin.addPeer(ID_ONE, KEY_ONE, "ENABLED");
    // append t2
    admin.appendPeerTableCFs(ID_ONE, table2);
    tableCFs = admin.getPeerTableCFs(ID_ONE);

    assertEquals("t2", tableCFs);
    admin.removePeer(ID_ONE);
  }
  
  private void removeExistPeerIds() throws Exception {
    Set<String> peerIds = admin.listPeers().keySet();
    for (String peerId : peerIds) {
      admin.removePeer(peerId);
    }
    assertEquals(0, admin.getPeersCount());
  }
  
  @Test
  public void testAddRemovePeerWithProtocol() throws Exception {
    assertEquals(0, manager.getSources().size());
    removeExistPeerIds();
    // Add a valid peer
    admin.addPeer(ID_ONE, KEY_ONE, null, "", null,
      ReplicationZookeeper.PeerProtocol.THRIFT.name());
    // try adding the same (fails)
    try {
      admin.addPeer(ID_ONE, KEY_ONE);
      fail();
    } catch (IllegalArgumentException iae) {
      // OK!
    }
    assertEquals(1, admin.getPeersCount());
    assertEquals(ReplicationZookeeper.PeerProtocol.THRIFT.name(), admin.getPeerProtocol(ID_ONE));
    // Try to remove an inexisting peer
    try {
      admin.removePeer(ID_SECOND);
      fail();
    } catch (IllegalArgumentException iae) {
      // OK!
    }
    assertEquals(1, admin.getPeersCount());
    // Add a second since multi-slave is supported
    try {
      admin.addPeer(ID_SECOND, KEY_SECOND);
    } catch (IllegalStateException iae) {
      fail();
      // OK!
    }
    assertEquals(2, admin.getPeersCount());
    assertEquals(ReplicationZookeeper.PeerProtocol.NATIVE.name(), admin.getPeerProtocol(ID_SECOND));
    // Remove the first peer we added
    admin.removePeer(ID_ONE);
    assertEquals(1, admin.getPeersCount());
    admin.removePeer(ID_SECOND);
  }

  @org.junit.Rule
  public org.apache.hadoop.hbase.ResourceCheckerJUnitRule cu =
    new org.apache.hadoop.hbase.ResourceCheckerJUnitRule();
}

