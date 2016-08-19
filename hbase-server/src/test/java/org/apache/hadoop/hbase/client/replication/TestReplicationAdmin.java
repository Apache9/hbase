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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationPeer;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;



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

  private static ReplicationAdmin admin;

  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniZKCluster();
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setBoolean(HConstants.REPLICATION_ENABLE_KEY, HConstants.REPLICATION_ENABLE_DEFAULT);
    admin = new ReplicationAdmin(conf);
  }

  /**
   * Simple testing of adding and removing peers, basically shows that
   * all interactions with ZK work
   * @throws Exception
   */
  @Test
  public void testAddRemovePeer() throws Exception {
    ReplicationPeerConfig rpc1 = new ReplicationPeerConfig();
    rpc1.setClusterKey(KEY_ONE);
    ReplicationPeerConfig rpc2 = new ReplicationPeerConfig();
    rpc2.setClusterKey(KEY_SECOND);
    // Add a valid peer
    admin.addPeer(ID_ONE, rpc1, null);
    // try adding the same (fails)
    try {
      admin.addPeer(ID_ONE, rpc1, null);
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
      admin.addPeer(ID_SECOND, rpc2, null);
    } catch (IllegalStateException iae) {
      fail();
    }
    assertEquals(2, admin.getPeersCount());
    // Remove the first peer we added
    admin.removePeer(ID_ONE);
    assertEquals(1, admin.getPeersCount());
    admin.removePeer(ID_SECOND);
    assertEquals(0, admin.getPeersCount());
  }

  /**
   * basic checks that when we add a peer that it is enabled, and that we can disable
   * @throws Exception
   */
  @Test
  public void testEnableDisable() throws Exception {
    ReplicationPeerConfig rpc1 = new ReplicationPeerConfig();
    rpc1.setClusterKey(KEY_ONE);
    admin.addPeer(ID_ONE, rpc1, null);
    assertEquals(1, admin.getPeersCount());
    assertTrue(admin.getPeerState(ID_ONE));
    admin.disablePeer(ID_ONE);

    assertFalse(admin.getPeerState(ID_ONE));
    try {
      admin.getPeerState(ID_SECOND);
    } catch (IllegalArgumentException iae) {
      // OK!
    }
    admin.removePeer(ID_ONE);
  }

  @Test
  public void testSettingThePeerProtocol() throws Exception {
    ReplicationPeerConfig config = new ReplicationPeerConfig();
    config.setClusterKey(KEY_ONE);
    config.setProtocol(ReplicationPeer.PeerProtocol.THRIFT);
    admin.addPeer(ID_ONE, config, null);
    assertEquals(1, admin.getPeersCount());
    assertTrue(admin.getPeerState(ID_ONE));
    assertEquals(ReplicationPeer.PeerProtocol.THRIFT, admin.getPeerConfig(ID_ONE).getProtocol());
  }

  @Test
  public void testAppendPeerTableCFs() throws Exception {
    ReplicationPeerConfig rpc1 = new ReplicationPeerConfig();
    rpc1.setClusterKey(KEY_ONE);
    TableName tab1 = TableName.valueOf("t1");
    TableName tab2 = TableName.valueOf("t2");
    TableName tab3 = TableName.valueOf("t3");
    TableName tab4 = TableName.valueOf("t4");
    TableName tab5 = TableName.valueOf("t5");
    TableName tab6 = TableName.valueOf("t6");

    // Add a valid peer
    admin.addPeer(ID_ONE, rpc1, null);

    Map<TableName, List<String>> tableCFs = new HashMap<TableName, List<String>>();
    tableCFs.put(tab1, null);
    admin.appendPeerTableCFs(ID_ONE, tableCFs);
    Map<TableName, List<String>> result = admin.getPeerTableCFs(ID_ONE);
    assertEquals(1, result.size());
    assertEquals(true, result.containsKey(tab1));
    assertNull(result.get(tab1));

    // append table t2 to replication
    tableCFs.clear();
    tableCFs.put(tab2, null);
    admin.appendPeerTableCFs(ID_ONE, tableCFs);
    result = admin.getPeerTableCFs(ID_ONE);
    assertEquals(2, result.size());
    assertTrue("Should contain t1", result.containsKey(tab1));
    assertTrue("Should contain t2", result.containsKey(tab2));
    assertNull(result.get(tab1));
    assertNull(result.get(tab2));

    // append table column family: f1 of t3 to replication
    tableCFs.clear();
    tableCFs.put(tab3, new ArrayList<String>());
    tableCFs.get(tab3).add("f1");
    admin.appendPeerTableCFs(ID_ONE, tableCFs);
    result = admin.getPeerTableCFs(ID_ONE);
    assertEquals(3, result.size());
    assertTrue("Should contain t1", result.containsKey(tab1));
    assertTrue("Should contain t2", result.containsKey(tab2));
    assertTrue("Should contain t3", result.containsKey(tab3));
    assertNull(result.get(tab1));
    assertNull(result.get(tab2));
    assertEquals(1, result.get(tab3).size());
    assertEquals("f1", result.get(tab3).get(0));

    tableCFs.clear();
    tableCFs.put(tab4, new ArrayList<String>());
    tableCFs.get(tab4).add("f1");
    tableCFs.get(tab4).add("f2");
    admin.appendPeerTableCFs(ID_ONE, tableCFs);
    result = admin.getPeerTableCFs(ID_ONE);
    assertEquals(4, result.size());
    assertTrue("Should contain t1", result.containsKey(tab1));
    assertTrue("Should contain t2", result.containsKey(tab2));
    assertTrue("Should contain t3", result.containsKey(tab3));
    assertTrue("Should contain t4", result.containsKey(tab4));
    assertNull(result.get(tab1));
    assertNull(result.get(tab2));
    assertEquals(1, result.get(tab3).size());
    assertEquals("f1", result.get(tab3).get(0));
    assertEquals(2, result.get(tab4).size());
    assertEquals("f1", result.get(tab4).get(0));
    assertEquals("f2", result.get(tab4).get(1));

    // append "table5" => [], then append "table5" => ["f1"]
    tableCFs.clear();
    tableCFs.put(tab5, new ArrayList<String>());
    admin.appendPeerTableCFs(ID_ONE, tableCFs);
    tableCFs.clear();
    tableCFs.put(tab5, new ArrayList<String>());
    tableCFs.get(tab5).add("f1");
    admin.appendPeerTableCFs(ID_ONE, tableCFs);
    result = ReplicationSerDeHelper.parseTableCFsFromConfig(admin.getPeerTableCFs(ID_ONE));
    assertEquals(5, result.size());
    assertTrue("Should contain t5", result.containsKey(tab5));
    // null means replication all cfs of tab5
    assertNull(result.get(tab5));

    // append "table6" => ["f1"], then append "table6" => []
    tableCFs.clear();
    tableCFs.put(tab6, new ArrayList<String>());
    tableCFs.get(tab6).add("f1");
    admin.appendPeerTableCFs(ID_ONE, tableCFs);
    tableCFs.clear();
    tableCFs.put(tab6, new ArrayList<String>());
    admin.appendPeerTableCFs(ID_ONE, tableCFs);
    result = ReplicationSerDeHelper.parseTableCFsFromConfig(admin.getPeerTableCFs(ID_ONE));
    assertEquals(6, result.size());
    assertTrue("Should contain t6", result.containsKey(tab6));
    // null means replication all cfs of tab6
    assertNull(result.get(tab6));

    admin.removePeer(ID_ONE);
  }

  @Test
  public void testRemovePeerTableCFs() throws Exception {
    ReplicationPeerConfig rpc1 = new ReplicationPeerConfig();
    rpc1.setClusterKey(KEY_ONE);
    TableName tab1 = TableName.valueOf("t1");
    TableName tab2 = TableName.valueOf("t2");
    TableName tab3 = TableName.valueOf("t3");
    // Add a valid peer
    admin.addPeer(ID_ONE, rpc1, null);
    Map<TableName, List<String>> tableCFs = new HashMap<TableName, List<String>>();
    try {
      tableCFs.put(tab3, null);
      admin.removePeerTableCFs(ID_ONE, tableCFs);
      assertTrue(false);
    } catch (ReplicationException e) {
    }
    assertNull(admin.getPeerTableCFs(ID_ONE));

    tableCFs.clear();
    tableCFs.put(tab1, null);
    tableCFs.put(tab2, new ArrayList<String>());
    tableCFs.get(tab2).add("cf1");
    admin.setPeerTableCFs(ID_ONE, tableCFs);
    try {
      tableCFs.clear();
      tableCFs.put(tab3, null);
      admin.removePeerTableCFs(ID_ONE, tableCFs);
      assertTrue(false);
    } catch (ReplicationException e) {
    }
    Map<TableName, List<String>> result = admin.getPeerTableCFs(ID_ONE);
    assertEquals(2, result.size());
    assertTrue("Should contain t1", result.containsKey(tab1));
    assertTrue("Should contain t2", result.containsKey(tab2));
    assertNull(result.get(tab1));
    assertEquals(1, result.get(tab2).size());
    assertEquals("cf1", result.get(tab2).get(0));

    try {
      tableCFs.clear();
      tableCFs.put(tab1, new ArrayList<String>());
      tableCFs.get(tab1).add("f1");
      admin.removePeerTableCFs(ID_ONE, tableCFs);
      assertTrue(false);
    } catch (ReplicationException e) {
    }
    tableCFs.clear();
    tableCFs.put(tab1, null);
    admin.removePeerTableCFs(ID_ONE, tableCFs);
    result = admin.getPeerTableCFs(ID_ONE);
    assertEquals(1, result.size());
    assertEquals(1, result.get(tab2).size());
    assertEquals("cf1", result.get(tab2).get(0));

    try {
      tableCFs.clear();
      tableCFs.put(tab2, null);
      admin.removePeerTableCFs(ID_ONE, tableCFs);
      assertTrue(false);
    } catch (ReplicationException e) {
    }
    tableCFs.clear();
    tableCFs.put(tab2, new ArrayList<String>());
    tableCFs.get(tab2).add("cf1");
    admin.removePeerTableCFs(ID_ONE, tableCFs);
    assertNull(admin.getPeerTableCFs(ID_ONE));
    admin.removePeer(ID_ONE);
  }

  @Test
  public void testSetPeerBandwidth() throws Exception {
    ReplicationPeerConfig rpc = new ReplicationPeerConfig();
    rpc.setClusterKey(KEY_ONE);
    // Add a valid peer
    admin.addPeer(ID_ONE, rpc, null);
    assertEquals(0, admin.getPeerConfig(ID_ONE).getBandwidth());

    admin.setPeerBandwidth(ID_ONE, 102400);
    assertEquals(102400, admin.getPeerConfig(ID_ONE).getBandwidth());

    rpc.setClusterKey(KEY_SECOND);
    rpc.setBandwidth(1048576);
    admin.addPeer(ID_SECOND, rpc, null);
    assertEquals(1048576, admin.getPeerConfig(ID_SECOND).getBandwidth());
    admin.removePeer(ID_ONE);
    admin.removePeer(ID_SECOND);
  }
}
