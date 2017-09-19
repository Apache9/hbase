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

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos.ReplicationState.State;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.TestReplicationBase;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Category({ MediumTests.class })
public class TestCreateTableForReplicatedPeers extends TestReplicationBase {
  static HConnection connection1;
  static HConnection connection2;
  static HBaseAdmin admin1;
  static HBaseAdmin admin2;

  static final String ID_FIRST = "1";
  static final String ID_THIRD = "3";
  static final String TEST_NAMESPACE_STR = "test_ns";
  static final NamespaceDescriptor TEST_NAMESPACE =
      NamespaceDescriptor.create(TEST_NAMESPACE_STR).build();
  static final TableName TEST_TABLE_NAME1 = TableName.valueOf(TEST_NAMESPACE_STR, "test_table1");
  static final TableName TEST_TABLE_NAME2 = TableName.valueOf(TEST_NAMESPACE_STR, "test_table2");
  static final TableName TEST_TABLE_NAME3 = TableName.valueOf(TEST_NAMESPACE_STR, "test_table3");
  static final String COLUMN_FAMILY = "f1";

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TestReplicationBase.setUpBeforeClass();
    connection1 = HConnectionManager.createConnection(conf1);
    connection2 = HConnectionManager.createConnection(conf2);
    admin1 = new HBaseAdmin(connection1.getConfiguration());
    admin2 = new HBaseAdmin(connection2.getConfiguration());
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    admin1.close();
    admin2.close();
    connection1.close();
    connection2.close();
    TestReplicationBase.tearDownAfterClass();
  }

  @Test
  public void testOneWayReplication() throws IOException, ReplicationException {
    admin1.createNamespace(TEST_NAMESPACE);
    admin2.createNamespace(TEST_NAMESPACE);

    ReplicationAdmin replAdmin = new ReplicationAdmin(conf1);

    ReplicationPeerConfig rpc = new ReplicationPeerConfig();
    rpc.setClusterKey(utility2.getClusterKey());
    rpc.setReplicateAllUserTables(false);
    Map<TableName, List<String>> tableNames = new HashMap<TableName, List<String>>();
    tableNames.put(TEST_TABLE_NAME1, Collections.emptyList());
    rpc.setTableCFsMap(tableNames);
    rpc.setState(State.ENABLED);

    // add_peer '1', 'hbase://<cluster2>', TABLE_CFS => {"test_ns:test_table2"=>[]},
    // STATE=>'enabled'
    replAdmin.addPeer(ID_FIRST, rpc);

    HTableDescriptor htd1 = new HTableDescriptor(TEST_TABLE_NAME1);
    HColumnDescriptor hcd1 = new HColumnDescriptor(COLUMN_FAMILY);
    htd1.addFamily(hcd1);

    // create TEST_TABLE_NAME1 in source cluster, then sink cluster will create table too.
    Assert.assertEquals(admin1.tableExists(TEST_TABLE_NAME1), false);
    Assert.assertEquals(admin2.tableExists(TEST_TABLE_NAME1), false);
    admin1.createTable(htd1);
    Assert.assertEquals(admin1.tableExists(TEST_TABLE_NAME1), true);
    Assert.assertEquals(admin2.tableExists(TEST_TABLE_NAME1), true);

    ReplicationPeerConfig rpc2 = new ReplicationPeerConfig();
    rpc2.setClusterKey(utility2.getClusterKey());
    rpc2.setReplicateAllUserTables(false);
    rpc2.setNamespaces(Collections.singleton(TEST_NAMESPACE_STR));
    rpc2.setState(State.ENABLED);

    // add_peer '3', 'hbase://<cluster2>', NAMESPACE=>['test_ns'], STATE=>'enabled'
    replAdmin.addPeer(ID_THIRD, rpc2);

    HTableDescriptor htd2 = new HTableDescriptor(TEST_TABLE_NAME2);
    HColumnDescriptor hcd2 = new HColumnDescriptor(COLUMN_FAMILY);
    htd2.addFamily(hcd2);

    // create TEST_TABLE_NAME2 in source cluster, then sink cluster will create table too.
    Assert.assertEquals(admin1.tableExists(TEST_TABLE_NAME2), false);
    Assert.assertEquals(admin2.tableExists(TEST_TABLE_NAME2), false);
    admin1.createTable(htd2);
    Assert.assertEquals(admin1.tableExists(TEST_TABLE_NAME2), true);
    Assert.assertEquals(admin2.tableExists(TEST_TABLE_NAME2), true);

    // create TEST_TABLE_NAME3 in sink cluster, source cluster will not create table.
    HTableDescriptor htd3 = new HTableDescriptor(TEST_TABLE_NAME3);
    HColumnDescriptor hcd3 = new HColumnDescriptor(COLUMN_FAMILY);
    htd2.addFamily(hcd3);
    admin2.createTable(htd3);
    Assert.assertEquals(admin1.tableExists(TEST_TABLE_NAME3), false);
    Assert.assertEquals(admin2.tableExists(TEST_TABLE_NAME3), true);

    // cleanup
    replAdmin.removePeer(ID_FIRST);
    replAdmin.removePeer(ID_THIRD);
    replAdmin.close();
    utility1.deleteTable(TEST_TABLE_NAME1);
    utility2.deleteTable(TEST_TABLE_NAME1);
    utility1.deleteTable(TEST_TABLE_NAME2);
    utility2.deleteTable(TEST_TABLE_NAME2);
    utility2.deleteTable(TEST_TABLE_NAME3);
    admin1.deleteNamespace(TEST_NAMESPACE_STR);
    admin2.deleteNamespace(TEST_NAMESPACE_STR);
  }

  @Test
  public void testBidirectionalReplication() throws IOException, ReplicationException {
    admin1.createNamespace(TEST_NAMESPACE);
    admin2.createNamespace(TEST_NAMESPACE);

    ReplicationAdmin replAdmin1 = new ReplicationAdmin(conf1);
    ReplicationAdmin replAdmin2 = new ReplicationAdmin(conf2);

    ReplicationPeerConfig rpc = new ReplicationPeerConfig();
    rpc.setClusterKey(utility2.getClusterKey());
    rpc.setReplicateAllUserTables(false);
    Map<TableName, List<String>> tableNames = new HashMap<TableName, List<String>>();
    tableNames.put(TEST_TABLE_NAME1, Collections.emptyList());
    rpc.setTableCFsMap(tableNames);
    rpc.setState(State.ENABLED);

    // add_peer '1', 'hbase://<cluster2>', TABLE_CFS => {"test_ns:test_table2"=>[]},
    // STATE=>'enabled'
    replAdmin1.addPeer(ID_FIRST, rpc);
    rpc.setClusterKey(utility1.getClusterKey());
    replAdmin2.addPeer(ID_FIRST, rpc);

    HTableDescriptor htd1 = new HTableDescriptor(TEST_TABLE_NAME1);
    HColumnDescriptor hcd1 = new HColumnDescriptor(COLUMN_FAMILY);
    htd1.addFamily(hcd1);

    // create TEST_TABLE_NAME1 in source cluster, then sink cluster will create table too.
    Assert.assertEquals(admin1.tableExists(TEST_TABLE_NAME1), false);
    Assert.assertEquals(admin2.tableExists(TEST_TABLE_NAME1), false);
    admin1.createTable(htd1);
    Assert.assertEquals(admin1.tableExists(TEST_TABLE_NAME1), true);
    Assert.assertEquals(admin2.tableExists(TEST_TABLE_NAME1), true);

    ReplicationPeerConfig rpc2 = new ReplicationPeerConfig();
    rpc2.setClusterKey(utility2.getClusterKey());
    rpc2.setReplicateAllUserTables(false);
    rpc2.setNamespaces(Collections.singleton(TEST_NAMESPACE_STR));
    rpc2.setState(State.ENABLED);

    // add_peer '3', 'hbase://<cluster2>', NAMESPACE=>['test_ns'], STATE=>'enabled'
    replAdmin1.addPeer(ID_THIRD, rpc2);
    rpc2.setClusterKey(utility1.getClusterKey());
    replAdmin2.addPeer(ID_THIRD, rpc2);

    HTableDescriptor htd2 = new HTableDescriptor(TEST_TABLE_NAME2);
    HColumnDescriptor hcd2 = new HColumnDescriptor(COLUMN_FAMILY);
    htd2.addFamily(hcd2);

    // create TEST_TABLE_NAME2 in source cluster, then sink cluster will create table too.
    Assert.assertEquals(admin1.tableExists(TEST_TABLE_NAME2), false);
    Assert.assertEquals(admin2.tableExists(TEST_TABLE_NAME2), false);
    admin1.createTable(htd2);
    Assert.assertEquals(admin1.tableExists(TEST_TABLE_NAME2), true);
    Assert.assertEquals(admin2.tableExists(TEST_TABLE_NAME2), true);

    // create TEST_TABLE_NAME3 in sink cluster, source cluster will not create table.
    HTableDescriptor htd3 = new HTableDescriptor(TEST_TABLE_NAME3);
    HColumnDescriptor hcd3 = new HColumnDescriptor(COLUMN_FAMILY);
    htd2.addFamily(hcd3);
    admin2.createTable(htd3);
    Assert.assertEquals(admin1.tableExists(TEST_TABLE_NAME3), true);
    Assert.assertEquals(admin2.tableExists(TEST_TABLE_NAME3), true);

    // cleanup
    replAdmin1.removePeer(ID_FIRST);
    replAdmin1.removePeer(ID_THIRD);
    replAdmin1.close();
    replAdmin2.removePeer(ID_FIRST);
    replAdmin2.removePeer(ID_THIRD);
    replAdmin2.close();
    utility1.deleteTable(TEST_TABLE_NAME1);
    utility2.deleteTable(TEST_TABLE_NAME1);
    utility1.deleteTable(TEST_TABLE_NAME2);
    utility2.deleteTable(TEST_TABLE_NAME2);
    utility1.deleteTable(TEST_TABLE_NAME3);
    utility2.deleteTable(TEST_TABLE_NAME3);
    admin1.deleteNamespace(TEST_NAMESPACE_STR);
    admin2.deleteNamespace(TEST_NAMESPACE_STR);
  }
}
