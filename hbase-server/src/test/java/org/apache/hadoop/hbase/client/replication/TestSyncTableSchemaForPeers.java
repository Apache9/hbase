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

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.TestReplicationBase;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Category({ MediumTests.class })
public class TestSyncTableSchemaForPeers extends TestReplicationBase {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestSyncTableSchemaForPeers.class);

  static Connection connection1;
  static Connection connection2;
  static Admin admin1;
  static Admin admin2;

  static final String ID_FIRST = "1";
  static final String ID_THIRD = "3";
  static final String TEST_NAMESPACE_STR = "test_ns";
  static final NamespaceDescriptor TEST_NAMESPACE =
      NamespaceDescriptor.create(TEST_NAMESPACE_STR).build();
  static final TableName TEST_TABLE_NAME1 = TableName.valueOf(TEST_NAMESPACE_STR, "test_table1");
  static final TableName TEST_TABLE_NAME2 = TableName.valueOf(TEST_NAMESPACE_STR, "test_table2");
  static final TableName TEST_TABLE_NAME3 = TableName.valueOf(TEST_NAMESPACE_STR, "test_table3");
  static final byte[] COLUMN_FAMILY = Bytes.toBytes("f1");
  static final byte[] NEW_COLUMN_FAMILY = Bytes.toBytes("new_f2");

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    CONF1.setBoolean(HConstants.REPLICATION_SYNC_TABLE_SCHEMA, true);
    TestReplicationBase.setUpBeforeClass();
    connection1 = ConnectionFactory.createConnection(CONF1);
    connection2 = ConnectionFactory.createConnection(CONF2);
    admin1 = connection1.getAdmin();
    admin2 = connection2.getAdmin();
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

    Map<TableName, List<String>> tableNames = new HashMap<>();
    tableNames.put(TEST_TABLE_NAME1, Collections.emptyList());
    ReplicationPeerConfig rpc =
        ReplicationPeerConfig.newBuilder().setClusterKey(UTIL2.getClusterKey())
            .setReplicateAllUserTables(false).setTableCFsMap(tableNames).build();

    // add_peer '1', 'hbase://<cluster2>', TABLE_CFS => {"test_ns:test_table"=>[]},
    // STATE=>'enabled'
    admin1.addReplicationPeer(ID_FIRST, rpc);

    TableDescriptor htd1 = TableDescriptorBuilder.newBuilder(TEST_TABLE_NAME1)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(COLUMN_FAMILY).build()).build();

    // create TEST_TABLE_NAME1 in source cluster, then sink cluster will create table too.
    assertEquals(admin1.tableExists(TEST_TABLE_NAME1), false);
    assertEquals(admin2.tableExists(TEST_TABLE_NAME1), false);
    admin1.createTable(htd1);
    assertEquals(admin1.tableExists(TEST_TABLE_NAME1), true);
    assertEquals(admin2.tableExists(TEST_TABLE_NAME1), true);

    // alter 't1', NAME => 'new_f2', VERSIONS => 5 (TEST_TABLE_NAME1)
    ColumnFamilyDescriptor hcd = ColumnFamilyDescriptorBuilder.newBuilder(NEW_COLUMN_FAMILY)
        .setMaxVersions(5).setMinVersions(5).build();
    assertTrue(
      admin1.getDescriptor(TEST_TABLE_NAME1).getColumnFamily(NEW_COLUMN_FAMILY) == null);
    assertTrue(
      admin2.getDescriptor(TEST_TABLE_NAME1).getColumnFamily(NEW_COLUMN_FAMILY) == null);
    admin1.addColumnFamily(TEST_TABLE_NAME1, hcd);

    ColumnFamilyDescriptor hcdSource =
        admin1.getDescriptor(TEST_TABLE_NAME1).getColumnFamily(NEW_COLUMN_FAMILY);
    ColumnFamilyDescriptor hcdPeer =
        admin2.getDescriptor(TEST_TABLE_NAME1).getColumnFamily(NEW_COLUMN_FAMILY);
    assertNotNull(hcdSource);
    assertNotNull(hcdPeer);
    assertEquals(hcdSource, hcdPeer);

    // Modify the same column. and column change will not sync to peer cluster.
    hcd = ColumnFamilyDescriptorBuilder.newBuilder(NEW_COLUMN_FAMILY).setMaxVersions(2)
        .setMinVersions(1).build();
    admin1.modifyColumnFamily(TEST_TABLE_NAME1, hcd);
    hcdSource = admin1.getDescriptor(TEST_TABLE_NAME1).getColumnFamily(NEW_COLUMN_FAMILY);
    hcdPeer = admin2.getDescriptor(TEST_TABLE_NAME1).getColumnFamily(NEW_COLUMN_FAMILY);
    assertNotNull(hcdSource);
    assertNotNull(hcdPeer);
    assertEquals(1, hcdSource.getMinVersions());
    assertEquals(2, hcdSource.getMaxVersions());
    assertEquals(5, hcdPeer.getMaxVersions());
    assertEquals(5, hcdPeer.getMaxVersions());
    assertNotEquals(hcdSource, hcdPeer);

    // delete the column.
    admin1.deleteColumnFamily(TEST_TABLE_NAME1, NEW_COLUMN_FAMILY);
    assertNull(admin1.getDescriptor(TEST_TABLE_NAME1).getColumnFamily(NEW_COLUMN_FAMILY));
    assertNull(admin2.getDescriptor(TEST_TABLE_NAME1).getColumnFamily(NEW_COLUMN_FAMILY));

    ReplicationPeerConfig rpc2 = ReplicationPeerConfig.newBuilder()
        .setClusterKey(UTIL2.getClusterKey()).setReplicateAllUserTables(false)
        .setNamespaces(Collections.singleton(TEST_NAMESPACE_STR)).build();

    // add_peer '3', 'hbase://<cluster2>', NAMESPACE=>['test_ns'], STATE=>'enabled'
    admin1.addReplicationPeer(ID_THIRD, rpc2);

    TableDescriptor htd2 = TableDescriptorBuilder.newBuilder(TEST_TABLE_NAME2)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(COLUMN_FAMILY).build()).build();

    // create TEST_TABLE_NAME2 in source cluster, then sink cluster will create table too.
    assertEquals(admin1.tableExists(TEST_TABLE_NAME2), false);
    assertEquals(admin2.tableExists(TEST_TABLE_NAME2), false);
    admin1.createTable(htd2);
    assertEquals(admin1.tableExists(TEST_TABLE_NAME2), true);
    assertEquals(admin2.tableExists(TEST_TABLE_NAME2), true);

    // alter 't2', NAME => 'new_f2', VERSIONS => 5 (TEST_TABLE_NAME2)
    hcd = ColumnFamilyDescriptorBuilder.newBuilder(NEW_COLUMN_FAMILY).setMaxVersions(5)
        .setMinVersions(5).build();
    assertTrue(
      admin1.getDescriptor(TEST_TABLE_NAME2).getColumnFamily(NEW_COLUMN_FAMILY) == null);
    assertTrue(
      admin2.getDescriptor(TEST_TABLE_NAME2).getColumnFamily(NEW_COLUMN_FAMILY) == null);

    admin1.addColumnFamily(TEST_TABLE_NAME2, hcd);
    hcdSource = admin1.getDescriptor(TEST_TABLE_NAME2).getColumnFamily(NEW_COLUMN_FAMILY);
    hcdPeer = admin2.getDescriptor(TEST_TABLE_NAME2).getColumnFamily(NEW_COLUMN_FAMILY);
    assertNotNull(hcdSource);
    assertNotNull(hcdPeer);
    assertEquals(hcdSource, hcdPeer);

    // Modify the same column. and column change will not sync to peer cluster.
    hcd = ColumnFamilyDescriptorBuilder.newBuilder(NEW_COLUMN_FAMILY).setMaxVersions(2)
        .setMinVersions(1).build();
    admin1.modifyColumnFamily(TEST_TABLE_NAME2, hcd);
    hcdSource = admin1.getDescriptor(TEST_TABLE_NAME2).getColumnFamily(NEW_COLUMN_FAMILY);
    hcdPeer = admin2.getDescriptor(TEST_TABLE_NAME2).getColumnFamily(NEW_COLUMN_FAMILY);
    assertNotNull(hcdSource);
    assertNotNull(hcdPeer);
    assertEquals(1, hcdSource.getMinVersions());
    assertEquals(2, hcdSource.getMaxVersions());
    assertEquals(5, hcdPeer.getMinVersions());
    assertEquals(5, hcdPeer.getMaxVersions());
    assertNotEquals(hcdSource, hcdPeer);

    // delete the column.
    admin1.deleteColumnFamily(TEST_TABLE_NAME2, NEW_COLUMN_FAMILY);
    assertNull(admin1.getDescriptor(TEST_TABLE_NAME2).getColumnFamily(NEW_COLUMN_FAMILY));
    assertNull(admin2.getDescriptor(TEST_TABLE_NAME2).getColumnFamily(NEW_COLUMN_FAMILY));

    // create TEST_TABLE_NAME3 in sink cluster, source cluster will not create table.
    TableDescriptor htd3 = TableDescriptorBuilder.newBuilder(TEST_TABLE_NAME3)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(COLUMN_FAMILY).build()).build();

    admin2.createTable(htd3);
    assertEquals(admin1.tableExists(TEST_TABLE_NAME3), false);
    assertEquals(admin2.tableExists(TEST_TABLE_NAME3), true);

    // cleanup
    admin1.removeReplicationPeer(ID_FIRST);
    admin1.removeReplicationPeer(ID_THIRD);
    UTIL1.deleteTable(TEST_TABLE_NAME1);
    UTIL2.deleteTable(TEST_TABLE_NAME1);
    UTIL1.deleteTable(TEST_TABLE_NAME2);
    UTIL2.deleteTable(TEST_TABLE_NAME2);
    UTIL2.deleteTable(TEST_TABLE_NAME3);
    admin1.deleteNamespace(TEST_NAMESPACE_STR);
    admin2.deleteNamespace(TEST_NAMESPACE_STR);
  }

  @Test
  public void testBidirectionalReplication() throws IOException, ReplicationException {
    admin1.createNamespace(TEST_NAMESPACE);
    admin2.createNamespace(TEST_NAMESPACE);

    Map<TableName, List<String>> tableNames = new HashMap<>();
    tableNames.put(TEST_TABLE_NAME1, Collections.emptyList());
    ReplicationPeerConfig rpc =
        ReplicationPeerConfig.newBuilder().setClusterKey(UTIL2.getClusterKey())
            .setReplicateAllUserTables(false).setTableCFsMap(tableNames).build();

    // add_peer '1', 'hbase://<cluster2>', TABLE_CFS => {"test_ns:test_table2"=>[]},
    // STATE=>'enabled'
    admin1.addReplicationPeer(ID_FIRST, rpc);
    rpc = ReplicationPeerConfig.newBuilder().setClusterKey(UTIL1.getClusterKey())
        .setReplicateAllUserTables(false).setTableCFsMap(tableNames).build();
    admin2.addReplicationPeer(ID_FIRST, rpc);

    TableDescriptor htd1 = TableDescriptorBuilder.newBuilder(TEST_TABLE_NAME1)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(COLUMN_FAMILY).build()).build();

    // create TEST_TABLE_NAME1 in source cluster, then sink cluster will create table too.
    assertEquals(admin1.tableExists(TEST_TABLE_NAME1), false);
    assertEquals(admin2.tableExists(TEST_TABLE_NAME1), false);
    admin1.createTable(htd1);
    assertEquals(admin1.tableExists(TEST_TABLE_NAME1), true);
    assertEquals(admin2.tableExists(TEST_TABLE_NAME1), true);

    // alter 't1', NAME => 'new_f2', VERSIONS => 5 (TEST_TABLE_NAME1)
    ColumnFamilyDescriptor hcd = ColumnFamilyDescriptorBuilder.newBuilder(NEW_COLUMN_FAMILY)
        .setMaxVersions(5).setMinVersions(5).build();
    assertTrue(
      admin1.getDescriptor(TEST_TABLE_NAME1).getColumnFamily(NEW_COLUMN_FAMILY) == null);
    assertTrue(
      admin2.getDescriptor(TEST_TABLE_NAME1).getColumnFamily(NEW_COLUMN_FAMILY) == null);
    admin1.addColumnFamily(TEST_TABLE_NAME1, hcd);
    ColumnFamilyDescriptor hcdSource =
        admin1.getDescriptor(TEST_TABLE_NAME1).getColumnFamily(NEW_COLUMN_FAMILY);
    ColumnFamilyDescriptor hcdPeer =
        admin2.getDescriptor(TEST_TABLE_NAME1).getColumnFamily(NEW_COLUMN_FAMILY);
    assertNotNull(hcdSource);
    assertNotNull(hcdPeer);
    assertEquals(hcdSource, hcdPeer);

    // Modify the same column. and column change will not sync to peer cluster.
    hcd = ColumnFamilyDescriptorBuilder.newBuilder(NEW_COLUMN_FAMILY).setMaxVersions(2)
        .setMinVersions(1).build();
    admin1.modifyColumnFamily(TEST_TABLE_NAME1, hcd);
    hcdSource = admin1.getDescriptor(TEST_TABLE_NAME1).getColumnFamily(NEW_COLUMN_FAMILY);
    hcdPeer = admin2.getDescriptor(TEST_TABLE_NAME1).getColumnFamily(NEW_COLUMN_FAMILY);
    assertNotNull(hcdSource);
    assertNotNull(hcdPeer);
    assertEquals(1, hcdSource.getMinVersions());
    assertEquals(2, hcdSource.getMaxVersions());
    assertEquals(5, hcdPeer.getMinVersions());
    assertEquals(5, hcdPeer.getMaxVersions());
    assertNotEquals(hcdSource, hcdPeer);

    // delete the column.
    admin1.deleteColumnFamily(TEST_TABLE_NAME1, NEW_COLUMN_FAMILY);
    assertNull(admin1.getDescriptor(TEST_TABLE_NAME1).getColumnFamily(NEW_COLUMN_FAMILY));
    assertNull(admin2.getDescriptor(TEST_TABLE_NAME1).getColumnFamily(NEW_COLUMN_FAMILY));

    ReplicationPeerConfig rpc2 = ReplicationPeerConfig.newBuilder()
        .setClusterKey(UTIL2.getClusterKey()).setReplicateAllUserTables(false)
        .setNamespaces(Collections.singleton(TEST_NAMESPACE_STR)).build();

    // add_peer '3', 'hbase://<cluster2>', NAMESPACE=>['test_ns'], STATE=>'enabled'
    admin1.addReplicationPeer(ID_THIRD, rpc2);
    rpc2 = ReplicationPeerConfig.newBuilder().setClusterKey(UTIL1.getClusterKey())
        .setReplicateAllUserTables(false).setNamespaces(Collections.singleton(TEST_NAMESPACE_STR))
        .build();
    admin2.addReplicationPeer(ID_THIRD, rpc2);

    TableDescriptor htd2 = TableDescriptorBuilder.newBuilder(TEST_TABLE_NAME2)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(COLUMN_FAMILY).build()).build();

    // create TEST_TABLE_NAME2 in source cluster, then sink cluster will create table too.
    assertFalse(admin1.tableExists(TEST_TABLE_NAME2));
    assertFalse(admin2.tableExists(TEST_TABLE_NAME2));
    admin1.createTable(htd2);
    assertTrue(admin1.tableExists(TEST_TABLE_NAME2));
    assertTrue(admin2.tableExists(TEST_TABLE_NAME2));

    // alter 't2', NAME => 'new_f2', VERSIONS => 5 (TEST_TABLE_NAME2)
    hcd = ColumnFamilyDescriptorBuilder.newBuilder(NEW_COLUMN_FAMILY).setMaxVersions(5)
        .setMinVersions(5).build();
    assertTrue(
      admin1.getDescriptor(TEST_TABLE_NAME2).getColumnFamily(NEW_COLUMN_FAMILY) == null);
    assertTrue(
      admin2.getDescriptor(TEST_TABLE_NAME2).getColumnFamily(NEW_COLUMN_FAMILY) == null);
    admin1.addColumnFamily(TEST_TABLE_NAME2, hcd);
    hcdSource = admin1.getDescriptor(TEST_TABLE_NAME2).getColumnFamily(NEW_COLUMN_FAMILY);
    hcdPeer = admin2.getDescriptor(TEST_TABLE_NAME2).getColumnFamily(NEW_COLUMN_FAMILY);
    assertNotNull(hcdSource);
    assertNotNull(hcdPeer);
    assertEquals(hcdSource, hcdPeer);

    // Modify the same column. and column change will not sync to peer cluster.
    hcd = ColumnFamilyDescriptorBuilder.newBuilder(NEW_COLUMN_FAMILY).setMaxVersions(2)
        .setMinVersions(1).build();
    admin1.modifyColumnFamily(TEST_TABLE_NAME2, hcd);
    hcdSource = admin1.getDescriptor(TEST_TABLE_NAME2).getColumnFamily(NEW_COLUMN_FAMILY);
    hcdPeer = admin2.getDescriptor(TEST_TABLE_NAME2).getColumnFamily(NEW_COLUMN_FAMILY);
    assertNotNull(hcdSource);
    assertNotNull(hcdPeer);
    assertEquals(1, hcdSource.getMinVersions());
    assertEquals(2, hcdSource.getMaxVersions());
    assertEquals(5, hcdPeer.getMinVersions());
    assertEquals(5, hcdPeer.getMaxVersions());
    assertNotEquals(hcdSource, hcdPeer);

    // delete the column.
    admin1.deleteColumnFamily(TEST_TABLE_NAME2, NEW_COLUMN_FAMILY);
    assertNull(admin1.getDescriptor(TEST_TABLE_NAME2).getColumnFamily(NEW_COLUMN_FAMILY));
    assertNull(admin2.getDescriptor(TEST_TABLE_NAME2).getColumnFamily(NEW_COLUMN_FAMILY));

    // create TEST_TABLE_NAME3 in sink cluster, source cluster will not create table.
    TableDescriptor htd3 = TableDescriptorBuilder.newBuilder(TEST_TABLE_NAME3)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(COLUMN_FAMILY).build()).build();
    admin2.createTable(htd3);
    assertFalse(admin1.tableExists(TEST_TABLE_NAME3));
    assertTrue(admin2.tableExists(TEST_TABLE_NAME3));

    // cleanup
    admin1.removeReplicationPeer(ID_FIRST);
    admin1.removeReplicationPeer(ID_THIRD);
    admin2.removeReplicationPeer(ID_FIRST);
    admin2.removeReplicationPeer(ID_THIRD);
    UTIL1.deleteTable(TEST_TABLE_NAME1);
    UTIL2.deleteTable(TEST_TABLE_NAME1);
    UTIL1.deleteTable(TEST_TABLE_NAME2);
    UTIL2.deleteTable(TEST_TABLE_NAME2);
    UTIL2.deleteTable(TEST_TABLE_NAME3);
    admin1.deleteNamespace(TEST_NAMESPACE_STR);
    admin2.deleteNamespace(TEST_NAMESPACE_STR);
  }
}
