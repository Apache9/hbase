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
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Category({ MediumTests.class })
public class TestSyncTableSchemaForPeers extends TestReplicationBase {
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
    TestReplicationBase.setUpBeforeClass();
    connection1 = ConnectionFactory.createConnection(conf1);
    connection2 = ConnectionFactory.createConnection(conf2);
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
        ReplicationPeerConfig.newBuilder().setClusterKey(utility2.getClusterKey())
            .setReplicateAllUserTables(false).setTableCFsMap(tableNames).build();

    // add_peer '1', 'hbase://<cluster2>', TABLE_CFS => {"test_ns:test_table"=>[]},
    // STATE=>'enabled'
    admin1.addReplicationPeer(ID_FIRST, rpc);

    TableDescriptor htd1 = TableDescriptorBuilder.newBuilder(TEST_TABLE_NAME1)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(COLUMN_FAMILY).build()).build();

    // create TEST_TABLE_NAME1 in source cluster, then sink cluster will create table too.
    Assert.assertEquals(admin1.tableExists(TEST_TABLE_NAME1), false);
    Assert.assertEquals(admin2.tableExists(TEST_TABLE_NAME1), false);
    admin1.createTable(htd1);
    Assert.assertEquals(admin1.tableExists(TEST_TABLE_NAME1), true);
    Assert.assertEquals(admin2.tableExists(TEST_TABLE_NAME1), true);

    // alter 't1', NAME => 'new_f2', VERSIONS => 5 (TEST_TABLE_NAME1)
    ColumnFamilyDescriptor hcd = ColumnFamilyDescriptorBuilder.newBuilder(NEW_COLUMN_FAMILY)
        .setMaxVersions(5).setMinVersions(5).build();
    Assert.assertTrue(
      admin1.getDescriptor(TEST_TABLE_NAME1).getColumnFamily(NEW_COLUMN_FAMILY) == null);
    Assert.assertTrue(
      admin2.getDescriptor(TEST_TABLE_NAME1).getColumnFamily(NEW_COLUMN_FAMILY) == null);
    admin1.addColumnFamily(TEST_TABLE_NAME1, hcd);

    ColumnFamilyDescriptor hcdSource =
        admin1.getDescriptor(TEST_TABLE_NAME1).getColumnFamily(NEW_COLUMN_FAMILY);
    ColumnFamilyDescriptor hcdPeer =
        admin2.getDescriptor(TEST_TABLE_NAME1).getColumnFamily(NEW_COLUMN_FAMILY);
    Assert.assertNotNull(hcdSource);
    Assert.assertNotNull(hcdPeer);
    Assert.assertEquals(hcdSource, hcdPeer);

    // Modify the same column. and column change will not sync to peer cluster.
    hcd = ColumnFamilyDescriptorBuilder.newBuilder(NEW_COLUMN_FAMILY).setMaxVersions(2)
        .setMinVersions(1).build();
    admin1.modifyColumnFamily(TEST_TABLE_NAME1, hcd);
    hcdSource = admin1.getDescriptor(TEST_TABLE_NAME1).getColumnFamily(NEW_COLUMN_FAMILY);
    hcdPeer = admin2.getDescriptor(TEST_TABLE_NAME1).getColumnFamily(NEW_COLUMN_FAMILY);
    Assert.assertNotNull(hcdSource);
    Assert.assertNotNull(hcdPeer);
    Assert.assertEquals(1, hcdSource.getMinVersions());
    Assert.assertEquals(2, hcdSource.getMaxVersions());
    Assert.assertEquals(5, hcdPeer.getMaxVersions());
    Assert.assertEquals(5, hcdPeer.getMaxVersions());
    Assert.assertNotEquals(hcdSource, hcdPeer);

    // delete the column.
    admin1.deleteColumnFamily(TEST_TABLE_NAME1, NEW_COLUMN_FAMILY);
    Assert.assertNull(admin1.getDescriptor(TEST_TABLE_NAME1).getColumnFamily(NEW_COLUMN_FAMILY));
    Assert.assertNull(admin2.getDescriptor(TEST_TABLE_NAME1).getColumnFamily(NEW_COLUMN_FAMILY));

    ReplicationPeerConfig rpc2 = ReplicationPeerConfig.newBuilder()
        .setClusterKey(utility2.getClusterKey()).setReplicateAllUserTables(false)
        .setNamespaces(Collections.singleton(TEST_NAMESPACE_STR)).build();

    // add_peer '3', 'hbase://<cluster2>', NAMESPACE=>['test_ns'], STATE=>'enabled'
    admin1.addReplicationPeer(ID_THIRD, rpc2);

    TableDescriptor htd2 = TableDescriptorBuilder.newBuilder(TEST_TABLE_NAME2)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(COLUMN_FAMILY).build()).build();

    // create TEST_TABLE_NAME2 in source cluster, then sink cluster will create table too.
    Assert.assertEquals(admin1.tableExists(TEST_TABLE_NAME2), false);
    Assert.assertEquals(admin2.tableExists(TEST_TABLE_NAME2), false);
    admin1.createTable(htd2);
    Assert.assertEquals(admin1.tableExists(TEST_TABLE_NAME2), true);
    Assert.assertEquals(admin2.tableExists(TEST_TABLE_NAME2), true);

    // alter 't2', NAME => 'new_f2', VERSIONS => 5 (TEST_TABLE_NAME2)
    hcd = ColumnFamilyDescriptorBuilder.newBuilder(NEW_COLUMN_FAMILY).setMaxVersions(5)
        .setMinVersions(5).build();
    Assert.assertTrue(
      admin1.getDescriptor(TEST_TABLE_NAME2).getColumnFamily(NEW_COLUMN_FAMILY) == null);
    Assert.assertTrue(
      admin2.getDescriptor(TEST_TABLE_NAME2).getColumnFamily(NEW_COLUMN_FAMILY) == null);

    admin1.addColumnFamily(TEST_TABLE_NAME2, hcd);
    hcdSource = admin1.getDescriptor(TEST_TABLE_NAME2).getColumnFamily(NEW_COLUMN_FAMILY);
    hcdPeer = admin2.getDescriptor(TEST_TABLE_NAME2).getColumnFamily(NEW_COLUMN_FAMILY);
    Assert.assertNotNull(hcdSource);
    Assert.assertNotNull(hcdPeer);
    Assert.assertEquals(hcdSource, hcdPeer);

    // Modify the same column. and column change will not sync to peer cluster.
    hcd = ColumnFamilyDescriptorBuilder.newBuilder(NEW_COLUMN_FAMILY).setMaxVersions(2)
        .setMinVersions(1).build();
    admin1.modifyColumnFamily(TEST_TABLE_NAME2, hcd);
    hcdSource = admin1.getDescriptor(TEST_TABLE_NAME2).getColumnFamily(NEW_COLUMN_FAMILY);
    hcdPeer = admin2.getDescriptor(TEST_TABLE_NAME2).getColumnFamily(NEW_COLUMN_FAMILY);
    Assert.assertNotNull(hcdSource);
    Assert.assertNotNull(hcdPeer);
    Assert.assertEquals(1, hcdSource.getMinVersions());
    Assert.assertEquals(2, hcdSource.getMaxVersions());
    Assert.assertEquals(5, hcdPeer.getMinVersions());
    Assert.assertEquals(5, hcdPeer.getMaxVersions());
    Assert.assertNotEquals(hcdSource, hcdPeer);

    // delete the column.
    admin1.deleteColumnFamily(TEST_TABLE_NAME2, NEW_COLUMN_FAMILY);
    Assert.assertNull(admin1.getDescriptor(TEST_TABLE_NAME2).getColumnFamily(NEW_COLUMN_FAMILY));
    Assert.assertNull(admin2.getDescriptor(TEST_TABLE_NAME2).getColumnFamily(NEW_COLUMN_FAMILY));

    // create TEST_TABLE_NAME3 in sink cluster, source cluster will not create table.
    TableDescriptor htd3 = TableDescriptorBuilder.newBuilder(TEST_TABLE_NAME3)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(COLUMN_FAMILY).build()).build();

    admin2.createTable(htd3);
    Assert.assertEquals(admin1.tableExists(TEST_TABLE_NAME3), false);
    Assert.assertEquals(admin2.tableExists(TEST_TABLE_NAME3), true);

    // cleanup
    admin1.removeReplicationPeer(ID_FIRST);
    admin1.removeReplicationPeer(ID_THIRD);
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

    Map<TableName, List<String>> tableNames = new HashMap<>();
    tableNames.put(TEST_TABLE_NAME1, Collections.emptyList());
    ReplicationPeerConfig rpc =
        ReplicationPeerConfig.newBuilder().setClusterKey(utility2.getClusterKey())
            .setReplicateAllUserTables(false).setTableCFsMap(tableNames).build();

    // add_peer '1', 'hbase://<cluster2>', TABLE_CFS => {"test_ns:test_table2"=>[]},
    // STATE=>'enabled'
    admin1.addReplicationPeer(ID_FIRST, rpc);
    rpc = ReplicationPeerConfig.newBuilder().setClusterKey(utility1.getClusterKey())
        .setReplicateAllUserTables(false).setTableCFsMap(tableNames).build();
    admin2.addReplicationPeer(ID_FIRST, rpc);

    TableDescriptor htd1 = TableDescriptorBuilder.newBuilder(TEST_TABLE_NAME1)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(COLUMN_FAMILY).build()).build();

    // create TEST_TABLE_NAME1 in source cluster, then sink cluster will create table too.
    Assert.assertEquals(admin1.tableExists(TEST_TABLE_NAME1), false);
    Assert.assertEquals(admin2.tableExists(TEST_TABLE_NAME1), false);
    admin1.createTable(htd1);
    Assert.assertEquals(admin1.tableExists(TEST_TABLE_NAME1), true);
    Assert.assertEquals(admin2.tableExists(TEST_TABLE_NAME1), true);

    // alter 't1', NAME => 'new_f2', VERSIONS => 5 (TEST_TABLE_NAME1)
    ColumnFamilyDescriptor hcd = ColumnFamilyDescriptorBuilder.newBuilder(NEW_COLUMN_FAMILY)
        .setMaxVersions(5).setMinVersions(5).build();
    Assert.assertTrue(
      admin1.getDescriptor(TEST_TABLE_NAME1).getColumnFamily(NEW_COLUMN_FAMILY) == null);
    Assert.assertTrue(
      admin2.getDescriptor(TEST_TABLE_NAME1).getColumnFamily(NEW_COLUMN_FAMILY) == null);
    admin1.addColumnFamily(TEST_TABLE_NAME1, hcd);
    ColumnFamilyDescriptor hcdSource =
        admin1.getDescriptor(TEST_TABLE_NAME1).getColumnFamily(NEW_COLUMN_FAMILY);
    ColumnFamilyDescriptor hcdPeer =
        admin2.getDescriptor(TEST_TABLE_NAME1).getColumnFamily(NEW_COLUMN_FAMILY);
    Assert.assertNotNull(hcdSource);
    Assert.assertNotNull(hcdPeer);
    Assert.assertEquals(hcdSource, hcdPeer);

    // Modify the same column. and column change will not sync to peer cluster.
    hcd = ColumnFamilyDescriptorBuilder.newBuilder(NEW_COLUMN_FAMILY).setMaxVersions(2)
        .setMinVersions(1).build();
    admin1.modifyColumnFamily(TEST_TABLE_NAME1, hcd);
    hcdSource = admin1.getDescriptor(TEST_TABLE_NAME1).getColumnFamily(NEW_COLUMN_FAMILY);
    hcdPeer = admin2.getDescriptor(TEST_TABLE_NAME1).getColumnFamily(NEW_COLUMN_FAMILY);
    Assert.assertNotNull(hcdSource);
    Assert.assertNotNull(hcdPeer);
    Assert.assertEquals(1, hcdSource.getMinVersions());
    Assert.assertEquals(2, hcdSource.getMaxVersions());
    Assert.assertEquals(5, hcdPeer.getMinVersions());
    Assert.assertEquals(5, hcdPeer.getMaxVersions());
    Assert.assertNotEquals(hcdSource, hcdPeer);

    // delete the column.
    admin1.deleteColumnFamily(TEST_TABLE_NAME1, NEW_COLUMN_FAMILY);
    Assert.assertNull(admin1.getDescriptor(TEST_TABLE_NAME1).getColumnFamily(NEW_COLUMN_FAMILY));
    Assert.assertNull(admin2.getDescriptor(TEST_TABLE_NAME1).getColumnFamily(NEW_COLUMN_FAMILY));

    ReplicationPeerConfig rpc2 = ReplicationPeerConfig.newBuilder()
        .setClusterKey(utility2.getClusterKey()).setReplicateAllUserTables(false)
        .setNamespaces(Collections.singleton(TEST_NAMESPACE_STR)).build();

    // add_peer '3', 'hbase://<cluster2>', NAMESPACE=>['test_ns'], STATE=>'enabled'
    admin1.addReplicationPeer(ID_THIRD, rpc2);
    rpc2 = ReplicationPeerConfig.newBuilder().setClusterKey(utility1.getClusterKey())
        .setReplicateAllUserTables(false).setNamespaces(Collections.singleton(TEST_NAMESPACE_STR))
        .build();
    admin2.addReplicationPeer(ID_THIRD, rpc2);

    TableDescriptor htd2 = TableDescriptorBuilder.newBuilder(TEST_TABLE_NAME2)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(COLUMN_FAMILY).build()).build();

    // create TEST_TABLE_NAME2 in source cluster, then sink cluster will create table too.
    Assert.assertEquals(admin1.tableExists(TEST_TABLE_NAME2), false);
    Assert.assertEquals(admin2.tableExists(TEST_TABLE_NAME2), false);
    admin1.createTable(htd2);
    Assert.assertEquals(admin1.tableExists(TEST_TABLE_NAME2), true);
    Assert.assertEquals(admin2.tableExists(TEST_TABLE_NAME2), true);

    // alter 't2', NAME => 'new_f2', VERSIONS => 5 (TEST_TABLE_NAME2)
    hcd = ColumnFamilyDescriptorBuilder.newBuilder(NEW_COLUMN_FAMILY).setMaxVersions(5)
        .setMinVersions(5).build();
    Assert.assertTrue(
      admin1.getDescriptor(TEST_TABLE_NAME2).getColumnFamily(NEW_COLUMN_FAMILY) == null);
    Assert.assertTrue(
      admin2.getDescriptor(TEST_TABLE_NAME2).getColumnFamily(NEW_COLUMN_FAMILY) == null);
    admin1.addColumnFamily(TEST_TABLE_NAME2, hcd);
    hcdSource = admin1.getDescriptor(TEST_TABLE_NAME2).getColumnFamily(NEW_COLUMN_FAMILY);
    hcdPeer = admin2.getDescriptor(TEST_TABLE_NAME2).getColumnFamily(NEW_COLUMN_FAMILY);
    Assert.assertNotNull(hcdSource);
    Assert.assertNotNull(hcdPeer);
    Assert.assertEquals(hcdSource, hcdPeer);

    // Modify the same column. and column change will not sync to peer cluster.
    hcd = ColumnFamilyDescriptorBuilder.newBuilder(NEW_COLUMN_FAMILY).setMaxVersions(2)
        .setMinVersions(1).build();
    admin1.modifyColumnFamily(TEST_TABLE_NAME2, hcd);
    hcdSource = admin1.getDescriptor(TEST_TABLE_NAME2).getColumnFamily(NEW_COLUMN_FAMILY);
    hcdPeer = admin2.getDescriptor(TEST_TABLE_NAME2).getColumnFamily(NEW_COLUMN_FAMILY);
    Assert.assertNotNull(hcdSource);
    Assert.assertNotNull(hcdPeer);
    Assert.assertEquals(1, hcdSource.getMinVersions());
    Assert.assertEquals(2, hcdSource.getMaxVersions());
    Assert.assertEquals(5, hcdPeer.getMinVersions());
    Assert.assertEquals(5, hcdPeer.getMaxVersions());
    Assert.assertNotEquals(hcdSource, hcdPeer);

    // delete the column.
    admin1.deleteColumnFamily(TEST_TABLE_NAME2, NEW_COLUMN_FAMILY);
    Assert.assertNull(admin1.getDescriptor(TEST_TABLE_NAME2).getColumnFamily(NEW_COLUMN_FAMILY));
    Assert.assertNull(admin2.getDescriptor(TEST_TABLE_NAME2).getColumnFamily(NEW_COLUMN_FAMILY));

    // create TEST_TABLE_NAME3 in sink cluster, source cluster will not create table.
    TableDescriptor htd3 = TableDescriptorBuilder.newBuilder(TEST_TABLE_NAME3)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(COLUMN_FAMILY).build()).build();
    admin2.createTable(htd3);
    Assert.assertEquals(admin1.tableExists(TEST_TABLE_NAME3), true);
    Assert.assertEquals(admin2.tableExists(TEST_TABLE_NAME3), true);

    // cleanup
    admin1.removeReplicationPeer(ID_FIRST);
    admin1.removeReplicationPeer(ID_THIRD);
    admin2.removeReplicationPeer(ID_FIRST);
    admin2.removeReplicationPeer(ID_THIRD);
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
