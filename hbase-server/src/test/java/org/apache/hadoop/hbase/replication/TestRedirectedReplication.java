/**
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

import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.replication.regionserver.RedirectingInterClusterReplicationEndpoint;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({LargeTests.class})
public class TestRedirectedReplication extends TestReplicationBase {
  private static final Log LOG = LogFactory.getLog(TestRedirectedReplication.class);

  private static final byte[] row2 = Bytes.toBytes("row2");

  private static String ns1 = "ns1";
  private static String ns2 = "ns2";

  private static String ns1T1Name = "ns1:t1";
  private static String ns2T1Name = "ns2:t1";
  private static String ns1T2Name = "ns1:t2";
  private static String ns2T2Name = "ns2:t2";

  private static final String SPECIAL_PEER_ID = "3";

  private static final TableName ns1T1 = TableName.valueOf(ns1T1Name);
  private static final TableName ns2T1 = TableName.valueOf(ns2T1Name);
  private static final TableName ns1T2 = TableName.valueOf(ns1T2Name);
  private static final TableName ns2T2 = TableName.valueOf(ns2T2Name);

  private static final byte[] f1Name = Bytes.toBytes("f1");

  private static HTableDescriptor tabLeftNs1T1;
  private static HTableDescriptor tabLeftNs2T1;
  private static HTableDescriptor tabRightNs1T2;
  private static HTableDescriptor tabRightNs2T2;

  private static HBaseAdmin admin1;
  private static HBaseAdmin admin2;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TestReplicationBase.setUpBeforeClass();

    admin1 = new HBaseAdmin(conf1);
    admin2 = new HBaseAdmin(conf2);

    admin1.createNamespace(NamespaceDescriptor.create(ns1).build());
    admin1.createNamespace(NamespaceDescriptor.create(ns2).build());
    admin2.createNamespace(NamespaceDescriptor.create(ns1).build());
    admin2.createNamespace(NamespaceDescriptor.create(ns2).build());

    HColumnDescriptor fam = new HColumnDescriptor(f1Name);
    fam.setScope(HConstants.REPLICATION_SCOPE_GLOBAL);

    tabLeftNs1T1 = new HTableDescriptor(ns1T1);
    tabLeftNs1T1.addFamily(fam);

    tabLeftNs2T1 = new HTableDescriptor(ns2T1);
    tabLeftNs2T1.addFamily(fam);

    tabRightNs1T2 = new HTableDescriptor(ns1T2);
    tabRightNs1T2.addFamily(fam);

    tabRightNs2T2 = new HTableDescriptor(ns2T2);
    tabRightNs2T2.addFamily(fam);

    ReplicationPeerConfig rpc = new ReplicationPeerConfig();
    rpc.setClusterKey(utility2.getClusterKey());
    rpc.setReplicateAllUserTables(true);
    rpc.setReplicationEndpointImpl(
        RedirectingInterClusterReplicationEndpoint.class.getName());
    // Init the replication peer with redirect config
    rpc.getConfiguration().put(ns1T1Name, ns1T2Name);
    admin1.addReplicationPeer(SPECIAL_PEER_ID, rpc);

    admin1.createTable(tabLeftNs1T1);
    admin1.createTable(tabLeftNs2T1);
    admin2.createTable(tabRightNs1T2);
    admin2.createTable(tabRightNs2T2);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    admin1.removeReplicationPeer(SPECIAL_PEER_ID);

    admin1.disableTable(ns1T1);
    admin1.deleteTable(ns1T1);
    admin1.disableTable(ns2T1);
    admin1.deleteTable(ns2T1);

    admin2.disableTable(ns1T1);
    admin2.deleteTable(ns1T1);
    admin2.disableTable(ns1T2);
    admin2.deleteTable(ns1T2);
    admin2.disableTable(ns2T1);
    admin2.deleteTable(ns2T1);
    admin2.disableTable(ns2T2);
    admin2.deleteTable(ns2T2);

    admin1.deleteNamespace(ns1);
    admin1.deleteNamespace(ns2);
    admin2.deleteNamespace(ns1);
    admin2.deleteNamespace(ns2);

    TestReplicationBase.tearDownAfterClass();
  }

  @Test
  public void testRedirectedReplication() throws Exception {
    LOG.info("Starting RedirectedReplication test");
    HTable htabLeftNs1T1 = new HTable(conf1, ns1T1);
    HTable htabLeftNs2T1 = new HTable(conf1, ns2T1);
    HTable htabRightNs1T1 = new HTable(conf2, ns1T1);
    HTable htabRightNs1T2 = new HTable(conf2, ns1T2);
    HTable htabRightNs2T2 = new HTable(conf2, ns2T2);

    // Step 1: "Redirection to different table name works"
    // The redirect table map was configured when init the replication peer
    put(htabLeftNs1T1, row, f1Name);
    ensureRowExisted(htabLeftNs1T1, row, f1Name);
    ensureRowExisted(htabRightNs1T2, row, f1Name);
    delete(htabLeftNs1T1, row, f1Name);
    ensureRowNotExisted(htabLeftNs1T1, row, f1Name);
    ensureRowNotExisted(htabRightNs1T2, row, f1Name);

    // Step 2: "Redirection to different namespace works"
    ReplicationPeerConfig rpc = admin1.getReplicationPeerConfig(SPECIAL_PEER_ID);
    rpc.getConfiguration().put(ns1T1Name, ns2T2Name);
    admin1.updateReplicationPeerConfig(SPECIAL_PEER_ID, rpc);
    put(htabLeftNs1T1, row, f1Name);
    ensureRowExisted(htabLeftNs1T1, row, f1Name);
    ensureRowExisted(htabRightNs2T2, row, f1Name);
    delete(htabLeftNs1T1, row, f1Name);
    ensureRowNotExisted(htabLeftNs1T1, row, f1Name);
    ensureRowNotExisted(htabRightNs2T2, row, f1Name);
    rpc.getConfiguration().remove(ns1T1Name);

    // Step 3: "Multiple redirection rules work at the same time"
    rpc.getConfiguration().putAll(new HashMap<String, String>() {
      {
        put(ns1T1Name, ns1T2Name);
        put(ns2T1Name, ns2T2Name);
      }
    });
    admin1.updateReplicationPeerConfig(SPECIAL_PEER_ID, rpc);
    put(htabLeftNs1T1, row, f1Name);
    put(htabLeftNs2T1, row2, f1Name);
    ensureRowExisted(htabLeftNs1T1, row, f1Name);
    ensureRowExisted(htabRightNs1T2, row, f1Name);
    ensureRowExisted(htabLeftNs2T1, row2, f1Name);
    ensureRowExisted(htabRightNs2T2, row2, f1Name);
    delete(htabLeftNs1T1, row, f1Name);
    delete(htabLeftNs2T1, row2, f1Name);
    ensureRowNotExisted(htabLeftNs1T1, row, f1Name);
    ensureRowNotExisted(htabRightNs1T2, row, f1Name);
    ensureRowNotExisted(htabLeftNs2T1, row2, f1Name);
    ensureRowNotExisted(htabRightNs2T2, row2, f1Name);
  }
}