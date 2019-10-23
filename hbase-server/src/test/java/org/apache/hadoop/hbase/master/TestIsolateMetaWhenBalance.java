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
package org.apache.hadoop.hbase.master;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.master.balancer.SimpleLoadBalancer;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(LargeTests.class)
public class TestIsolateMetaWhenBalance {
  private static final Log LOG = LogFactory.getLog(TestIsolateMetaWhenBalance.class);

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static final byte[] FAMILY = Bytes.toBytes("cf");

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration()
        .set(HConstants.HBASE_MASTER_LOADBALANCER_CLASS, SimpleLoadBalancer.class.getName());
    TEST_UTIL.getConfiguration().setBoolean(HConstants.HBASE_MASTER_LOADBALANCE_BYTABLE, true);
    TEST_UTIL.getConfiguration().setInt(HConstants.HBASE_BALANCER_PERIOD, 3600000);
    TEST_UTIL.getConfiguration()
        .setBoolean(HConstants.HBASE_MASTER_ISOLATE_META_WHEN_BALANCE, true);
    TEST_UTIL.startMiniCluster(5);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  private void createTable(TableName tableName) throws Exception {
    TEST_UTIL.createMultiRegionTable(tableName, FAMILY, 100);
    TEST_UTIL.waitTableAvailable(tableName.getName());
  }

  @Test
  public void testCreateTable() throws Exception {
    TableName tableName = TableName.valueOf("testCreateTable");
    createTable(tableName);
    TEST_UTIL.getHBaseAdmin().balancer();
    assertMetaIsolated();
    assertTrue(
        TEST_UTIL.getMiniHBaseCluster().getMaster().isClusterBalancedExcludeMetaRegionServer());
    assertFalse(TEST_UTIL.getHBaseAdmin().balancer());
    TEST_UTIL.deleteTable(tableName);
  }

  @Test
  public void testServerCrashed() throws Exception {
    TableName tableName = TableName.valueOf("testServerCrashed");
    createTable(tableName);
    TEST_UTIL.getMiniHBaseCluster().stopRegionServer(0);
    TEST_UTIL.getMiniHBaseCluster().waitOnRegionServer(0);

    waitForRunBalancer();
    assertMetaIsolated();
    assertTrue(
        TEST_UTIL.getMiniHBaseCluster().getMaster().isClusterBalancedExcludeMetaRegionServer());
    assertFalse(TEST_UTIL.getHBaseAdmin().balancer());
    TEST_UTIL.deleteTable(tableName);
  }

  @Test
  public void testUnbalanceCase() throws Exception {
    TableName tableName = TableName.valueOf("testUnbalanceCase");
    createTable(tableName);
    TEST_UTIL.getHBaseAdmin().balancer();
    assertMetaIsolated();
    assertTrue(
        TEST_UTIL.getMiniHBaseCluster().getMaster().isClusterBalancedExcludeMetaRegionServer());
    assertFalse(TEST_UTIL.getHBaseAdmin().balancer());

    ServerName metaRegionServer = TEST_UTIL.getMiniHBaseCluster().getMaster().getMetaRegionServer();
    ServerName targetServer = null;
    for (JVMClusterUtil.RegionServerThread regionServerThread : TEST_UTIL.getMiniHBaseCluster()
        .getRegionServerThreads()) {
      ServerName serverName = regionServerThread.getRegionServer().getServerName();
      if (!serverName.equals(metaRegionServer)) {
        targetServer = serverName;
        break;
      }
    }

    RegionStates regionStates =
        TEST_UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager().getRegionStates();
    Map<ServerName, Integer> counter = new HashMap<>();
    for (Map.Entry<HRegionInfo, ServerName> entry : regionStates.getRegionAssignments()
        .entrySet()) {
      ServerName serverName = entry.getValue();
      if (!serverName.equals(metaRegionServer) && !serverName.equals(targetServer)) {
        // Move every server's 5 regions to target server
        if (counter.compute(serverName, (k, v) -> v == null ? 5 : v - 1) <= 0) {
          LOG.debug("Move region " + entry.getKey() + " to " + targetServer);
          TEST_UTIL.getHBaseAdmin().move(entry.getKey().getEncodedNameAsBytes(),
              Bytes.toBytes(targetServer.getServerName()));
        }
      }
    }

    waitForRunBalancer();
    assertMetaIsolated();
    assertTrue(
        TEST_UTIL.getMiniHBaseCluster().getMaster().isClusterBalancedExcludeMetaRegionServer());
    assertFalse(TEST_UTIL.getHBaseAdmin().balancer());
    TEST_UTIL.deleteTable(tableName);
  }

  private void waitForRunBalancer() throws Exception {
    long startTime = System.currentTimeMillis();
    boolean balancerRuned = TEST_UTIL.getHBaseAdmin().balancer();
    while (!balancerRuned && (System.currentTimeMillis() - startTime) < 90000) {
      Thread.sleep(10000);
      balancerRuned = TEST_UTIL.getHBaseAdmin().balancer();
    }
  }

  private void assertMetaIsolated() {
    RegionStates regionStates =
        TEST_UTIL.getMiniHBaseCluster().getMaster().getAssignmentManager().getRegionStates();
    ServerName metaRegionServer = TEST_UTIL.getMiniHBaseCluster().getMaster().getMetaRegionServer();
    assertEquals("Should only serve meta region", 1,
        regionStates.getServerRegions(metaRegionServer).size());
  }
}
