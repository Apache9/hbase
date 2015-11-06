/**
 * Copyright 2010 The Apache Software Foundation
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
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HDFSBlocksDistribution;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.ManualEnvironmentEdge;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestRegionLocationFinder {
  private static final Log LOG = LogFactory.getLog(TestRegionLocationFinder.class);
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static MiniHBaseCluster cluster;
  private static ManualEnvironmentEdge envEdge;

  private final static byte[] tableName = Bytes.toBytes("testTable");
  private final static byte[] FAMILY = Bytes.toBytes("cf");
  private static HTable table;
  private final static int ServerNum = 5;

  private static RegionLocationFinder finder = new RegionLocationFinder();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    cluster = TEST_UTIL.startMiniCluster(1, ServerNum);
    table = TEST_UTIL.createTable(tableName, FAMILY);
    TEST_UTIL.waitTableAvailable(tableName, 1000);
    TEST_UTIL.createMultiRegions(table, FAMILY);
    TEST_UTIL.loadTable(table, FAMILY);

    envEdge = new ManualEnvironmentEdge();
    envEdge.setValue(EnvironmentEdgeManager.currentTimeMillis());
    EnvironmentEdgeManager.injectEdge(envEdge);

    for (int i = 0; i < ServerNum; i++) {
      HRegionServer server = cluster.getRegionServer(i);
      for (HRegionInfo region : server.getOnlineRegions()) {
        server.flushRegion(region);
      }
    }
    // wait flush region
    Thread.sleep(5000);

    finder.setConf(TEST_UTIL.getConfiguration());
    finder.setServices(cluster.getMaster());
    finder.setClusterStatus(cluster.getMaster().getClusterStatus());
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    EnvironmentEdgeManager.reset();
    table.close();
    TEST_UTIL.deleteTable(tableName);
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testInternalGetTopBlockLocation() throws Exception {
    for (int i = 0; i < ServerNum; i++) {
      HRegionServer server = cluster.getRegionServer(i);
      for (HRegion region : server.getOnlineRegions(tableName)) {
        // get region's hdfs block distribution by region and RegionLocationFinder, 
        // they should have same result
        HDFSBlocksDistribution blocksDistribution1 = region.getHDFSBlocksDistribution();
        HDFSBlocksDistribution blocksDistribution2 = finder.getBlockDistribution(region
            .getRegionInfo());
        assertEquals(blocksDistribution1.getUniqueBlocksTotalWeight(),
          blocksDistribution2.getUniqueBlocksTotalWeight());
        if (blocksDistribution1.getUniqueBlocksTotalWeight() != 0) {
          assertEquals(blocksDistribution1.getTopHosts().get(0), blocksDistribution2.getTopHosts()
              .get(0));
        }
      }
    }
  }

  @Test
  public void testMapHostNameToServerName() throws Exception {
    List<String> topHosts = new ArrayList<String>();
    for (int i = 0; i < ServerNum; i++) {
      HRegionServer server = cluster.getRegionServer(i);
      String serverHost = server.getServerName().getHostname();
      if (!topHosts.contains(serverHost)) {
        topHosts.add(serverHost);
      }
    }
    // mini cluster, all rs in one host
    assertEquals(1, topHosts.size());
    List<ServerName> servers = finder.mapHostNameToServerName(topHosts);
    assertEquals(ServerNum, servers.size());
    for (int i = 0; i < ServerNum; i++) {
      ServerName server = cluster.getRegionServer(i).getServerName();
      assertTrue(servers.contains(server));
    }
  }

  @Test
  public void testGetTopBlockLocations() throws Exception {
    for (int i = 0; i < ServerNum; i++) {
      HRegionServer server = cluster.getRegionServer(i);
      for (HRegion region : server.getOnlineRegions(tableName)) {
        List<ServerName> servers = finder.getTopBlockLocations(region.getRegionInfo());
        // test table may have empty region
        if (region.getHDFSBlocksDistribution().getUniqueBlocksTotalWeight() == 0) {
          continue;
        }
        List<String> topHosts = region.getHDFSBlocksDistribution().getTopHosts();
        // rs and datanode may have different host in local machine test
        if (!topHosts.contains(server.getServerName().getHostname())) {
          continue;
        }
        assertEquals(ServerNum, servers.size());
        for (int j = 0; j < ServerNum; j++) {
          ServerName serverName = cluster.getRegionServer(j).getServerName();
          assertTrue(servers.contains(serverName));
        }
      }
    }
  }
}
