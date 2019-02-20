/*
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
package org.apache.hadoop.hbase;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category({MediumTests.class})
public class TestRegionLoad {

  private static final Logger LOG = LoggerFactory.getLogger(TestRegionLoad.class);
  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static HBaseAdmin admin;

  private static final TableName TABLE_1 = TableName.valueOf("table_1");
  private static final TableName TABLE_2 = TableName.valueOf("table_2");
  private static final TableName TABLE_3 = TableName.valueOf("table_3");
  private static final TableName[] tables = new TableName[] { TABLE_1, TABLE_2, TABLE_3 };
  private static final byte[] FAMILY = Bytes.toBytes("f");
  private static final int MSG_INTERVAL = 500; // ms

  @BeforeClass
  public static void beforeClass() throws Exception {
    // Make servers report eagerly. This test is about looking at the cluster status reported.
    // Make it so we don't have to wait around too long to see change.
    UTIL.getConfiguration().setInt("hbase.regionserver.msginterval", MSG_INTERVAL);
    UTIL.startMiniCluster(4);
    admin = UTIL.getHBaseAdmin();
    admin.setBalancerRunning(false, true);
    createTables();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  private static void createTables() throws IOException, InterruptedException {
    for (TableName tableName : tables) {
      HTable table =
          UTIL.createTable(tableName, FAMILY, HBaseTestingUtility.KEYS_FOR_HBA_CREATE_TABLE);
      UTIL.waitTableAvailable(tableName.getName());
      UTIL.loadTable(table, FAMILY);
    }
  }

  @Test
  public void testRegionLoad() throws Exception {

    // Check if regions match with the regionLoad from the server
    for (ServerName serverName : admin.getClusterStatus().getServers()) {
      List<HRegionInfo> regions = admin.getOnlineRegions(serverName);
      LOG.info("serverName=" + serverName + ", regions=" +
          regions.stream().map(r -> r.getRegionNameAsString()).collect(Collectors.toList()));
      Collection<RegionLoad> regionLoads = admin.getRegionLoads(serverName);
      LOG.info("serverName=" + serverName + ", regionLoads=" +
          regionLoads.stream().map(r -> Bytes.toString(r.getName())).
              collect(Collectors.toList()));
      checkRegionsAndRegionLoads(regions, regionLoads);
    }

    // Check if regionLoad matches the table's regions and nothing is missed
    for (TableName table : new TableName[] { TABLE_1, TABLE_2, TABLE_3 }) {
      List<HRegionInfo> tableRegions = admin.getTableRegions(table);

      List<RegionLoad> regionLoads = Lists.newArrayList();
      for (ServerName serverName : admin.getClusterStatus().getServers()) {
        regionLoads.addAll(admin.getRegionLoads(serverName, table));
      }
      checkRegionsAndRegionLoads(tableRegions, regionLoads);
    }

    // Just wait here. If this fixes the test, come back and do a better job.
    // Would have to redo the below so can wait on cluster status changing.
    // Admin#getClusterStatus retrieves data from HMaster. Admin#getRegionLoads, by contrast,
    // get the data from RS. Hence, it will fail if we do the assert check before RS has done
    // the report.
    TimeUnit.MILLISECONDS.sleep(3 * MSG_INTERVAL);

    // Check RegionLoad matches the regionLoad from ClusterStatus
    ClusterStatus clusterStatus = admin.getClusterStatus();
    for (ServerName serverName : clusterStatus.getServers()) {
      ServerLoad serverLoad = clusterStatus.getLoad(serverName);
      Map<byte[], RegionLoad> regionLoads = admin.getRegionLoads(serverName).stream()
          .collect(Collectors.toMap(e -> e.getName(), e -> e, (v1, v2) -> {
            throw new RuntimeException("impossible!!");
          }, () -> new TreeMap<>(Bytes.BYTES_COMPARATOR)));
      LOG.debug("serverName=" + serverName + ", getRegionLoads=" +
          serverLoad.getRegionsLoad().keySet().stream().map(r -> Bytes.toString(r)).
              collect(Collectors.toList()));
      LOG.debug("serverName=" + serverName + ", regionLoads=" +
          regionLoads.keySet().stream().map(r -> Bytes.toString(r)).
              collect(Collectors.toList()));
      compareRegionLoads(serverLoad.getRegionsLoad(), regionLoads);
    }
  }

  private void compareRegionLoads(Map<byte[], RegionLoad> regionLoadCluster,
      Map<byte[], RegionLoad> regionLoads) {

    assertEquals("No of regionLoads from clusterStatus and regionloads from RS doesn't match",
        regionLoadCluster.size(), regionLoads.size());

    // The contents of region load from cluster and server should match
    for (byte[] regionName : regionLoadCluster.keySet()) {
      regionLoads.remove(regionName);
    }
    assertEquals("regionLoads from SN should be empty", 0, regionLoads.size());
  }

  private void checkRegionsAndRegionLoads(Collection<HRegionInfo> regions,
      Collection<RegionLoad> regionLoads) {

    for (RegionLoad load : regionLoads) {
      assertNotNull(load.regionLoadPB);
    }

    assertEquals("No of regions and regionloads doesn't match", regions.size(), regionLoads.size());

    Map<byte[], RegionLoad> regionLoadMap = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
    for (RegionLoad regionLoad : regionLoads) {
      regionLoadMap.put(regionLoad.getName(), regionLoad);
    }
    for (HRegionInfo info : regions) {
      assertTrue(
          "Region not in regionLoadMap region:" + info.getRegionNameAsString() + " regionMap: " +
              regionLoadMap, regionLoadMap.containsKey(info.getRegionName()));
    }
  }
}