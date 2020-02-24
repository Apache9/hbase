/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */
package org.apache.hadoop.hbase.master.balancer;

import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.ArrayList;
import java.util.TreeMap;
import java.util.Random;
import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableDescriptors;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.AssignmentManager;
import org.apache.hadoop.hbase.master.ServerManager;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.master.balancer.GalaxyGroupInfoManager.GroupInfo;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;

/**
 * Test GalaxyGroupBasedLoadBalancer with SimpleLoadBalancer as internal balancer
 */
@Category(LargeTests.class)
public class TestGalaxyGroupLoadBalancer {
  private static final Logger LOG = LoggerFactory.getLogger(TestGalaxyGroupLoadBalancer.class);
  private static GalaxyGroupLoadBalancer loadBalancer;
  private static List<ServerName> servers;
  private static List<HTableDescriptor> tableDescs;
  private static final Map<TableName, String> tableMap = new HashMap<>();
  private static final int[] regionAssignment = new int[] { 2, 5, 7, 10, 4, 3, 1 };
  private static final String[] groups = new String[] { "default", "fds", "emq" };
  private static final TableName[] tables = new TableName[] { TableName.valueOf("galaxy_hadoop_online_table"),
      TableName.valueOf("galaxy_emq_meta"), TableName.valueOf("table1"),
      TableName.valueOf("table2") };
  private static int regionId = 0;
  private static final Random rand = new Random();

  @BeforeClass
  public static void beforeAllTests() throws Exception {
    servers = generateServers(7);

    tableDescs = constructTableDesc();
    Configuration conf = HBaseConfiguration.create();
    conf.set("hbase.regions.slop", "0.1");
    conf.set("hbase.galaxy.group.internal.loadbalancer.class", SimpleLoadBalancer.class.getCanonicalName());
    conf.set("hbase.galaxy.group", "fds,emq");
    conf.set("hbase.galaxy.group.fds.servers", "server0:1000,server1:1000");
    conf.set("hbase.galaxy.group.fds.table.prefix", "galaxy_hadoop");
    conf.set("hbase.galaxy.group.emq.servers", "server2:1000,server3:1000");
    conf.set("hbase.galaxy.group.emq.table.prefix", "galaxy_emq");

    loadBalancer = new GalaxyGroupLoadBalancer();
    loadBalancer.setMasterServices(getMockedMaster());
    loadBalancer.setConf(conf);
    loadBalancer.initialize();
  }

  private static MasterServices getMockedMaster() throws IOException {
    Map<String, HTableDescriptor> tableDescriptorMap = new HashMap<>();
    tableDescriptorMap.put(tables[0].getNameAsString(), tableDescs.get(0));
    tableDescriptorMap.put(tables[1].getNameAsString(), tableDescs.get(1));
    tableDescriptorMap.put(tables[2].getNameAsString(), tableDescs.get(2));
    tableDescriptorMap.put(tables[3].getNameAsString(), tableDescs.get(3));
    TableDescriptors tds = Mockito.mock(TableDescriptors.class);
    Mockito.when(tds.get(tables[0])).thenReturn(tableDescs.get(0));
    Mockito.when(tds.get(tables[1])).thenReturn(tableDescs.get(1));
    Mockito.when(tds.get(tables[2])).thenReturn(tableDescs.get(2));
    Mockito.when(tds.get(tables[3])).thenReturn(tableDescs.get(3));
    Mockito.when(tds.getAll()).thenReturn(tableDescriptorMap);
    MasterServices services = Mockito.mock(HMaster.class);
    Mockito.when(services.getTableDescriptors()).thenReturn(tds);
    AssignmentManager am = Mockito.mock(AssignmentManager.class);
    Mockito.when(services.getAssignmentManager()).thenReturn(am);

    ServerManager serverManager = Mockito.mock(ServerManager.class);
    Mockito.when(serverManager.getOnlineServersList()).thenReturn(servers);
    Mockito.when(services.getServerManager()).thenReturn(serverManager);
    return services;
  }


  private static List<ServerName> generateServers(int numServers) {
    List<ServerName> servers = new ArrayList<>(numServers);
    for (int i = 0; i < numServers; i++) {
      String host = "server" + i;
      int port = 1000;
      servers.add(ServerName.valueOf(host, port, -1));
    }
    return servers;
  }

  private static List<HTableDescriptor> constructTableDesc() {
    List<HTableDescriptor> tds = Lists.newArrayList();
    int index = rand.nextInt(groups.length);
    for (int i = 0; i < tables.length; i++) {
      HTableDescriptor htd = new HTableDescriptor(tables[i]);
      int grpIndex = (i + index) % groups.length;
      String groupName = groups[grpIndex];
      tableMap.put(tables[i], groupName);
      tds.add(htd);
    }
    return tds;
  }

  private ArrayListMultimap<String, ServerAndLoad> convertToGroupBasedMap(
      final Map<ServerName, List<HRegionInfo>> serversMap) throws IOException {
    ArrayListMultimap<String, ServerAndLoad> loadMap = ArrayListMultimap.create();
    for (GroupInfo gInfo : loadBalancer.getGalaxyGroupInfoManager().getAllGroupInfo()) {
      Set<ServerName> groupServers = gInfo.getServers();
      for (ServerName serverName : groupServers) {
        ServerName actual = null;
        for (ServerName entry : servers) {
          if (entry.equals(serverName)) {
            actual = entry;
            break;
          }
        }
        List<HRegionInfo> regions = serversMap.get(actual);
        assertTrue("No load for " + actual, regions != null);
        loadMap.put(gInfo.getName(), new ServerAndLoad(actual, regions.size()));
      }
    }
    return loadMap;
  }


  /**
   * Invariant is that all servers of a group have load between floor(avg) and ceiling(avg) number
   * of regions.
   */
  private void assertClusterAsBalanced(ArrayListMultimap<String, ServerAndLoad> groupLoadMap) {
    for (String gName : groupLoadMap.keySet()) {
      List<ServerAndLoad> groupLoad = groupLoadMap.get(gName);
      int numServers = groupLoad.size();
      int numRegions = 0;
      int maxRegions = 0;
      int minRegions = Integer.MAX_VALUE;
      for (ServerAndLoad server : groupLoad) {
        int nr = server.getLoad();
        if (nr > maxRegions) {
          maxRegions = nr;
        }
        if (nr < minRegions) {
          minRegions = nr;
        }
        numRegions += nr;
      }
      if (maxRegions - minRegions < 2) {
        // less than 2 between max and min, can't balance
        return;
      }
      int min = numRegions / numServers;
      int max = numRegions % numServers == 0 ? min : min + 1;

      for (ServerAndLoad server : groupLoad) {
        assertTrue(server.getLoad() <= max);
        assertTrue(server.getLoad() >= min);
      }
    }
  }

  private Map<ServerName, List<HRegionInfo>> mockClusterServers() throws IOException {
    assertTrue(servers.size() == regionAssignment.length);
    Map<ServerName, List<HRegionInfo>> assignment = new TreeMap<>();
    for (int i = 0; i < servers.size(); i++) {
      int numRegions = regionAssignment[i];
      List<HRegionInfo> regions = assignedRegions(numRegions, servers.get(i));
      assignment.put(servers.get(i), regions);
    }
    return assignment;
  }

  private boolean isSameAddress(final ServerName left, final ServerName right) {
    // TODO: Make this left.getAddress().equals(right.getAddress())
    if (left == null) return false;
    if (right == null) return false;
    return left.getHostname().compareToIgnoreCase(right.getHostname()) == 0
        && left.getPort() == right.getPort();
  }

  private void updateLoad(ArrayListMultimap<String, ServerAndLoad> previousLoad,
      final ServerName sn, final int diff) {
    for (String groupName : previousLoad.keySet()) {
      ServerAndLoad newSAL = null;
      ServerAndLoad oldSAL = null;
      for (ServerAndLoad sal : previousLoad.get(groupName)) {
        if (isSameAddress(sn, sal.getServerName())) {
          oldSAL = sal;
          newSAL = new ServerAndLoad(sn, sal.getLoad() + diff);
          break;
        }
      }
      if (newSAL != null) {
        previousLoad.remove(groupName, oldSAL);
        previousLoad.put(groupName, newSAL);
        break;
      }
    }
  }

  /**
   * Generate assigned regions to a given server using group information.
   * @param numRegions the num regions to generate
   * @param sn the servername
   * @return the list of regions
   * @throws java.io.IOException Signals that an I/O exception has occurred.
   */
  private List<HRegionInfo> assignedRegions(int numRegions, ServerName sn) throws IOException {
    List<HRegionInfo> regions = new ArrayList<>(numRegions);
    byte[] start = new byte[16];
    byte[] end = new byte[16];
    Bytes.putInt(start, 0, numRegions << 1);
    Bytes.putInt(end, 0, (numRegions << 1) + 1);
    for (int i = 0; i < numRegions; i++) {
      TableName tableName = getTableName(sn);
      regions.add(new HRegionInfo(tableName, start, end, false, regionId++));
    }
    return regions;
  }

  private TableName getTableName(ServerName sn) throws IOException {
    TableName tableName = null;
    GroupInfo groupOfServer = null;
    groupOfServer = loadBalancer.getGalaxyGroupInfoManager().getDefaultGroupInfo();
    for (GroupInfo gInfo : loadBalancer.getGalaxyGroupInfoManager().getGalaxyGroupInfo()) {
      if (gInfo.getServers().contains(sn)) {
        groupOfServer = gInfo;
        break;
      }
    }
    for (HTableDescriptor desc : tableDescs) {
      if (loadBalancer.getGalaxyGroupInfoManager().getGroupInfoForTable(desc.getTableName()).getName()
          .endsWith(groupOfServer.getName())) {
        tableName = desc.getTableName();
      }
    }
    return tableName;
  }

  private ArrayListMultimap<String, ServerAndLoad>
      reconcile(ArrayListMultimap<String, ServerAndLoad> previousLoad, List<RegionPlan> plans) {
    ArrayListMultimap<String, ServerAndLoad> result = ArrayListMultimap.create();
    result.putAll(previousLoad);
    if (plans != null) {
      for (RegionPlan plan : plans) {
        ServerName source = plan.getSource();
        updateLoad(result, source, -1);
        ServerName destination = plan.getDestination();
        updateLoad(result, destination, +1);
      }
    }
    return result;
  }

  /**
   * Test the load balancing algorithm. Invariant is that all servers of the group should be hosting
   * either floor(average) or ceiling(average)
   */
  @Test
  public void testBalanceCluster() throws Exception {
    Map<ServerName, List<HRegionInfo>> servers = mockClusterServers();
    ArrayListMultimap<String, ServerAndLoad> list = convertToGroupBasedMap(servers);
    List<RegionPlan> plans = loadBalancer.balanceCluster(servers);
    ArrayListMultimap<String, ServerAndLoad> balancedCluster = reconcile(list, plans);
    assertClusterAsBalanced(balancedCluster);
  }

}
