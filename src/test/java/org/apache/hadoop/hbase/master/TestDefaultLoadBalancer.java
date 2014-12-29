/**
 * Copyright 2011 The Apache Software Foundation
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
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.ServerName;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;


/**
 * Test the load balancer that is created by default.
 */
@Category(MediumTests.class)
public class TestDefaultLoadBalancer extends BalancerTestBase {
  private static final Log LOG = LogFactory.getLog(TestDefaultLoadBalancer.class);

  private static LoadBalancer loadBalancer;

  private static Random rand;

  @BeforeClass
  public static void beforeAllTests() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    conf.set("hbase.regions.slop", "0");
    loadBalancer = new DefaultLoadBalancer();
    loadBalancer.setConf(conf);
    rand = new Random();
  }

  // int[testnum][servernumber] -> numregions
  int [][] clusterStateMocks = new int [][] {
      // 1 node
      new int [] { 0 },
      new int [] { 1 },
      new int [] { 10 },
      // 2 node
      new int [] { 0, 0 },
      new int [] { 2, 0 },
      new int [] { 2, 1 },
      new int [] { 2, 2 },
      new int [] { 2, 3 },
      new int [] { 2, 4 },
      new int [] { 1, 1 },
      new int [] { 0, 1 },
      new int [] { 10, 1 },
      new int [] { 14, 1432 },
      new int [] { 47, 53 },
      // 3 node
      new int [] { 0, 1, 2 },
      new int [] { 1, 2, 3 },
      new int [] { 0, 2, 2 },
      new int [] { 0, 3, 0 },
      new int [] { 0, 4, 0 },
      new int [] { 20, 20, 0 },
      // 4 node
      new int [] { 0, 1, 2, 3 },
      new int [] { 4, 0, 0, 0 },
      new int [] { 5, 0, 0, 0 },
      new int [] { 6, 6, 0, 0 },
      new int [] { 6, 2, 0, 0 },
      new int [] { 6, 1, 0, 0 },
      new int [] { 6, 0, 0, 0 },
      new int [] { 4, 4, 4, 7 },
      new int [] { 4, 4, 4, 8 },
      new int [] { 0, 0, 0, 7 },
      // 5 node
      new int [] { 1, 1, 1, 1, 4 },
      // more nodes
      new int [] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 },
      new int [] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 10 },
      new int [] { 6, 6, 5, 6, 6, 6, 6, 6, 6, 1 },
      new int [] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 54 },
      new int [] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 55 },
      new int [] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 56 },
      new int [] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 16 },
      new int [] { 1, 1, 1, 1, 1, 1, 1, 1, 1, 8 },
      new int [] { 1, 1, 1, 1, 1, 1, 1, 1, 1, 9 },
      new int [] { 1, 1, 1, 1, 1, 1, 1, 1, 1, 10 },
      new int [] { 1, 1, 1, 1, 1, 1, 1, 1, 1, 123 },
      new int [] { 1, 1, 1, 1, 1, 1, 1, 1, 1, 155 },
      new int [] { 0, 0, 144, 1, 1, 1, 1, 1123, 133, 138, 12, 1444 },
      new int [] { 0, 0, 144, 1, 0, 4, 1, 1123, 133, 138, 12, 1444 },
      new int [] { 1538, 1392, 1561, 1557, 1535, 1553, 1385, 1542, 1619 }
  };

  int [][] regionsAndServersMocks = new int [][] {
      // { num regions, num servers }
      new int [] { 0, 0 },
      new int [] { 0, 1 },
      new int [] { 1, 1 },
      new int [] { 2, 1 },
      new int [] { 10, 1 },
      new int [] { 1, 2 },
      new int [] { 2, 2 },
      new int [] { 3, 2 },
      new int [] { 1, 3 },
      new int [] { 2, 3 },
      new int [] { 3, 3 },
      new int [] { 25, 3 },
      new int [] { 2, 10 },
      new int [] { 2, 100 },
      new int [] { 12, 10 },
      new int [] { 12, 100 },
  };

  /**
   * Test the load balancing algorithm.
   *
   * Invariant is that all servers should be hosting either
   * floor(average) or ceiling(average)
   *
   * @throws Exception
   */
  @Test
  public void testBalanceCluster() throws Exception {

    for(int [] mockCluster : clusterStateMocks) {
      Map<ServerName, List<HRegionInfo>> servers =  mockClusterServers(mockCluster);
      List <ServerAndLoad> list = convertToList(servers);
      LOG.info("Mock Cluster : " + printMock(list) + " " + printStats(list));
      List<RegionPlan> plans = loadBalancer.balanceCluster(servers);
      List<ServerAndLoad> balancedCluster = reconcile(list, plans, servers);
      LOG.info("Mock Balance : " + printMock(balancedCluster));
      assertClusterAsBalanced(balancedCluster);
      for(Map.Entry<ServerName, List<HRegionInfo>> entry : servers.entrySet()) {
        returnRegions(entry.getValue());
        returnServer(entry.getKey());
      }
    }

  }

  /**
   * Tests immediate assignment.
   *
   * Invariant is that all regions have an assignment.
   *
   * @throws Exception
   */
  @Test
  public void testImmediateAssignment() throws Exception {
    for(int [] mock : regionsAndServersMocks) {
      LOG.debug("testImmediateAssignment with " + mock[0] + " regions and " + mock[1] + " servers");
      List<HRegionInfo> regions = randomRegions(mock[0]);
      List<ServerAndLoad> servers = randomServers(mock[1], 0);
      List<ServerName> list = getListOfServerNames(servers);
      Map<HRegionInfo,ServerName> assignments =
        loadBalancer.immediateAssignment(regions, list);
      assertImmediateAssignment(regions, list, assignments);
      returnRegions(regions);
      returnServers(list);
    }
  }

  /**
   * All regions have an assignment.
   * @param regions
   * @param servers
   * @param assignments
   */
  private void assertImmediateAssignment(List<HRegionInfo> regions,
      List<ServerName> servers, Map<HRegionInfo, ServerName> assignments) {
    for(HRegionInfo region : regions) {
      assertTrue(assignments.containsKey(region));
    }
  }

  /**
   * Tests the bulk assignment used during cluster startup.
   *
   * Round-robin.  Should yield a balanced cluster so same invariant as the load
   * balancer holds, all servers holding either floor(avg) or ceiling(avg).
   *
   * @throws Exception
   */
  @Test
  public void testBulkAssignment() throws Exception {
    for(int [] mock : regionsAndServersMocks) {
      LOG.debug("testBulkAssignment with " + mock[0] + " regions and " + mock[1] + " servers");
      List<HRegionInfo> regions = randomRegions(mock[0]);
      List<ServerAndLoad> servers = randomServers(mock[1], 0);
      List<ServerName> list = getListOfServerNames(servers);
      Map<ServerName, List<HRegionInfo>> assignments =
        loadBalancer.roundRobinAssignment(regions, list);
      float average = (float)regions.size()/servers.size();
      int min = (int)Math.floor(average);
      int max = (int)Math.ceil(average);
      if(assignments != null && !assignments.isEmpty()) {
        for(List<HRegionInfo> regionList : assignments.values()) {
          assertTrue(regionList.size() == min || regionList.size() == max);
        }
      }
      returnRegions(regions);
      returnServers(list);
    }
  }

  /**
   * Test the cluster startup bulk assignment which attempts to retain
   * assignment info.
   * @throws Exception
   */
  @Test
  public void testRetainAssignment() throws Exception {
    // Test simple case where all same servers are there
    List<ServerAndLoad> servers = randomServers(10, 10);
    List<HRegionInfo> regions = randomRegions(100);
    Map<HRegionInfo, ServerName> existing =
      new TreeMap<HRegionInfo, ServerName>();
    for (int i = 0; i < regions.size(); i++) {
      ServerName sn = servers.get(i % servers.size()).getServerName();
      // The old server would have had same host and port, but different
      // start code!
      ServerName snWithOldStartCode =
        new ServerName(sn.getHostname(), sn.getPort(), sn.getStartcode() - 10);
      existing.put(regions.get(i), snWithOldStartCode);
    }
    List<ServerName> listOfServerNames = getListOfServerNames(servers);
    Map<ServerName, List<HRegionInfo>> assignment =
      loadBalancer.retainAssignment(existing, listOfServerNames);
    assertRetainedAssignment(existing, listOfServerNames, assignment);

    // Include two new servers that were not there before
    List<ServerAndLoad> servers2 =
      new ArrayList<ServerAndLoad>(servers);
    servers2.add(randomServer(10));
    servers2.add(randomServer(10));
    listOfServerNames = getListOfServerNames(servers2);
    assignment = loadBalancer.retainAssignment(existing, listOfServerNames);
    assertRetainedAssignment(existing, listOfServerNames, assignment);

    // Remove two of the servers that were previously there
    List<ServerAndLoad> servers3 =
      new ArrayList<ServerAndLoad>(servers);
    servers3.remove(0);
    servers3.remove(0);
    listOfServerNames = getListOfServerNames(servers3);
    assignment = loadBalancer.retainAssignment(existing, listOfServerNames);
    assertRetainedAssignment(existing, listOfServerNames, assignment);
  }

  private List<ServerName> getListOfServerNames(final List<ServerAndLoad> sals) {
    List<ServerName> list = new ArrayList<ServerName>();
    for (ServerAndLoad e: sals) {
      list.add(e.getServerName());
    }
    return list;
  }

  /**
   * Asserts a valid retained assignment plan.
   * <p>
   * Must meet the following conditions:
   * <ul>
   *   <li>Every input region has an assignment, and to an online server
   *   <li>If a region had an existing assignment to a server with the same
   *       address a a currently online server, it will be assigned to it
   * </ul>
   * @param existing
   * @param servers
   * @param assignment
   */
  private void assertRetainedAssignment(
      Map<HRegionInfo, ServerName> existing, List<ServerName> servers,
      Map<ServerName, List<HRegionInfo>> assignment) {
    // Verify condition 1, every region assigned, and to online server
    Set<ServerName> onlineServerSet = new TreeSet<ServerName>(servers);
    Set<HRegionInfo> assignedRegions = new TreeSet<HRegionInfo>();
    for (Map.Entry<ServerName, List<HRegionInfo>> a : assignment.entrySet()) {
      assertTrue("Region assigned to server that was not listed as online",
          onlineServerSet.contains(a.getKey()));
      for (HRegionInfo r : a.getValue()) assignedRegions.add(r);
    }
    assertEquals(existing.size(), assignedRegions.size());

    // Verify condition 2, if server had existing assignment, must have same
    Set<String> onlineHostNames = new TreeSet<String>();
    for (ServerName s : servers) {
      onlineHostNames.add(s.getHostname());
    }
    
    for (Map.Entry<ServerName, List<HRegionInfo>> a : assignment.entrySet()) {
      ServerName assignedTo = a.getKey();
      for (HRegionInfo r : a.getValue()) {
        ServerName address = existing.get(r);
        if (address != null && onlineHostNames.contains(address.getHostname())) {
          // this region was prevously assigned somewhere, and that
          // host is still around, then it should be re-assigned on the
          // same host
          assertEquals(address.getHostname(), assignedTo.getHostname());
        }
      }
    }
  }

  @org.junit.Rule
  public org.apache.hadoop.hbase.ResourceCheckerJUnitRule cu =
    new org.apache.hadoop.hbase.ResourceCheckerJUnitRule();
}

