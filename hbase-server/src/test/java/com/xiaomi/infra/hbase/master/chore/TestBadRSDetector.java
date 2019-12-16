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

package com.xiaomi.infra.hbase.master.chore;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

@Category(MediumTests.class)
public class TestBadRSDetector {
  private static final Logger LOG = LoggerFactory.getLogger(BadRSDetector.class.getName());
  private static final int REGION_SERVER_COUNT = 5;
  private static final TableName TABLE_NAME = TableName.valueOf("t");
  private static final byte[] ROW = Bytes.toBytes("r");
  private static final byte[] COLUMN_FAMILY = Bytes.toBytes("cf");
  private static final byte[] QUALIFIER = Bytes.toBytes("q");
  private static final byte[] VALUE = Bytes.toBytes("v");

  private static Configuration conf;
  private static HBaseTestingUtility UTIL;
  private static HConnection connection;
  private static HBaseAdmin admin;
  private static HMaster master;
  private static HTableInterface tableInterface;

  @BeforeClass
  public static void setupCluster() throws Exception {
    conf = HBaseConfiguration.create();
    conf.setInt(HConstants.BAD_REGIONSERVER_DETECTOR_PERIOD, (int) TimeUnit.SECONDS.toMillis(1));
    conf.setBoolean(HConstants.BAD_REGIONSERVER_DETECTOR_ENABLE, false);

    UTIL = new HBaseTestingUtility(conf);
    UTIL.startMiniCluster(REGION_SERVER_COUNT);
    connection = HConnectionManager.createConnection(UTIL.getConfiguration());
    admin = UTIL.getHBaseAdmin();
    master = UTIL.getMiniHBaseCluster().getMaster();
  }

  @Before
  public void setup() throws Exception {
    HTable table = UTIL.createTable(TABLE_NAME, COLUMN_FAMILY);

    UTIL.createMultiRegions(UTIL.getConfiguration(), table, COLUMN_FAMILY,20);
    tableInterface = connection.getTable(TABLE_NAME);

    for (char first = 'a'; first <= 'z'; ++first) {
      String row = "" + first;
      for (char second = 'a'; second <= 'z'; ++second) {
        row += second;
        for (char third = 'a'; third <= 'z'; ++third) {
          row += third;
          Put put = new Put(ROW);
          put.add(COLUMN_FAMILY, QUALIFIER, VALUE);
          tableInterface.put(put);
        }
      }
    }

  }

  @AfterClass
  public static void shutdownCluster() throws Exception {
    admin.close();
    connection.close();
    UTIL.shutdownMiniCluster();
  }

  @After
  public void shutdown() throws Exception {
    tableInterface.close();
    admin.disableTable(TABLE_NAME);
    admin.deleteTable(TABLE_NAME);
  }

  @Test
  public void testBadRsDetector() throws Exception {
    BadRSDetector badRSDetector =
        new BadRSDetector(master, master, (int) TimeUnit.MINUTES.toMillis(5));
    Assert.assertEquals(UTIL.getMiniHBaseCluster().getRegionServerThreads().size(), REGION_SERVER_COUNT);
    ServerName server0 = UTIL.getMiniHBaseCluster().getRegionServer(0).getServerName();
    ServerName server1 = UTIL.getMiniHBaseCluster().getRegionServer(1).getServerName();

    Collection<ServerName> serverNames =
        master.getAssignmentManager().getRegionStates().getRegionAssignments().values();
    for (ServerName serverName : serverNames) {
      ServerName dest =
          (serverName.getHostAndPort().contains(server0.getHostAndPort()) ? server1 : server0);
      Set<HRegionInfo> sourceRegionInfos =
          master.getAssignmentManager().getRegionStates().getServerRegions(serverName);
      Map<ServerName, Set<HRegionInfo>> sourceMap = new HashMap<>();
      sourceMap.put(serverName, sourceRegionInfos);
      Set<HRegionInfo> failedRegionInfos =
          badRSDetector.vacateRegionServers(sourceMap, Arrays.asList(dest));
      Assert.assertTrue("Fail to move RegionInfo " + badRSDetector.toString(failedRegionInfos),
          failedRegionInfos.isEmpty());
    }
  }

  @Test
  public void testRestartServer() throws Exception {
    Assert.assertEquals(UTIL.getMiniHBaseCluster().getRegionServerThreads().size(), REGION_SERVER_COUNT);
    ServerName server0 = UTIL.getMiniHBaseCluster().getRegionServer(0).getServerName();
    BadRSDetector badRSDetector =
        new BadRSDetector(master, master, (int) TimeUnit.MINUTES.toMillis(5));
    badRSDetector.stopRegionServer(server0.getHostAndPort());
    UTIL.getMiniHBaseCluster().waitForRegionServerToStop(server0, TimeUnit.MINUTES.toMillis(1));
    boolean isStoppedSuccess = master.getServerManager().getOnlineServersList().stream()
        .noneMatch(s -> s.getHostAndPort().equals(server0.getHostAndPort()));
    Assert.assertTrue("Failed to stopped " + server0, isStoppedSuccess);
  }

  @Test
  public void testRecover() throws Exception {
    BadRSDetector badRSDetector =
        new BadRSDetector(master, master, (int) TimeUnit.MINUTES.toMillis(5));
    Assert.assertEquals(UTIL.getMiniHBaseCluster().getRegionServerThreads().size(), REGION_SERVER_COUNT);
    ServerName server0 = UTIL.getMiniHBaseCluster().getRegionServer(0).getServerName();
    ServerName server1 = UTIL.getMiniHBaseCluster().getRegionServer(1).getServerName();

    Map<HRegionInfo, ServerName> regionAssignments =
        master.getAssignmentManager().getRegionStates().getRegionAssignments();
    Map<ServerName, Set<HRegionInfo>> serverMap = reverseToMap(regionAssignments);
    for (ServerName serverName : serverMap.keySet()) {
      ServerName dest =
          (serverName.getHostAndPort().contains(server0.getHostAndPort()) ? server1 : server0);
      Set<HRegionInfo> sourceRegionInfos =
          master.getAssignmentManager().getRegionStates().getServerRegions(dest);
      Map<ServerName, Set<HRegionInfo>> sourceMap = new HashMap<>();
      sourceMap.put(serverName, sourceRegionInfos);
      Set<HRegionInfo> failedRegionInfos =
          badRSDetector.recoverRegionServers(sourceMap, Arrays.asList(dest));
      Assert.assertTrue("Fail to move RegionInfo " + badRSDetector.toString(failedRegionInfos),
          failedRegionInfos.isEmpty());
    }
  }

  private Map<ServerName, Set<HRegionInfo>> reverseToMap(
      Map<HRegionInfo, ServerName> regionAssignments) {
    return regionAssignments.entrySet().stream().collect(Collectors
        .toMap(Map.Entry::getValue, e -> Sets.newHashSet(e.getKey()),
            (Set<HRegionInfo> oldSet, Set<HRegionInfo> newSet) -> {
              oldSet.addAll(newSet);
              return oldSet;
            }));
  }
}