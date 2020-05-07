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

import static com.xiaomi.infra.hbase.master.chore.BusyRegionDetector.BUSY_REGION_DETECTOR_ENABLE;
import static com.xiaomi.infra.hbase.master.chore.BusyRegionDetector.BUSY_REGION_DETECTOR_PERIOD;
import static org.apache.hadoop.hbase.util.Bytes.toBytes;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.RegionMetrics;
import org.apache.hadoop.hbase.ServerMetrics;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.assignment.RegionStates;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.PairOfSameType;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category(MediumTests.class)
public class TestBusyRegionDetector {

  private static final Logger LOG = LoggerFactory.getLogger(TestBusyRegionDetector.class);

  private static final int REGION_SERVER_COUNT = 5;
  private static final TableName TABLE_NAME = TableName.valueOf("testBusyRegionDetector");
  private static final byte[] FAMILY_C = toBytes("C");
  private static final byte[] QUALIFIER = toBytes("Q");

  private static Configuration conf;
  private static HBaseTestingUtility UTIL;
  private static Connection connection;
  private static Admin admin;
  private static HMaster master;
  private static Table table;

  @BeforeClass
  public static void setupCluster() throws Exception {
    conf = HBaseConfiguration.create();
    conf.setInt(BUSY_REGION_DETECTOR_PERIOD, (int) TimeUnit.SECONDS.toMillis(1));
    conf.setBoolean(BUSY_REGION_DETECTOR_ENABLE, false);

    UTIL = new HBaseTestingUtility(conf);
    UTIL.startMiniCluster(REGION_SERVER_COUNT);
    connection = UTIL.getConnection();
    admin = UTIL.getAdmin();
    master = UTIL.getMiniHBaseCluster().getMaster();
  }

  @Before
  public void setup() throws Exception {
    table = UTIL.createMultiRegionTable(TABLE_NAME, FAMILY_C, 20);
    putData(table, FAMILY_C, QUALIFIER);
  }

  @AfterClass
  public static void shutdownCluster() throws Exception {
    admin.close();
    connection.close();
    UTIL.shutdownMiniCluster();
  }

  @After
  public void shutdown() throws Exception {
    table.close();
    admin.disableTable(TABLE_NAME);
    admin.deleteTable(TABLE_NAME);
  }

  @Test(timeout = 50000)
  public void testSplitRegion() throws IOException, InterruptedException {
    BusyRegionDetector detector =
      new BusyRegionDetector(master, master, (int) TimeUnit.MINUTES.toMillis(3));
    HRegionLocation regionLocation = table.getRegionLocator().getAllRegionLocations().get(0);
    BusyRegionDetector.RegionBusyInfo
      busyInfo = new BusyRegionDetector.RegionBusyInfo(regionLocation.getRegion().getRegionName());
    busyInfo.updateAndIncreaseTimes(mock(RegionMetrics.class), mock(ServerName.class));
    detector.splitRegions(Collections.singletonList(busyInfo));
    PairOfSameType<RegionInfo> daughters = null;
    long now = System.currentTimeMillis();
    while (daughters == null) {
      daughters = detector.tryToGetDaughters(regionLocation.getRegion().getRegionName());
      if (daughters == null) {
        Thread.sleep(100);
      }
    }

    assertNotNull(daughters);
    LOG.info("Got daughters: {} and {}, parent region: {}" +
        daughters.getFirst().getRegionNameAsString(),
      daughters.getSecond().getRegionNameAsString(),
      regionLocation.getRegion().getRegionNameAsString());
  }

  @Test (timeout = 50000)
  public void testMoveRegion() throws IOException, InterruptedException {
    BusyRegionDetector detector =
      new BusyRegionDetector(master, master, (int) TimeUnit.MINUTES.toMillis(3));

    HRegionLocation regionLocation = table.getRegionLocator().getAllRegionLocations().get(0);
    RegionInfo hri = regionLocation.getRegion();
    ServerName sn = regionLocation.getServerName();

    assertTrue(detector.moveRegion(hri));

    RegionStates regionStates = master.getAssignmentManager().getRegionStates();

    long timeoutTime = System.currentTimeMillis() + 30000;
    while (true) {
      ServerName server = regionStates.getRegionServerOfRegion(hri);
      if (server != null && !sn.equals(server)) {
        break;
      }
      if (System.currentTimeMillis() > timeoutTime) {
        fail("Failed to move the region in time: " + regionStates.getRegionState(hri));
      }
      regionStates.wait(50);
    }
  }

  @Test
  public void testCanSplit() throws IOException {
    BusyRegionDetector detector =
      new BusyRegionDetector(master, master, (int) TimeUnit.MINUTES.toMillis(3));
    HRegionLocation regionLocation = table.getRegionLocator().getAllRegionLocations().get(0);
    assertTrue(detector.canSplit(regionLocation.getRegion().getRegionName()));

    HRegionLocation metaRegionLocation =
      connection.getRegionLocator(TableName.META_TABLE_NAME).getAllRegionLocations().get(0);
    assertFalse(detector.canSplit(metaRegionLocation.getRegion().getRegionName()));
  }

  @Test
  public void testMaybeHasBusyRegion() throws IOException {
    BusyRegionDetector detector =
      new BusyRegionDetector(master, master, (int) TimeUnit.MINUTES.toMillis(3));
    ServerMetrics serverMetrics = mock(ServerMetrics.class);
    when(serverMetrics.getRequestCountPerSecond()).thenReturn(50000L);
    assertTrue(detector.maybeHasBusyRegion(serverMetrics));

    when(serverMetrics.getRequestCountPerSecond()).thenReturn(100000L);
    assertTrue(detector.maybeHasBusyRegion(serverMetrics));

    when(serverMetrics.getRequestCountPerSecond()).thenReturn(40000L);
    assertFalse(detector.maybeHasBusyRegion(serverMetrics));
  }

  @Test
  public void testIsBusyRegion() throws IOException {
    BusyRegionDetector detector =
      new BusyRegionDetector(master, master, (int) TimeUnit.MINUTES.toMillis(3));
    RegionMetrics regionMetrics = mock(RegionMetrics.class);
    when(regionMetrics.getReadRequestsPerSecond()).thenReturn(50000L);
    when(regionMetrics.getWriteRequestsPerSecond()).thenReturn(50000L);
    assertTrue(detector.isBusyRegion(regionMetrics));

    when(regionMetrics.getReadRequestsPerSecond()).thenReturn(40000L);
    when(regionMetrics.getWriteRequestsPerSecond()).thenReturn(100000L);
    assertTrue(detector.isBusyRegion(regionMetrics));

    when(regionMetrics.getReadRequestsPerSecond()).thenReturn(40000L);
    when(regionMetrics.getWriteRequestsPerSecond()).thenReturn(50000L);
    assertFalse(detector.isBusyRegion(regionMetrics));
  }

  @Test
  public void testRegionBusyInfo() {
    BusyRegionDetector.RegionBusyInfo
      info1 = buildRegionBusyInfo(toBytes("region1"), 1, 50000L, 50000L);
    BusyRegionDetector.RegionBusyInfo
      info2 = buildRegionBusyInfo(toBytes("region2"), 1, 50000L, 50000L);
    assertTrue(info1.compareTo(info2) == 0);

    info1 = buildRegionBusyInfo(toBytes("region1"), 2, 50000L, 50000L);
    info2 = buildRegionBusyInfo(toBytes("region2"), 1, 50000L, 50000L);
    assertTrue(info1.compareTo(info2) < 0);

    info1 = buildRegionBusyInfo(toBytes("region1"), 2, 100000L, 50000L);
    info2 = buildRegionBusyInfo(toBytes("region2"), 2, 50000L, 50000L);
    assertTrue(info1.compareTo(info2) < 0);

    info1 = buildRegionBusyInfo(toBytes("region1"), 2, 50000L, 100000L);
    info2 = buildRegionBusyInfo(toBytes("region2"), 2, 50000L, 50000L);
    assertTrue(info1.compareTo(info2) < 0);
  }

  private BusyRegionDetector.RegionBusyInfo buildRegionBusyInfo(byte[] regionName, int busyTimes, long read, long write) {
    BusyRegionDetector.RegionBusyInfo info = new BusyRegionDetector.RegionBusyInfo(regionName);
    RegionMetrics regionMetrics = mock(RegionMetrics.class);
    when(regionMetrics.getReadRequestsPerSecond()).thenReturn(read);
    when(regionMetrics.getWriteRequestsPerSecond()).thenReturn(write);
    for (int i = 0; i < busyTimes; i++) {
      info.updateAndIncreaseTimes(regionMetrics, mock(ServerName.class));
    }
    return info;
  }

  private void putData(Table table, byte[] family, byte[] qualifier) throws IOException {
    for (char c = 'a'; c <= 'z'; c++) {
      for (int i = 0; i < 10000; i++) {
        Put put = new Put(toBytes("" + c + i));
        put.addColumn(family, qualifier, toBytes(i));
        table.put(put);
      }
    }
  }

}
