package com.xiaomi.infra.hbase.master.chore;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.RegionLoad;
import org.apache.hadoop.hbase.ServerLoad;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.RegionStates;
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

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.xiaomi.infra.hbase.master.chore.BusyRegionDetector.BUSY_REGION_DETECTOR_ENABLE;
import static com.xiaomi.infra.hbase.master.chore.BusyRegionDetector.BUSY_REGION_DETECTOR_PERIOD;
import static com.xiaomi.infra.hbase.master.chore.BusyRegionDetector.RegionBusyInfo;
import static org.apache.hadoop.hbase.util.Bytes.toBytes;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Category(MediumTests.class)
public class TestBusyRegionDetector {

  private static final Logger LOG = LoggerFactory.getLogger(TestBusyRegionDetector.class);

  private static final int REGION_SERVER_COUNT = 5;
  private static final TableName TABLE_NAME = TableName.valueOf("testBusyRegionDetector");
  private static final byte[] FAMILY_C = toBytes("C");
  private static final byte[] QUALIFIER = toBytes("Q");

  private static Configuration conf;
  private static HBaseTestingUtility UTIL;
  private static HConnection connection;
  private static HBaseAdmin admin;
  private static HMaster master;
  private static HTable table;

  @BeforeClass
  public static void setupCluster() throws Exception {
    conf = HBaseConfiguration.create();
    conf.setInt(BUSY_REGION_DETECTOR_PERIOD, (int) TimeUnit.SECONDS.toMillis(1));
    conf.setBoolean(BUSY_REGION_DETECTOR_ENABLE, false);

    UTIL = new HBaseTestingUtility(conf);
    UTIL.startMiniCluster(REGION_SERVER_COUNT);
    connection = HConnectionManager.createConnection(UTIL.getConfiguration());
    admin = UTIL.getHBaseAdmin();
    master = UTIL.getMiniHBaseCluster().getMaster();
  }

  @Before
  public void setup() throws Exception {
    table = UTIL.createTable(TABLE_NAME, FAMILY_C);
    UTIL.createMultiRegions(UTIL.getConfiguration(), table, FAMILY_C,20);

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

  @Test (timeout = 50000)
  public void testSplitRegion() throws IOException, InterruptedException {
    BusyRegionDetector detector =
        new BusyRegionDetector(master, master, (int) TimeUnit.MINUTES.toMillis(3));
    Map.Entry<HRegionInfo, ServerName> regionLocation = table.getRegionLocations().firstEntry();
    RegionBusyInfo busyInfo = new RegionBusyInfo(regionLocation.getKey().getRegionName());
    busyInfo.updateAndIncreaseTimes(mock(RegionLoad.class));
    detector.splitRegions(Collections.singletonList(busyInfo));
    PairOfSameType<HRegionInfo> daughters = null;
    long now = System.currentTimeMillis();
    while (daughters == null) {
      daughters = detector.tryToGetDaughters(regionLocation.getKey().getRegionName());
      if (daughters == null) {
        Thread.sleep(100);
      }
    }

    assertNotNull(daughters);
    LOG.info("Got daughters: {} and {}, parent region: {}" +
        daughters.getFirst().getRegionNameAsString(),
        daughters.getSecond().getRegionNameAsString(),
        regionLocation.getKey().getRegionNameAsString());
  }

  @Test (timeout = 50000)
  public void testMoveRegion() throws IOException, InterruptedException {
    BusyRegionDetector detector =
        new BusyRegionDetector(master, master, (int) TimeUnit.MINUTES.toMillis(3));

    Map.Entry<HRegionInfo, ServerName> entry = table.getRegionLocations().firstEntry();
    HRegionInfo hri = entry.getKey();
    ServerName sn = entry.getValue();

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
      regionStates.waitForUpdate(50);
    }
  }

  @Test
  public void testCanSplit() throws IOException {
    BusyRegionDetector detector =
        new BusyRegionDetector(master, master, (int) TimeUnit.MINUTES.toMillis(3));
    Map.Entry<HRegionInfo, ServerName> regionLocation = table.getRegionLocations().firstEntry();
    assertTrue(detector.canSplit(regionLocation.getKey().getRegionName()));

    HRegionLocation metaRegionLocation =
        connection.getRegionLocation(TableName.META_TABLE_NAME, TABLE_NAME.getName(), false);
    assertFalse(detector.canSplit(metaRegionLocation.getRegionInfo().getRegionName()));
  }

  @Test
  public void testMaybeHasBusyRegion() throws IOException {
    BusyRegionDetector detector =
        new BusyRegionDetector(master, master, (int) TimeUnit.MINUTES.toMillis(3));
    ServerLoad serverLoad = mock(ServerLoad.class);
    when(serverLoad.getReadRequestsPerSecond()).thenReturn(50000L);
    when(serverLoad.getWriteRequestsPerSecond()).thenReturn(50000L);
    assertTrue(detector.maybeHasBusyRegion(serverLoad));

    when(serverLoad.getReadRequestsPerSecond()).thenReturn(40000L);
    when(serverLoad.getWriteRequestsPerSecond()).thenReturn(100000L);
    assertTrue(detector.maybeHasBusyRegion(serverLoad));

    when(serverLoad.getReadRequestsPerSecond()).thenReturn(40000L);
    when(serverLoad.getWriteRequestsPerSecond()).thenReturn(50000L);
    assertFalse(detector.maybeHasBusyRegion(serverLoad));
  }

  @Test
  public void testIsBusyRegion() throws IOException {
    BusyRegionDetector detector =
        new BusyRegionDetector(master, master, (int) TimeUnit.MINUTES.toMillis(3));
    RegionLoad regionLoad = mock(RegionLoad.class);
    when(regionLoad.getReadRequestsPerSecond()).thenReturn(50000L);
    when(regionLoad.getWriteRequestsPerSecond()).thenReturn(50000L);
    assertTrue(detector.isBusyRegion(regionLoad));

    when(regionLoad.getReadRequestsPerSecond()).thenReturn(40000L);
    when(regionLoad.getWriteRequestsPerSecond()).thenReturn(100000L);
    assertTrue(detector.isBusyRegion(regionLoad));

    when(regionLoad.getReadRequestsPerSecond()).thenReturn(40000L);
    when(regionLoad.getWriteRequestsPerSecond()).thenReturn(50000L);
    assertFalse(detector.isBusyRegion(regionLoad));
  }

  @Test
  public void testRegionBusyInfo() {
    RegionBusyInfo info1 = buildRegionBusyInfo(toBytes("region1"), 1, 50000L, 50000L);
    RegionBusyInfo info2 = buildRegionBusyInfo(toBytes("region2"), 1, 50000L, 50000L);
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

  private RegionBusyInfo buildRegionBusyInfo(byte[] regionName, int busyTimes, long read, long write) {
    RegionBusyInfo info = new RegionBusyInfo(regionName);
    RegionLoad regionLoad = mock(RegionLoad.class);
    when(regionLoad.getReadRequestsPerSecond()).thenReturn(read);
    when(regionLoad.getWriteRequestsPerSecond()).thenReturn(write);
    for (int i = 0; i < busyTimes; i++) {
      info.updateAndIncreaseTimes(regionLoad);
    }
    return info;
  }

  private void putData(HTable table, byte[] family, byte[] qualifier) throws IOException {
    for (char c = 'a'; c <= 'z'; c++) {
      for (int i = 0; i < 10000; i++) {
        Put put = new Put(toBytes("" + c + i));
        put.add(family, qualifier, toBytes(i));
        table.put(put);
      }
    }
    table.flushCommits();
  }

}