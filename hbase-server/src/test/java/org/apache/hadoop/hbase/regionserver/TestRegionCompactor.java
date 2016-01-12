package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertEquals;

import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.ManualEnvironmentEdge;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

@Category(SmallTests.class)
public class TestRegionCompactor {
  private static final Log LOG = LogFactory.getLog(TestRegionCompactor.class);
  private static ManualEnvironmentEdge envEdge;
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static MiniHBaseCluster cluster;
  
  private final static byte[] tableName = Bytes.toBytes("testTable");
  private final static byte[] FAMILY = Bytes.toBytes("cf");
  private final static byte[] QUALIFIER = Bytes.toBytes("q");
  private static HTable table;
  
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setBoolean(RegionCompactor.REGION_ATUO_COMPACT_ENABLE, true);
    TEST_UTIL.getConfiguration().setLong(RegionCompactor.REGION_ATUO_COMPACT_INTERVAL, 24 * 3600 * 1000);
    TEST_UTIL.getConfiguration().setInt(RegionCompactor.REGION_ATUO_COMPACT_PERIOD, 60 * 1000);
    TEST_UTIL.getConfiguration().setInt("hbase.offpeak.start.hour", 0);
    TEST_UTIL.getConfiguration().setInt("hbase.offpeak.end.hour", 6);
    cluster = TEST_UTIL.startMiniCluster(1);
    table = TEST_UTIL.createTable(tableName, FAMILY);
    TEST_UTIL.waitTableAvailable(tableName, 1000);
    insertData(1000);
    
    envEdge = new ManualEnvironmentEdge();
    envEdge.setValue(EnvironmentEdgeManager.currentTimeMillis());
    EnvironmentEdgeManager.injectEdge(envEdge);
  }
  
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    EnvironmentEdgeManager.reset();
    table.close();
    TEST_UTIL.deleteTable(tableName);
    TEST_UTIL.shutdownMiniCluster();
  }
  
  private static void insertData(int rowNum) throws Exception {
    for (int i = 0; i < rowNum; i++) {
      Put put = new Put(Bytes.toBytes("row-" + i));
      put.add(FAMILY, QUALIFIER, Bytes.toBytes("data-" + i));
      table.put(put);
    }
  }
  
  @Test
  public void testWhenDisableRegionCompactor() {
    HRegionServer server = cluster.getRegionServer(0);
    server.getConfiguration().setBoolean(RegionCompactor.REGION_ATUO_COMPACT_ENABLE, false);
    // make sure test in offpeak
    server.getConfiguration().setInt("hbase.offpeak.end.hour", 23);
    RegionCompactor compactor = new RegionCompactor(server, 60000);
    long lastCompactTime = compactor.getLastAutoCompactTime();
    // make sure compact interval is big enough
    envEdge.incValue(36 * 3600 * 1000);
    compactor.chore();
    // no compact
    assertEquals(lastCompactTime, compactor.getLastAutoCompactTime());
    
    server.getConfiguration().setBoolean(RegionCompactor.REGION_ATUO_COMPACT_ENABLE, true);
    compactor = new RegionCompactor(server, 60000);
    lastCompactTime = compactor.getLastAutoCompactTime();
    // make sure compact interval is big enough
    envEdge.incValue(36 * 3600 * 1000);
    compactor.chore();
    // compact
    assertEquals(lastCompactTime + 36 * 3600 * 1000, compactor.getLastAutoCompactTime());
  }
  
  @Test
  public void testCompactInterval() {
    HRegionServer server = cluster.getRegionServer(0);
    // make sure test in offpeak
    server.getConfiguration().setInt("hbase.offpeak.end.hour", 23);
    RegionCompactor compactor = new RegionCompactor(server, 60000);
    long lastCompactTime = compactor.getLastAutoCompactTime();
    // compact interval is smaller than config
    envEdge.incValue(12 * 3600 * 1000);
    compactor.chore();
    // no compact
    assertEquals(lastCompactTime, compactor.getLastAutoCompactTime());
    
    envEdge.incValue(13 * 3600 * 1000);
    compactor.chore();
    // compact
    assertEquals(lastCompactTime + 25 * 3600 * 1000, compactor.getLastAutoCompactTime());
  }
  
  @Test
  public void testOffPeak() {
    HRegionServer server = cluster.getRegionServer(0);
    // not valid config, so isOffPeak always be false
    server.getConfiguration().setInt("hbase.offpeak.end.hour", -1);
    RegionCompactor compactor = new RegionCompactor(server, 60000);
    long lastCompactTime = compactor.getLastAutoCompactTime();
    // make sure compact interval is big enough
    envEdge.incValue(36 * 3600 * 1000);
    compactor.chore();
    // no compact
    assertEquals(lastCompactTime, compactor.getLastAutoCompactTime());
    
    // make sure test in offpeak
    server.getConfiguration().setInt("hbase.offpeak.end.hour", 23);
    compactor = new RegionCompactor(server, 60000);
    lastCompactTime = compactor.getLastAutoCompactTime();
    // make sure compact interval is big enough
    envEdge.incValue(36 * 3600 * 1000);
    compactor.chore();
    // compact
    assertEquals(lastCompactTime + 36 * 3600 * 1000, compactor.getLastAutoCompactTime());
  }
  
  @Test
  public void testLoclityThreshold() throws Exception {
    HRegionServer server = cluster.getRegionServer(0);
    for (HRegion region : server.getOnlineRegions(TableName.valueOf(tableName))) {
      region.flushcache();
    }
    // make sure test in offpeak
    server.getConfiguration().setInt("hbase.offpeak.end.hour", 23);
    server.getConfiguration().setFloat(RegionCompactor.REGION_COMPACT_LOCALITY_THRESHOLD, 0.0f);
    RegionCompactor compactor = new RegionCompactor(server, 60000);
    long lastCompactTime = compactor.getLastAutoCompactTime();
    // in local ut, dn host is 127.0.0.1, rs host is localhost
    compactor.setServerHostName(TEST_UTIL.getDFSCluster().getDataNodes().get(0).getDatanodeId()
        .getHostName());
    // make sure compact interval is big enough
    envEdge.incValue(36 * 3600 * 1000);
    compactor.chore();
    // can compact, but compact request is 0
    assertEquals(lastCompactTime + 36 * 3600 * 1000, compactor.getLastAutoCompactTime());
    assertEquals(0, compactor.getLastTotalCompactRequestCount());
    
    server.getConfiguration().setFloat(RegionCompactor.REGION_COMPACT_LOCALITY_THRESHOLD, 1.0f);
    compactor = new RegionCompactor(server, 60000);
    compactor.setServerHostName(TEST_UTIL.getDFSCluster().getDataNodes().get(0).getDatanodeId()
      .getHostName());
    lastCompactTime = compactor.getLastAutoCompactTime();
    // make sure compact interval is big enough
    envEdge.incValue(36 * 3600 * 1000);
    compactor.chore();
    // compact, because locality threshold is 1.0
    assertEquals(lastCompactTime + 36 * 3600 * 1000, compactor.getLastAutoCompactTime());
    assertEquals(1, compactor.getLastTotalCompactRequestCount());
  }
  
}