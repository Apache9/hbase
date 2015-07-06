package org.apache.hadoop.hbase.quotas;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil.RegionServerThread;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category({ SmallTests.class })
public class TestQuotaCache {
  final Log LOG = LogFactory.getLog(getClass());

  private final static int REFRESH_TIME = 5 * 3600;

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  public static final byte[] ROW = Bytes.toBytes("row");
  private final static byte[] FAMILY = Bytes.toBytes("cf");
  private final static byte[] QUALIFIER = Bytes.toBytes("q");

  private final static TableName[] TABLE_NAMES = new TableName[] { TableName.valueOf("TestQuota0"),
      TableName.valueOf("TestQuota1"), TableName.valueOf("TestQuota2") };

  private static HTable[] tables;

  private static Map<TableName, Integer> tableRegionsNumMap = new HashMap<TableName, Integer>();

  private static final int regionServerNum = 5;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setBoolean(QuotaUtil.QUOTA_CONF_KEY, true);
    TEST_UTIL.getConfiguration().setInt(QuotaCache.REFRESH_CONF_KEY, REFRESH_TIME);
    TEST_UTIL.getConfiguration().setInt("hbase.hstore.compactionThreshold", 10);
    TEST_UTIL.getConfiguration().setInt("hbase.regionserver.msginterval", 100);
    TEST_UTIL.getConfiguration().setInt("hbase.client.pause", 250);
    TEST_UTIL.getConfiguration().setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 6);
    TEST_UTIL.getConfiguration().setBoolean("hbase.master.enabletable.roundrobin", true);
    TEST_UTIL.startMiniCluster(regionServerNum);
    TEST_UTIL.waitTableAvailable(QuotaTableUtil.QUOTA_TABLE_NAME.getName());
    QuotaCache.TEST_FORCE_REFRESH = true;

    tableRegionsNumMap.put(TABLE_NAMES[0], 4);
    tableRegionsNumMap.put(TABLE_NAMES[1], 5);
    tableRegionsNumMap.put(TABLE_NAMES[2], 8);

    tables = new HTable[TABLE_NAMES.length];
    for (int i = 0; i < TABLE_NAMES.length; ++i) {
      tables[i] = TEST_UTIL.createTable(TABLE_NAMES[i], new byte[][] { FAMILY }, 3,
        Bytes.toBytes("aaaaa"), Bytes.toBytes("zzzzz"), tableRegionsNumMap.get(TABLE_NAMES[i]));
      TEST_UTIL.waitTableAvailable(TABLE_NAMES[i].getName());
    }
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    for (int i = 0; i < tables.length; ++i) {
      if (tables[i] != null) {
        tables[i].close();
        TEST_UTIL.deleteTable(TABLE_NAMES[i]);
      }
    }

    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testCreateRegionSeverQuotaLimiter() {
    QuotaLimiter rsQuotaLimiter = QuotaCache.createRegionServerQuotaLimiter(TEST_UTIL
        .getConfiguration());
    assertEquals(QuotaCache.DEFAULT_REGION_SERVER_READ_LIMIT, rsQuotaLimiter.getReadReqsAvailable());
    assertEquals(QuotaCache.DEFAULT_REGION_SERVER_WRITE_LIMIT,
      rsQuotaLimiter.getWriteReqsAvailable());
    TEST_UTIL.getConfiguration().setInt(QuotaCache.REGION_SERVER_READ_LIMIT_KEY, 2000);
    TEST_UTIL.getConfiguration().setInt(QuotaCache.REGION_SERVER_WRITE_LIMIT_KEY, 2000);
    rsQuotaLimiter = QuotaCache.createRegionServerQuotaLimiter(TEST_UTIL.getConfiguration());
    assertEquals(2000, rsQuotaLimiter.getReadReqsAvailable());
    assertEquals(2000, rsQuotaLimiter.getWriteReqsAvailable());
  }

  @Test
  public void testGetRegionSeverLimiter() {
    RegionServerQuotaManager quotaManager = TEST_UTIL.getMiniHBaseCluster().getRegionServer(0)
        .getRegionServerQuotaManager();
    QuotaCache quotaCache = quotaManager.getQuotaCache();
    QuotaLimiter rsQuotaLimiter = quotaCache.getRegionServerLimiter();
    LOG.info(rsQuotaLimiter);
    assertEquals(QuotaCache.DEFAULT_REGION_SERVER_READ_LIMIT, rsQuotaLimiter.getReadReqsAvailable());
    assertEquals(QuotaCache.DEFAULT_REGION_SERVER_WRITE_LIMIT,
      rsQuotaLimiter.getWriteReqsAvailable());
  }

  @Test
  public void testUpdateLocalQuotaFactors() throws Exception {
    assertEquals(5, TEST_UTIL.getMiniHBaseCluster().getRegionServerThreads().size());

    HBaseAdmin admin = TEST_UTIL.getHBaseAdmin();
    String userName = User.getCurrent().getShortName();
    for (int i = 0; i < TABLE_NAMES.length; ++i) {
      admin.setQuota(QuotaSettingsFactory.throttleUser(userName, TABLE_NAMES[i],
        ThrottleType.REQUEST_NUMBER, 1000, TimeUnit.SECONDS));
    }

    for (RegionServerThread rst : TEST_UTIL.getMiniHBaseCluster().getRegionServerThreads()) {
      RegionServerQuotaManager quotaManager = rst.getRegionServer().getRegionServerQuotaManager();
      QuotaCache quotaCache = quotaManager.getQuotaCache();
      for (int i = 0; i < TABLE_NAMES.length; i++) {
        if (!rst.getRegionServer().getOnlineTables().contains(TABLE_NAMES[i])) {
          continue;
        }
        // for update, need get first
        quotaCache.getUserLimiter(User.getCurrent().getUGI(), TABLE_NAMES[i]);
        quotaCache.triggerCacheRefresh();
        Thread.sleep(250);
        QuotaLimiter quotaLimiter = quotaCache.getUserLimiter(User.getCurrent().getUGI(),
          TABLE_NAMES[i]);
        LOG.info(quotaLimiter);
        if (i == 0) {
          // table have 4 regions, if rs have 1 region, quota is 1000/4, else quota is 1000
          if (rst.getRegionServer().getOnlineRegions(TABLE_NAMES[i]).size() >= 1) {
            assertEquals(250, quotaLimiter.getReqsAvailable());
          } else {
            assertEquals(1000, quotaLimiter.getReqsAvailable());
          }
        } else if (i == 1) {
          // table have 5 regions, which is equal to rs number, so quota is 1000/5
          assertTrue(quotaLimiter.getReqsAvailable() == 200);
        } else if (i == 2) {
          // table have 8 regions, if rs have more or equal to 2 regions, then quota is 1000 * 2/8
          // otherwise quota is 1000 * 1/8
          if (rst.getRegionServer().getOnlineRegions(TABLE_NAMES[i]).size() >= 2) {
            assertEquals(250, quotaLimiter.getReqsAvailable());
          } else {
            assertEquals(125, quotaLimiter.getReqsAvailable());
          }
        }
      }
    }

    admin.setQuota(QuotaSettingsFactory.unthrottleUser(userName));
  }

  @Test
  public void testComputeLocalFactor() {
    // Case 1: table's regions number is small than the regionserver number
    // Case 1.1 : regionserver have more than 1 region of this table
    assertEquals(1.0, QuotaCache.computeLocalFactor(5, 1, 1), 0.001);
    assertEquals(0.2, QuotaCache.computeLocalFactor(10, 5, 1), 0.001);
    assertEquals(0.2, QuotaCache.computeLocalFactor(10, 5, 2), 0.001);
    assertEquals(0.1, QuotaCache.computeLocalFactor(15, 10, 1), 0.001);
    assertEquals(0.1, QuotaCache.computeLocalFactor(15, 10, 2), 0.001);
    assertEquals(0.1, QuotaCache.computeLocalFactor(15, 10, 3), 0.001);
    
    // Case 1.2 : regionserver have no region of this table
    assertEquals(1.0, QuotaCache.computeLocalFactor(5, 1, 0), 0.001);
    assertEquals(1.0, QuotaCache.computeLocalFactor(10, 5, 0), 0.001);
    assertEquals(1.0, QuotaCache.computeLocalFactor(15, 10, 0), 0.001);
    
    // Case 2: table's regions number is more or equal to the regionserver number
    // Case 2.1 : table's regions number mod regionserver number equal to 0
    // 5 % 5 = 0, quota will be distributed by the table's region number
    assertEquals(0.2, QuotaCache.computeLocalFactor(5, 5, 0), 0.001);
    assertEquals(0.2, QuotaCache.computeLocalFactor(5, 5, 1), 0.001);
    assertEquals(0.2, QuotaCache.computeLocalFactor(5, 5, 2), 0.001);
    // 20 % 10 = 0, quota will be distributed by the regionserver number
    assertEquals(0.1, QuotaCache.computeLocalFactor(10, 20, 1), 0.001);
    assertEquals(0.1, QuotaCache.computeLocalFactor(10, 20, 2), 0.001);
    assertEquals(0.1, QuotaCache.computeLocalFactor(10, 20, 3), 0.001);
    
    // Case 2.2 : table's regions cann't average distribute quota to every regionserver   
    // Case 2.2.1 : regionserver have more regions of this table than the average
    // the average number is 8/5 = 1.6, more than 1.6 will get 2/8 quota
    assertEquals(0.25, QuotaCache.computeLocalFactor(5, 8, 2), 0.001);
    assertEquals(0.25, QuotaCache.computeLocalFactor(5, 8, 4), 0.001);
    // the average number is 25/10 = 2.5, more than 2.5 will get 3/25 quota
    assertEquals(0.12, QuotaCache.computeLocalFactor(10, 25, 3), 0.001);
    assertEquals(0.12, QuotaCache.computeLocalFactor(10, 25, 5), 0.001);
    
    // Case 2.2.2 : regionserver have fewer regions of this table than the average
    // the average number is 8/5 = 1.6, fewer than 1.6 will get 1/8 quota
    assertEquals(0.125, QuotaCache.computeLocalFactor(5, 8, 0), 0.001);
    assertEquals(0.125, QuotaCache.computeLocalFactor(5, 8, 1), 0.001);
    // the average number is 25/10 = 2.5, fewer than 2.5 will get 2/25 quota
    assertEquals(0.08, QuotaCache.computeLocalFactor(10, 25, 0), 0.001);
    assertEquals(0.08, QuotaCache.computeLocalFactor(10, 25, 1), 0.001);
    assertEquals(0.08, QuotaCache.computeLocalFactor(10, 25, 2), 0.001);
  }
}
