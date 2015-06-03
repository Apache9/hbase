package org.apache.hadoop.hbase.quotas;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;

@Category({SmallTests.class})
public class TestQuotaCache {
  final Log LOG = LogFactory.getLog(getClass());

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setBoolean(QuotaUtil.QUOTA_CONF_KEY, true);
    TEST_UTIL.getConfiguration().setInt("hbase.hstore.compactionThreshold", 10);
    TEST_UTIL.getConfiguration().setInt("hbase.regionserver.msginterval", 100);
    TEST_UTIL.getConfiguration().setInt("hbase.client.pause", 250);
    TEST_UTIL.getConfiguration().setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 6);
    TEST_UTIL.getConfiguration().setBoolean("hbase.master.enabletable.roundrobin", true);
    TEST_UTIL.startMiniCluster(1);
    TEST_UTIL.waitTableAvailable(QuotaTableUtil.QUOTA_TABLE_NAME.getName());
    QuotaCache.TEST_FORCE_REFRESH = true;
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testCreateRegionSeverQuotaLimiter() {  
    QuotaLimiter rsQuotaLimiter = QuotaCache.createRegionServerQuotaLimiter(TEST_UTIL.getConfiguration());
    assertEquals(QuotaCache.DEFAULT_REGION_SERVER_READ_LIMIT, rsQuotaLimiter.getReadReqsAvailable());
    assertEquals(QuotaCache.DEFAULT_REGION_SERVER_WRITE_LIMIT, rsQuotaLimiter.getWriteReqsAvailable());
    TEST_UTIL.getConfiguration().setInt(QuotaCache.REGION_SERVER_READ_LIMIT_KEY, 2000);
    TEST_UTIL.getConfiguration().setInt(QuotaCache.REGION_SERVER_WRITE_LIMIT_KEY, 2000);
    rsQuotaLimiter = QuotaCache.createRegionServerQuotaLimiter(TEST_UTIL.getConfiguration());
    assertEquals(2000, rsQuotaLimiter.getReadReqsAvailable());
    assertEquals(2000, rsQuotaLimiter.getWriteReqsAvailable());    
  }
  
  @Test
  public void testgetRegionSeverLimiter() {
    RegionServerQuotaManager quotaManager = TEST_UTIL.getMiniHBaseCluster().
		getRegionServer(0).getRegionServerQuotaManager();
	QuotaCache quotaCache = quotaManager.getQuotaCache();
    QuotaLimiter rsQuotaLimiter = quotaCache.getRegionServerLimiter();
    assertEquals(QuotaCache.DEFAULT_REGION_SERVER_READ_LIMIT, rsQuotaLimiter.getReadReqsAvailable());
    assertEquals(QuotaCache.DEFAULT_REGION_SERVER_WRITE_LIMIT, rsQuotaLimiter.getWriteReqsAvailable());
  }
  
  @Test
  public void testUpdateRegionServerQuotaLimiter() throws InterruptedException {
    RegionServerQuotaManager quotaManager = TEST_UTIL.getMiniHBaseCluster().
    	getRegionServer(0).getRegionServerQuotaManager();
	QuotaCache quotaCache = quotaManager.getQuotaCache();  
	TEST_UTIL.getMiniHBaseCluster().getRegionServer(0).getConfiguration().setInt(QuotaCache.REGION_SERVER_READ_LIMIT_KEY, 5000);
	TEST_UTIL.getMiniHBaseCluster().getRegionServer(0).getConfiguration().setInt(QuotaCache.REGION_SERVER_WRITE_LIMIT_KEY, 12000);
	quotaCache.triggerCacheRefresh();
	Thread.sleep(250);
	QuotaLimiter rsQuotaLimiter = quotaCache.getRegionServerLimiter();
	// when limit is bigger than before, the avail will update
    assertEquals(5000, rsQuotaLimiter.getReadReqsAvailable());
    assertEquals(12000, rsQuotaLimiter.getWriteReqsAvailable());
  }
}
