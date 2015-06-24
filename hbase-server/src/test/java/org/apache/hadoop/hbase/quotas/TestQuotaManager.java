package org.apache.hadoop.hbase.quotas;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.quotas.OperationQuota.OperationType;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil.RegionServerThread;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category({ SmallTests.class })
public class TestQuotaManager {
  final Log LOG = LogFactory.getLog(getClass());

  private final static int REFRESH_TIME = 5 * 3600;

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  public static final byte[] ROW = Bytes.toBytes("row");
  private final static byte[] FAMILY = Bytes.toBytes("cf");

  private final static TableName TABLE_NAME = TableName.valueOf("TestQuota0");

  private static HTable table;

  private static final int regionServerNum = 1;

  private HBaseAdmin admin;
  private String userName;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setBoolean(QuotaUtil.QUOTA_CONF_KEY, true);
    TEST_UTIL.getConfiguration().setInt(QuotaCache.REFRESH_CONF_KEY, REFRESH_TIME);
    TEST_UTIL.getConfiguration().setInt("hbase.hstore.compactionThreshold", 10);
    TEST_UTIL.getConfiguration().setInt("hbase.regionserver.msginterval", 100);
    TEST_UTIL.getConfiguration().setInt("hbase.client.pause", 250);
    TEST_UTIL.getConfiguration().setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 6);
    TEST_UTIL.getConfiguration().setBoolean("hbase.master.enabletable.roundrobin", true);
    TEST_UTIL.getConfiguration().setInt(QuotaCache.REGION_SERVER_READ_LIMIT_KEY, 10);
    TEST_UTIL.getConfiguration().setInt(QuotaCache.REGION_SERVER_WRITE_LIMIT_KEY, 10);
    TEST_UTIL.getConfiguration().setClass(RateLimiter.QUOTA_RATE_LIMITER_CONF_KEY,
      FixedIntervalRateLimiter.class, RateLimiter.class);
    TEST_UTIL.startMiniCluster(regionServerNum);
    TEST_UTIL.waitTableAvailable(QuotaTableUtil.QUOTA_TABLE_NAME.getName());
    QuotaCache.TEST_FORCE_REFRESH = true;

    table = TEST_UTIL.createTable(TABLE_NAME, new byte[][] { FAMILY });
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    table.close();
    TEST_UTIL.deleteTable(TABLE_NAME);
    TEST_UTIL.shutdownMiniCluster();
  }

  @After
  public void tearDown() throws Exception {
    for (RegionServerThread rst : TEST_UTIL.getMiniHBaseCluster().getRegionServerThreads()) {
      RegionServerQuotaManager quotaManager = rst.getRegionServer().getRegionServerQuotaManager();
      QuotaCache quotaCache = quotaManager.getQuotaCache();
      quotaCache.getNamespaceQuotaCache().clear();
      quotaCache.getTableQuotaCache().clear();
      quotaCache.getUserQuotaCache().clear();
    }
  }

  @Before
  public void setupQuota() throws IOException {
    admin = TEST_UTIL.getHBaseAdmin();
    userName = User.getCurrent().getShortName();

    admin.setQuota(QuotaSettingsFactory.throttleUser(userName, TABLE_NAME,
      ThrottleType.READ_NUMBER, 5, TimeUnit.SECONDS));
  }

  @Test
  public void testCheckQuota() throws IOException, InterruptedException {
    final RegionServerQuotaManager quotaManager = TEST_UTIL.getMiniHBaseCluster()
        .getRegionServer(0).getRegionServerQuotaManager();
    final HRegion region = TEST_UTIL.getMiniHBaseCluster().getRegionServer(0)
        .getOnlineRegions(TABLE_NAME).get(0);
    // update cache need one get first
    quotaManager.getQuotaCache().getUserLimiter(User.getCurrent().getUGI(), table.getName());
    Thread.sleep(1000);
    quotaManager.getQuotaCache().triggerCacheRefresh();
    Thread.sleep(1000);

    // allow exceed to 10
    for (int i = 0; i < 10; i++) {
      quotaManager.checkQuota(region, OperationType.GET);
    }

    runWithExpectedException(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        quotaManager.checkQuota(region, OperationType.GET);
        return null;
      }
    }, ThrottlingException.class);
  }

  @Test
  public void testUserNotSetQuota() throws IOException, InterruptedException {
    // remove user quota
    admin.setQuota(QuotaSettingsFactory.unthrottleUser(userName));
    final RegionServerQuotaManager quotaManager = TEST_UTIL.getMiniHBaseCluster()
        .getRegionServer(0).getRegionServerQuotaManager();
    final HRegion region = TEST_UTIL.getMiniHBaseCluster().getRegionServer(0)
        .getOnlineRegions(TABLE_NAME).get(0);
    // update cache need one get first
    quotaManager.getQuotaCache().getUserLimiter(User.getCurrent().getUGI(), table.getName());
    Thread.sleep(1000);
    quotaManager.getQuotaCache().triggerCacheRefresh();
    Thread.sleep(1000);

    // will throttle by rs quota, allow exceed to 10
    for (int i = 0; i < 10; i++) {
      quotaManager.checkQuota(region, OperationType.GET);
    }

    runWithExpectedException(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        quotaManager.checkQuota(region, OperationType.GET);
        return null;
      }
    }, ThrottlingException.class);
  }

  @Test
  public void testGrabQuota() throws IOException, InterruptedException {
    final RegionServerQuotaManager quotaManager = TEST_UTIL.getMiniHBaseCluster()
        .getRegionServer(0).getRegionServerQuotaManager();
    final UserGroupInformation ugi = User.getCurrent().getUGI();
    // update cache need one get first
    quotaManager.getQuotaCache().getUserLimiter(ugi, table.getName());
    Thread.sleep(1000);
    quotaManager.getQuotaCache().triggerCacheRefresh();
    Thread.sleep(1000);

    // allow exceed to 10
    for (int i = 0; i < 10; i++) {
      quotaManager.checkQuota(ugi, table.getName(), OperationType.GET);
      quotaManager.grabQuota(ugi, table.getName(), Result.create(new ArrayList<Cell>()));
    }
    runWithExpectedException(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        quotaManager.checkQuota(ugi, table.getName(), OperationType.GET);
        return null;
      }
    }, ThrottlingException.class);
  }

  @Test
  public void testGetQuota() throws IOException, InterruptedException {
    RegionServerQuotaManager quotaManager = TEST_UTIL.getMiniHBaseCluster().getRegionServer(0)
        .getRegionServerQuotaManager();
    UserGroupInformation ugi = User.getCurrent().getUGI();
    OperationQuota quota = quotaManager.getQuota(ugi, table.getName());
    Thread.sleep(1000);
    quota = quotaManager.getQuota(ugi, table.getName());
    assertTrue(quota.getClass().getName().equals(AllowExceedOperationQuota.class.getName()));
  }

  @Test
  public void testSimulateThrottle() throws Exception {
    RegionServerQuotaManager quotaManager = TEST_UTIL.getMiniHBaseCluster().getRegionServer(0)
        .getRegionServerQuotaManager();
    HRegion region = TEST_UTIL.getMiniHBaseCluster().getRegionServer(0)
        .getOnlineRegions(TABLE_NAME).get(0);
    // update cache need one get first
    quotaManager.getQuotaCache().getUserLimiter(User.getCurrent().getUGI(), table.getName());
    Thread.sleep(1000);
    quotaManager.getQuotaCache().triggerCacheRefresh();
    Thread.sleep(1000);

    quotaManager.setThrottleSimulated(true);
    assertTrue(quotaManager.isThrottleSimulated());

    try {
      for (int i = 0; i < 100; i++) {
        quotaManager.checkQuota(region, OperationType.GET);
      }
    } catch (ThrottlingException e) {
      fail("Should have not throw exception, because QuotaManager is simulating throttle");
    }

    quotaManager.setThrottleSimulated(false);
    assertFalse(quotaManager.isThrottleSimulated());
  }

  private static <V, E> void runWithExpectedException(Callable<V> callable, Class<E> exceptionClass) {
    try {
      callable.call();
    } catch (Exception ex) {
      Assert.assertEquals(exceptionClass, ex.getClass());
      return;
    }
    fail("Should have thrown exception " + exceptionClass);
  }

}
