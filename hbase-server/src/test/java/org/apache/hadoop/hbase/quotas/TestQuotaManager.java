package org.apache.hadoop.hbase.quotas;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Action;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.quotas.OperationQuota.OperationType;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegion.Operation;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManagerTestHelper;
import org.apache.hadoop.hbase.util.ManualEnvironmentEdge;
import org.apache.hadoop.hbase.util.JVMClusterUtil.RegionServerThread;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MediumTests.class })
public class TestQuotaManager {
  final Log LOG = LogFactory.getLog(getClass());

  private final static int REFRESH_TIME = 5 * 3600;

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  public static final byte[] ROW = Bytes.toBytes("row");
  private final static byte[] FAMILY = Bytes.toBytes("cf");
  private final static byte[] QUALIFIER = Bytes.toBytes("q");

  private final static TableName[] TABLE_NAMES = new TableName[] { TableName.valueOf("TestQuota0"),
      TableName.valueOf("TestQuota1") };

  private static HTable[] tables;

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
    TEST_UTIL.startMiniCluster(1);
    TEST_UTIL.waitTableAvailable(QuotaTableUtil.QUOTA_TABLE_NAME.getName());
    QuotaCache.TEST_FORCE_REFRESH = true;

    tables = new HTable[TABLE_NAMES.length];
    for (int i = 0; i < TABLE_NAMES.length; ++i) {
      tables[i] = TEST_UTIL.createTable(TABLE_NAMES[i], FAMILY);
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
    HBaseAdmin admin = TEST_UTIL.getHBaseAdmin();
    String userName = User.getCurrent().getShortName();

    admin.setQuota(QuotaSettingsFactory.throttleUser(userName, TABLE_NAMES[0],
      ThrottleType.READ_NUMBER, 5, TimeUnit.SECONDS));
    admin.setQuota(QuotaSettingsFactory.throttleUser(userName, TABLE_NAMES[1],
      ThrottleType.READ_NUMBER, 5, TimeUnit.SECONDS));

    Put put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, Bytes.toBytes("test-value"));
    for (int i = 0; i < TABLE_NAMES.length; ++i) {
      tables[i].put(put);
    }

    RegionServerQuotaManager quotaManager = TEST_UTIL.getMiniHBaseCluster().getRegionServer(0)
        .getRegionServerQuotaManager();
    quotaManager.getQuotaCache().triggerCacheRefresh();
  }

  @Test
  public void testCheckQuota() throws IOException, InterruptedException {
    final RegionServerQuotaManager quotaManager = TEST_UTIL.getMiniHBaseCluster()
        .getRegionServer(0).getRegionServerQuotaManager();
    final HRegion region = TEST_UTIL.getMiniHBaseCluster().getRegionServer(0)
        .getOnlineRegions(TABLE_NAMES[0]).get(0);
    // update cache need one get first
    quotaManager.getQuotaCache().getUserLimiter(User.getCurrent().getUGI(), tables[0].getName());
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
  public void testGrabQuota() throws IOException, InterruptedException {
    final RegionServerQuotaManager quotaManager = TEST_UTIL.getMiniHBaseCluster()
        .getRegionServer(0).getRegionServerQuotaManager();
    final UserGroupInformation ugi = User.getCurrent().getUGI();
    // update cache need one get first
    quotaManager.getQuotaCache().getUserLimiter(ugi, tables[0].getName());
    quotaManager.getQuotaCache().getUserLimiter(ugi, tables[1].getName());
    Thread.sleep(1000);
    quotaManager.getQuotaCache().triggerCacheRefresh();
    Thread.sleep(1000);
    Result result0 = tables[0].get(new Get(ROW));
    Result result1 = tables[1].get(new Get(ROW));

    // the above get consume 1 quota, so table0 and table1 have 4 quota
    for (int i = 0; i < 4; i++) {
      quotaManager.checkQuota(ugi, tables[0].getName(), OperationType.GET);
      quotaManager.grabQuota(ugi, tables[0].getName(), result0);
      quotaManager.checkQuota(ugi, tables[1].getName(), OperationType.GET);
      quotaManager.grabQuota(ugi, tables[1].getName(), result1);
    }
    runWithExpectedException(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        quotaManager.checkQuota(ugi, tables[0].getName(), OperationType.GET);
        return null;
      }
    }, ThrottlingException.class);
    runWithExpectedException(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        quotaManager.checkQuota(ugi, tables[1].getName(), OperationType.GET);
        return null;
      }
    }, ThrottlingException.class);
  }

  @Test
  public void testGetQuota() throws IOException, InterruptedException {
    RegionServerQuotaManager quotaManager = TEST_UTIL.getMiniHBaseCluster().getRegionServer(0)
        .getRegionServerQuotaManager();
    UserGroupInformation ugi = User.getCurrent().getUGI();
    OperationQuota quota = quotaManager.getQuota(ugi, tables[0].getName());
    Thread.sleep(1000);
    quota = quotaManager.getQuota(ugi, tables[0].getName());
    assertTrue(quota.getClass().getName().equals(AllowExceedOperationQuota.class.getName()));
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
