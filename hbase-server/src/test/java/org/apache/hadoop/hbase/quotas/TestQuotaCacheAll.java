package org.apache.hadoop.hbase.quotas;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManagerTestHelper;
import org.apache.hadoop.hbase.util.ManualEnvironmentEdge;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestQuotaCacheAll {
  final Log LOG = LogFactory.getLog(getClass());

  private final static int REFRESH_TIME = 5 * 3600;

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  public static final byte[] ROW = Bytes.toBytes("row");
  private final static byte[] FAMILY = Bytes.toBytes("cf");

  private final String[] NAMESPACES = new String[] { "NS0", "NS1", "NS2" };
  private final TableName[] TABLE_NAMES = new TableName[] { TableName.valueOf("TestQuota0"),
      TableName.valueOf("TestQuota1"), TableName.valueOf("TestQuota2") };
  private final String[] USERS = new String[] { "User0", "User1", "User2" };
  
  private static final int regionServerNum = 1;

  private static ManualEnvironmentEdge envEdge;

  private static QuotaCache quotaCache;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setBoolean(QuotaUtil.QUOTA_CONF_KEY, true);
    TEST_UTIL.getConfiguration().setInt(QuotaCache.REFRESH_CONF_KEY, REFRESH_TIME);
    TEST_UTIL.getConfiguration().setBoolean(QuotaCache.QUOTA_CACHE_CONF_KEY, true);
    TEST_UTIL.startMiniCluster(regionServerNum);
    TEST_UTIL.waitTableAvailable(QuotaTableUtil.QUOTA_TABLE_NAME.getName());
    QuotaCache.TEST_FORCE_REFRESH = true;

    quotaCache = TEST_UTIL.getMiniHBaseCluster().getRegionServer(0)
        .getRegionServerQuotaManager().getQuotaCache();

    envEdge = new ManualEnvironmentEdge();
    envEdge.setValue(EnvironmentEdgeManager.currentTimeMillis());
    EnvironmentEdgeManagerTestHelper.injectEdge(envEdge);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testNamespaceCache() throws Exception {
    HBaseAdmin admin = TEST_UTIL.getHBaseAdmin();
    for (String ns: NAMESPACES) {
      admin.setQuota(QuotaSettingsFactory
        .throttleNamespace(ns, ThrottleType.REQUEST_NUMBER, 5, TimeUnit.SECONDS));
    }
    envEdge.incValue(REFRESH_TIME);
    triggerCacheRefresh();
    Thread.sleep(1000);
    assertEquals(NAMESPACES.length, quotaCache.getNamespaceQuotaCache().size());
    for (Map.Entry<String, QuotaState> entry : quotaCache.getNamespaceQuotaCache().entrySet()) {
      assertEquals(5, entry.getValue().getGlobalLimiter().getReqsAvailable());
    }

    // long time not query/update
    envEdge.incValue(QuotaCache.EVICT_PERIOD_FACTOR * REFRESH_TIME * 2);
    triggerCacheRefresh();
    Thread.sleep(1000);
    assertEquals(NAMESPACES.length, quotaCache.getNamespaceQuotaCache().size());
    for (Map.Entry<String, QuotaState> entry : quotaCache.getNamespaceQuotaCache().entrySet()) {
      assertEquals(5, entry.getValue().getGlobalLimiter().getReqsAvailable());
    }

    // update quota
    for (String ns: NAMESPACES) {
      admin.setQuota(QuotaSettingsFactory
        .throttleNamespace(ns, ThrottleType.REQUEST_NUMBER, 15, TimeUnit.SECONDS));
    }
    envEdge.incValue(REFRESH_TIME);
    triggerCacheRefresh();
    Thread.sleep(1000);
    assertEquals(NAMESPACES.length, quotaCache.getNamespaceQuotaCache().size());
    for (Map.Entry<String, QuotaState> entry : quotaCache.getNamespaceQuotaCache().entrySet()) {
      assertEquals(15, entry.getValue().getGlobalLimiter().getReqsAvailable());
    }

    // unthrottle namespace[0]'s quota
    admin.setQuota(QuotaSettingsFactory.unthrottleNamespace(NAMESPACES[0]));
    envEdge.incValue(REFRESH_TIME);
    triggerCacheRefresh();
    Thread.sleep(1000);
    assertEquals(NAMESPACES.length - 1, quotaCache.getNamespaceQuotaCache().size());
    for (Map.Entry<String, QuotaState> entry : quotaCache.getNamespaceQuotaCache().entrySet()) {
      assertEquals(15, entry.getValue().getGlobalLimiter().getReqsAvailable());
    }
  }

  @Test
  public void testTableQuotaCache() throws Exception {
    HBaseAdmin admin = TEST_UTIL.getHBaseAdmin();
    for (TableName table: TABLE_NAMES) {
      admin.setQuota(QuotaSettingsFactory
        .throttleTable(table, ThrottleType.REQUEST_NUMBER, 5, TimeUnit.SECONDS));
    }
    envEdge.incValue(REFRESH_TIME);
    triggerCacheRefresh();
    Thread.sleep(1000);
    assertEquals(TABLE_NAMES.length, quotaCache.getTableQuotaCache().size());
    for (Map.Entry<TableName, QuotaState> entry : quotaCache.getTableQuotaCache().entrySet()) {
      assertEquals(5, entry.getValue().getGlobalLimiter().getReqsAvailable());
    }

    // long time not query/update
    envEdge.incValue(QuotaCache.EVICT_PERIOD_FACTOR * REFRESH_TIME * 2);
    triggerCacheRefresh();
    Thread.sleep(1000);
    assertEquals(TABLE_NAMES.length, quotaCache.getTableQuotaCache().size());
    for (Map.Entry<TableName, QuotaState> entry : quotaCache.getTableQuotaCache().entrySet()) {
      assertEquals(5, entry.getValue().getGlobalLimiter().getReqsAvailable());
    }

    // update quota
    for (TableName table: TABLE_NAMES) {
      admin.setQuota(QuotaSettingsFactory
        .throttleTable(table, ThrottleType.REQUEST_NUMBER, 15, TimeUnit.SECONDS));
    }
    envEdge.incValue(REFRESH_TIME);
    triggerCacheRefresh();
    Thread.sleep(1000);
    assertEquals(TABLE_NAMES.length, quotaCache.getTableQuotaCache().size());
    for (Map.Entry<TableName, QuotaState> entry : quotaCache.getTableQuotaCache().entrySet()) {
      assertEquals(15, entry.getValue().getGlobalLimiter().getReqsAvailable());
    }

    // unthrottle table[0]'s quota
    admin.setQuota(QuotaSettingsFactory.unthrottleTable(TABLE_NAMES[0]));
    envEdge.incValue(REFRESH_TIME);
    triggerCacheRefresh();
    Thread.sleep(1000);
    assertEquals(TABLE_NAMES.length - 1, quotaCache.getTableQuotaCache().size());
    for (Map.Entry<TableName, QuotaState> entry : quotaCache.getTableQuotaCache().entrySet()) {
      assertEquals(15, entry.getValue().getGlobalLimiter().getReqsAvailable());
    }
  }

  @Test
  public void testUserQuotaState() throws Exception {
    HBaseAdmin admin = TEST_UTIL.getHBaseAdmin();
    for (String user: USERS) {
      admin.setQuota(QuotaSettingsFactory
        .throttleUser(user, ThrottleType.REQUEST_NUMBER, 15, TimeUnit.MINUTES));

      for (TableName table: TABLE_NAMES) {
        admin.setQuota(QuotaSettingsFactory
          .throttleUser(user, table, ThrottleType.REQUEST_NUMBER, 5, TimeUnit.MINUTES));
      }
    }
    envEdge.incValue(REFRESH_TIME);
    triggerCacheRefresh();
    Thread.sleep(1000);
    assertEquals(USERS.length, quotaCache.getUserQuotaCache().size());
    for (Map.Entry<String, UserQuotaState> entry : quotaCache.getUserQuotaCache().entrySet()) {
      assertEquals(15, entry.getValue().getGlobalLimiter().getReqsAvailable());
      for (TableName table: TABLE_NAMES) {
        assertEquals(5, entry.getValue().getTableLimiter(table).getReqsAvailable());
      }
    }

    // long time not query
    envEdge.incValue(QuotaCache.EVICT_PERIOD_FACTOR * REFRESH_TIME * 2);
    triggerCacheRefresh();
    Thread.sleep(1000);
    assertEquals(USERS.length, quotaCache.getUserQuotaCache().size());
    for (Map.Entry<String, UserQuotaState> entry : quotaCache.getUserQuotaCache().entrySet()) {
      assertEquals(15, entry.getValue().getGlobalLimiter().getReqsAvailable());
      for (TableName table: TABLE_NAMES) {
        assertEquals(5, entry.getValue().getTableLimiter(table).getReqsAvailable());
      }
    }

    // update quota
    for (String user: USERS) {
      for (TableName table: TABLE_NAMES) {
        admin.setQuota(QuotaSettingsFactory
          .throttleUser(user, table, ThrottleType.REQUEST_NUMBER, 15, TimeUnit.MINUTES));
      }
    }
    envEdge.incValue(REFRESH_TIME);
    triggerCacheRefresh();
    Thread.sleep(1000);
    assertEquals(USERS.length, quotaCache.getUserQuotaCache().size());
    for (Map.Entry<String, UserQuotaState> entry : quotaCache.getUserQuotaCache().entrySet()) {
      for (TableName table: TABLE_NAMES) {
        assertEquals(15, entry.getValue().getTableLimiter(table).getReqsAvailable());
      }
    }

    // unthrottle user[0]'s quota
    admin.setQuota(QuotaSettingsFactory.unthrottleUser(USERS[0]));
    envEdge.incValue(REFRESH_TIME);
    triggerCacheRefresh();
    Thread.sleep(1000);
    assertEquals(USERS.length - 1, quotaCache.getUserQuotaCache().size());
    for (Map.Entry<String, UserQuotaState> entry : quotaCache.getUserQuotaCache().entrySet()) {
      assertEquals(15, entry.getValue().getGlobalLimiter().getReqsAvailable());
      for (TableName table: TABLE_NAMES) {
        assertEquals(15, entry.getValue().getTableLimiter(table).getReqsAvailable());
      }
    }
  }

  private void triggerCacheRefresh() {
    quotaCache.triggerCacheRefresh();
  }
}
