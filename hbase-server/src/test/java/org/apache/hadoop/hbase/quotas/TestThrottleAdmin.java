/**
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

package org.apache.hadoop.hbase.quotas;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManagerTestHelper;
import org.apache.hadoop.hbase.util.IncrementingEnvironmentEdge;
import org.apache.hadoop.hbase.util.ManualEnvironmentEdge;
import org.apache.hadoop.hbase.util.JVMClusterUtil.RegionServerThread;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * minicluster tests that validate that quota  entries are properly set in the quota table
 */
@Category({MediumTests.class})
public class TestThrottleAdmin {
  final Log LOG = LogFactory.getLog(getClass());

  private final static int REFRESH_TIME = 5 * 3600;

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private final static byte[] FAMILY = Bytes.toBytes("cf");
  private final static byte[] QUALIFIER = Bytes.toBytes("q");

  private final static TableName TABLE_NAME = TableName.valueOf("TestThrottle0");

  private static HTable table;

  private static final int regionServerNum = 1;
  
  private static ManualEnvironmentEdge envEdge;
  
  private static HBaseAdmin admin;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setBoolean(QuotaUtil.QUOTA_CONF_KEY, true);
    TEST_UTIL.getConfiguration().setInt(QuotaCache.REFRESH_CONF_KEY, REFRESH_TIME);
    TEST_UTIL.getConfiguration().setInt("hbase.hstore.compactionThreshold", 10);
    TEST_UTIL.getConfiguration().setInt("hbase.regionserver.msginterval", 100);
    TEST_UTIL.getConfiguration().setInt("hbase.client.pause", 250);
    TEST_UTIL.getConfiguration().setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 6);
    TEST_UTIL.getConfiguration().setBoolean("hbase.master.enabletable.roundrobin", true);
    TEST_UTIL.getConfiguration().setBoolean("hbase.quota.allow.exceed", false);
    TEST_UTIL.startMiniCluster(regionServerNum);
    TEST_UTIL.waitTableAvailable(QuotaTableUtil.QUOTA_TABLE_NAME.getName());
    QuotaCache.TEST_FORCE_REFRESH = true;

    table = TEST_UTIL.createTable(TABLE_NAME, new byte[][] { FAMILY });
    
    envEdge = new ManualEnvironmentEdge();
    envEdge.setValue(EnvironmentEdgeManager.currentTimeMillis());
    EnvironmentEdgeManagerTestHelper.injectEdge(envEdge);
    
    admin = TEST_UTIL.getHBaseAdmin();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    table.close();
    TEST_UTIL.deleteTable(TABLE_NAME);
    TEST_UTIL.shutdownMiniCluster();
  }
  
  @Before
  public void setupQuota() throws Exception {
    String userName = User.getCurrent().getShortName();
    admin.setQuota(QuotaSettingsFactory.throttleUser(userName, TABLE_NAME,
      ThrottleType.READ_NUMBER, 10, TimeUnit.MINUTES));
    admin.setQuota(QuotaSettingsFactory.throttleUser(userName, TABLE_NAME,
      ThrottleType.WRITE_NUMBER, 10, TimeUnit.MINUTES));
    triggerUserCacheRefresh(false, TABLE_NAME);
  }

  @Test
  public void testSimulateThrottle() throws Exception {
    RegionServerQuotaManager quotaManager = TEST_UTIL.getMiniHBaseCluster()
        .getRegionServer(0).getRegionServerQuotaManager();
    assertTrue(quotaManager.isQuotaEnabled());
    assertFalse(quotaManager.isStopped());
    assertFalse(quotaManager.isThrottleSimulated());

    assertEquals(10, doPuts(100, table));
    assertEquals(10, doGets(100, table));
    
    // simulate throttle
    admin.switchThrottle(false, true, false);
    
    assertFalse(quotaManager.isStopped());
    assertTrue(quotaManager.isThrottleSimulated());
    
    assertEquals(100, doPuts(100, table));
    assertEquals(100, doGets(100, table));
    
    quotaManager.setThrottleSimulated(false);
  }
  
  @Test
  public void testStartAndStopThrottle() throws Exception {
    RegionServerQuotaManager quotaManager = TEST_UTIL.getMiniHBaseCluster()
        .getRegionServer(0).getRegionServerQuotaManager();
    assertTrue(quotaManager.isQuotaEnabled());
    assertFalse(quotaManager.isStopped());
    assertFalse(quotaManager.isThrottleSimulated());

    assertEquals(10, doPuts(100, table));
    assertEquals(10, doGets(100, table));
    
    // stop throttle
    admin.switchThrottle(false, false, true);
    
    assertTrue(quotaManager.isStopped());
    assertFalse(quotaManager.isThrottleSimulated());
    
    assertEquals(100, doPuts(100, table));
    assertEquals(100, doGets(100, table));
    
    // start throttle
    admin.switchThrottle(true, false, false);
    assertTrue(quotaManager.isQuotaEnabled());
    assertFalse(quotaManager.isStopped());
    assertFalse(quotaManager.isThrottleSimulated());
  }
  
  private int doPuts(int maxOps, final HTable... tables) throws Exception {
    int count = 0;
    try {
      while (count < maxOps) {
        Put put = new Put(Bytes.toBytes("row-" + count));
        put.add(FAMILY, QUALIFIER, Bytes.toBytes("data-" + count));
        for (final HTable table: tables) {
          table.put(put);
        }
        count += tables.length;
      }
    } catch (RetriesExhaustedWithDetailsException e) {
      for (Throwable t: e.getCauses()) {
        if (!(t instanceof ThrottlingException)) {
          throw e;
        }
      }
      LOG.error("put failed after nRetries=" + count, e);
    }
    return count;
  }

  private long doGets(int maxOps, final HTable... tables) throws Exception {
    int count = 0;
    try {
      while (count < maxOps) {
        Get get = new Get(Bytes.toBytes("row-" + count));
        for (final HTable table: tables) {
          table.get(get);
        }
        count += tables.length;
      }
    } catch (ThrottlingException e) {
      LOG.error("get failed after nRetries=" + count, e);
    }
    return count;
  }

  private void triggerUserCacheRefresh(boolean bypass, TableName... tables) throws Exception {
    triggerCacheRefresh(bypass, true, false, false, tables);
  }

  private void triggerTableCacheRefresh(boolean bypass, TableName... tables) throws Exception {
    triggerCacheRefresh(bypass, false, true, false, tables);
  }

  private void triggerNamespaceCacheRefresh(boolean bypass, TableName... tables) throws Exception {
    triggerCacheRefresh(bypass, false, false, true, tables);
  }

  private void triggerCacheRefresh(boolean bypass, boolean userLimiter, boolean tableLimiter,
      boolean nsLimiter, final TableName... tables) throws Exception {
  envEdge.incValue(2 * REFRESH_TIME);
    for (RegionServerThread rst: TEST_UTIL.getMiniHBaseCluster().getRegionServerThreads()) {
      RegionServerQuotaManager quotaManager = rst.getRegionServer().getRegionServerQuotaManager();
      QuotaCache quotaCache = quotaManager.getQuotaCache();

      quotaCache.triggerCacheRefresh();
      Thread.sleep(250);

      for (TableName table: tables) {
        quotaCache.getTableLimiter(table);
      }

      boolean isUpdated = false;
      while (!isUpdated) {
        isUpdated = true;
        for (TableName table: tables) {
          boolean isBypass = true;
          if (userLimiter) {
            isBypass &= quotaCache.getUserLimiter(User.getCurrent().getUGI(), table).isBypass();
          }
          if (tableLimiter) {
            isBypass &= quotaCache.getTableLimiter(table).isBypass();
          }
          if (nsLimiter) {
            isBypass &= quotaCache.getNamespaceLimiter(table.getNamespaceAsString()).isBypass();
          }
          if (isBypass != bypass) {
            isUpdated = false;
            Thread.sleep(250);
            break;
          }
        }
      }

      LOG.debug("QuotaCache");
      LOG.debug(quotaCache.getNamespaceQuotaCache());
      LOG.debug(quotaCache.getTableQuotaCache());
      LOG.debug(quotaCache.getUserQuotaCache());
    }
  }

  private void waitMinuteQuota() {
    envEdge.incValue(70000);
  }
}
