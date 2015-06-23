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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
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
import org.apache.hadoop.hbase.util.JVMClusterUtil.RegionServerThread;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * minicluster tests that validate that quota  entries are properly set in the quota table
 */
@Category({MediumTests.class})
public class TestQuotaAdmin {
  final Log LOG = LogFactory.getLog(getClass());

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  public static final byte[] ROW = Bytes.toBytes("row");
  private final static byte[] FAMILY = Bytes.toBytes("cf");

  private final static TableName[] TABLE_NAMES = new TableName[] { TableName.valueOf("TestQuota0"),
      TableName.valueOf("TestQuota1"), TableName.valueOf("TestQuota2") };

  private static HTable[] tables;

  private static Map<TableName, Integer> tableRegionsNumMap = new HashMap<TableName, Integer>();

  private static final int regionServerNum = 5;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setBoolean(QuotaUtil.QUOTA_CONF_KEY, true);
    TEST_UTIL.getConfiguration().setInt(QuotaCache.REFRESH_CONF_KEY, 2000);
    TEST_UTIL.getConfiguration().setInt("hbase.hstore.compactionThreshold", 10);
    TEST_UTIL.getConfiguration().setInt("hbase.regionserver.msginterval", 100);
    TEST_UTIL.getConfiguration().setInt("hbase.client.pause", 250);
    TEST_UTIL.getConfiguration().setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 6);
    TEST_UTIL.getConfiguration().setBoolean("hbase.master.enabletable.roundrobin", true);
    TEST_UTIL.startMiniCluster(regionServerNum);
    TEST_UTIL.waitTableAvailable(QuotaTableUtil.QUOTA_TABLE_NAME.getName());

    tableRegionsNumMap.put(TABLE_NAMES[0], 4);
    tableRegionsNumMap.put(TABLE_NAMES[1], 5);
    tableRegionsNumMap.put(TABLE_NAMES[2], 8);

    tables = new HTable[TABLE_NAMES.length];
    for (int i = 0; i < TABLE_NAMES.length; ++i) {
      tables[i] = TEST_UTIL.createTable(TABLE_NAMES[i], new byte[][] { FAMILY }, 3,
        Bytes.toBytes("aaaaa"), Bytes.toBytes("zzzzz"), tableRegionsNumMap.get(TABLE_NAMES[i]));
    }
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testUpdateQuota() throws Exception {
    MasterQuotaManager quotaManager = TEST_UTIL.getHBaseCluster().getMaster().getMasterQuotaManager();
    assertEquals(0, quotaManager.getTotalExistedReadLimit());
    assertEquals(0, quotaManager.getTotalExistedWriteLimit());

    HBaseAdmin admin = TEST_UTIL.getHBaseAdmin();
    String userName = User.getCurrent().getShortName();

    // table[1] have 5 region, so quota is distributed by regionServerNum
    admin.setQuota(QuotaSettingsFactory.throttleUser(userName, TABLE_NAMES[1], ThrottleType.READ_NUMBER, 5000,
      TimeUnit.SECONDS));
    admin.setQuota(QuotaSettingsFactory.throttleUser(userName, TABLE_NAMES[1], ThrottleType.WRITE_NUMBER, 20000,
      TimeUnit.SECONDS));
    // read limit increase 5000 / 5, write limit increase 20000 / 5
    assertEquals(1000, quotaManager.getTotalExistedReadLimit());
    assertEquals(4000, quotaManager.getTotalExistedWriteLimit());

    // update to bigger quota
    admin.setQuota(QuotaSettingsFactory.throttleUser(userName, TABLE_NAMES[1], ThrottleType.READ_NUMBER, 10000,
      TimeUnit.SECONDS));
    admin.setQuota(QuotaSettingsFactory.throttleUser(userName, TABLE_NAMES[1], ThrottleType.WRITE_NUMBER, 30000,
      TimeUnit.SECONDS));
    // read limit increase 10000 / 5, write limit increase 30000 / 5
    assertEquals(2000, quotaManager.getTotalExistedReadLimit());
    assertEquals(6000, quotaManager.getTotalExistedWriteLimit());

    // update to smaller quota
    admin.setQuota(QuotaSettingsFactory.throttleUser(userName, TABLE_NAMES[1], ThrottleType.READ_NUMBER, 1000,
      TimeUnit.SECONDS));
    admin.setQuota(QuotaSettingsFactory.throttleUser(userName, TABLE_NAMES[1], ThrottleType.WRITE_NUMBER, 5000,
      TimeUnit.SECONDS));
    // read limit increase 1000 / 5, write limit increase 5000 / 5
    assertEquals(200, quotaManager.getTotalExistedReadLimit());
    assertEquals(1000, quotaManager.getTotalExistedWriteLimit());

    admin.setQuota(QuotaSettingsFactory.unthrottleUser(userName));
    assertNumResults(0, null);
    assertEquals(0, quotaManager.getTotalExistedReadLimit());
    assertEquals(0, quotaManager.getTotalExistedWriteLimit());
  }

  @Test
  public void testSetQuotaExceedCheck() throws Exception {
    MasterQuotaManager quotaManager = TEST_UTIL.getHBaseCluster().getMaster().getMasterQuotaManager();
    assertEquals(0, quotaManager.getTotalExistedReadLimit());
    assertEquals(0, quotaManager.getTotalExistedWriteLimit());

    final HBaseAdmin admin = TEST_UTIL.getHBaseAdmin();
    final String userName = User.getCurrent().getShortName();

    // table[1] have 5 region, so quota is distributed by regionServerNum
    // read default limit : 3000 * 0.7 = 2100, write default limit: 10000 * 0.7 = 7000
    admin.setQuota(QuotaSettingsFactory.throttleUser(userName, TABLE_NAMES[1], ThrottleType.READ_NUMBER, 2100 * regionServerNum,
      TimeUnit.SECONDS));
    admin.setQuota(QuotaSettingsFactory.throttleUser(userName, TABLE_NAMES[1], ThrottleType.WRITE_NUMBER, 7000 * regionServerNum,
      TimeUnit.SECONDS));
    assertEquals(2100, quotaManager.getTotalExistedReadLimit());
    assertEquals(7000, quotaManager.getTotalExistedWriteLimit());

    runWithExpectedException(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        admin.setQuota(QuotaSettingsFactory.throttleUser(userName, TABLE_NAMES[0], ThrottleType.READ_NUMBER, 2101 * regionServerNum,
          TimeUnit.SECONDS));
        return null;
      }
    }, QuotaExceededException.class);

    runWithExpectedException(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        admin.setQuota(QuotaSettingsFactory.throttleUser(userName, TABLE_NAMES[2], ThrottleType.WRITE_NUMBER, 7001 * regionServerNum,
          TimeUnit.SECONDS));
        return null;
      }
    }, QuotaExceededException.class);

    runWithExpectedException(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        admin.setQuota(QuotaSettingsFactory.throttleUser(userName, TABLE_NAMES[0], ThrottleType.READ_NUMBER, 10,
          TimeUnit.SECONDS));
        return null;
      }
    }, QuotaExceededException.class);

    runWithExpectedException(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        admin.setQuota(QuotaSettingsFactory.throttleUser(userName, TABLE_NAMES[2], ThrottleType.WRITE_NUMBER, 10,
          TimeUnit.SECONDS));
        return null;
      }
    }, QuotaExceededException.class);

    admin.setQuota(QuotaSettingsFactory.unthrottleUser(userName));
    assertNumResults(0, null);
    assertEquals(0, quotaManager.getTotalExistedReadLimit());
    assertEquals(0, quotaManager.getTotalExistedWriteLimit());
  }

  @Test
  public void testThrottleType() throws Exception {
    HBaseAdmin admin = TEST_UTIL.getHBaseAdmin();
    String userName = User.getCurrent().getShortName();

    admin.setQuota(QuotaSettingsFactory.throttleUser(userName, ThrottleType.READ_NUMBER, 6,
      TimeUnit.MINUTES));
    admin.setQuota(QuotaSettingsFactory.throttleUser(userName, ThrottleType.WRITE_NUMBER, 12,
      TimeUnit.MINUTES));
    admin.setQuota(QuotaSettingsFactory.bypassGlobals(userName, true));

    QuotaRetriever scanner = QuotaRetriever.open(TEST_UTIL.getConfiguration());
    try {
      int countThrottle = 0;
      int countGlobalBypass = 0;
      for (QuotaSettings settings: scanner) {
        LOG.debug(settings);
        switch (settings.getQuotaType()) {
          case THROTTLE:
            ThrottleSettings throttle = (ThrottleSettings)settings;
            if (throttle.getSoftLimit() == 6) {
              assertEquals(ThrottleType.READ_NUMBER, throttle.getThrottleType());
            } else if (throttle.getSoftLimit() == 12) {
              assertEquals(ThrottleType.WRITE_NUMBER, throttle.getThrottleType());
            } else {
              fail("should not come here, because don't set quota with this limit");
            }
            assertEquals(userName, throttle.getUserName());
            assertEquals(null, throttle.getTableName());
            assertEquals(null, throttle.getNamespace());
            assertEquals(TimeUnit.MINUTES, throttle.getTimeUnit());
            countThrottle++;
            break;
          case GLOBAL_BYPASS:
            countGlobalBypass++;
            break;
          default:
            fail("unexpected settings type: " + settings.getQuotaType());
        }
      }
      assertEquals(2, countThrottle);
      assertEquals(1, countGlobalBypass);
    } finally {
      scanner.close();
    }

    admin.setQuota(QuotaSettingsFactory.unthrottleUser(userName));
    assertNumResults(1, null);
    admin.setQuota(QuotaSettingsFactory.bypassGlobals(userName, false));
    assertNumResults(0, null);
  }

  @Test
  public void testSimpleScan() throws Exception {
    HBaseAdmin admin = TEST_UTIL.getHBaseAdmin();
    String userName = User.getCurrent().getShortName();

    admin.setQuota(QuotaSettingsFactory
      .throttleUser(userName, ThrottleType.REQUEST_NUMBER, 6, TimeUnit.MINUTES));
    admin.setQuota(QuotaSettingsFactory.bypassGlobals(userName, true));

    QuotaRetriever scanner = QuotaRetriever.open(TEST_UTIL.getConfiguration());
    try {
      int countThrottle = 0;
      int countGlobalBypass = 0;
      for (QuotaSettings settings: scanner) {
        LOG.debug(settings);
        switch (settings.getQuotaType()) {
          case THROTTLE:
            ThrottleSettings throttle = (ThrottleSettings)settings;
            assertEquals(userName, throttle.getUserName());
            assertEquals(null, throttle.getTableName());
            assertEquals(null, throttle.getNamespace());
            assertEquals(6, throttle.getSoftLimit());
            assertEquals(TimeUnit.MINUTES, throttle.getTimeUnit());
            countThrottle++;
            break;
          case GLOBAL_BYPASS:
            countGlobalBypass++;
            break;
          default:
            fail("unexpected settings type: " + settings.getQuotaType());
        }
      }
      assertEquals(1, countThrottle);
      assertEquals(1, countGlobalBypass);
    } finally {
      scanner.close();
    }

    admin.setQuota(QuotaSettingsFactory.unthrottleUser(userName));
    assertNumResults(1, null);
    admin.setQuota(QuotaSettingsFactory.bypassGlobals(userName, false));
    assertNumResults(0, null);
  }

  @Test
  public void testQuotaRetrieverFilter() throws Exception {
    HBaseAdmin admin = TEST_UTIL.getHBaseAdmin();
    TableName[] tables = new TableName[] {
      TableName.valueOf("T0"), TableName.valueOf("T01"), TableName.valueOf("NS0:T2"),
    };
    String[] namespaces = new String[] { "NS0", "NS01", "NS2" };
    String[] users = new String[] { "User0", "User01", "User2" };

    for (String user: users) {
      admin.setQuota(QuotaSettingsFactory
        .throttleUser(user, ThrottleType.REQUEST_NUMBER, 1, TimeUnit.MINUTES));

      for (TableName table: tables) {
        admin.setQuota(QuotaSettingsFactory
          .throttleUser(user, table, ThrottleType.REQUEST_NUMBER, 2, TimeUnit.MINUTES));
      }

      for (String ns: namespaces) {
        admin.setQuota(QuotaSettingsFactory
          .throttleUser(user, ns, ThrottleType.REQUEST_NUMBER, 3, TimeUnit.MINUTES));
      }
    }
    assertNumResults(21, null);

    for (TableName table: tables) {
      admin.setQuota(QuotaSettingsFactory
        .throttleTable(table, ThrottleType.REQUEST_NUMBER, 4, TimeUnit.MINUTES));
    }
    assertNumResults(24, null);

    for (String ns: namespaces) {
      admin.setQuota(QuotaSettingsFactory
        .throttleNamespace(ns, ThrottleType.REQUEST_NUMBER, 5, TimeUnit.MINUTES));
    }
    assertNumResults(27, null);

    assertNumResults(7, new QuotaFilter().setUserFilter("User0"));
    assertNumResults(0, new QuotaFilter().setUserFilter("User"));
    assertNumResults(21, new QuotaFilter().setUserFilter("User.*"));
    assertNumResults(3, new QuotaFilter().setUserFilter("User.*").setTableFilter("T0"));
    assertNumResults(3, new QuotaFilter().setUserFilter("User.*").setTableFilter("NS.*"));
    assertNumResults(0, new QuotaFilter().setUserFilter("User.*").setTableFilter("T"));
    assertNumResults(6, new QuotaFilter().setUserFilter("User.*").setTableFilter("T.*"));
    assertNumResults(3, new QuotaFilter().setUserFilter("User.*").setNamespaceFilter("NS0"));
    assertNumResults(0, new QuotaFilter().setUserFilter("User.*").setNamespaceFilter("NS"));
    assertNumResults(9, new QuotaFilter().setUserFilter("User.*").setNamespaceFilter("NS.*"));
    assertNumResults(6, new QuotaFilter().setUserFilter("User.*")
                                            .setTableFilter("T0").setNamespaceFilter("NS0"));
    assertNumResults(1, new QuotaFilter().setTableFilter("T0"));
    assertNumResults(0, new QuotaFilter().setTableFilter("T"));
    assertNumResults(2, new QuotaFilter().setTableFilter("T.*"));
    assertNumResults(3, new QuotaFilter().setTableFilter(".*T.*"));
    assertNumResults(1, new QuotaFilter().setNamespaceFilter("NS0"));
    assertNumResults(0, new QuotaFilter().setNamespaceFilter("NS"));
    assertNumResults(3, new QuotaFilter().setNamespaceFilter("NS.*"));

    for (String user: users) {
      admin.setQuota(QuotaSettingsFactory.unthrottleUser(user));
      for (TableName table: tables) {
        admin.setQuota(QuotaSettingsFactory.unthrottleUser(user, table));
      }
      for (String ns: namespaces) {
        admin.setQuota(QuotaSettingsFactory.unthrottleUser(user, ns));
      }
    }
    assertNumResults(6, null);

    for (TableName table: tables) {
      admin.setQuota(QuotaSettingsFactory.unthrottleTable(table));
    }
    assertNumResults(3, null);

    for (String ns: namespaces) {
      admin.setQuota(QuotaSettingsFactory.unthrottleNamespace(ns));
    }
    assertNumResults(0, null);
  }

  private void assertNumResults(int expected, final QuotaFilter filter) throws Exception {
    assertEquals(expected, countResults(filter));
  }

  private int countResults(final QuotaFilter filter) throws Exception {
    QuotaRetriever scanner = QuotaRetriever.open(TEST_UTIL.getConfiguration(), filter);
    try {
      int count = 0;
      for (QuotaSettings settings: scanner) {
        LOG.debug(settings);
        count++;
      }
      return count;
    } finally {
      scanner.close();
    }
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
