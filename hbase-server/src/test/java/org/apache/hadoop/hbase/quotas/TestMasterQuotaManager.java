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
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.SetQuotaRequest;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManagerTestHelper;
import org.apache.hadoop.hbase.util.IncrementingEnvironmentEdge;
import org.apache.hadoop.hbase.util.JVMClusterUtil.RegionServerThread;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * minicluster tests that validate that quota  entries are properly set in the quota table
 */
@Category({MediumTests.class})
public class TestMasterQuotaManager {
  final Log LOG = LogFactory.getLog(getClass());

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  public static final byte[] ROW = Bytes.toBytes("row");
  private final static byte[] FAMILY = Bytes.toBytes("cf");

  private final static TableName[] TABLE_NAMES = new TableName[] { TableName.valueOf("TestQuota0"),
      TableName.valueOf("TestQuota1"), TableName.valueOf("TestQuota2") };
  private static HTable[] tables;

  private static Map<TableName, Integer> tableRegionsNumMap = new HashMap<TableName, Integer>();

  private static final int regionServerNum = 5;
  
  private MasterQuotaManager quotaManager;
  private String userName;
  private static HBaseAdmin admin;

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

    admin = TEST_UTIL.getHBaseAdmin();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void beforeTest() throws Exception {
    quotaManager = TEST_UTIL.getHBaseCluster().getMaster().getMasterQuotaManager();
    assertEquals(0, quotaManager.getTotalExistedReadLimit());
    assertEquals(0, quotaManager.getTotalExistedWriteLimit());
    userName = User.getCurrent().getShortName();
  }

  @After
  public void afterTest() throws Exception {
    admin.setQuota(QuotaSettingsFactory.unthrottleUser(userName));
    assertNumResults(0, null);
    assertEquals(0, quotaManager.getTotalExistedReadLimit());
    assertEquals(0, quotaManager.getTotalExistedWriteLimit());
  }

  @Test
  public void testCheckRegionServerLimit() throws Exception {
    try {
      quotaManager.checkRegionServerQuota(1000, 2000, 2999);
    } catch (Exception e) {
      assertEquals(QuotaExceededException.class, e.getClass());
    }

    try {
      quotaManager.checkRegionServerQuota(1200, 9000, 10000);
    } catch (Exception e) {
      assertEquals(QuotaExceededException.class, e.getClass());
    }

    try {
      quotaManager.checkRegionServerQuota(0, 2000, 3000);
    } catch (Exception e) {
      fail("Should not throw exception " + e);
    }

    try {
      // if user not update quota to bigger, not throw exception even totalExistedLimit > rsLimit
      quotaManager.checkRegionServerQuota(0, 2000, 1800);
    } catch (Exception e) {
      fail("Should not throw exception " + e);
    }

    try {
      // if user update quota to be smaller, not throw exception even totalExistedLimit > rsLimit
      quotaManager.checkRegionServerQuota(-500, 2000, 1000);
    } catch (Exception e) {
      fail("Should not throw exception " + e);
    }
  }

  @Test
  public void testCheckRegionServerQuotaWithTable() throws Exception {
    admin.setQuota(QuotaSettingsFactory.throttleUser(userName, TABLE_NAMES[0], ThrottleType.READ_NUMBER, 2000,
      TimeUnit.SECONDS));
    admin.setQuota(QuotaSettingsFactory.throttleUser(userName, TABLE_NAMES[1], ThrottleType.READ_NUMBER, 5000,
      TimeUnit.SECONDS));
    assertEquals(1500, quotaManager.getTotalExistedReadLimit());

    try {
      quotaManager.checkRegionServerQuota(TABLE_NAMES[1], 5, 8000,
        ProtobufUtil.toProtoThrottleType(ThrottleType.READ_NUMBER));
      admin.setQuota(QuotaSettingsFactory.throttleUser(userName, TABLE_NAMES[1],
        ThrottleType.READ_NUMBER, 8000, TimeUnit.SECONDS));
    } catch (Exception e) {
      fail("Should not throw exception " + e);
    }
    assertEquals(2100, quotaManager.getTotalExistedReadLimit());

    try {
      quotaManager.checkRegionServerQuota(TABLE_NAMES[0], 4, 3000,
        ProtobufUtil.toProtoThrottleType(ThrottleType.READ_NUMBER));
    } catch (Exception e) {
      assertEquals(QuotaExceededException.class, e.getClass());
    }
    assertEquals(2100, quotaManager.getTotalExistedReadLimit());

    try {
      quotaManager.checkRegionServerQuota(TABLE_NAMES[2], 8, 4000,
        ProtobufUtil.toProtoThrottleType(ThrottleType.READ_NUMBER));
    } catch (Exception e) {
      assertEquals(QuotaExceededException.class, e.getClass());
    }
    assertEquals(2100, quotaManager.getTotalExistedReadLimit());

    try {
      quotaManager.checkRegionServerQuota(TABLE_NAMES[1], 5, 5000,
        ProtobufUtil.toProtoThrottleType(ThrottleType.READ_NUMBER));
      admin.setQuota(QuotaSettingsFactory.throttleUser(userName, TABLE_NAMES[1],
        ThrottleType.READ_NUMBER, 5000, TimeUnit.SECONDS));
    } catch (Exception e) {
      fail("Should not throw exception " + e);
    }
    assertEquals(1500, quotaManager.getTotalExistedReadLimit());
  }

  @Test
  public void testTableExistedLimit() throws Exception {
    admin.setQuota(QuotaSettingsFactory.throttleUser(userName, TABLE_NAMES[0], ThrottleType.READ_NUMBER, 500,
      TimeUnit.SECONDS));
    admin.setQuota(QuotaSettingsFactory.throttleUser(userName, TABLE_NAMES[0], ThrottleType.WRITE_NUMBER, 1000,
      TimeUnit.SECONDS));
    assertEquals(500, quotaManager.getTableExistedReadLimit(TABLE_NAMES[0]));
    assertEquals(1000, quotaManager.getTableExistedWriteLimit(TABLE_NAMES[0]));

    admin.setQuota(QuotaSettingsFactory.throttleUser(userName, TABLE_NAMES[1], ThrottleType.READ_NUMBER, 2000,
      TimeUnit.SECONDS));
    admin.setQuota(QuotaSettingsFactory.throttleUser(userName, TABLE_NAMES[1], ThrottleType.WRITE_NUMBER, 4000,
      TimeUnit.SECONDS));
    assertEquals(2000, quotaManager.getTableExistedReadLimit(TABLE_NAMES[1]));
    assertEquals(4000, quotaManager.getTableExistedWriteLimit(TABLE_NAMES[1]));
  }

  @Test
  public void testTotalExistedLimit() throws Exception {
    // table[0] have 4 region, is smaller than regionServerNum, so quota is distributed by tableReigonsNum
    admin.setQuota(QuotaSettingsFactory.throttleUser(userName, TABLE_NAMES[0], ThrottleType.READ_NUMBER, 500,
      TimeUnit.SECONDS));
    admin.setQuota(QuotaSettingsFactory.throttleUser(userName, TABLE_NAMES[0], ThrottleType.WRITE_NUMBER, 1000,
      TimeUnit.SECONDS));
    // read limit increase 500 / 4, write limit increase 1000 / 4
    assertEquals(125, quotaManager.getTotalExistedReadLimit());
    assertEquals(250, quotaManager.getTotalExistedWriteLimit());

    // table[1] have 5 region, so quota is distributed by regionServerNum
    admin.setQuota(QuotaSettingsFactory.throttleUser(userName, TABLE_NAMES[1], ThrottleType.READ_NUMBER, 1000,
      TimeUnit.SECONDS));
    admin.setQuota(QuotaSettingsFactory.throttleUser(userName, TABLE_NAMES[1], ThrottleType.WRITE_NUMBER, 1000,
      TimeUnit.SECONDS));
    // read limit increase 1000 / 5, write limit increase 1000 / 5
    assertEquals(325, quotaManager.getTotalExistedReadLimit());
    assertEquals(450, quotaManager.getTotalExistedWriteLimit());

    // table[2] have 8 region, quota will be distributed by facotr 2/8 or 1/8
    admin.setQuota(QuotaSettingsFactory.throttleUser(userName, TABLE_NAMES[2], ThrottleType.READ_NUMBER, 2000,
      TimeUnit.SECONDS));
    admin.setQuota(QuotaSettingsFactory.throttleUser(userName, TABLE_NAMES[2], ThrottleType.WRITE_NUMBER, 2000,
      TimeUnit.SECONDS));
    // read limit increase 2000 * 2 / 8, write limit increase 2000 * 2 / 8
    assertEquals(825, quotaManager.getTotalExistedReadLimit());
    assertEquals(950, quotaManager.getTotalExistedWriteLimit());
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
}
