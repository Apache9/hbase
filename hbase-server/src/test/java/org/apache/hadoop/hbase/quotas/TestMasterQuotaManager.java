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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.SetQuotaRequest;
import org.apache.hadoop.hbase.protobuf.generated.QuotaProtos.Quotas;
import org.apache.hadoop.hbase.protobuf.generated.QuotaProtos.Throttle;
import org.apache.hadoop.hbase.protobuf.generated.QuotaProtos.ThrottleRequest;
import org.apache.hadoop.hbase.protobuf.generated.QuotaProtos.TimedQuota;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

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

  // the read limit for cluster is 5 * 3000 * 0.7 default, write limit is 3 * 10000 * 0.7
  private static final int regionServerNum = 5;
  
  private MasterQuotaManager quotaManager;
  private String userName;
  private static HBaseAdmin admin;

  private org.apache.hadoop.hbase.protobuf.generated.QuotaProtos.ThrottleType pbReadNumber = ProtobufUtil
      .toProtoThrottleType(ThrottleType.READ_NUMBER);
  private org.apache.hadoop.hbase.protobuf.generated.QuotaProtos.ThrottleType pbWriteNumber = ProtobufUtil
      .toProtoThrottleType(ThrottleType.WRITE_NUMBER);
  
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
    admin.setQuota(QuotaSettingsFactory.throttleNamespace(
      NamespaceDescriptor.DEFAULT_NAMESPACE_NAME_STR, ThrottleType.READ_NUMBER, 10000l,
      TimeUnit.SECONDS));
    admin.setQuota(QuotaSettingsFactory.throttleNamespace(
      NamespaceDescriptor.DEFAULT_NAMESPACE_NAME_STR, ThrottleType.WRITE_NUMBER, 10000l,
      TimeUnit.SECONDS));
    // configured namespace read/write quota beforeClass
    quotaManager = TEST_UTIL.getHBaseCluster().getMaster().getMasterQuotaManager();
    assertEquals(10000, quotaManager.getCumulativeSoftLimit(null, pbReadNumber));
    assertEquals(10000, quotaManager.getCumulativeSoftLimit(null, pbWriteNumber));
    assertEquals(0, quotaManager.getTotalExistedReadLimit());
    assertEquals(0, quotaManager.getTotalExistedWriteLimit());
    userName = User.getCurrent().getShortName();
  }

  @After
  public void afterTest() throws Exception {
    for (int i = 0; i < TABLE_NAMES.length; ++i) {
      admin.setQuota(QuotaSettingsFactory.unthrottleUser(userName, TABLE_NAMES[i]));
    }
    assertEquals(0, quotaManager.getTotalExistedReadLimit());
    assertEquals(0, quotaManager.getTotalExistedWriteLimit());
  }

  @Test
  public void testCheckQuotaUpdate() throws Exception {
    try {
      quotaManager.checkQuotaUpdate("test", 1000, 0,  2000, 2999);
    } catch (Exception e) {
      assertEquals(QuotaExceededException.class, e.getClass());
    }

    try {
      quotaManager.checkQuotaUpdate("test", 1200, 0, 9000, 10000);
    } catch (Exception e) {
      assertEquals(QuotaExceededException.class, e.getClass());
    }

    try {
      quotaManager.checkQuotaUpdate("test", 0, 0, 2000, 3000);
    } catch (Exception e) {
      fail("Should not throw exception " + e);
    }

    try {
      // if user not update quota to bigger, not throw exception even totalExistedLimit > rsLimit
      quotaManager.checkQuotaUpdate("test", 0, 0, 2000, 1800);
    } catch (Exception e) {
      fail("Should not throw exception " + e);
    }

    try {
      // if user update quota to be smaller, not throw exception even totalExistedLimit > rsLimit
      quotaManager.checkQuotaUpdate("test", 0, 500, 2000, 1000);
    } catch (Exception e) {
      fail("Should not throw exception " + e);
    }
  }

  @Test
  public void testGetSoftLimitFromRequest() throws Exception {
    SetQuotaRequest.Builder builder = SetQuotaRequest.newBuilder();
    
    // throttle type error
    ThrottleRequest.Builder throttleBuilder = ThrottleRequest.newBuilder();
    throttleBuilder.setType(ProtobufUtil.toProtoThrottleType(ThrottleType.REQUEST_NUMBER));
    builder.setThrottle(throttleBuilder.build());
    try {
      quotaManager.getSoftLimitFromRequest(builder.build());
    } catch (IOException e) {
      assertTrue(e.getMessage().contains("throttle type not support"));
    }
    
    // time unit error
    throttleBuilder.setType(ProtobufUtil.toProtoThrottleType(ThrottleType.READ_NUMBER));
    TimedQuota.Builder timeQuotaBuilder = TimedQuota.newBuilder();
    timeQuotaBuilder.setTimeUnit(org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.TimeUnit.DAYS);
    throttleBuilder.setTimedQuota(timeQuotaBuilder.build());
    builder.setThrottle(throttleBuilder.build());
    try {
      quotaManager.getSoftLimitFromRequest(builder.build());
    } catch (IOException e) {
      assertTrue(e.getMessage().contains("time unit must be seconds"));
    }
    
    timeQuotaBuilder.setTimeUnit(org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.TimeUnit.SECONDS);
    timeQuotaBuilder.setSoftLimit(1000l);
    throttleBuilder.setTimedQuota(timeQuotaBuilder.build());
    builder.setThrottle(throttleBuilder.build());
    assertEquals(1000l, quotaManager.getSoftLimitFromRequest(builder.build()));
  }
  
  @Test
  public void testCheckNamespaceQuota() throws Exception {
    // test for default namespace
    admin.setQuota(QuotaSettingsFactory.throttleUser(userName, TABLE_NAMES[0],
      ThrottleType.READ_NUMBER, 2000, TimeUnit.SECONDS));
    String anotherUser = userName + "1";
    admin.setQuota(QuotaSettingsFactory.throttleUser(anotherUser, TABLE_NAMES[1],
      ThrottleType.READ_NUMBER, 5000, TimeUnit.SECONDS));
    // quota for new table
    try {
      quotaManager.checkNamespaceQuota(userName, TABLE_NAMES[2], 2999, pbReadNumber);
    } catch (Exception e) {
      fail("configured quota is 10000, expected is 9999, should allow");
    }
    
    try {
      quotaManager.checkNamespaceQuota(userName, TABLE_NAMES[2], 3001, pbReadNumber);
      fail("configured quota is 10000, expected is 10001, should deny");
    } catch (Exception e) {
    }
    
    // quota for existed table and user
    try {
      quotaManager.checkNamespaceQuota(userName, TABLE_NAMES[0], 4999, pbReadNumber);
    } catch (Exception e) {
      fail("configured quota is 10000, expected is 9999, should allow");
    }
    
    try {
      quotaManager.checkNamespaceQuota(anotherUser, TABLE_NAMES[1], 8001, pbReadNumber);
      fail("configured quota is 10000, expected is 10001, should deny");
    } catch (Exception e) {
    }
    try {
      quotaManager.checkNamespaceQuota(anotherUser, TABLE_NAMES[1], 7999, pbReadNumber);
    } catch (Exception e) {
      fail("configured quota is 10000, expected is 9999, should allow");
    }
    
    
    // reduce default namespace quota, allowed
    try {
      quotaManager.checkNamespaceQuota(TABLE_NAMES[0].getNamespaceAsString(), 7001, pbReadNumber);
    } catch (Exception e) {
      fail();
    }
    // reduce default namespace quota, denied
    try {
      quotaManager.checkNamespaceQuota(TABLE_NAMES[0].getNamespaceAsString(), 6999, pbReadNumber);
      fail();
    } catch (Exception e) {
    }
    
    QuotaUtil.deleteUserQuota(TEST_UTIL.getConfiguration(), anotherUser, TABLE_NAMES[1]);
    
    // test for no-default namespace
    String testNs = "test_ns";
    String thirdUser = anotherUser + "1";
    QuotaUtil.addNamespaceQuota(TEST_UTIL.getConfiguration(), testNs,
      getQuotas(ThrottleType.WRITE_NUMBER, 3000));
    TableName testNsTab1 = TableName.valueOf(testNs, "testNsTab1");
    QuotaUtil.addUserQuota(TEST_UTIL.getConfiguration(), userName, testNsTab1,
      getQuotas(ThrottleType.WRITE_NUMBER, 1000));
    QuotaUtil.addUserQuota(TEST_UTIL.getConfiguration(), anotherUser, testNsTab1,
      getQuotas(ThrottleType.WRITE_NUMBER, 1000));
    QuotaUtil.addUserQuota(TEST_UTIL.getConfiguration(), thirdUser, testNsTab1,
      getQuotas(ThrottleType.WRITE_NUMBER, 500));
    
    try {
      quotaManager.checkNamespaceQuota(userName, testNsTab1, 1600, pbWriteNumber);
      fail();
    } catch (Exception e) {
    }
    quotaManager.checkNamespaceQuota(userName, testNsTab1, 1400, pbWriteNumber);

    TableName testNsTab2 = TableName.valueOf(testNs, "testNsTab2");
    try {
      quotaManager.checkNamespaceQuota(userName, testNsTab2, 600, pbWriteNumber);
      fail();
    } catch (Exception e) {
    }
    quotaManager.checkNamespaceQuota(userName, testNsTab2, 400, pbWriteNumber);
    
    // reduce namespace quota, allowed
    try {
      quotaManager.checkNamespaceQuota(testNs, 2501, pbWriteNumber);
    } catch (Exception e) {
      fail();
    }
    // reduce namespace quota, denied
    try {
      quotaManager.checkNamespaceQuota(testNs, 2499, pbWriteNumber);
      fail();
    } catch (Exception e) {
    }
    
    QuotaUtil.deleteUserQuota(TEST_UTIL.getConfiguration(), userName, testNsTab1);
    QuotaUtil.deleteUserQuota(TEST_UTIL.getConfiguration(), anotherUser, testNsTab1);
    QuotaUtil.deleteUserQuota(TEST_UTIL.getConfiguration(), thirdUser, testNsTab1);
    QuotaUtil.deleteNamespaceQuota(TEST_UTIL.getConfiguration(), testNs);
  }
  
  @Test
  public void testCheckRegionServerQuotaForTable() throws Exception {
    admin.setQuota(QuotaSettingsFactory.throttleNamespace(
      NamespaceDescriptor.DEFAULT_NAMESPACE_NAME_STR, ThrottleType.READ_NUMBER, 10500l,
      TimeUnit.SECONDS));
    
    admin.setQuota(QuotaSettingsFactory.throttleUser(userName, TABLE_NAMES[0],
      ThrottleType.READ_NUMBER, 2000, TimeUnit.SECONDS));
    admin.setQuota(QuotaSettingsFactory.throttleUser(userName, TABLE_NAMES[1],
      ThrottleType.READ_NUMBER, 5000, TimeUnit.SECONDS));
    assertEquals(1500, quotaManager.getTotalExistedReadLimit());

    try {
      quotaManager.checkRegionServerQuota(userName, TABLE_NAMES[1], 8000, pbReadNumber, 0.2);
      admin.setQuota(QuotaSettingsFactory.throttleUser(userName, TABLE_NAMES[1],
        ThrottleType.READ_NUMBER, 8000, TimeUnit.SECONDS));
    } catch (Exception e) {
      fail("Should not throw exception " + e);
    }
    assertEquals(2100, quotaManager.getTotalExistedReadLimit());

    try {
      quotaManager.checkRegionServerQuota(userName, TABLE_NAMES[0], 3000, pbReadNumber, 0.25);
    } catch (Exception e) {
      assertEquals(QuotaExceededException.class, e.getClass());
    }
    assertEquals(2100, quotaManager.getTotalExistedReadLimit());

    try {
      quotaManager.checkRegionServerQuota(userName, TABLE_NAMES[2], 4000, pbReadNumber, 0.25);
    } catch (Exception e) {
      assertEquals(QuotaExceededException.class, e.getClass());
    }
    assertEquals(2100, quotaManager.getTotalExistedReadLimit());

    try {
      quotaManager.checkRegionServerQuota(userName, TABLE_NAMES[1], 5000, pbReadNumber, 0.2);
      admin.setQuota(QuotaSettingsFactory.throttleUser(userName, TABLE_NAMES[1],
        ThrottleType.READ_NUMBER, 5000, TimeUnit.SECONDS));
    } catch (Exception e) {
      fail("Should not throw exception " + e);
    }
    assertEquals(1500, quotaManager.getTotalExistedReadLimit());    
    
    String anotherUser = "anotherUser";
    try {
      quotaManager.checkRegionServerQuota(anotherUser, TABLE_NAMES[0], 2000, pbReadNumber, 0.25);
      admin.setQuota(QuotaSettingsFactory.throttleUser(anotherUser, TABLE_NAMES[0],
        ThrottleType.READ_NUMBER, 2000, TimeUnit.SECONDS));
    } catch (Exception e) {
      fail("Should not throw exception " + e);
    }
    assertEquals(2000, quotaManager.getTotalExistedReadLimit());
    admin.setQuota(QuotaSettingsFactory.unthrottleUser(anotherUser, TABLE_NAMES[0]));
    assertEquals(1500, quotaManager.getTotalExistedReadLimit());
  }

  @Test
  public void testCheckRegionServerQuotaForNamespace() throws Exception {
    // update namespace quota for default namespace
    // allowed
    try {
      quotaManager.checkRegionServerQuota(NamespaceDescriptor.DEFAULT_NAMESPACE_NAME_STR, 10500l,
        5, pbReadNumber);
    } catch (Exception e) {
      fail();
    }
    
    // denied
    try {
      quotaManager.checkRegionServerQuota(NamespaceDescriptor.DEFAULT_NAMESPACE_NAME_STR, 10501l,
        5, pbReadNumber);
      fail();
    } catch (Exception e) {
    }
    
    // update namespace quota for other namespace
    try {
      quotaManager.checkRegionServerQuota("ns_testCheckRegionServerQuota", 500l, 5, pbReadNumber);
    } catch (Exception e) {
      fail();
    }
    
    try {
      quotaManager.checkRegionServerQuota("ns_testCheckRegionServerQuota", 501l, 5, pbReadNumber);
      fail();
    } catch (Exception e) {
    }
  }
  
  private Quotas getQuotas(ThrottleType type, long limit) throws IOException {
    Quotas.Builder builder = Quotas.newBuilder();
    Throttle.Builder throttleBuilder = Throttle.newBuilder();
    if (type == ThrottleType.READ_NUMBER) {
      throttleBuilder.setReadNum(getTimedQuota(limit));
    } else {
      throttleBuilder.setWriteNum(getTimedQuota(limit));
    }
    builder.setThrottle(throttleBuilder.build());
    return builder.build();
  }
  
  private TimedQuota.Builder getTimedQuota(long limit) {
    return TimedQuota.newBuilder()
        .setTimeUnit(org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.TimeUnit.SECONDS)
        .setSoftLimit(limit);
  }
  
  @Test
  public void testGetSoftLimitForUserAndTable() throws IOException {
    QuotaUtil.addUserQuota(TEST_UTIL.getConfiguration(), userName, TABLE_NAMES[0],
      getQuotas(ThrottleType.READ_NUMBER, 500));
    assertEquals(500, quotaManager.getSoftLimitForUserAndTable(userName, TABLE_NAMES[0],
      ProtobufUtil.toProtoThrottleType(ThrottleType.READ_NUMBER)));
    
    QuotaUtil.addUserQuota(TEST_UTIL.getConfiguration(), userName, TABLE_NAMES[0],
      getQuotas(ThrottleType.WRITE_NUMBER, 3000));
    assertEquals(3000, quotaManager.getSoftLimitForUserAndTable(userName, TABLE_NAMES[0],
      ProtobufUtil.toProtoThrottleType(ThrottleType.WRITE_NUMBER)));
    
    QuotaUtil.deleteUserQuota(TEST_UTIL.getConfiguration(), userName, TABLE_NAMES[0]);
  }
  
  @Test
  public void testGetSoftLimitForNamespace() throws IOException {
    assertEquals(10000l, quotaManager.getSoftLimitForNamespace(
      NamespaceDescriptor.DEFAULT_NAMESPACE_NAME_STR, pbReadNumber));
    assertEquals(10000l, quotaManager.getSoftLimitForNamespace(
      NamespaceDescriptor.DEFAULT_NAMESPACE_NAME_STR, pbWriteNumber));
    
    String namespace = "test_ns";
    QuotaUtil.addNamespaceQuota(TEST_UTIL.getConfiguration(), namespace,
      getQuotas(ThrottleType.READ_NUMBER, 500));
    assertEquals(500, quotaManager.getSoftLimitForNamespace(namespace, pbReadNumber));
    
    QuotaUtil.addNamespaceQuota(TEST_UTIL.getConfiguration(), namespace,
      getQuotas(ThrottleType.WRITE_NUMBER, 3000));
    assertEquals(3000, quotaManager.getSoftLimitForNamespace(namespace, pbWriteNumber));
    
    QuotaUtil.deleteNamespaceQuota(TEST_UTIL.getConfiguration(), namespace);
  }
  
  @Test
  public void testGetConsumedSoftLimitForNamespace() throws IOException {
    for (int i = 0; i < TABLE_NAMES.length; ++i) {
      QuotaUtil.addUserQuota(TEST_UTIL.getConfiguration(), userName, TABLE_NAMES[i],
        getQuotas(ThrottleType.READ_NUMBER, 500));
    }    
    assertEquals(500 * TABLE_NAMES.length, quotaManager.getConsumedSoftLimitForNamespace(
      NamespaceDescriptor.DEFAULT_NAMESPACE_NAME_STR, pbReadNumber));
    
    
    for (int i = 0; i < TABLE_NAMES.length; ++i) {
      QuotaUtil.addUserQuota(TEST_UTIL.getConfiguration(), userName, TABLE_NAMES[i],
        getQuotas(ThrottleType.WRITE_NUMBER, 1000));
    }
    
    assertEquals(1000 * TABLE_NAMES.length, quotaManager.getConsumedSoftLimitForNamespace(
      NamespaceDescriptor.DEFAULT_NAMESPACE_NAME_STR, pbWriteNumber));
    
    for (int i = 0; i < TABLE_NAMES.length; ++i) {
      QuotaUtil.deleteUserQuota(TEST_UTIL.getConfiguration(), userName, TABLE_NAMES[i]);
    }
  }
  
  @Test
  public void testGetConsumedSoftLimitForAllNamespaces() throws IOException {
    String[] namespaces = new String[]{"ns1", "ns2"};
    for (int i = 0; i < namespaces.length; ++i) {
      QuotaUtil.addNamespaceQuota(TEST_UTIL.getConfiguration(), namespaces[i],
        getQuotas(ThrottleType.READ_NUMBER, 500));
    }
    // 10000l is read quota for default namespace, added in BeforeClass
    assertEquals(500 * namespaces.length + 10000l,
      quotaManager.getConsumedSoftLimitForAllNamespaces(pbReadNumber));
    
    for (int i = 0; i < namespaces.length; ++i) {
      QuotaUtil.addNamespaceQuota(TEST_UTIL.getConfiguration(), namespaces[i],
        getQuotas(ThrottleType.WRITE_NUMBER, 1000));
    }
    
    // 10000l is write quota for default namespace, added in BeforeClass
    assertEquals(1000 * namespaces.length + 10000l,
      quotaManager.getConsumedSoftLimitForAllNamespaces(pbWriteNumber));
    
    for (int i = 0; i < namespaces.length; ++i) {
      QuotaUtil.deleteNamespaceQuota(TEST_UTIL.getConfiguration(), namespaces[i]);
    }
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

    // table[2] have 8 region, quota will be distributed by factor 2/8 or 1/8
    admin.setQuota(QuotaSettingsFactory.throttleUser(userName, TABLE_NAMES[2], ThrottleType.READ_NUMBER, 2000,
      TimeUnit.SECONDS));
    admin.setQuota(QuotaSettingsFactory.throttleUser(userName, TABLE_NAMES[2], ThrottleType.WRITE_NUMBER, 2000,
      TimeUnit.SECONDS));
    // read limit increase 2000 * 2 / 8, write limit increase 2000 * 2 / 8
    assertEquals(825, quotaManager.getTotalExistedReadLimit());
    assertEquals(950, quotaManager.getTotalExistedWriteLimit());
  }
}
