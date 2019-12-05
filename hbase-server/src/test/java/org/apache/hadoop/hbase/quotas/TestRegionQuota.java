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

import static org.apache.hadoop.hbase.quotas.TestQuotaThrottle.doGets;
import static org.apache.hadoop.hbase.quotas.TestQuotaThrottle.doPuts;
import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManagerTestHelper;
import org.apache.hadoop.hbase.util.ManualEnvironmentEdge;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MediumTests.class })
public class TestRegionQuota {
  final Log LOG = LogFactory.getLog(getClass());

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private final static byte[] FAMILY = Bytes.toBytes("cf");
  private final static TableName TABLE_NAME = TableName.valueOf("TestRegionQuota");
  private static HTable table;
  private static ManualEnvironmentEdge envEdge;
  private static HBaseAdmin admin;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setInt("hbase.regionserver.msginterval", 100);
    TEST_UTIL.getConfiguration().setInt("hbase.client.pause", 250);
    TEST_UTIL.getConfiguration().setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 1);
    TEST_UTIL.startMiniCluster(1);

    table = TEST_UTIL.createTable(TABLE_NAME, FAMILY);
    envEdge = new ManualEnvironmentEdge();
    envEdge.setValue(EnvironmentEdgeManager.currentTimeMillis());
    EnvironmentEdgeManagerTestHelper.injectEdge(envEdge);
    admin = TEST_UTIL.getHBaseAdmin();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    table.close();
    TEST_UTIL.deleteTable(TABLE_NAME);
    admin.close();
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test(timeout = 60000)
  public void testRegionQuota() throws Exception {
    List<HRegionInfo> tableRegions = admin.getTableRegions(TABLE_NAME);
    assertEquals(1, tableRegions.size());
    assertEquals(100, doPuts(100, table));
    // set write limit to 1
    admin.setRegionQuota(tableRegions.get(0).getEncodedNameAsBytes(), ThrottleType.WRITE_NUMBER, 1,
      TimeUnit.MINUTES);
    assertEquals(1, admin.listRegionQuota().size());
    // check RS region quota is refreshed
    TestQuotaThrottle.waitRegionQuotasRefreshed(TEST_UTIL, 1, 0);
    assertEquals(1, doPuts(2, table));
    assertEquals(51, doGets(51, table));
    // set read limit to 50
    admin.setRegionQuota(tableRegions.get(0).getEncodedNameAsBytes(), ThrottleType.READ_NUMBER, 50,
      TimeUnit.MINUTES);
    assertEquals(2, admin.listRegionQuota().size());
    // check RS region quota is refreshed
    TestQuotaThrottle.waitRegionQuotasRefreshed(TEST_UTIL, 1, 1);
    assertEquals(1, doPuts(2, table));
    assertEquals(50, doGets(51, table));
    // remove region quota
    admin.removeRegionQuota(tableRegions.get(0).getEncodedNameAsBytes());
    assertEquals(0, admin.listRegionQuota().size());
  }
}
