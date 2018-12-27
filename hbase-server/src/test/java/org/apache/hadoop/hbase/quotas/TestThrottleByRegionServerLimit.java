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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RetriesExhaustedException;
import org.apache.hadoop.hbase.codec.KeyValueCodec;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({MediumTests.class})
public class TestThrottleByRegionServerLimit {

  final Log LOG = LogFactory.getLog(getClass());

  private final static int REFRESH_TIME = 30 * 60000;

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private final static byte[] FAMILY = Bytes.toBytes("cf");
  private final static byte[] QUALIFIER = Bytes.toBytes("q");

  private final static TableName TABLE_NAME = TableName.valueOf("TestQuotaThrottleForMulti1");

  private static HTable table;

  private static final int REGION_SERVER_WRITE_LIMIT = 200;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setBoolean(QuotaUtil.QUOTA_CONF_KEY, true);
    TEST_UTIL.getConfiguration().setInt(QuotaCache.REFRESH_CONF_KEY, REFRESH_TIME);
    TEST_UTIL.getConfiguration().setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 1);
    TEST_UTIL.getConfiguration().setBoolean(QuotaUtil.QUOTA_ALLOW_EXCEED_CONF_KEY, true);
    TEST_UTIL.getConfiguration()
        .setInt(QuotaCache.REGION_SERVER_WRITE_LIMIT_KEY, REGION_SERVER_WRITE_LIMIT);
    TEST_UTIL.getConfiguration()
        .set(HConstants.RPC_CODEC_CONF_KEY, KeyValueCodec.class.getCanonicalName());
    TEST_UTIL.startMiniCluster(1);
    TEST_UTIL.waitTableAvailable(QuotaTableUtil.QUOTA_TABLE_NAME.getName());
    QuotaCache.TEST_FORCE_REFRESH = true;
    table = TEST_UTIL.createTable(TABLE_NAME, FAMILY);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.deleteTable(TABLE_NAME);
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testMultiPut() throws Exception {
    assertTrue(doPuts(REGION_SERVER_WRITE_LIMIT - 100));
    assertFalse(doPuts(REGION_SERVER_WRITE_LIMIT + 100));
  }

  private boolean doPuts(int maxOps) throws Exception {
    try {
      List<Put> puts = new ArrayList<>();
      for (int i = 0; i < maxOps; i++) {
        Put put = new Put(Bytes.toBytes("row-" + i));
        put.add(FAMILY, QUALIFIER, Bytes.toBytes("data-" + i));
        puts.add(put);
      }
      table.put(puts);
    } catch (RetriesExhaustedException e) {
      assertTrue(e.getCause() instanceof ThrottlingException);
      LOG.error("puts failed", e);
      return false;
    }
    return true;
  }
}
