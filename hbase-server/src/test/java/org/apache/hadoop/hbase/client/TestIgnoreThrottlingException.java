/**
 *
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
package org.apache.hadoop.hbase.client;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.quotas.ThrottlingException;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

@Category({ MediumTests.class })
public class TestIgnoreThrottlingException {

  final Log LOG = LogFactory.getLog(getClass());

  protected static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static byte[] ROW = Bytes.toBytes("row");

  private static byte[] FAMILY = Bytes.toBytes("cf");

  private static byte[] QUALIFIER = Bytes.toBytes("cq");

  private static byte[] VALUE = Bytes.toBytes("value");

  @Rule
  public TestName testName = new TestName();

  private static final int RETRY_NUM = 3;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, RETRY_NUM);
    conf.setInt(AsyncProcess.START_LOG_ERRORS_AFTER_COUNT_KEY, 0);
    TEST_UTIL.startMiniCluster(1);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  private void createTable(TableName tableName) throws Exception {
    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(new HColumnDescriptor(FAMILY));
    desc.addCoprocessor(ThrowThrottlingExceptionCoprocessor.class.getName());
    TEST_UTIL.getHBaseAdmin().createTable(desc);
    TEST_UTIL.waitUntilAllRegionsAssigned(tableName);
  }

  private void deleteTable(TableName tableName) throws Exception {
    TEST_UTIL.getHBaseAdmin().disableTable(tableName);
    TEST_UTIL.getHBaseAdmin().deleteTable(tableName);
  }

  @Test
  public void testGetWithIgnoreThrottlingException() throws Exception {
    TableName tableName = TableName.valueOf(testName.getMethodName());
    createTable(tableName);

    Configuration conf = new Configuration(TEST_UTIL.getConfiguration());
    conf.setBoolean(HConstants.HBASE_CLIENT_IGNORE_THROTTLING_EXCEPTION, true);
    try (HConnection connection = HConnectionManager.createConnection(conf);
        HTableInterface table = connection.getTable(tableName)) {
      Get get = new Get(ROW).addColumn(FAMILY, QUALIFIER);
      table.get(get);
    }

    deleteTable(tableName);
  }

  @Test(expected = RetriesExhaustedException.class)
  public void testGetWithoutIgnoreThrottlingException() throws Exception {
    TableName tableName = TableName.valueOf(testName.getMethodName());
    createTable(tableName);

    try (HConnection connection = HConnectionManager.createConnection(TEST_UTIL.getConfiguration());
        HTableInterface table = connection.getTable(tableName)) {
      Get get = new Get(ROW).addColumn(FAMILY, QUALIFIER);
      table.get(get);
    }

    deleteTable(tableName);
  }

  @Test
  public void testPutWithIgnoreThrottlingException() throws Exception {
    TableName tableName = TableName.valueOf(testName.getMethodName());
    createTable(tableName);

    Configuration conf = new Configuration(TEST_UTIL.getConfiguration());
    conf.setBoolean(HConstants.HBASE_CLIENT_IGNORE_THROTTLING_EXCEPTION, true);
    try (HConnection connection = HConnectionManager.createConnection(conf);
        HTableInterface table = connection.getTable(tableName)) {
      Put put = new Put(ROW).add(FAMILY, QUALIFIER, VALUE);
      table.put(put);
    }

    deleteTable(tableName);
  }

  @Test(expected = RetriesExhaustedException.class)
  public void testPutWithoutIgnoreThrottlingException() throws Exception {
    TableName tableName = TableName.valueOf(testName.getMethodName());
    createTable(tableName);

    try (HConnection connection = HConnectionManager.createConnection(TEST_UTIL.getConfiguration());
        HTableInterface table = connection.getTable(tableName)) {
      Put put = new Put(ROW).add(FAMILY, QUALIFIER, VALUE);
      table.put(put);
    }

    deleteTable(tableName);
  }

  @Test
  public void testBufferedPutWithIgnoreThrottlingException() throws Exception {
    TableName tableName = TableName.valueOf(testName.getMethodName());
    createTable(tableName);

    Configuration conf = new Configuration(TEST_UTIL.getConfiguration());
    conf.setBoolean(HConstants.HBASE_CLIENT_IGNORE_THROTTLING_EXCEPTION, true);
    try (HConnection connection = HConnectionManager.createConnection(conf);
        HTableInterface table = connection.getTable(tableName)) {
      table.setAutoFlush(false);
      Put put = new Put(ROW).add(FAMILY, QUALIFIER, VALUE);
      table.put(put);
      table.flushCommits();
    }

    deleteTable(tableName);
  }

  public static class ThrowThrottlingExceptionCoprocessor extends BaseRegionObserver {

    private AtomicLong count = new AtomicLong(0);

    public ThrowThrottlingExceptionCoprocessor() {
    }

    @Override
    public void preGet(final ObserverContext<RegionCoprocessorEnvironment> c, final Get get,
        final List<KeyValue> result) throws IOException {
      if (count.getAndIncrement() <= RETRY_NUM) {
        throw new ThrottlingException("Get throttled");
      }
    }

    @Override
    public void prePut(final ObserverContext<RegionCoprocessorEnvironment> e, final Put put,
        final WALEdit edit, final Durability durability) throws IOException {
      if (count.getAndIncrement() <= RETRY_NUM) {
        throw new ThrottlingException("Put throttled");
      }
    }

    @Override
    public void preBatchMutate(final ObserverContext<RegionCoprocessorEnvironment> c,
        final MiniBatchOperationInProgress<Mutation> miniBatchOp) throws IOException {
      if (count.getAndIncrement() <= RETRY_NUM) {
        throw new ThrottlingException("Batch throttled");
      }
    }
  }
}
