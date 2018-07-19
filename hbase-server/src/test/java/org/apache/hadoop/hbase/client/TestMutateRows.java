/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.client;

import com.google.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.MultiRowMutationEndpoint;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.protobuf.generated.MultiRowMutationProtos;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@Category(MediumTests.class)
public class TestMutateRows {

  private static final Logger LOG = Logger.getLogger(TestMutateRows.class);
  private static final int THREAD_SIZE = 10;
  private static HBaseTestingUtility UTIL = null;
  private final String TEST_TABLE_NAME = "test_table";
  private final String COLUMN_FAMILY = "cf";
  private final String CQ = "cq";
  private AtomicInteger successCount = new AtomicInteger(0);

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestMutateRows.class);

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    conf.setStrings(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
        MultiRowMutationEndpoint.class.getName());

    UTIL = new HBaseTestingUtility(conf);
    UTIL.startMiniCluster();
    UTIL.getMiniHBaseCluster().waitForActiveAndReadyMaster();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  public Table createTable() throws Exception {
    TableDescriptorBuilder tdb = TableDescriptorBuilder.newBuilder(
        TableName.valueOf(TEST_TABLE_NAME));
    tdb.setColumnFamily(ColumnFamilyDescriptorBuilder.of(COLUMN_FAMILY));
    return UTIL.createTable(tdb.build(), null);
  }

  public List<Integer> mutateRowsWithConditions(Table table, List<Mutation> mutations,
      List<Condition> conditions) throws IOException, ServiceException {
    MultiRowMutationProtos.MutateRowsRequest.Builder requestBuilder =
        MultiRowMutationProtos.MutateRowsRequest.newBuilder();
    requestBuilder.addAllCondition(ProtobufUtil.toConditions(conditions));
    for (Mutation m : mutations) {
      if (m instanceof Put) {
        requestBuilder.addMutationRequest(
            ProtobufUtil.toMutation(ClientProtos.MutationProto.MutationType.PUT, m));
      }
    }
    CoprocessorRpcChannel channel = table.coprocessorService(mutations.get(0).getRow());
    MultiRowMutationProtos.MultiRowMutationService.BlockingInterface service =
        MultiRowMutationProtos.MultiRowMutationService.newBlockingStub(channel);
    return service.mutateRows(null, requestBuilder.build()).getUnmetConditionsList();
  }

  @Test
  public void testMutateRowsWithConditions() throws Exception {
    Table table = createTable();
    for (int i = 0; i < 1000; i++) {
      Put put = new Put(Bytes.toBytes("row-" + i));
      put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes(CQ), Bytes.toBytes(i));
      table.put(put);
    }
    Thread[] threads = new Thread[THREAD_SIZE];
    for (int i = 0; i < THREAD_SIZE; i++) {
      Thread t = new Thread() {
        public void run() {
          try {
            List<Mutation> mutations = new ArrayList<Mutation>();
            List<Condition> conditions = new ArrayList<Condition>();

            // get current value.
            Get get = new Get(Bytes.toBytes("row-0"));
            Result result = table.get(get);
            byte[] values = result.getValue(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes(CQ));
            int x = Bytes.toInt(values);

            mutations.add(new Put(Bytes.toBytes("row-0"))
                .addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes(CQ), Bytes.toBytes(x + 1)));
            conditions.add(new Condition(Bytes.toBytes("row-0"), Bytes.toBytes(COLUMN_FAMILY),
                Bytes.toBytes(CQ), CompareOperator.EQUAL, Bytes.toBytes(x)));

            List<Integer> unmetConditionsList =
                mutateRowsWithConditions(table, mutations, conditions);

            if (unmetConditionsList.size() == 0) {
              successCount.getAndIncrement();
            }
          } catch (Exception e) {
            LOG.error(e);
          }
        }
      };
      threads[i] = t;
    }

    for (int i = 0; i < THREAD_SIZE; i++) {
      threads[i].start();
    }

    for (int i = 0; i < THREAD_SIZE; i++) {
      threads[i].join();
    }

    Get get = new Get(Bytes.toBytes("row-0"));
    Result result = table.get(get);
    byte[] value = result.getValue(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes(CQ));
    Assert.assertArrayEquals(value, Bytes.toBytes(successCount.get()));
  }
}