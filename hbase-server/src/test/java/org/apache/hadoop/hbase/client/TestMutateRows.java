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
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.coprocessor.AggregateImplementation;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.MultiRowMutationEndpoint;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.protobuf.generated.MultiRowMutationProtos;
import org.apache.hadoop.hbase.regionserver.TestServerCustomProtocol;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class TestMutateRows {

  private static final Logger LOG = Logger.getLogger(TestMutateRows.class);
  private static final int THREAD_SIZE = 10;
  private static HBaseTestingUtility UTIL = null;
  private final String TEST_TABLE_NAME = "test_table";
  private final String COLUMN_FAMILY = "cf";
  private final String CQ = "cq";
  private AtomicInteger successCount = new AtomicInteger(0);

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    conf.setInt("hbase.master.info.port", 0);
    conf.setStrings(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
      AggregateImplementation.class.getName(), MultiRowMutationEndpoint.class.getName(),
      TestServerCustomProtocol.PingHandler.class.getName());

    UTIL = new HBaseTestingUtility(conf);
    UTIL.startMiniCluster();
    UTIL.getMiniHBaseCluster().waitForActiveAndReadyMaster();

  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  public void createTable() throws Exception {
    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(TEST_TABLE_NAME));
    HColumnDescriptor hcd = new HColumnDescriptor(COLUMN_FAMILY);
    htd.addFamily(hcd);
    UTIL.getHBaseAdmin().createTable(htd);
    UTIL.waitTableAvailable(Bytes.toBytes(TEST_TABLE_NAME));
  }

  public List<Integer> mutateRowsWithConditions(HTable client, List<Mutation> mutations,
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
    CoprocessorRpcChannel channel = client.coprocessorService(mutations.get(0).getRow());
    MultiRowMutationProtos.MultiRowMutationService.BlockingInterface service =
        MultiRowMutationProtos.MultiRowMutationService.newBlockingStub(channel);
    return service.mutateRows(null, requestBuilder.build()).getUnmetConditionsList();
  }

  @Test
  public void test() throws Exception {
    createTable();

    HTable htable = new HTable(UTIL.getConfiguration(), TEST_TABLE_NAME);

    for (int i = 0; i < 1000; i++) {
      Put put = new Put(Bytes.toBytes("row-" + i));
      put.add(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes(CQ), Bytes.toBytes(i));
      htable.put(put);
    }

    Thread[] threads = new Thread[THREAD_SIZE];
    for (int i = 0; i < THREAD_SIZE; i++) {
      Thread t = new Thread() {
        public void run() {
          HTable client = null;
          try {
            client = new HTable(UTIL.getConfiguration(), TEST_TABLE_NAME);
            List<Mutation> mutations = new ArrayList<Mutation>();
            List<Condition> conditions = new ArrayList<Condition>();

            // get current value.
            Get get = new Get(Bytes.toBytes("row-0"));
            Result result = client.get(get);
            byte[] values = result.getValue(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes(CQ));
            int x = Bytes.toInt(values);

            mutations.add(new Put(Bytes.toBytes("row-0")).add(Bytes.toBytes(COLUMN_FAMILY),
              Bytes.toBytes(CQ), Bytes.toBytes(x + 1)));
            conditions.add(new Condition(Bytes.toBytes("row-0"), Bytes.toBytes(COLUMN_FAMILY),
                Bytes.toBytes(CQ), CompareOp.EQUAL, Bytes.toBytes(x)));

            List<Integer> unmetConditionsList =
                mutateRowsWithConditions(client, mutations, conditions);

            if (unmetConditionsList.size() == 0) {
              successCount.getAndIncrement();
            }
          } catch (Exception e) {
            LOG.error(e);
          } finally {
            if (client != null) try {
              client.close();
            } catch (Exception e) {
            }
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
    Result result = htable.get(get);
    byte[] value = result.getValue(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes(CQ));
    Assert.assertArrayEquals(value, Bytes.toBytes(successCount.get()));
  }
}