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
package org.apache.hadoop.hbase.ipc;

import static org.apache.hadoop.hbase.ipc.RWQueueRpcExecutor.DEFAULT_MULTI_GET_THRESHOLD;
import static org.apache.hadoop.hbase.ipc.RWQueueRpcExecutor.MULTI_GET_THRESHOLD;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Action;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.RandomRowFilter;
import org.apache.hadoop.hbase.protobuf.RequestConverter;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MultiRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutateRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ScanRequest;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.QueueCounter;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import com.google.protobuf.Message;

@Category(SmallTests.class)
public class TestRWQueueRpcExecutor {

  private Configuration conf;
  private byte[] regionName = Bytes.toBytes("region");
  private ScanRequest scanWithoutFilter;
  private ScanRequest scanWithFilter;
  private ScanRequest scanLimitOneRow;
  private MutateRequest mutateRequest;
  private MultiRequest smallMultiGetRequest;
  private MultiRequest bigMultiGetRequest;
  private MultiRequest multiPutRequest;

  @Before
  public void setUp() throws IOException {
    conf = HBaseConfiguration.create();
    scanWithoutFilter = RequestConverter.buildScanRequest(regionName, new Scan(), 1, true);
    scanWithFilter = RequestConverter
        .buildScanRequest(regionName, new Scan().setFilter(new RandomRowFilter(10)), 1, true);
    scanLimitOneRow = RequestConverter.buildScanRequest(regionName,
        new Scan().withStartRow(regionName).withStopRow(regionName).setRaw(true)
            .setFilter(new FirstKeyOnlyFilter()).setCacheBlocks(false).setOneRowLimit(), 10, true);
    mutateRequest = RequestConverter.buildMutateRequest(regionName, new Put(regionName));

    int multiGetThreshold = 10;
    conf.setInt(MULTI_GET_THRESHOLD, multiGetThreshold);
    smallMultiGetRequest = buildMultiRequest(true, multiGetThreshold - 1);
    bigMultiGetRequest = buildMultiRequest(true, multiGetThreshold + 1);
    multiPutRequest = buildMultiRequest(false, multiGetThreshold);
  }

  private MultiRequest buildMultiRequest(boolean isGet, int number) throws IOException {
    MultiRequest.Builder multiRequestBuilder = MultiRequest.newBuilder();
    ClientProtos.RegionAction.Builder regionActionBuilder = ClientProtos.RegionAction.newBuilder();
    ClientProtos.Action.Builder actionBuilder = ClientProtos.Action.newBuilder();
    ClientProtos.MutationProto.Builder mutationBuilder = ClientProtos.MutationProto.newBuilder();
    List<Action<Row>> actions = new ArrayList<>();
    for (int i = 0; i < number; i++) {
      if (isGet) {
        actions.add(new Action<>(new Get(Bytes.toBytes(i)), i));
      } else {
        actions.add(new Action<>(new Put(Bytes.toBytes(i)), i));
      }
    }
    regionActionBuilder.clear();
    regionActionBuilder.setRegion(RequestConverter
        .buildRegionSpecifier(HBaseProtos.RegionSpecifier.RegionSpecifierType.REGION_NAME,
            regionName));
    regionActionBuilder = RequestConverter
        .buildRegionAction(regionName, actions, regionActionBuilder, actionBuilder,
            mutationBuilder);
    multiRequestBuilder.addRegionAction(regionActionBuilder.build());
    return multiRequestBuilder.build();
  }

  @Test
  public void testIsHeavyScanRequest() {
    RWQueueRpcExecutor rwQueueRpcExecutor =
        new RWQueueRpcExecutor("test", 100, 4, 0.5f, 100, conf, null);
    // scan without filter
    assertFalse(rwQueueRpcExecutor.isHeavyReadRequest(scanWithoutFilter));
    // scan with filter
    assertTrue(rwQueueRpcExecutor.isHeavyReadRequest(scanWithFilter));
    // scan with filter and limit one row (like Canary scan request)
    assertFalse(rwQueueRpcExecutor.isHeavyReadRequest(scanLimitOneRow));
    // small multi get
    assertFalse(rwQueueRpcExecutor.isHeavyReadRequest(smallMultiGetRequest));
    // big multi get
    assertTrue(rwQueueRpcExecutor.isHeavyReadRequest(bigMultiGetRequest));

    assertFalse(rwQueueRpcExecutor.isWriteRequest(null, smallMultiGetRequest));
    assertFalse(rwQueueRpcExecutor.isWriteRequest(null, bigMultiGetRequest));
    assertTrue(rwQueueRpcExecutor.isWriteRequest(null, multiPutRequest));
    rwQueueRpcExecutor.stop();
  }

  @Test
  public void testHandlerCountMoreThanQueueCount() throws Exception {
    float readShare = 0.5f;
    double scanShare = 0.5;
    int handlerCount = 256;
    int numQueues = 4;
    int maxQueueLength = 100;
    RWQueueRpcExecutor rpcExecutor =
        checkHandlersCount(readShare, scanShare, handlerCount, numQueues, maxQueueLength);
    rpcExecutor.dispatch(createMockCall(scanLimitOneRow));
    checkQueueCounters(rpcExecutor, 0, 1, 0);
    rpcExecutor.dispatch(createMockCall(scanWithFilter));
    checkQueueCounters(rpcExecutor, 0, 1, 1);
    rpcExecutor.dispatch(createMockCall(scanWithoutFilter));
    checkQueueCounters(rpcExecutor, 0, 2, 1);
    rpcExecutor.dispatch(createMockCall(mutateRequest));
    checkQueueCounters(rpcExecutor, 1, 2, 1);
    rpcExecutor.dispatch(createMockCall(multiPutRequest));
    checkQueueCounters(rpcExecutor, 2, 2, 1);
    rpcExecutor.dispatch(createMockCall(smallMultiGetRequest));
    checkQueueCounters(rpcExecutor, 2, 3, 1);
    rpcExecutor.dispatch(createMockCall(bigMultiGetRequest));
    checkQueueCounters(rpcExecutor, 2, 3, 2);
    rpcExecutor.stop();
  }

  @Test
  public void testQueueCountMoreThanHandlerCount() throws Exception {
    float readShare = 0.5f;
    double scanShare = 0.5;
    int handlerCount = 256;
    int numQueues = 1000;
    int maxQueueLength = 100;
    RWQueueRpcExecutor rpcExecutor =
        checkHandlersCount(readShare, scanShare, handlerCount, numQueues, maxQueueLength);
    rpcExecutor.dispatch(createMockCall(scanLimitOneRow));
    checkQueueCounters(rpcExecutor, 0, 1, 0);
    rpcExecutor.dispatch(createMockCall(scanWithFilter));
    checkQueueCounters(rpcExecutor, 0, 1, 1);
    rpcExecutor.dispatch(createMockCall(scanWithoutFilter));
    checkQueueCounters(rpcExecutor, 0, 2, 1);
    rpcExecutor.dispatch(createMockCall(mutateRequest));
    checkQueueCounters(rpcExecutor, 1, 2, 1);
    rpcExecutor.stop();
  }

  @Test
  public void testZeroScanHandlerCount() throws Exception {
    float readShare = 0.5f;
    double scanShare = 0;
    int handlerCount = 256;
    int numQueues = 1000;
    int maxQueueLength = 100;
    RWQueueRpcExecutor rpcExecutor =
        checkHandlersCount(readShare, scanShare, handlerCount, numQueues, maxQueueLength);
    rpcExecutor.dispatch(createMockCall(scanLimitOneRow));
    checkQueueCounters(rpcExecutor, 0, 1, 0);
    rpcExecutor.dispatch(createMockCall(scanWithFilter));
    checkQueueCounters(rpcExecutor, 0, 2, 0);
    rpcExecutor.dispatch(createMockCall(scanWithoutFilter));
    checkQueueCounters(rpcExecutor, 0, 3, 0);
    rpcExecutor.dispatch(createMockCall(mutateRequest));
    checkQueueCounters(rpcExecutor, 1, 3, 0);
    rpcExecutor.stop();
  }

  private RWQueueRpcExecutor checkHandlersCount(float readShare, double scanShare, int handlerCount,
      int numQueues, int maxQueueLength) {
    conf.setDouble(SimpleRpcScheduler.CALL_QUEUE_SCAN_SHARE_CONF_KEY, scanShare);
    RWQueueRpcExecutor rwQueueRpcExecutor = new RWQueueRpcExecutor("test", handlerCount, numQueues,
        readShare, maxQueueLength, conf, null);
    rwQueueRpcExecutor.start(1000);
    handlerCount = Math.max(handlerCount, numQueues);
    int scanHandlersCount = (int) (handlerCount * readShare * scanShare);
    int readHandlersCount = (int) (handlerCount * readShare - scanHandlersCount);
    int writeHandlersCount = handlerCount - readHandlersCount - scanHandlersCount;
    // check handlers count
    assertEquals(writeHandlersCount, rwQueueRpcExecutor.getWriteHandlersCount());
    assertEquals(readHandlersCount, rwQueueRpcExecutor.getReadHandlersCount());
    assertEquals(scanHandlersCount, rwQueueRpcExecutor.getScanHandlersCount());
    // check queue count
    int scanQueuesNum = (int) (numQueues * readShare * scanShare);
    int readQueuesNum = (int) (numQueues * readShare - scanQueuesNum);
    int writeQueuesNum = numQueues - readQueuesNum - scanQueuesNum;
    assertEquals(writeQueuesNum, rwQueueRpcExecutor.getNumWriteQueues());
    assertEquals(readQueuesNum, rwQueueRpcExecutor.getNumReadQueues());
    assertEquals(scanQueuesNum, rwQueueRpcExecutor.getNumScanQueues());
    // check queue balancer
    if (scanHandlersCount == 0) {
      assertTrue(rwQueueRpcExecutor.getScanBalancer() == null);
    } else {
      assertTrue(rwQueueRpcExecutor.getScanBalancer() != null);
    }
    return rwQueueRpcExecutor;
  }

  private void checkQueueCounters(RWQueueRpcExecutor rpcExecutor, int expectedWriteCount,
      int expectedReadCount, int expectedScanCount) {
    List<QueueCounter> queueCounters = rpcExecutor.getQueueCounters();
    assertEquals(3, queueCounters.size());
    assertEquals(expectedReadCount, queueCounters.get(0).getIncomeRequestCount());
    assertEquals(expectedWriteCount, queueCounters.get(1).getIncomeRequestCount());
    assertEquals(expectedScanCount, queueCounters.get(2).getIncomeRequestCount());
  }

  private CallRunner createMockCall(Message message) {
    RpcServer.Call call = mock(RpcServer.Call.class);
    when(call.getParam()).thenReturn(message);
    CallRunner task = mock(CallRunner.class);
    when(task.getCall()).thenReturn(call);
    return task;
  }
}
