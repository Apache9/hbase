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

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.RandomRowFilter;
import org.apache.hadoop.hbase.ipc.MetaRWQueueRpcExecutor.HandlerAndQueue;
import org.apache.hadoop.hbase.protobuf.RequestConverter;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.GetRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutateRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ScanRequest;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.QueueCounter;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.protobuf.Message;

@Category(SmallTests.class)
public class TestMetaRWQueueRpcExecutor {

  private Configuration conf;
  private byte[] regionName = Bytes.toBytes("region");
  private String serverUser = "hbase_srv";
  private ScanRequest scanFromServer;
  private GetRequest readFromServer;
  private ScanRequest scanFromClient;
  private MutateRequest mutateRequest;

  @Before
  public void setUp() throws IOException {
    conf = HBaseConfiguration.create();
    conf.set("hbase.regionserver.kerberos.principal", serverUser);
    scanFromServer = RequestConverter.buildScanRequest(regionName, new Scan(), 1, true);
    readFromServer = RequestConverter.buildGetRequest(regionName, new Get(regionName));
    scanFromClient = RequestConverter.buildScanRequest(regionName,
      new Scan().setFilter(new RandomRowFilter(10)), 1, true);
    mutateRequest = RequestConverter.buildMutateRequest(regionName, new Put(regionName));
  }

  @Test
  public void testHandlerCountMoreThanQueueCount() throws Exception {
    int handlerCount = 64;
    int numQueues = 8;
    int maxQueueLength = 1024;
    MetaRWQueueRpcExecutor rpcExecutor =
        checkHandlersCount(handlerCount, numQueues, maxQueueLength, 8, 1, 24, 3, 32, 4);
    checkDispatch(rpcExecutor);
    rpcExecutor.stop();
  }

  @Test
  public void testQueueCountMoreThanHandlerCount() throws Exception {
    int handlerCount = 64;
    int numQueues = 128;
    int maxQueueLength = 1024;
    MetaRWQueueRpcExecutor rpcExecutor =
        checkHandlersCount(handlerCount, numQueues, maxQueueLength, 16, 16, 48, 48, 64, 64);
    checkDispatch(rpcExecutor);
    rpcExecutor.stop();
  }

  @Test
  public void testHandlerCount() throws Exception {
    int handlerCount = 5;
    int numQueues = 5;
    int maxQueueLength = 1024;
    MetaRWQueueRpcExecutor rpcExecutor =
        checkHandlersCount(handlerCount, numQueues, maxQueueLength, 2, 2, 1, 1, 2, 2);
    checkDispatch(rpcExecutor);
    rpcExecutor.stop();
  }

  private MetaRWQueueRpcExecutor checkHandlersCount(int handlerCount, int numQueues,
      int maxQueueLength, int expectedWriteHandlerCount, int expectedWriteQueueCount,
      int expectedServerReadHandlerCount, int expectedServerReadQueueCount,
      int expectedClientReadHandlerCount, int expectedClientReadQueueCount) {
    MetaRWQueueRpcExecutor rwQueueRpcExecutor = new MetaRWQueueRpcExecutor("test", handlerCount, numQueues,
         maxQueueLength, conf, null);
    rwQueueRpcExecutor.start(1000);
    HandlerAndQueue writeHandlerAndQueue = rwQueueRpcExecutor.getWriteHandlerAndQueue();
    HandlerAndQueue serverReadHandlerAndQueue = rwQueueRpcExecutor.getServerReadHandlerAndQueue();
    HandlerAndQueue clientReadHandlerAndQueue = rwQueueRpcExecutor.getClientReadHandlerAndQueue();
    // check handlers count
    assertEquals(expectedWriteHandlerCount, writeHandlerAndQueue.getHandlerCount());
    assertEquals(expectedServerReadHandlerCount, serverReadHandlerAndQueue.getHandlerCount());
    assertEquals(expectedClientReadHandlerCount, clientReadHandlerAndQueue.getHandlerCount());
    // check queue count
    assertEquals(expectedWriteQueueCount, writeHandlerAndQueue.getQueues().size());
    assertEquals(expectedServerReadQueueCount, serverReadHandlerAndQueue.getQueues().size());
    assertEquals(expectedClientReadQueueCount, clientReadHandlerAndQueue.getQueues().size());
    return rwQueueRpcExecutor;
  }

  private void checkDispatch(MetaRWQueueRpcExecutor rpcExecutor) throws Exception {
    rpcExecutor.dispatch(createMockCall(scanFromServer, true));
    checkQueueCounters(rpcExecutor, 0, 1, 0);
    rpcExecutor.dispatch(createMockCall(scanFromClient, true));
    checkQueueCounters(rpcExecutor, 0, 2, 0);
    rpcExecutor.dispatch(createMockCall(readFromServer, true));
    checkQueueCounters(rpcExecutor, 0, 3, 0);
    rpcExecutor.dispatch(createMockCall(mutateRequest, true));
    checkQueueCounters(rpcExecutor, 1, 3, 0);
  }

  private void checkQueueCounters(MetaRWQueueRpcExecutor rpcExecutor, int expectedWriteCount,
      int expectedServerReadCount, int expectedClientReadCount) {
    List<QueueCounter> queueCounters = rpcExecutor.getQueueCounters();
    assertEquals(3, queueCounters.size());
    assertEquals(expectedWriteCount, queueCounters.get(0).getIncomeRequestCount());
    assertEquals(expectedServerReadCount, queueCounters.get(1).getIncomeRequestCount());
    assertEquals(expectedClientReadCount, queueCounters.get(2).getIncomeRequestCount());
  }

  private CallRunner createMockCall(Message message, boolean server) {
    RpcServer.Call call = mock(RpcServer.Call.class);
    when(call.getRemoteUser()).thenReturn(mock(UserGroupInformation.class));
    if (server) {
      when(call.getRemoteUser().getShortUserName()).thenReturn(serverUser);
    } else {
      when(call.getRemoteUser().getShortUserName()).thenReturn("test");
    }
    when(call.getParam()).thenReturn(message);
    CallRunner task = mock(CallRunner.class);
    when(task.getCall()).thenReturn(call);
    return task;
  }
}
