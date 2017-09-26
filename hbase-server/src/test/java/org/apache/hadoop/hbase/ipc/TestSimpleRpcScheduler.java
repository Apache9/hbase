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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.protobuf.Message;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.ipc.RpcServer.Call;
import org.apache.hadoop.hbase.protobuf.RequestConverter;
import org.apache.hadoop.hbase.protobuf.generated.RPCProtos;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Category(SmallTests.class)
public class TestSimpleRpcScheduler {

  private final RpcScheduler.Context CONTEXT = new RpcScheduler.Context() {
    @Override
    public InetSocketAddress getListenerAddress() {
      return InetSocketAddress.createUnresolved("127.0.0.1", 1000);
    }
  };
  private Configuration conf;

  @Before
  public void setUp() {
    conf = HBaseConfiguration.create();
  }

  @Test
  public void testBasic() throws IOException, InterruptedException {
    PriorityFunction qosFunction = mock(PriorityFunction.class);
    RpcScheduler scheduler = new SimpleRpcScheduler(
      conf, 10, 0, 0, qosFunction, null, 0);
    scheduler.init(CONTEXT);
    scheduler.start();
    CallRunner task = createMockTask();
    scheduler.dispatch(task);
    verify(task, timeout(1000)).run();
    scheduler.stop();
  }

  @Test
  public void testHandlerIsolation() throws IOException, InterruptedException {
    CallRunner generalTask = createMockTask();
    CallRunner priorityTask = createMockTask();
    CallRunner replicationTask = createMockTask();
    List<CallRunner> tasks = ImmutableList.of(
        generalTask,
        priorityTask,
        replicationTask);
    Map<CallRunner, Integer> qos = ImmutableMap.of(
        generalTask, 0,
        priorityTask, HConstants.HIGH_QOS + 1,
        replicationTask, HConstants.REPLICATION_QOS);
    PriorityFunction qosFunction = mock(PriorityFunction.class);
    final Map<CallRunner, Thread> handlerThreads = Maps.newHashMap();
    final CountDownLatch countDownLatch = new CountDownLatch(tasks.size());
    Answer<Void> answerToRun = new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocationOnMock) throws Throwable {
        synchronized (handlerThreads) {
          handlerThreads.put(
              (CallRunner) invocationOnMock.getMock(),
              Thread.currentThread());
        }
        countDownLatch.countDown();
        return null;
      }
    };
    for (CallRunner task : tasks) {
      doAnswer(answerToRun).when(task).run();
    }

    RpcScheduler scheduler = new SimpleRpcScheduler(
      conf, 1, 1 ,1, qosFunction, null, HConstants.HIGH_QOS);
    scheduler.init(CONTEXT);
    scheduler.start();
    for (CallRunner task : tasks) {
      when(qosFunction.getPriority((RPCProtos.RequestHeader) anyObject(), (Message) anyObject()))
          .thenReturn(qos.get(task));
      scheduler.dispatch(task);
    }
    for (CallRunner task : tasks) {
      verify(task, timeout(1000)).run();
    }
    scheduler.stop();

    // Tests that these requests are handled by three distinct threads.
    countDownLatch.await();
    assertEquals(3, ImmutableSet.copyOf(handlerThreads.values()).size());
  }

  private CallRunner createMockTask() {
    Call call = mock(Call.class);
    CallRunner task = mock(CallRunner.class);
    when(task.getCall()).thenReturn(call);
    return task;
  }

  @Test
  public void testCoDelScheduling() throws Exception {
    Configuration schedConf = HBaseConfiguration.create();

    schedConf.set(SimpleRpcScheduler.CALL_QUEUE_TYPE_CONF_KEY,
      SimpleRpcScheduler.CALL_QUEUE_TYPE_CODEL_CONF_VALUE);

    PriorityFunction priority = mock(PriorityFunction.class);
    when(priority.getPriority(any(RPCProtos.RequestHeader.class), any(Message.class))).thenReturn(
      HConstants.NORMAL_QOS);
    SimpleRpcScheduler scheduler = new SimpleRpcScheduler(schedConf, 1, 1, 1, priority,
      null, HConstants.QOS_THRESHOLD);
    try {
      scheduler.start();

      // calls faster than min delay
      for (int i = 0; i < 100; i++) {
        CallRunner cr = getMockedCallRunner();
        Thread.sleep(5);
        scheduler.dispatch(cr);
      }
      Thread.sleep(100); // make sure fast calls are handled
      assertEquals("None of these calls should have been discarded", 0,
        scheduler.getNumGeneralCallsDropped());

      // calls slower than min delay, but not individually slow enough to be dropped
      for (int i = 0; i < 20; i++) {
        CallRunner cr = getMockedCallRunner();
        Thread.sleep(6);
        scheduler.dispatch(cr);
      }

      Thread.sleep(100); // make sure somewhat slow calls are handled
      assertEquals("None of these calls should have been discarded", 0,
        scheduler.getNumGeneralCallsDropped());

      // now slow calls and the ones to be dropped
      for (int i = 0; i < 20; i++) {
        CallRunner cr = getMockedCallRunner();
        Thread.sleep(12);
        scheduler.dispatch(cr);
      }

      Thread.sleep(100); // make sure somewhat slow calls are handled
      assertTrue("There should have been at least 12 calls dropped",
        scheduler.getNumGeneralCallsDropped() > 12);
    } finally {
      scheduler.stop();
    }
  }

  private CallRunner getMockedCallRunner() throws IOException {
    CallRunner putCallTask = mock(CallRunner.class);
    RpcServer.Call putCall = mock(RpcServer.Call.class);
    putCall.param = RequestConverter.buildMutateRequest(Bytes.toBytes("abc"),
      new Put(Bytes.toBytes("row")));
    RPCProtos.RequestHeader putHead = RPCProtos.RequestHeader.newBuilder().setMethodName("mutate")
        .build();
    when(putCallTask.getCall()).thenReturn(putCall);
    when(putCall.getHeader()).thenReturn(putHead);
    putCall.timestamp = System.currentTimeMillis();
    return putCallTask;
  }
}
