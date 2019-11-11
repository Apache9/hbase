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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.util.QueueCounter;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.annotations.VisibleForTesting;

/**
 * RPC Executor that uses different queues for writes, reads from server and reads from client for
 * meta table. Each handler has its own queue and there is no stealing.
 */
@InterfaceAudience.Private
public class MetaRWQueueRpcExecutor extends RpcExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(MetaRWQueueRpcExecutor.class);

  // The handlers and queues nums ratio of writes, reads from server, reads from client is
  // 1:3:4 by default.
  public static final String META_CALL_QUEUE_SERVER_READ_RATIO_CONF_KEY =
      "hbase.ipc.server.meta.callqueue.server.read.ratio";
  public static final float META_CALL_QUEUE_SERVER_READ_RATIO_CONF_VALUE = 0.375f;
  public static final String META_CALL_QUEUE_CLIENT_READ_RATIO_CONF_KEY =
      "hbase.ipc.server.meta.callqueue.client.read.ratio";
  public static final float META_CALL_QUEUE_CLIENT_READ_RATIO_CONF_VALUE = 0.5f;
  // 1 queue is handled by 8 handlers by default
  public static final String META_CALL_QUEUE_HANDLER_FACTOR_CONF_KEY =
      "hbase.ipc.server.meta.callqueue.handler.factor";
  public static final float META_CALL_QUEUE_HANDLER_FACTOR_CONF_VALUE = 0.125f;

  private final HandlerAndQueue writeHandlerAndQueue;
  private final HandlerAndQueue serverReadHandlerAndQueue;
  private final HandlerAndQueue clientReadHandlerAndQueue;
  private final String rsKerberos;

  public MetaRWQueueRpcExecutor(final String name, int handlerCount, final int numQueues,
      final int maxQueueLength, final Configuration conf, final Abortable abortable) {
    super(name, Math.max(handlerCount, numQueues), conf, abortable);
    handlerCount = Math.max(handlerCount, numQueues);
    float serverReadRatio = conf.getFloat(META_CALL_QUEUE_SERVER_READ_RATIO_CONF_KEY,
      META_CALL_QUEUE_SERVER_READ_RATIO_CONF_VALUE);
    float clientReadRatio = conf.getFloat(META_CALL_QUEUE_CLIENT_READ_RATIO_CONF_KEY,
      META_CALL_QUEUE_CLIENT_READ_RATIO_CONF_VALUE);
    int serverReadHandlerCount = calculate(handlerCount, serverReadRatio);
    int numServerReadQueues = calculate(numQueues, serverReadRatio);
    int clientReadHandlerCount = calculate(handlerCount, clientReadRatio);
    int numClientReadQueues = calculate(numQueues, clientReadRatio);
    int writeHandlerCount =
        Math.max(1, handlerCount - serverReadHandlerCount - clientReadHandlerCount);
    int numWriteQueues = Math.max(1, numQueues - numServerReadQueues - numClientReadQueues);
    LOG.info(name + " writeQueues=" + numWriteQueues + " writeHandlers=" + writeHandlerCount
        + " serverReadQueues=" + numServerReadQueues + " serverReadHandlers="
        + serverReadHandlerCount + " clientReadQueues=" + numClientReadQueues
        + " clientReadHandlers=" + clientReadHandlerCount);

    this.writeHandlerAndQueue =
        new HandlerAndQueue("write", writeHandlerCount, numWriteQueues, maxQueueLength);
    this.serverReadHandlerAndQueue = new HandlerAndQueue("serverRead", serverReadHandlerCount,
        numServerReadQueues, maxQueueLength);
    this.clientReadHandlerAndQueue = new HandlerAndQueue("clientRead", clientReadHandlerCount,
        numClientReadQueues, maxQueueLength);
    this.rsKerberos =
        conf.get(HConstants.REGIONSERVER_KERBEROS, "").replace("/hadoop@XIAOMI.HADOOP", "");
  }

  private int calculate(int count, float ratio) {
    return Math.max(1, (int) (count * ratio));
  }

  @Override
  protected void startHandlers(final int port) {
    startHandlers(writeHandlerAndQueue, port);
    startHandlers(serverReadHandlerAndQueue, port);
    startHandlers(clientReadHandlerAndQueue, port);
  }

  private void startHandlers(HandlerAndQueue handlerAndQueue, int port) {
    startHandlers("." + handlerAndQueue.getName(), handlerAndQueue.getHandlerCount(),
      handlerAndQueue.getQueues(), 0, handlerAndQueue.getQueues().size(), port,
      handlerAndQueue.getActiveHandlerCount());
  }

  @Override
  public void dispatch(final CallRunner callTask) throws IOException, InterruptedException {
    RpcServer.Call call = callTask.getCall();
    HandlerAndQueue handlerAndQueue;
    if (isWriteRequest(call.getHeader(), call.getParam())) {
      handlerAndQueue = writeHandlerAndQueue;
    } else if (isReadRequestFromServer(call.getRemoteUser())) {
      handlerAndQueue = serverReadHandlerAndQueue;
    } else {
      handlerAndQueue = clientReadHandlerAndQueue;
    }
    QueueCounter queueCounter = handlerAndQueue.getQueueCounter();
    queueCounter.incIncomeRequestCount();
    BlockingQueue<CallRunner> queue = handlerAndQueue.getNextQueue();
    if (!queue.offer(callTask)) {
      callTask.resetCallQueueSize();
      String queueType = handlerAndQueue.getName();
      callTask.doRespond(null, new IOException(),
        "IPC server unable to " + queueType + " call method");
      queueCounter.setQueueFull(true);
      queueCounter.incRejectedRequestCount();
    } else {
      queueCounter.setQueueFull(false);
    }
  }

  @Override
  protected void startQueueFullErrorLogger() {
    Thread t = new Thread(() -> {
      long lastRejectedWriteCount = 0;
      long lastRejectedServerReadCount = 0;
      long lastRejectedClientReadCount = 0;
      while (true) {
        try {
          lastRejectedWriteCount = logQueueFullError(writeHandlerAndQueue, lastRejectedWriteCount);
          lastRejectedServerReadCount =
              logQueueFullError(serverReadHandlerAndQueue, lastRejectedServerReadCount);
          lastRejectedClientReadCount =
              logQueueFullError(clientReadHandlerAndQueue, lastRejectedClientReadCount);
          TimeUnit.SECONDS.sleep(logInterval);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    });
    t.setDaemon(true);
    t.setName("MetaRWQueue-QueueFullLogger");
    t.start();
  }

  private long logQueueFullError(HandlerAndQueue handlerAndQueue, long lastRejectedCount) {
    return logQueueFullError(handlerAndQueue.getQueueCounter(), lastRejectedCount,
      handlerAndQueue.getName());
  }

  private boolean isReadRequestFromServer(final UserGroupInformation user) {
    if (user != null && user.getShortUserName() != null) {
      if (user.getShortUserName().equals(rsKerberos)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public int getQueueLength() {
    return getWriteQueueLength() + getReadQueueLength();
  }

  @Override
  public int getWriteQueueLength() {
    return writeHandlerAndQueue.getQueueLength();
  }

  @Override
  public int getReadQueueLength() {
    return serverReadHandlerAndQueue.getQueueLength() + clientReadHandlerAndQueue.getQueueLength();
  }

  @Override
  public int getActiveHandlerCount() {
    return getActiveReadHandlerCount() + getActiveWriteHandlerCount();
  }

  @Override
  public int getActiveWriteHandlerCount() {
    return writeHandlerAndQueue.getActiveHandlerCount().get();
  }

  @Override
  public int getActiveReadHandlerCount() {
    return serverReadHandlerAndQueue.getActiveHandlerCount().get()
        + clientReadHandlerAndQueue.getActiveHandlerCount().get();
  }

  @Override
  public List<QueueCounter> getQueueCounters() {
    return Arrays.asList(writeHandlerAndQueue.getQueueCounter(),
      serverReadHandlerAndQueue.getQueueCounter(), clientReadHandlerAndQueue.getQueueCounter());
  }

  @Override
  protected List<BlockingQueue<CallRunner>> getQueues() {
    List<BlockingQueue<CallRunner>> queues = new ArrayList<>();
    queues.addAll(writeHandlerAndQueue.getQueues());
    queues.addAll(serverReadHandlerAndQueue.getQueues());
    queues.addAll(clientReadHandlerAndQueue.getQueues());
    return queues;
  }

  @VisibleForTesting
  protected HandlerAndQueue getWriteHandlerAndQueue() {
    return writeHandlerAndQueue;
  }

  @VisibleForTesting
  protected HandlerAndQueue getServerReadHandlerAndQueue() {
    return serverReadHandlerAndQueue;
  }

  @VisibleForTesting
  protected HandlerAndQueue getClientReadHandlerAndQueue() {
    return clientReadHandlerAndQueue;
  }

  @VisibleForTesting
  protected class HandlerAndQueue {
    private final String name;
    private final int handlerCount;
    private final AtomicInteger activeHandlerCount = new AtomicInteger(0);
    private final List<BlockingQueue<CallRunner>> queues;
    private final QueueCounter queueCounter;
    private final QueueBalancer balancer;

    private HandlerAndQueue(String name, int handlerCount, int queueNum, int maxQueueLength) {
      this.balancer = getBalancer(queueNum);
      this.queueCounter = new QueueCounter(name);
      this.handlerCount = handlerCount;
      this.name = name;
      queues = new ArrayList<>(queueNum);
      for (int i = 0; i < queueNum; ++i) {
        queues.add(new LinkedBlockingQueue<>(maxQueueLength));
      }
    }

    String getName() {
      return name;
    }

    int getHandlerCount() {
      return handlerCount;
    }

    AtomicInteger getActiveHandlerCount() {
      return activeHandlerCount;
    }

    List<BlockingQueue<CallRunner>> getQueues() {
      return queues;
    }

    QueueCounter getQueueCounter() {
      return queueCounter;
    }

    private BlockingQueue<CallRunner> getNextQueue() {
      return queues.get(balancer.getNextQueue());
    }

    int getQueueLength() {
      int length = 0;
      for (int i = 0; i < queues.size(); i++) {
        length += queues.get(i).size();
      }
      return length;
    }
  }
}
