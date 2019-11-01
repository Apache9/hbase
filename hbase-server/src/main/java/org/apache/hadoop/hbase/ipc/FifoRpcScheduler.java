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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.DaemonThreadFactory;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.util.QueueCounter;

/**
 * A very simple {@code }RpcScheduler} that serves incoming requests in order.
 *
 * This can be used for HMaster, where no prioritization is needed.
 */
public class FifoRpcScheduler extends RpcScheduler {

  protected int handlerCount;
  protected final int maxQueueLength;
  protected ThreadPoolExecutor executor;
  protected final QueueCounter queueCounter;

  public FifoRpcScheduler(Configuration conf, int handlerCount) {
    this.handlerCount = handlerCount;
    this.maxQueueLength = conf.getInt("hbase.ipc.server.max.callqueue.length",
      conf.getInt("ipc.server.max.callqueue.length",
        handlerCount * RpcServer.DEFAULT_MAX_CALLQUEUE_LENGTH_PER_HANDLER));
    this.queueCounter = new QueueCounter("Fifo");
  }

  @Override
  public void init(Context context) {
    // no-op
  }

  @Override
  public void start() {
    this.executor = new ThreadPoolExecutor(
        handlerCount,
        handlerCount,
        60,
        TimeUnit.SECONDS,
        new ArrayBlockingQueue<Runnable>(maxQueueLength),
        new DaemonThreadFactory("FifoRpcScheduler.handler"),
        new ThreadPoolExecutor.CallerRunsPolicy());
  }

  @Override
  public void stop() {
    this.executor.shutdown();
  }

  @Override
  public void dispatch(final CallRunner task) throws IOException, InterruptedException {
    try {
      queueCounter.incIncomeRequestCount();
      executor.submit(new Runnable() {
        @Override
        public void run() {
          task.run();
        }
      });
      queueCounter.setQueueFull(false);
    } catch (RejectedExecutionException e) {
      queueCounter.setQueueFull(true);
      queueCounter.incRejectedRequestCount();
      throw e;
    }
  }

  @Override
  public int getGeneralQueueLength() {
    return executor.getQueue().size();
  }

  @Override
  public int getPriorityQueueLength() {
    return 0;
  }

  @Override
  public int getReplicationQueueLength() {
    return 0;
  }

  @Override
  public int getActiveRpcHandlerCount() {
    return executor.getActiveCount();
  }

  @Override
  public int getWriteQueueLength() {
    return 0;
  }

  @Override
  public int getReadQueueLength() {
    return 0;
  }

  @Override
  public int getScanQueueLength() {
    return 0;
  }

  @Override
  public int getActiveWriteRpcHandlerCount() {
    return 0;
  }

  @Override
  public int getActiveReadRpcHandlerCount() {
    return 0;
  }

  @Override
  public int getActiveScanRpcHandlerCount() {
    return 0;
  }

  @Override
  public List<QueueCounter> getQueueCounters() {
    return Collections.singletonList(queueCounter);
  }

  @Override
  public long getNumGeneralCallsDropped() {
    return 0;
  }

  @Override
  public long getNumLifoModeSwitches() {
    return 0;
  }
}
