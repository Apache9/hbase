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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.DaemonThreadFactory;

/**
 * Special rpc scheduler only used for master.
 */
public class MasterFifoRpcScheduler extends FifoRpcScheduler {

  private ThreadPoolExecutor rsReportExecutor;

  public MasterFifoRpcScheduler(Configuration conf, int handlerCount) {
    super(conf, handlerCount);
    this.handlerCount = Math.max(1, this.handlerCount / 2);
  }

  @Override
  public void start() {
    this.executor = new ThreadPoolExecutor(handlerCount, handlerCount, 60, TimeUnit.SECONDS,
        new ArrayBlockingQueue<Runnable>(maxQueueLength),
        new DaemonThreadFactory("MasterFifoRpcScheduler.call.handler"),
        new ThreadPoolExecutor.CallerRunsPolicy());
    this.rsReportExecutor = new ThreadPoolExecutor(handlerCount, handlerCount, 60, TimeUnit.SECONDS,
        new ArrayBlockingQueue<Runnable>(maxQueueLength),
        new DaemonThreadFactory("MasterFifoRpcScheduler.RSReport.handler"),
        new ThreadPoolExecutor.CallerRunsPolicy());
  }

  @Override
  public void stop() {
    this.executor.shutdown();
    this.rsReportExecutor.shutdown();
  }

  @Override
  public void dispatch(final CallRunner task) throws IOException, InterruptedException {
    try {
      queueCounter.incIncomeRequestCount();
      RpcServer.Call call = task.getCall();
      if (rsReportExecutor != null
          && call.getHeader().getMethodName().equals("RegionServerReport")) {
        rsReportExecutor.submit(() -> task.run());
      } else {
        executor.submit(() -> task.run());
      }
      queueCounter.setQueueFull(false);
    } catch (RejectedExecutionException e) {
      queueCounter.setQueueFull(true);
      queueCounter.incRejectedRequestCount();
      throw e;
    }
  }

  @Override
  public int getGeneralQueueLength() {
    return executor.getQueue().size() + rsReportExecutor.getQueue().size();
  }

  @Override
  public int getActiveRpcHandlerCount() {
    return executor.getActiveCount() + rsReportExecutor.getActiveCount();
  }
}
