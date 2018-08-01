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

import java.util.HashMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.DaemonThreadFactory;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Special rpc scheduler only used for master.
 */
@InterfaceAudience.Private
public class MasterFifoRpcScheduler extends FifoRpcScheduler {

  private ThreadPoolExecutor rsReportExecutor;

  public MasterFifoRpcScheduler(Configuration conf, int handlerCount) {
    super(conf, handlerCount);
    this.handlerCount = Math.max(1, this.handlerCount / 2);
  }

  @Override
  public void start() {
    this.executor = new ThreadPoolExecutor(handlerCount, handlerCount, 60,
        TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(maxQueueLength),
        new DaemonThreadFactory("MasterFifoRpcScheduler.call.handler"),
        new ThreadPoolExecutor.CallerRunsPolicy());
    this.rsReportExecutor = new ThreadPoolExecutor(handlerCount, handlerCount, 60,
        TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(maxQueueLength),
        new DaemonThreadFactory("MasterFifoRpcScheduler.RSReport.handler"),
        new ThreadPoolExecutor.CallerRunsPolicy());
  }

  @Override
  public void stop() {
    this.executor.shutdown();
    this.rsReportExecutor.shutdown();
  }

  @Override
  protected void executeRpcCall(final CallRunner task) {
    String method = getCallMethod(task);
    if (rsReportExecutor != null && method != null && method.equals("RegionServerReport")) {
      rsReportExecutor.execute(getFifoCallRunner(task));
    } else {
      executor.execute(getFifoCallRunner(task));
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

  @Override
  public CallQueueInfo getCallQueueInfo() {
    String queueName = "Master Fifo Queue";

    HashMap<String, Long> methodCount = new HashMap<>();
    HashMap<String, Long> methodSize = new HashMap<>();

    CallQueueInfo callQueueInfo = new CallQueueInfo();
    callQueueInfo.setCallMethodCount(queueName, methodCount);
    callQueueInfo.setCallMethodSize(queueName, methodSize);

    getCallQueueMethodInfo(executor.getQueue(), methodCount, methodSize);
    getCallQueueMethodInfo(rsReportExecutor.getQueue(), methodCount, methodSize);

    return callQueueInfo;
  }
}
