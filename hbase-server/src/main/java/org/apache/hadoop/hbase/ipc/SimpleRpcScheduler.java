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
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.util.QueueCounter;

/**
 * A scheduler that maintains isolated handler pools for general, high-priority and replication
 * requests.
 */
@InterfaceAudience.LimitedPrivate({HBaseInterfaceAudience.COPROC, HBaseInterfaceAudience.PHOENIX})
@InterfaceStability.Evolving
public class SimpleRpcScheduler extends RpcScheduler {
  public static final Log LOG = LogFactory.getLog(SimpleRpcScheduler.class);

  public static final String CALL_QUEUE_TYPE_CONF_KEY = "hbase.ipc.server.callqueue.type";
  public static final String CALL_QUEUE_TYPE_CODEL_CONF_VALUE = "codel";
  public static final String CALL_QUEUE_TYPE_FIFO_CONF_VALUE = "fifo";

  private static final String META_CALL_QUEUE_TYPE_CONF_KEY =
      "hbase.ipc.server.meta.callqueue.type";
  private static final String META_CALL_QUEUE_TYPE_CONF_VALUE = "MetaRWQ";

  public static final String CALL_QUEUE_READ_SHARE_CONF_KEY =
    "hbase.ipc.server.callqueue.read.share";
  public static final String CALL_QUEUE_SCAN_SHARE_CONF_KEY =
      "hbase.ipc.server.callqueue.scan.ratio";
  public static final String CALL_QUEUE_HANDLER_FACTOR_CONF_KEY =
    "hbase.ipc.server.callqueue.handler.factor";
  public static final String CALL_QUEUE_MAX_LENGTH_CONF_KEY =
    "hbase.ipc.server.max.callqueue.length";

  // These 3 are only used by Codel executor
  public static final String CALL_QUEUE_CODEL_TARGET_DELAY = "hbase.ipc.server.callqueue.codel.target.delay";
  public static final String CALL_QUEUE_CODEL_INTERVAL = "hbase.ipc.server.callqueue.codel.interval";
  public static final String CALL_QUEUE_CODEL_LIFO_THRESHOLD = "hbase.ipc.server.callqueue.codel.lifo.threshold";

  public static final int CALL_QUEUE_CODEL_DEFAULT_TARGET_DELAY = 5;
  public static final int CALL_QUEUE_CODEL_DEFAULT_INTERVAL = 100;
  public static final double CALL_QUEUE_CODEL_DEFAULT_LIFO_THRESHOLD = 0.8;

  private AtomicLong numGeneralCallsDropped = new AtomicLong();
  private AtomicLong numLifoModeSwitches = new AtomicLong();

  private int port;
  private final PriorityFunction priority;
  private final RpcExecutor callExecutor;
  private final RpcExecutor priorityExecutor;
  private final RpcExecutor replicationExecutor;

  /** What level a high priority call is at. */
  private final int highPriorityLevel;
  
  private final Abortable abortable;

  /**
   * @param conf
   * @param handlerCount the number of handler threads that will be used to process calls
   * @param priorityHandlerCount How many threads for priority handling.
   * @param replicationHandlerCount How many threads for replication handling.
   * @param highPriorityLevel
   * @param priority Function to extract request priority.
   */
  public SimpleRpcScheduler(
      Configuration conf,
      int handlerCount,
      int priorityHandlerCount,
      int replicationHandlerCount,
      PriorityFunction priority,
      Abortable abortable,
      int highPriorityLevel) {
    int maxQueueLength = conf.getInt(CALL_QUEUE_MAX_LENGTH_CONF_KEY,
      conf.getInt("ipc.server.max.callqueue.length",
        handlerCount * RpcServer.DEFAULT_MAX_CALLQUEUE_LENGTH_PER_HANDLER));
    this.priority = priority;
    this.highPriorityLevel = highPriorityLevel;
    this.abortable = abortable;

    String callQueueType = conf.get(CALL_QUEUE_TYPE_CONF_KEY, CALL_QUEUE_TYPE_FIFO_CONF_VALUE);
    float callqReadShare = conf.getFloat(CALL_QUEUE_READ_SHARE_CONF_KEY,
      conf.getFloat("ipc.server.callqueue.read.share", 0));

    int codelTargetDelay = conf.getInt(CALL_QUEUE_CODEL_TARGET_DELAY,
      CALL_QUEUE_CODEL_DEFAULT_TARGET_DELAY);
    int codelInterval = conf.getInt(CALL_QUEUE_CODEL_INTERVAL, CALL_QUEUE_CODEL_DEFAULT_INTERVAL);
    double codelLifoThreshold = conf.getDouble(CALL_QUEUE_CODEL_LIFO_THRESHOLD,
      CALL_QUEUE_CODEL_DEFAULT_LIFO_THRESHOLD);

    float callQueuesHandlersFactor = conf.getFloat(CALL_QUEUE_HANDLER_FACTOR_CONF_KEY,
      conf.getFloat("ipc.server.callqueue.handler.factor", 0));
    int numCallQueues = Math.max(1, (int)Math.round(handlerCount * callQueuesHandlersFactor));

    StringBuilder sb = new StringBuilder("init rpc executor, handlerCount=").append(handlerCount)
        .append(", callqReadShare=").append(callqReadShare).append(", numCallQueues=")
        .append(numCallQueues).append(", callQueuesHandlersFactor=")
        .append(callQueuesHandlersFactor).append(", maxQueueLength=").append(maxQueueLength)
        .append(", callQueueType=").append(callQueueType);
    if (isCodelQueueType(callQueueType)) {
      sb.append(", codelTargetDelay=").append(codelTargetDelay).append(", codelInterval=")
          .append(codelInterval).append(", codelLifoThreshold=").append(codelLifoThreshold);
    }
    LOG.info(sb.toString());

    if (numCallQueues > 1 && callqReadShare > 0) {
      // multiple read/write queues
      if (isCodelQueueType(callQueueType)) {
        Object[] callQueueInitArgs = { maxQueueLength, codelTargetDelay, codelInterval,
            codelLifoThreshold, numGeneralCallsDropped, numLifoModeSwitches };
        callExecutor = new RWQueueRpcExecutor("CodelRWQ.default", handlerCount, numCallQueues,
            callqReadShare, conf, abortable, AdaptiveLifoCoDelCallQueue.class, callQueueInitArgs,
            AdaptiveLifoCoDelCallQueue.class, callQueueInitArgs);
      } else {
        callExecutor = new RWQueueRpcExecutor("FifoRWQ.default", handlerCount, numCallQueues,
            callqReadShare, maxQueueLength, conf, abortable);
      }
    } else {
      // multiple queues
      if (isCodelQueueType(callQueueType)) {
        callExecutor = new BalancedQueueRpcExecutor("CodelBQ.default", handlerCount, numCallQueues,
            conf, abortable, AdaptiveLifoCoDelCallQueue.class, maxQueueLength, codelTargetDelay,
            codelInterval, codelLifoThreshold, numGeneralCallsDropped, numLifoModeSwitches);
      } else {
        callExecutor = new BalancedQueueRpcExecutor("FifoBQ.default", handlerCount, numCallQueues,
            maxQueueLength, conf, abortable);
      }
    }

    String metaCallQueueType = conf.get(META_CALL_QUEUE_TYPE_CONF_KEY, "");
    if (priorityHandlerCount > 0) {
      if (metaCallQueueType.equals(META_CALL_QUEUE_TYPE_CONF_VALUE)) {
        float priorityQueuesHandlersFactor =
            conf.getFloat(MetaRWQueueRpcExecutor.META_CALL_QUEUE_HANDLER_FACTOR_CONF_KEY,
              MetaRWQueueRpcExecutor.META_CALL_QUEUE_HANDLER_FACTOR_CONF_VALUE);
        int priorityQueueCount =
            Math.max(1, Math.round(priorityHandlerCount * priorityQueuesHandlersFactor));
        this.priorityExecutor = new MetaRWQueueRpcExecutor("MetaRWQ.Priority", priorityHandlerCount,
            priorityQueueCount, maxQueueLength, conf, abortable);
      } else {
        this.priorityExecutor = new BalancedQueueRpcExecutor("Priority", priorityHandlerCount, 1,
            maxQueueLength, conf, abortable);
      }
    } else {
      this.priorityExecutor = null;
    }
    this.replicationExecutor =
       replicationHandlerCount > 0 ? new BalancedQueueRpcExecutor("Replication",
         replicationHandlerCount, 1, maxQueueLength, conf, abortable) : null;
  }

  @Override
  public void init(Context context) {
    this.port = context.getListenerAddress().getPort();
  }

    @Override
  public void start() {
    callExecutor.start(port);
    if (priorityExecutor != null) priorityExecutor.start(port);
    if (replicationExecutor != null) replicationExecutor.start(port);
  }

  @Override
  public void stop() {
    callExecutor.stop();
    if (priorityExecutor != null) priorityExecutor.stop();
    if (replicationExecutor != null) replicationExecutor.stop();
  }

  @Override
  public void dispatch(CallRunner callTask) throws IOException, InterruptedException {
    RpcServer.Call call = callTask.getCall();
    int level = priority.getPriority(call.getHeader(), call.param);
    if (priorityExecutor != null && level > highPriorityLevel) {
      priorityExecutor.dispatch(callTask);
    } else if (replicationExecutor != null && level == HConstants.REPLICATION_QOS) {
      replicationExecutor.dispatch(callTask);
    } else {
      callExecutor.dispatch(callTask);
    }
  }

  @Override
  public int getGeneralQueueLength() {
    return callExecutor.getQueueLength();
  }

  @Override
  public int getPriorityQueueLength() {
    return priorityExecutor == null ? 0 : priorityExecutor.getQueueLength();
  }

  @Override
  public int getReplicationQueueLength() {
    return replicationExecutor == null ? 0 : replicationExecutor.getQueueLength();
  }

  @Override
  public int getActiveRpcHandlerCount() {
    return callExecutor.getActiveHandlerCount() +
           (priorityExecutor == null ? 0 : priorityExecutor.getActiveHandlerCount()) +
           (replicationExecutor == null ? 0 : replicationExecutor.getActiveHandlerCount());
  }

  @Override
  public int getWriteQueueLength() {
    return callExecutor.getWriteQueueLength();
  }

  @Override
  public int getReadQueueLength() {
    return callExecutor.getReadQueueLength();
  }

  @Override
  public int getScanQueueLength() {
    return callExecutor.getScanQueueLength();
  }

  @Override
  public int getActiveWriteRpcHandlerCount() {
    return callExecutor.getActiveWriteHandlerCount();
  }

  @Override
  public int getActiveReadRpcHandlerCount() {
    return callExecutor.getActiveReadHandlerCount();
  }

  @Override
  public int getActiveScanRpcHandlerCount() {
    return callExecutor.getActiveScanHandlerCount();
  }
  
  @Override
  public List<QueueCounter> getQueueCounters() {
    return callExecutor.getQueueCounters();
  }

  @Override
  public long getNumGeneralCallsDropped() {
    return numGeneralCallsDropped.get();
  }

  @Override
  public long getNumLifoModeSwitches() {
    return numLifoModeSwitches.get();
  }

  private static boolean isCodelQueueType(final String callQueueType) {
    return callQueueType.equals(CALL_QUEUE_TYPE_CODEL_CONF_VALUE);
  }
}

