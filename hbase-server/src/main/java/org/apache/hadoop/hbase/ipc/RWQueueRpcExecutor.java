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

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.Message;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.Scan;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ScanRequest;
import org.apache.hadoop.hbase.util.QueueCounter;
import org.apache.hadoop.hbase.util.ReflectionUtils;

/**
 * RPC Executor that uses different queues for reads and writes. Each handler has its own queue and
 * there is no stealing.
 */
@InterfaceAudience.LimitedPrivate({ HBaseInterfaceAudience.COPROC, HBaseInterfaceAudience.PHOENIX })
@InterfaceStability.Evolving
public class RWQueueRpcExecutor extends RpcExecutor {
  private static final Log LOG = LogFactory.getLog(RWQueueRpcExecutor.class);

  private final List<BlockingQueue<CallRunner>> queues;
  private final QueueBalancer writeBalancer;
  private final QueueBalancer readBalancer;
  private QueueBalancer scanBalancer;
  private final int writeHandlersCount;
  private final int readHandlersCount;
  private final int scanHandlersCount;
  private final int numWriteQueues;
  private final int numReadQueues;
  private final int numScanQueues;

  private final AtomicInteger activeWriteHandlerCount = new AtomicInteger(0);
  private final AtomicInteger activeReadHandlerCount = new AtomicInteger(0);
  private final AtomicInteger activeScanHandlerCount = new AtomicInteger(0);

  private final QueueCounter readQueueCounter;
  private final QueueCounter writeQueueCounter;
  private final QueueCounter scanQueueCounter;

  public RWQueueRpcExecutor(final String name, final int handlerCount, final int numQueues,
      final float readShare, final int maxQueueLength, final Configuration conf,
      final Abortable abortable) {
    this(name, handlerCount, numQueues, readShare, maxQueueLength, conf, abortable,
        LinkedBlockingQueue.class);
  }

  private RWQueueRpcExecutor(final String name, final int handlerCount, final int numQueues,
      final float readShare, final int maxQueueLength, final Configuration conf,
      final Abortable abortable, final Class<? extends BlockingQueue> readQueueClass,
      Object... readQueueInitArgs) {
    this(name, calcNumWriters(handlerCount, readShare), calcNumReaders(handlerCount, readShare),
        calcNumWriters(numQueues, readShare), calcNumReaders(numQueues, readShare), conf, abortable,
        LinkedBlockingQueue.class, new Object[] { maxQueueLength }, readQueueClass,
        ArrayUtils.addAll(new Object[] { maxQueueLength }, readQueueInitArgs));
  }

  public RWQueueRpcExecutor(final String name, final int handlerCount, final int numQueues,
      final float readShare, final Configuration conf, final Abortable abortable,
      final Class<? extends BlockingQueue> writeQueueClass, Object[] writeQueueInitArgs,
      final Class<? extends BlockingQueue> readQueueClass, Object[] readQueueInitArgs) {
    this(name, calcNumWriters(handlerCount, readShare), calcNumReaders(handlerCount, readShare),
        calcNumWriters(numQueues, readShare), calcNumReaders(numQueues, readShare), conf, abortable,
        writeQueueClass, writeQueueInitArgs, readQueueClass, readQueueInitArgs);
  }

  private RWQueueRpcExecutor(final String name, final int writeHandlers, int readHandlers,
      final int numWriteQueues, int numReadQueues, final Configuration conf,
      final Abortable abortable, final Class<? extends BlockingQueue> writeQueueClass,
      Object[] writeQueueInitArgs, final Class<? extends BlockingQueue> readQueueClass,
      Object[] readQueueInitArgs) {
    super(name, Math.max(writeHandlers, numWriteQueues) + Math.max(readHandlers, numReadQueues),
        conf, abortable);

    readHandlers = Math.max(readHandlers, numReadQueues);
    float scanShare = conf.getFloat(SimpleRpcScheduler.CALL_QUEUE_SCAN_SHARE_CONF_KEY, 0);
    int numScanQueues = calcNumScans(numReadQueues, scanShare);
    int scanHandlers = calcNumScans(readHandlers, scanShare);
    if ((numReadQueues - numScanQueues) > 0) {
      numReadQueues -= numScanQueues;
      readHandlers -= scanHandlers;
    } else {
      numScanQueues = 0;
      scanHandlers = 0;
    }

    this.writeHandlersCount = Math.max(writeHandlers, numWriteQueues);
    this.readHandlersCount = Math.max(readHandlers, numReadQueues);
    this.scanHandlersCount = Math.max(scanHandlers, numScanQueues);
    this.numWriteQueues = numWriteQueues;
    this.numReadQueues = numReadQueues;
    this.numScanQueues = numScanQueues;
    this.writeBalancer = getBalancer(numWriteQueues);
    this.readBalancer = getBalancer(numReadQueues);
    if (this.scanHandlersCount > 0) {
      this.scanBalancer = getBalancer(numScanQueues);
    }
    this.readQueueCounter = new QueueCounter("Read");
    this.writeQueueCounter = new QueueCounter("Write");
    this.scanQueueCounter = new QueueCounter("Scan");

    queues = new ArrayList<BlockingQueue<CallRunner>>(numWriteQueues + numReadQueues + numScanQueues);
    LOG.info(name + " writeQueues=" + numWriteQueues + " writeHandlers=" + writeHandlersCount
        + " readQueues=" + numReadQueues + " readHandlers=" + readHandlersCount
        + ((numScanQueues == 0) ? ""
            : " scanQueues=" + numScanQueues + " scanHandlers=" + scanHandlersCount));

    for (int i = 0; i < numWriteQueues; ++i) {
      queues.add((BlockingQueue<CallRunner>) ReflectionUtils.newInstance(writeQueueClass,
        writeQueueInitArgs));
    }

    for (int i = 0; i < numReadQueues + numScanQueues; ++i) {
      queues.add(
        (BlockingQueue<CallRunner>) ReflectionUtils.newInstance(readQueueClass, readQueueInitArgs));
    }
  }

  @Override
  protected void startHandlers(final int port) {
    startHandlers(".write", writeHandlersCount, queues, 0, numWriteQueues, port,
      activeWriteHandlerCount);
    startHandlers(".read", readHandlersCount, queues, numWriteQueues, numReadQueues, port,
      activeReadHandlerCount);
    if (this.scanHandlersCount > 0) {
      startHandlers(".scan", scanHandlersCount, queues, numWriteQueues + numReadQueues,
        numScanQueues, port, activeScanHandlerCount);
    }
  }

  @Override
  public void dispatch(final CallRunner callTask) throws IOException, InterruptedException {
    RpcServer.Call call = callTask.getCall();
    int queueIndex;
    QueueCounter queueCounter;
    if (isWriteRequest(call.getHeader(), call.getParam())) {
      queueIndex = writeBalancer.getNextQueue();
      queueCounter = writeQueueCounter;
    } else if (scanHandlersCount > 0 && isHeavyScanRequest(call.getParam())) {
      queueIndex = numWriteQueues + numReadQueues + scanBalancer.getNextQueue();
      queueCounter = scanQueueCounter;
    } else {
      queueIndex = numWriteQueues + readBalancer.getNextQueue();
      queueCounter = readQueueCounter;
    }
    queueCounter.incIncomeRequestCount();
    if (!queues.get(queueIndex).offer(callTask)) {
      callTask.resetCallQueueSize();
      String queueType = queueIndex >= numWriteQueues + numReadQueues ? "scan"
          : (queueIndex < numWriteQueues ? "write" : "read");
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
      long lastRejectedWriteCount = 0, lastRejectedReadCount = 0, lastRejectedScanCount = 0;
      while (true) {
        try {
          lastRejectedReadCount = logQueueFullError(readQueueCounter, lastRejectedReadCount, "read");
          lastRejectedWriteCount = logQueueFullError(writeQueueCounter, lastRejectedWriteCount, "write");
          lastRejectedScanCount = logQueueFullError(scanQueueCounter, lastRejectedScanCount, "scan");
          TimeUnit.SECONDS.sleep(logInterval);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
    });
    t.setDaemon(true);
    t.setName("RWQueue-QueueFullLogger");
    t.start();
  }

  @VisibleForTesting
  protected boolean isHeavyScanRequest(final Message param) {
    if (param instanceof ScanRequest) {
      ScanRequest scanRequest = (ScanRequest) param;
      Scan scan = scanRequest.getScan();
      if (scan.hasFilter() && !isScanFromCanary(scanRequest)) {
        return true;
      }
    }
    return false;
  }

  private boolean isScanFromCanary(ScanRequest scanRequest) {
    return scanRequest.hasLimitOfRows() && scanRequest.getLimitOfRows() == 1;
  }

  @Override
  public int getQueueLength() {
    int length = 0;
    for (final BlockingQueue<CallRunner> queue : queues) {
      length += queue.size();
    }
    return length;
  }

  @Override
  protected List<BlockingQueue<CallRunner>> getQueues() {
    return queues;
  }

  @Override
  public int getWriteQueueLength() {
    int length = 0;
    for (int i = 0; i < numWriteQueues; i++) {
      length += queues.get(i).size();
    }
    return length;
  }

  @Override
  public int getReadQueueLength() {
    int length = 0;
    for (int i = numWriteQueues; i < (numWriteQueues + numReadQueues); i++) {
      length += queues.get(i).size();
    }
    return length;
  }

  @Override
  public int getScanQueueLength() {
    int length = 0;
    for (int i =
        numWriteQueues + numReadQueues; i < (numWriteQueues + numReadQueues + numScanQueues); i++) {
      length += queues.get(i).size();
    }
    return length;
  }

  @Override
  public int getActiveHandlerCount() {
    return activeWriteHandlerCount.get() + activeReadHandlerCount.get()
        + activeScanHandlerCount.get();
  }

  @Override
  public int getActiveWriteHandlerCount() {
    return activeWriteHandlerCount.get();
  }

  @Override
  public int getActiveReadHandlerCount() {
    return activeReadHandlerCount.get();
  }

  @Override
  public int getActiveScanHandlerCount() {
    return activeScanHandlerCount.get();
  }

  /*
   * Calculate the number of writers based on the "total count" and the read share. You'll get at
   * least one writer.
   */
  private static int calcNumWriters(final int count, final float readShare) {
    return Math.max(1, count - Math.max(1, (int) Math.round(count * readShare)));
  }

  /*
   * Calculate the number of readers based on the "total count" and the read share. You'll get at
   * least one reader.
   */
  private static int calcNumReaders(final int count, final float readShare) {
    return count - calcNumWriters(count, readShare);
  }

  /*
   * Calculate the number of scans based on the "read count" and the scan share. You'll get at least
   * zero reader.
   */
  private static int calcNumScans(final int readCount, final float scanShare) {
    return Math.max(0, (int) (readCount * scanShare));
  }

  @Override
  public List<QueueCounter> getQueueCounters() {
    return Arrays.asList(readQueueCounter, writeQueueCounter, scanQueueCounter);
  }

  @VisibleForTesting
  protected int getWriteHandlersCount() {
    return writeHandlersCount;
  }

  @VisibleForTesting
  protected int getReadHandlersCount() {
    return readHandlersCount;
  }

  @VisibleForTesting
  protected int getScanHandlersCount() {
    return scanHandlersCount;
  }

  @VisibleForTesting
  protected QueueBalancer getScanBalancer() {
    return scanBalancer;
  }

  @VisibleForTesting
  protected int getNumWriteQueues() {
    return numWriteQueues;
  }

  @VisibleForTesting
  protected int getNumReadQueues() {
    return numReadQueues;
  }

  @VisibleForTesting
  protected int getNumScanQueues() {
    return numScanQueues;
  }
}
