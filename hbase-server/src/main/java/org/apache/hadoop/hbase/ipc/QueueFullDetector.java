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

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.ThreadInfoUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public class QueueFullDetector extends ScheduledChore {

  public static final String QUEUE_FULL_DETECTOR_ENABLED =
      "hbase.regionserver.queuefull.detector.enable";
  public static final boolean QUEUE_FULL_DETECTOR_ENABLED_DEFAULT = true;

  public static final String QUEUE_FULL_DETECTOR_SPARSE_PERIOD =
      "hbase.regionserver.queuefull.detector.sparseperiod";
  public static final int QUEUE_FULL_DETECTOR_SPARSE_PERIOD_DEFAULT = 5000;

  public static final String QUEUE_FULL_DETECTOR_DENSE_PERIOD =
      "hbase.regionserver.queuefull.detector.denseperiod";
  public static final int QUEUE_FULL_DETECTOR_DENSE_PERIOD_DEFAULT = 1000;
  public static final String QUEUE_FULL_DETECTOR_DENSE_CHECKNUM =
      "hbase.regionserver.queuefull.detector.densechecknum";
  public static final int QUEUE_FULL_DETECTOR_DENSE_CHECKNUM_DEFAULT = 100;
  public static final String QUEUE_FULL_DETECTOR_SAMPLE_THRESHOLD =
      "hbase.regionserver.queuefull.detector.samplethreshold";
  public static final int QUEUE_FULL_DETECTOR_SAMPLE_THRESHOLD_DEFAULT = 95;
  public static final String QUEUE_FULL_DETECTOR_REJECT_THRESHOLD =
      "hbase.regionserver.queuefull.detector.rejectthreshold";
  public static final int QUEUE_FULL_DETECTOR_REJECT_THRESHOLD_DEFAULT = 95;

  private static final Logger LOG = LoggerFactory.getLogger(QueueFullDetector.class);

  private final HRegionServer server;
  private int densePeriod;
  private int denseCheckNum;
  private int sampleThreshold;
  private int rejectThreshold;
  private List<QueueCounter> queueCounters;

  public QueueFullDetector(HRegionServer server, Configuration conf) {
    super("QueueFullDetector", server,
        conf.getInt(QUEUE_FULL_DETECTOR_SPARSE_PERIOD, QUEUE_FULL_DETECTOR_SPARSE_PERIOD_DEFAULT));
    this.server = server;
    this.densePeriod =
        conf.getInt(QUEUE_FULL_DETECTOR_DENSE_PERIOD, QUEUE_FULL_DETECTOR_DENSE_PERIOD_DEFAULT);
    this.denseCheckNum =
        conf.getInt(QUEUE_FULL_DETECTOR_DENSE_CHECKNUM, QUEUE_FULL_DETECTOR_DENSE_CHECKNUM_DEFAULT);
    this.sampleThreshold = conf.getInt(QUEUE_FULL_DETECTOR_SAMPLE_THRESHOLD,
      QUEUE_FULL_DETECTOR_SAMPLE_THRESHOLD_DEFAULT);
    if (sampleThreshold < 10 || sampleThreshold >= 100) {
      LOG.warn(
        "Sample threshold of queue full detector should be in range (10, 100), but the configured value is "
            + sampleThreshold + ", will use 95 instead");
      sampleThreshold = 95;
    }
    this.rejectThreshold = conf.getInt(QUEUE_FULL_DETECTOR_REJECT_THRESHOLD,
      QUEUE_FULL_DETECTOR_REJECT_THRESHOLD_DEFAULT);
    if (rejectThreshold < 10 || rejectThreshold >= 100) {
      LOG.warn(
        "Reject threshold of queue full detector should be in range (10, 100), but the configured value is "
            + rejectThreshold + ", will use 95 instead");
      rejectThreshold = 95;
    }

    this.queueCounters = server.getRpcServer().getScheduler().getQueueCounters();

    LOG.info("QueueFullDetector is started, denseCheckNum=" + denseCheckNum + ", densePeriod="
        + densePeriod + ", sampleThreshold=" + sampleThreshold + ", rejectThreshold="
        + rejectThreshold);
  }

  private void checkQueueFull(QueueCounter queueCounter) {
    boolean queueFull = queueCounter.getQueueFull();
    if (queueFull) {
      // Found "queue full" events, start dense detecting
      LOG.warn("Detected queue full events for " + queueCounter + ",  start dense checking");
      int queueFullNum = 0;
      int maxNotHit = Math.max((int) (denseCheckNum * (100 - sampleThreshold) / 100.0), 1);
      long beforeCheckRequestCount = queueCounter.getIncomeRequestCount();
      long beforeCheckRejectedRequestCount = queueCounter.getRejectedRequestCount();
      // To detect queue full events in a higher frequency
      for (int i = 0; i < denseCheckNum; i++) {
        try {
          Thread.sleep(densePeriod);
        } catch (InterruptedException e) {
          // Check if we should stop after the try-catch block
        }
        if (server.isStopping() || server.isStopped()) {
          LOG.info("Exit queue full dense checking for " + queueCounter
              + ", because region server is stopping or stopped!");
          return;
        }
        if (queueCounter.getQueueFull()) {
          queueFullNum++;
        }
        int notHit = i + 1 - queueFullNum;
        if (notHit > maxNotHit) {
          LOG.info("Exit queue full dense checking for " + queueCounter + ", queueFullNum= "
              + queueFullNum + ", notHit=" + notHit + ", maxNotHit=" + maxNotHit);
          return;
        }
      }

      long afterCheckRequestCount = queueCounter.getIncomeRequestCount();
      long afterCheckRejectedRequestCount = queueCounter.getRejectedRequestCount();
      long newRequestCount = afterCheckRequestCount - beforeCheckRequestCount;
      long newRejectedRequestCount =
          afterCheckRejectedRequestCount - beforeCheckRejectedRequestCount;
      StringBuilder sb = new StringBuilder();
      sb.append("After dense checking for ").append(queueCounter).append(", queueFullNum is ")
          .append(queueFullNum);
      sb.append(", new incoming requests count is ").append(newRequestCount)
          .append(", rejected requests count is ").append(newRejectedRequestCount);
      // If the following conditions are detected, we should exit gracefully:
      // a. the percentage of queueFullNum has reached the configured threshold
      // b. the region server is not idle (i.e. there are new incoming rpc)
      // c. the percentage of rejected new incoming request in this period (due to queue full) has
      // reached the configured threshold
      boolean shouldExit = false;
      if (queueFullNum * 100 > sampleThreshold * denseCheckNum) {
        if (newRequestCount > 0
            && (newRejectedRequestCount * 100 > rejectThreshold * newRequestCount)) {
          shouldExit = true;
        }
      }
      sb.append(", shouldExit is ").append(shouldExit);
      LOG.info(sb.toString());
      if (shouldExit) {
        ThreadInfoUtils.logThreadInfo("Thread dump from QueueFullDetector", true);
        // queueFullDetected = true;
        server.abort("Detected queue full and cann't come back to normal state in a long duration");
      }
    }
  }

  @Override
  protected void chore() {
    queueCounters.forEach(this::checkQueueFull);
  }

  public static boolean isEnabled(Configuration conf) {
    return conf != null
        && conf.getBoolean(QUEUE_FULL_DETECTOR_ENABLED, QUEUE_FULL_DETECTOR_ENABLED_DEFAULT);
  }
}
