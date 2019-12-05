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

package org.apache.hadoop.hbase.quotas;

import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hbase.protobuf.generated.QuotaProtos.Throttle;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.QuotaProtos.TimedQuota;
import org.apache.hadoop.hbase.quotas.OperationQuota.AvgOperationSize;
import org.apache.hadoop.hbase.quotas.OperationQuota.OperationType;

/**
 * Simple time based limiter that checks the quota Throttle
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class TimeBasedLimiter implements QuotaLimiter {
  private static final Log LOG = LogFactory.getLog(TimeBasedLimiter.class);

  private static final Configuration conf = HBaseConfiguration.create();
  private RateLimiter reqsLimiter = null;
  private RateLimiter reqSizeLimiter = null;
  private RateLimiter writeReqsLimiter = null;
  private RateLimiter writeSizeLimiter = null;
  private RateLimiter readReqsLimiter = null;
  private RateLimiter readSizeLimiter = null;
  private AvgOperationSize avgOpSize = new AvgOperationSize();
  public static final String MAX_LOG_THROTTLING_COUNT_KEY = "hbase.throttling.log.max.count";
  private int maxLogThrottlingCount = conf.getInt(MAX_LOG_THROTTLING_COUNT_KEY, 5);
  private int logThrottlingCount = maxLogThrottlingCount;
  private boolean bypassGlobals = false;
  private long minWaitInterval;

  TimeBasedLimiter() {
    if (FixedIntervalRateLimiter.class.getName().equals(
      conf.getClass(RateLimiter.QUOTA_RATE_LIMITER_CONF_KEY, AverageIntervalRateLimiter.class)
          .getName())) {
      reqsLimiter = new FixedIntervalRateLimiter();
      reqSizeLimiter = new FixedIntervalRateLimiter();
      writeReqsLimiter = new FixedIntervalRateLimiter();
      writeSizeLimiter = new FixedIntervalRateLimiter();
      readReqsLimiter = new FixedIntervalRateLimiter();
      readSizeLimiter = new FixedIntervalRateLimiter();
    } else {
      reqsLimiter = new AverageIntervalRateLimiter();
      reqSizeLimiter = new AverageIntervalRateLimiter();
      writeReqsLimiter = new AverageIntervalRateLimiter();
      writeSizeLimiter = new AverageIntervalRateLimiter();
      readReqsLimiter = new AverageIntervalRateLimiter();
      readSizeLimiter = new AverageIntervalRateLimiter();
    }
    minWaitInterval = conf.getLong(QuotaUtil.THROTTLING_MIN_WAIT_INTERVAL,
      QuotaUtil.DEFAULT_THROTTLING_MIN_WAIT_INTERVAL);
  }

  public static QuotaLimiter fromThrottle(final Throttle throttle) {
    TimeBasedLimiter limiter = new TimeBasedLimiter();
    boolean isBypass = true;
    if (throttle.hasReqNum()) {
      setFromTimedQuota(limiter.reqsLimiter, throttle.getReqNum());
      isBypass = false;
    }

    if (throttle.hasReqSize()) {
      setFromTimedQuota(limiter.reqSizeLimiter, throttle.getReqSize());
      isBypass = false;
    }

    if (throttle.hasWriteNum()) {
      setFromTimedQuota(limiter.writeReqsLimiter, throttle.getWriteNum());
      isBypass = false;
    }

    if (throttle.hasWriteSize()) {
      setFromTimedQuota(limiter.writeSizeLimiter, throttle.getWriteSize());
      isBypass = false;
    }

    if (throttle.hasReadNum()) {
      setFromTimedQuota(limiter.readReqsLimiter, throttle.getReadNum());
      isBypass = false;
    }

    if (throttle.hasReadSize()) {
      setFromTimedQuota(limiter.readSizeLimiter, throttle.getReadSize());
      isBypass = false;
    }
    
    return isBypass ? NoopQuotaLimiter.get() : limiter;
  }

  public void update(final TimeBasedLimiter other) {
    reqsLimiter.update(other.reqsLimiter);
    reqSizeLimiter.update(other.reqSizeLimiter);
    writeReqsLimiter.update(other.writeReqsLimiter);
    writeSizeLimiter.update(other.writeSizeLimiter);
    readReqsLimiter.update(other.readReqsLimiter);
    readSizeLimiter.update(other.readSizeLimiter);
    this.updateLogThrottlingCount();
  }

  private static void setFromTimedQuota(final RateLimiter limiter, final TimedQuota timedQuota) {
    limiter.set(timedQuota.getSoftLimit(), ProtobufUtil.toTimeUnit(timedQuota.getTimeUnit()));
  }

  @Override
  public void checkQuota(long writeSize, long readSize) throws ThrottlingException {
    if (!reqsLimiter.canExecute()) {
      ThrottlingException.throwNumRequestsExceeded(Math.max(reqsLimiter.waitInterval(),
        minWaitInterval));
    }
    if (!reqSizeLimiter.canExecute(writeSize + readSize)) {
      ThrottlingException.throwNumRequestsExceeded(Math.max(
        reqSizeLimiter.waitInterval(writeSize + readSize), minWaitInterval));
    }

    if (writeSize > 0) {
      if (!writeReqsLimiter.canExecute()) {
        ThrottlingException.throwNumWriteRequestsExceeded(Math.max(writeReqsLimiter.waitInterval(),
          minWaitInterval));
      }
      if (!writeSizeLimiter.canExecute(writeSize)) {
        ThrottlingException.throwWriteSizeExceeded(Math.max(
          writeSizeLimiter.waitInterval(writeSize), minWaitInterval));
      }
    }

    if (readSize > 0) {
      if (!readReqsLimiter.canExecute()) {
        ThrottlingException.throwNumReadRequestsExceeded(Math.max(readReqsLimiter.waitInterval(),
          minWaitInterval));
      }
      if (!readSizeLimiter.canExecute()) {
        ThrottlingException.throwReadSizeExceeded(Math.max(readSizeLimiter.waitInterval(readSize),
          minWaitInterval));
      }
    }
  }
  
  @Override
  public void checkQuotaByRequestUnit(long writeNum, long readNum) throws ThrottlingException {
    if (writeNum > 0) {
      if (!writeReqsLimiter.canExecute(writeNum)) {
        ThrottlingException.throwNumWriteRequestsExceeded(Math.max(
          writeReqsLimiter.waitInterval(writeNum), minWaitInterval));
      }
    }

    if (readNum > 0) {
      if (!readReqsLimiter.canExecute(readNum)) {
        ThrottlingException.throwNumReadRequestsExceeded(Math.max(
          readReqsLimiter.waitInterval(readNum), minWaitInterval));
      }
    }
  }

  @Override
  public void grabQuota(long writeSize, long readSize) {
    assert writeSize != 0 || readSize != 0;

    reqsLimiter.consume(1);
    reqSizeLimiter.consume(writeSize + readSize);

    if (writeSize > 0) {
      writeReqsLimiter.consume(1);
      writeSizeLimiter.consume(writeSize);
    }
    if (readSize > 0) {
      readReqsLimiter.consume(1);
      readSizeLimiter.consume(readSize);
    }
  }
  
  @Override
  public void grabQuotaByRequestUnit(long writeNum, long readNum) {
    if (writeNum > 0) {
      writeReqsLimiter.consume(writeNum);
    }

    if (readNum > 0) {
      readReqsLimiter.consume(readNum);
    }
  }

  @Override
  public void consumeWrite(final long size) {
    reqSizeLimiter.consume(size);
    writeSizeLimiter.consume(size);
  }

  @Override
  public void consumeRead(final long size) {
    reqSizeLimiter.consume(size);
    readSizeLimiter.consume(size);
  }

  @Override
  public boolean isBypass() {
    return false;
  }

  /**
   * For write request, return true if no write limit.
   * For read request, return true if no read limit.
   * For read and write request, return true if no write limit or no read limit.
   */
  @Override
  public boolean isBypass(long writeNum, long readNum) {
    if (writeNum > 0 && readNum == 0) {
      return writeReqsLimiter.isBypass();
    }
    if (writeNum == 0 && readNum > 0) {
      return readReqsLimiter.isBypass();
    }
    return writeReqsLimiter.isBypass() || readReqsLimiter.isBypass();
  }

  @Override
  public long getWriteAvailable() {
    return writeSizeLimiter.getAvailable();
  }

  @Override
  public long getReadAvailable() {
    return readSizeLimiter.getAvailable();
  }
  
  @Override
  public long getWriteReqsAvailable() {
    return writeReqsLimiter.getAvailable();
  }

  @Override
  public long getReadReqsAvailable() {
    return readReqsLimiter.getAvailable();
  }
  
  @Override
  public long getReqsAvailable() {
    return reqsLimiter.getAvailable();
  }
  
  @Override
  public long getReqsSizeAvailable() {
    return reqSizeLimiter.getAvailable();
  }

  @Override
  public void addOperationSize(OperationType type, long size) {
    avgOpSize.addOperationSize(type, size);
  }

  @Override
  public long getAvgOperationSize(OperationType type) {
    return avgOpSize.getAvgOperationSize(type);
  }

  @Override
  public boolean getBypassGlobals() {
    return this.bypassGlobals;
  }

  @Override
  public void setBypassGlobals(boolean bypassGlobals) {
    this.bypassGlobals = bypassGlobals;
  }

  /**
   * avoid log too much exception when overload.
   * limiter can log at most DEFAULT_MAX_LOG_THROTTLING_COUNT exception.
   */
  @Override
  public boolean canLogThrottlingException() {
    // config as -1, means print all throttling exception log
    // config as 0, means never log about throttling exception
    if (maxLogThrottlingCount == -1) {
      return true;
    }
    if (this.logThrottlingCount > 0) {
      this.logThrottlingCount -= 1;
      return true;
    }
    return false;
  }

  /**
   * logThrottlingCount will refresh when QuotaCache refresh every limiter
   */
  public void updateLogThrottlingCount() {
    this.logThrottlingCount = maxLogThrottlingCount;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("TimeBasedLimiter(");
    if (!reqsLimiter.isBypass()) builder.append("reqs=" + reqsLimiter);
    if (!reqSizeLimiter.isBypass()) builder.append(" resSize=" + reqSizeLimiter);
    if (!writeReqsLimiter.isBypass()) builder.append(" writeReqs=" + writeReqsLimiter);
    if (!writeSizeLimiter.isBypass()) builder.append(" writeSize=" + writeSizeLimiter);
    if (!readReqsLimiter.isBypass()) builder.append(" readReqs=" + readReqsLimiter);
    if (!readSizeLimiter.isBypass()) builder.append(" readSize=" + readSizeLimiter);
    builder.append(')');
    return builder.toString();
  }
}
