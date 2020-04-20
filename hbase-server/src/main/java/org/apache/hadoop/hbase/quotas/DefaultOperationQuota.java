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

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.quotas.QuotaLimiter.QuotaLimiterType;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.client.Result;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class DefaultOperationQuota implements OperationQuota {
  private static final Logger LOG = LoggerFactory.getLogger(DefaultOperationQuota.class);

  protected final List<QuotaLimiter> limiters;
  private final long writeCapacityUnit;
  private final long readCapacityUnit;

  // the available read quota size in bytes
  protected long readAvailable = 0;
  // estimated read quota
  protected long readConsumed = 0;
  protected long readCapacityUnitConsumed = 0;
  // real consumed read quota
  private final long[] operationSize;
  // difference between estimated quota and real consumed quota used in close method
  // to adjust quota amount. Also used by ExceedOperationQuota which is a subclass
  // of DefaultOperationQuota
  protected long readDiff = 0;
  protected long readCapacityUnitDiff = 0;

  // real consumed write quota
  protected long writeConsumed = 0;
  protected long writeCapacityUnitConsumed = 0;

  // record the read/scan count and size to get average data size for per request
  private QuotaLimiter userTableLimiter;
  private QuotaLimiter tableLimiter;

  public DefaultOperationQuota(final Configuration conf, final QuotaLimiter... limiters) {
    this(conf, Arrays.asList(limiters));
  }

  /**
   * NOTE: The order matters. It should be something like [user, table, namespace, global]
   */
  public DefaultOperationQuota(final Configuration conf, final List<QuotaLimiter> limiters) {
    this.writeCapacityUnit =
        conf.getLong(QuotaUtil.WRITE_CAPACITY_UNIT_CONF_KEY, QuotaUtil.DEFAULT_WRITE_CAPACITY_UNIT);
    this.readCapacityUnit =
        conf.getLong(QuotaUtil.READ_CAPACITY_UNIT_CONF_KEY, QuotaUtil.DEFAULT_READ_CAPACITY_UNIT);
    this.limiters = limiters;
    int size = ReadOperationType.values().length;
    operationSize = new long[size];

    for (int i = 0; i < size; ++i) {
      operationSize[i] = 0;
    }
    for (QuotaLimiter limiter : limiters) {
      if (limiter.getQuotaLimiterType() == QuotaLimiterType.USER_TABLE) {
        userTableLimiter = limiter;
      } else if (limiter.getQuotaLimiterType() == QuotaLimiterType.TABLE) {
        tableLimiter = limiter;
      }
    }
  }

  @Override
  public void checkReadQuota(int numReads, int numScans) throws RpcThrottlingException {
    updateEstimateReadConsumeQuota(numReads, numScans);
    checkQuota(0, numReads, numScans);
  }

  @Override
  public void checkWriteQuota(int numWrites, long writeSize) throws RpcThrottlingException {
    updateRealWriteConsumeQuota(numWrites, writeSize);
    checkQuota(numWrites, 0, 0);
  }

  protected void checkQuota(int numWrites, int numReads, int numScans)
      throws RpcThrottlingException {
    readAvailable = Long.MAX_VALUE;
    for (final QuotaLimiter limiter : limiters) {
      if (limiter.isBypass()) continue;

      limiter.checkQuota(numWrites, writeConsumed, numReads + numScans, readConsumed,
        writeCapacityUnitConsumed, readCapacityUnitConsumed);
      readAvailable = Math.min(readAvailable, limiter.getReadAvailable());
    }

    for (final QuotaLimiter limiter : limiters) {
      limiter.grabQuota(numWrites, writeConsumed, numReads + numScans, readConsumed,
        writeCapacityUnitConsumed, readCapacityUnitConsumed);
    }
  }

  @Override
  public void close() {
    // Adjust the quota consumed for the specified operation
    readDiff = operationSize[ReadOperationType.GET.ordinal()]
        + operationSize[ReadOperationType.SCAN.ordinal()] - readConsumed;
    readCapacityUnitDiff = calculateReadCapacityUnitDiff(
      operationSize[ReadOperationType.GET.ordinal()] + operationSize[ReadOperationType.SCAN.ordinal()],
      readConsumed);

    for (final QuotaLimiter limiter : limiters) {
      if (readDiff != 0) {
        limiter.consumeRead(readDiff, readCapacityUnitDiff);
      }
    }
  }

  @Override
  public long getReadAvailable() {
    return readAvailable;
  }

  @Override
  public void addGetResult(final Result result) {
    addOperationSize(ReadOperationType.GET, QuotaUtil.calculateResultSize(result));
  }

  @Override
  public void addGetResult(List<Cell> cells) {
    addOperationSize(ReadOperationType.GET, QuotaUtil.calculateCellSize(cells));
  }

  @Override
  public void addScanResult(final List<Result> results) {
    addOperationSize(ReadOperationType.SCAN, QuotaUtil.calculateResultSize(results));
  }

  private void addOperationSize(ReadOperationType type, long size) {
    operationSize[type.ordinal()] += size;
    if (userTableLimiter != null) {
      userTableLimiter.addOperationCountAndSize(type, 1, size);
    }
    if (tableLimiter != null) {
      tableLimiter.addOperationCountAndSize(type, 1, size);
    }
  }

  protected void updateEstimateReadConsumeQuota(int numReads, int numScans) {
    readConsumed = estimateConsume(ReadOperationType.GET, numReads);
    readConsumed += estimateConsume(ReadOperationType.SCAN, numScans);
    readCapacityUnitConsumed = calculateReadCapacityUnit(readConsumed);
  }

  protected void updateRealWriteConsumeQuota(int numWrites, long writeSize) {
    writeConsumed = writeSize;
    writeCapacityUnitConsumed = calculateWriteCapacityUnit(writeConsumed);
  }

  private long estimateConsume(final ReadOperationType type, int numReqs) {
    if (numReqs > 0) {
      long avgSize = getEstimateOperationSize(type);
      return avgSize * numReqs;
    }
    return 0;
  }

  private long calculateWriteCapacityUnit(final long size) {
    return (long) Math.ceil(size * 1.0 / this.writeCapacityUnit);
  }

  private long calculateReadCapacityUnit(final long size) {
    return (long) Math.ceil(size * 1.0 / this.readCapacityUnit);
  }

  private long calculateReadCapacityUnitDiff(final long actualSize, final long estimateSize) {
    long actualCU = actualSize <= 0 ? 1 : calculateReadCapacityUnit(actualSize);
    return actualCU - calculateReadCapacityUnit(estimateSize);
  }

  private long getEstimateOperationSize(ReadOperationType operationType) {
    long size = getAverageOperationSize(operationType);
    return size > 0 ? size : getDefaultOperationSize(operationType);
  }

  // get average data size for per request
  private long getAverageOperationSize(ReadOperationType type) {
    long size = getAverageOperationSize(userTableLimiter, type);
    return size > 0 ? size : getAverageOperationSize(tableLimiter, type);
  }

  private long getAverageOperationSize(QuotaLimiter limiter, ReadOperationType type) {
    return limiter != null ? limiter.getAverageOperationSize(type) : 0;
  }

  // get default data size for per request
  private long getDefaultOperationSize(ReadOperationType type) {
    long size;
    switch (type) {
      case GET:
        size = 100;
        break;
      case SCAN:
        size = 1000;
        break;
      default:
        throw new RuntimeException("Invalid operation type: " + type);
    }
    return size;
  }
}
