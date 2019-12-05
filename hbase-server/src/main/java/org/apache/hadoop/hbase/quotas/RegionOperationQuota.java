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
import java.util.stream.Collectors;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Region quota which is a hard limit
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class RegionOperationQuota implements OperationQuota {
  private final List<QuotaLimiter> limiters;

  public RegionOperationQuota(final QuotaLimiter... limiters) {
    this.limiters = Arrays.asList(limiters).stream()
        .filter(limiter -> limiter != null && !(limiter instanceof NoopQuotaLimiter))
        .collect(Collectors.toList());
  }

  @Override
  public void checkQuota(int numWrites, int numReads, int numScans) throws ThrottlingException {
    int readNeed = numReads + numScans;
    int writeNeed = numWrites;
    for (final QuotaLimiter limiter : limiters) {
      limiter.checkQuotaByRequestUnit(writeNeed, readNeed);
    }

    for (final QuotaLimiter limiter : limiters) {
      limiter.grabQuotaByRequestUnit(writeNeed, readNeed);
    }
  }

  @Override
  public void grabQuota(int numWrites, int numReads, int numScans) {
    int readNeed = numReads + numScans;
    int writeNeed = numWrites;
    for (QuotaLimiter limiter : limiters) {
      limiter.grabQuotaByRequestUnit(writeNeed, readNeed);
    }
  }

  @Override
  public void close() {
  }

  @Override
  public long getReadAvailable() {
    long value = Long.MAX_VALUE;
    for (QuotaLimiter limiter : limiters) {
      value = Math.min(value, limiter.getReadAvailable());
    }
    return value;
  }

  @Override
  public long getWriteAvailable() {
    long value = Long.MAX_VALUE;
    for (QuotaLimiter limiter : limiters) {
      value = Math.min(value, limiter.getWriteAvailable());
    }
    return value;
  }

  @Override
  public boolean canLogThrottlingException() {
    boolean ifLog = false;
    for (final QuotaLimiter limiter: limiters) {
      ifLog |= limiter.canLogThrottlingException();
    }
    return ifLog;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("RegionOperationQuota [");
    for (QuotaLimiter limiter : limiters) {
      builder.append(" ").append(limiter);
    }
    builder.append(" ]");
    return builder.toString();
  }
}
