/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */

package org.apache.hadoop.hbase.quotas;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class AllowExceedOperationQuota implements OperationQuota {
  private static final Log LOG = LogFactory.getLog(AllowExceedOperationQuota.class);

  private QuotaLimiter userLimiter;
  private QuotaLimiter regionServerLimiter;

  public AllowExceedOperationQuota(QuotaLimiter userLimiter, QuotaLimiter regionServerLimiter) {
    this.userLimiter = userLimiter;
    this.regionServerLimiter = regionServerLimiter;
  }

  @Override
  public void checkQuota(int numWrites, int numReads, int numScans) throws ThrottlingException {
    int readNeed = numReads + numScans;
    int writeNeed = numWrites;
    boolean userQuotaExceed = false;

    // Step1. Check user's quota
    try {
      userLimiter.checkQuotaByRequestUnit(writeNeed, readNeed);
      userLimiter.grabQuotaByRequestUnit(writeNeed, readNeed);
    } catch (ThrottlingException e) {
      // Support soft limit, so it doesn't throw the ThrottlingException directly
      userQuotaExceed = true;
    }

    // Step2. Check regionserver's quota.
    try {
      regionServerLimiter.checkQuotaByRequestUnit(writeNeed, readNeed);
      regionServerLimiter.grabQuotaByRequestUnit(writeNeed, readNeed);
    } catch (ThrottlingException e) {
      /*
       * Catch ThrottlingException means regionserver didn't have quota to run the requests.
       * The regionserver will throttle two type requests.
       * 1. User didn't have available quota too.
       * 2. User didn't config read/write quota.
       *
       * TODO: Divide ThrottlingException to ReadThrottlingException and WriteThrottlingException.
       * Now for read and write request, we don't know whether rs doesn't has read quota or
       * doesn't has write quota. So there is a case which need to fix.
       * * The request is read and write request.
       * * User only config read quota and have available read quota to run the requests.
       * * Regionserver didn't have available read quota to run the requests.
       * * It should not throw ThrottlingException. But now isBypass() will return true.
       */
      if (userQuotaExceed || userLimiter.isBypass(writeNeed, readNeed)) {
        throw e;
      }
      regionServerLimiter.grabQuotaByRequestUnit(writeNeed, readNeed);
    }
  }

  @Override
  public void grabQuota(int numWrites, int numReads, int numScans) {
    int readNeed = numReads + numScans;
    int writeNeed = numWrites;
    userLimiter.grabQuotaByRequestUnit(writeNeed, readNeed);
    regionServerLimiter.grabQuotaByRequestUnit(writeNeed, readNeed);
  }

  @Override
  public void close() {
    // Calculate and set the average size of get, scan and mutate for the current operation
  }

  @Override
  public long getReadAvailable() {
    long readReqsAvailable = 0;
    if (userLimiter.isBypass()) {
      readReqsAvailable = regionServerLimiter.getReadReqsAvailable();
    } else {
      // Beacuse allow user oversume, so return the max avail in rs limiter or user limiter
      readReqsAvailable = Math.max(regionServerLimiter.getReadReqsAvailable(),
        userLimiter.getReadReqsAvailable());
    }
    return readReqsAvailable;
  }

  @Override
  public long getWriteAvailable() {
    long writeReqsAvailable = 0;
    if (userLimiter.isBypass()) {
      writeReqsAvailable = regionServerLimiter.getWriteReqsAvailable();
    } else {
      // Beacuse allow user oversume, so return the max avail in rs limiter or user limiter
      writeReqsAvailable = Math.max(regionServerLimiter.getWriteReqsAvailable(),
        userLimiter.getWriteReqsAvailable());
    }
    return writeReqsAvailable;
  }

  @Override
  public boolean canLogThrottlingException() {
    return this.regionServerLimiter.canLogThrottlingException()
        || this.userLimiter.canLogThrottlingException();
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("AllowExceedOperationQuota [");
    builder.append(" " + userLimiter);
    builder.append(" " + regionServerLimiter + " ]");
    return builder.toString();
  }
}
