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

import java.io.IOException;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.quotas.OperationQuota.AvgOperationSize;
import org.apache.hadoop.hbase.quotas.OperationQuota.OperationType;

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
    try {
      userLimiter.checkQuotaByRequestUnit(writeNeed, readNeed);
      userLimiter.grabQuotaByRequestUnit(writeNeed, readNeed);
    } catch (ThrottlingException e) {
      userQuotaExceed = true;
    }
    try {
      regionServerLimiter.checkQuotaByRequestUnit(writeNeed, readNeed);
      regionServerLimiter.grabQuotaByRequestUnit(writeNeed, readNeed);
    } catch (ThrottlingException e) {
      if (userQuotaExceed || userLimiter.isBypass()) {
        throw e;
      }
      regionServerLimiter.grabQuotaByRequestUnit(writeNeed, readNeed);
    }
  }

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
    return Math.max(regionServerLimiter.getReadReqsAvailable(), userLimiter.getReadReqsAvailable())
        * QuotaUtil.READ_CAPACITY_UNIT;
  }

  @Override
  public long getWriteAvailable() {
    return Math.max(regionServerLimiter.getWriteReqsAvailable(), userLimiter.getWriteReqsAvailable())
        * QuotaUtil.WRITE_CAPACITY_UNIT;
  }

  @Override
  public void addGetResult(final Result result) {
  }

  @Override
  public void addScanResult(final List<Result> results) {
  }

  @Override
  public void addMutation(final Mutation mutation) {
  }

  @Override
  public long getAvgOperationSize(OperationType type) {
    return 0;
  }

  private long estimateConsume(final OperationType type, int numReqs, long avgSize) {
    return 0;
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
