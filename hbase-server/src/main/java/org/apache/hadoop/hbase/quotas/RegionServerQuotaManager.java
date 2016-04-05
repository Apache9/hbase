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

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.ipc.RpcScheduler;
import org.apache.hadoop.hbase.ipc.RequestContext;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.security.UserGroupInformation;
import org.cliffc.high_scale_lib.Counter;

import com.google.common.annotations.VisibleForTesting;

/**
 * Region Server Quota Manager.
 * It is responsible to provide access to the quota information of each user/table.
 *
 * The direct user of this class is the RegionServer that will get and check the
 * user/table quota for each operation (put, get, scan).
 * For system tables and user/table with a quota specified, the quota check will be a noop.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class RegionServerQuotaManager {
  private static final Log LOG = LogFactory.getLog(RegionServerQuotaManager.class);
  
  private final RegionServerServices rsServices;

  private QuotaCache quotaCache = null;

  private boolean isSimulated = false;

  private Counter grabQuotaFailedCount = new Counter();

  private final boolean allowExceed;

  private final int readCapacityUnit;
  private final int writeCapacityUnit;

  private final Configuration conf;

  public RegionServerQuotaManager(final RegionServerServices rsServices) {
    this.rsServices = rsServices;
    this.conf = rsServices.getConfiguration();
    this.allowExceed = this.conf.getBoolean(QuotaUtil.QUOTA_ALLOW_EXCEED_CONF_KEY,
      QuotaUtil.DEFAULT_QUOTA_ALLOW_EXCEED);
    this.readCapacityUnit = this.conf.getInt(QuotaUtil.READ_CAPACITY_UNIT_CONF_KEY,
      QuotaUtil.DEFAULT_READ_CAPACITY_UNIT);
    this.writeCapacityUnit = this.conf.getInt(QuotaUtil.WRITE_CAPACITY_UNIT_CONF_KEY,
      QuotaUtil.DEFAULT_WRITE_CAPACITY_UNIT);
  }

  public void start(final RpcScheduler rpcScheduler) throws IOException {
    if (!QuotaUtil.isQuotaEnabled(rsServices.getConfiguration())) {
      LOG.info("Quota support disabled");
      return;
    }

    LOG.info("Initializing quota support");

    // Initialize quota cache
    try {
      quotaCache = new QuotaCache(rsServices);
      quotaCache.start();
    } catch (Throwable t) {
      LOG.error("failed to start quotaCache", t);
      quotaCache = null;
    }

    this.grabQuotaFailedCount.set(0);
  }

  public void stop() {
    if (isQuotaEnabled()) {
      quotaCache.stop("shutdown");
      quotaCache = null;
    }
  }
  
  public boolean isStopped() {
    if (isQuotaEnabled()) {
      return quotaCache.isStopped();
    }
    return true;
  }

  public boolean isQuotaEnabled() {
    return quotaCache != null;
  }

  public boolean isThrottleSimulated() {
    return isSimulated;
  }

  public void setThrottleSimulated(boolean isSimulated) {
    this.isSimulated = isSimulated;
  }

  @VisibleForTesting
  QuotaCache getQuotaCache() {
    return quotaCache;
  }

  private Configuration getConfiguration() {
    return rsServices.getConfiguration();
  }

  protected int getReadCapacityUnit() {
    return this.readCapacityUnit;
  }

  protected int getWriteCapacityUnit() {
    return this.writeCapacityUnit;
  }

  /**
   * Returns the quota for an operation.
   *
   * @param ugi the user that is executing the operation
   * @param table the table where the operation will be executed
   * @return the OperationQuota
   */
  public OperationQuota getQuota(final UserGroupInformation ugi, final TableName table) {
    if (isQuotaEnabled() && !table.isSystemTable()) {
      UserQuotaState userQuotaState = quotaCache.getUserQuotaState(ugi);
      QuotaLimiter userLimiter = userQuotaState.getTableLimiter(table);
      QuotaLimiter rsLimiter = quotaCache.getRegionServerLimiter();
      boolean useNoop = userLimiter.isBypass();
      useNoop &= rsLimiter.isBypass();
      // For allow exceed quota, bypass globals means that user will not throttled by any limiter.
      // For default quota, bypass globals meeans that user only throttled by user own limiter.
      if (userQuotaState.hasBypassGlobals() || userLimiter.getBypassGlobals()) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("get quota for ugi=" + ugi + " table=" + table + " userLimiter=" + userLimiter);
        }
        if (!useNoop) {
          if (allowExceed) {
            return NoopOperationQuota.get();
          } else {
            return new DefaultOperationQuota(userLimiter);
          }
        }
      } else {
        QuotaLimiter nsLimiter = quotaCache.getNamespaceLimiter(table.getNamespaceAsString());
        QuotaLimiter tableLimiter = quotaCache.getTableLimiter(table);
        useNoop &= tableLimiter.isBypass() && nsLimiter.isBypass();
        if (LOG.isTraceEnabled()) {
          LOG.trace("get quota for ugi=" + ugi + " table=" + table + " userLimiter=" +
                    userLimiter + " tableLimiter=" + tableLimiter + " nsLimiter=" + nsLimiter);
        }
        if (!useNoop) {
          if (allowExceed) {
            return new AllowExceedOperationQuota(userLimiter, rsLimiter);
          } else {
            return new DefaultOperationQuota(userLimiter, tableLimiter, nsLimiter);
          }
        }
      }
    }
    return NoopOperationQuota.get();
  }
  
  private UserGroupInformation getUserGroupInformation(final HRegion region) throws IOException {
    UserGroupInformation ugi;
    if (RequestContext.isInRequestContext()) {
      ugi = RequestContext.getRequestUser().getUGI();
    } else {
      ugi = User.getCurrent().getUGI();
    }
    return ugi;
  }

  /**
   * Check the quota for the current (rpc-context) user. return OperationQuota
   * @param region the region where the operation will be performed
   * @param type the operation type
   * @throws IOException
   * @throws ThrottlingException
   */
  public OperationQuota checkQuota(final HRegion region, final OperationQuota.OperationType type)
      throws IOException, ThrottlingException {
    switch (type) {
    case SCAN:
      return checkQuota(region, 0, 0, 1);
    case GET:
      return checkQuota(region, 0, 1, 0);
    case MUTATE:
      return checkQuota(region, 1, 0, 0);
    }
    throw new RuntimeException("Invalid operation type: " + type);
  }

  public OperationQuota checkQuota(final UserGroupInformation ugi, final TableName table,
      final OperationQuota.OperationType type) throws IOException, ThrottlingException {
    switch (type) {
    case SCAN:
      return checkQuota(ugi, table, 0, 0, 1);
    case GET:
      return checkQuota(ugi, table, 0, 1, 0);
    case MUTATE:
      return checkQuota(ugi, table, 1, 0, 0);
    }
    throw new RuntimeException("Invalid operation type: " + type);
  }
  
  /**
   * check quota by Mutation. It will check quota by mutation size.
   * @param region
   * @param mutation
   * @throws IOException
   */
  public OperationQuota checkQuota(final HRegion region, final Mutation mutation) throws IOException,
      ThrottlingException {
    int numWrites = calculateRequestUnitNum(mutation);
    return checkQuota(region, numWrites, 0, 0);
  }

  public OperationQuota checkQuota(final UserGroupInformation ugi, final TableName table,
      final Mutation mutation) throws IOException, ThrottlingException {
    int numWrites = calculateRequestUnitNum(mutation);
    return checkQuota(ugi, table, numWrites, 0, 0);
  }

  /**
   * Check the quota for the current (rpc-context) user. return OperationQuota
   * @param region the region where the operation will be performed
   * @param type the operation type
   * @throws IOException
   * @throws ThrottlingException
   */
  public OperationQuota checkQuota(final HRegion region, final List<ClientProtos.Action> actions)
      throws IOException, ThrottlingException {
    int numWrites = 0;
    int numReads = 0;
    for (final ClientProtos.Action action : actions) {
      if (action.hasMutation()) {
        numWrites++;
      } else if (action.hasGet()) {
        numReads++;
      }
    }
    return checkQuota(region, numWrites, numReads, 0);
  }
  
  public OperationQuota checkQuota(final UserGroupInformation ugi, final TableName table,
      final List<ClientProtos.Action> actions) throws IOException, ThrottlingException {
    int numWrites = 0;
    int numReads = 0;
    for (final ClientProtos.Action action : actions) {
      if (action.hasMutation()) {
        numWrites++;
      } else if (action.hasGet()) {
        numReads++;
      }
    }
    return checkQuota(ugi, table, numWrites, numReads, 0);
  }

  /**
   * Check the quota for the current (rpc-context) user. Returns the OperationQuota used to get the
   * available quota and to report the data/usage of the operation.
   * @param region the region where the operation will be performed
   * @param numWrites number of writes to perform
   * @param numReads number of short-reads to perform
   * @param numScans number of scan to perform
   * @return the OperationQuota
   * @throws ThrottlingException if the operation cannot be executed due to quota exceeded.
   */
  private OperationQuota checkQuota(final HRegion region, final int numWrites, final int numReads,
      final int numScans) throws IOException, ThrottlingException {
    checkQuotaSupport();
    UserGroupInformation ugi = getUserGroupInformation(region);
    TableName table = region.getTableDesc().getTableName();
    OperationQuota quota = null;
    try {
      quota = getQuota(ugi, table);
      quota.checkQuota(numWrites, numReads, numScans);
    } catch (ThrottlingException e) {
      // avoid log too much exception when overload
      if (quota.canLogThrottlingException()) {
        LOG.error("Throttling exception for user=" + ugi.getUserName() + " table=" + table
            + " numWrites=" + numWrites + " numReads=" + numReads + " numScans=" + numScans + ": "
            + e.getMessage());
        LOG.info("Quota snapshot for user=" + ugi.getUserName() + " table=" + table + " : "
            + quota);
      }
      region.getMetrics().updateThrottledRead(numReads + numScans);
      region.getMetrics().updateThrottledWrite(numWrites);
      if (!isThrottleSimulated()) {
        throw e;
      }
    } catch (Throwable t) {
      throw new IOException("Unexcepted exception when check quota", t);
    }
    return quota;
  }

  private OperationQuota checkQuota(final UserGroupInformation ugi, final TableName table,
      final int numWrites, final int numReads, final int numScans) throws IOException,
      ThrottlingException {
    checkQuotaSupport();
    OperationQuota quota = null;
    try {
      quota = getQuota(ugi, table);
      quota.checkQuota(numWrites, numReads, numScans);
    } catch (ThrottlingException e) {
      // avoid log too much exception when overload
      if (quota.canLogThrottlingException()) {
        LOG.error("Throttling exception for user=" + ugi.getUserName() + " table=" + table
            + " numWrites=" + numWrites + " numReads=" + numReads + " numScans=" + numScans + ": "
            + e.getMessage());
        LOG.info("Quota snapshot for user=" + ugi.getUserName() + " table=" + table + " : "
            + quota);
      }
      if (!isThrottleSimulated()) {
        throw e;
      }
    } catch (Throwable t) {
      throw new IOException("Unexcepted exception when check quota", t);
    }
    return quota;
  }

  /**
   * Grab quota after the Get. It will not check quota, just grab by result.
   * @param region
   * @param result
   * @throws IOException
   */
  public void grabQuota(final HRegion region, final Result result) {
    UserGroupInformation ugi = null;
    TableName table = null;
    try {
      ugi = getUserGroupInformation(region);
      table = region.getTableDesc().getTableName();
      grabQuota(ugi, table, result);
    } catch (Throwable t) {
      this.grabQuotaFailedCount.increment();
      LOG.fatal("Unexpected exception when grab quota after Get, user=" + ugi + ", table=" + table,
        t);
    }
  }

  public void grabQuota(final UserGroupInformation ugi, final TableName table, final Result result) {
    OperationQuota quota = getQuota(ugi, table);
    if (quota instanceof AllowExceedOperationQuota) {
      ((AllowExceedOperationQuota) quota)
          .grabQuota(0, calculateRequestUnitNum(result) - 1, 0);
    } else {
      quota.addGetResult(result);
      quota.close();
    }
  }

  /**
   * Grab quota after the Scan. It will not check quota, just grab by results.
   * @param region
   * @param results
   * @throws IOException
   */
  public void grabQuota(final HRegion region, final List<Result> results) {
    UserGroupInformation ugi = null;
    TableName table = null;
    try {
      ugi = getUserGroupInformation(region);
      table = region.getTableDesc().getTableName();
      grabQuota(ugi, table, results);
    } catch (Throwable t) {
      this.grabQuotaFailedCount.increment();
      LOG.fatal("Unexpected exception when grab quota after Scan, user=" + ugi + ", table=" + table,
        t);
    }
  }
  
  public void grabQuota(final UserGroupInformation ugi, final TableName table, final List<Result> results) {
    OperationQuota quota = getQuota(ugi, table);
    if (quota instanceof AllowExceedOperationQuota) {
      ((AllowExceedOperationQuota) quota).grabQuota(0, 0,
        calculateRequestUnitNum(results) - 1);
    } else {
      quota.addScanResult(results);
      quota.close();
    }
  }

  /**
   * Grab quota after the Mutation. It will not check quota, just grab by mutation.
   * @param region
   * @param mutation
   * @throws IOException
   */
  public void grabQuota(final HRegion region, final Mutation mutation) {
    UserGroupInformation ugi = null;
    TableName table = null;
    try {
      ugi = getUserGroupInformation(region);
      table = region.getTableDesc().getTableName();
      grabQuota(ugi, table, mutation);
    } catch (Throwable t) {
      this.grabQuotaFailedCount.increment();
      LOG.fatal("Unexpected exception when grab quota after Mutation, user=" + ugi + ", table=" + table,
        t);
    }
  }
  
  public void grabQuota(final UserGroupInformation ugi, final TableName table, final Mutation mutation) {
    OperationQuota quota = getQuota(ugi, table);
    if (quota instanceof AllowExceedOperationQuota) {
      ((AllowExceedOperationQuota) quota).grabQuota(calculateRequestUnitNum(mutation) - 1, 0,
        0);
    } else {
      quota.addMutation(mutation);
      quota.close();
    }
  }

  /**
   * Grab quota after the RowMutations. It will not check quota, just grab by mutation.
   * @param region
   * @param mutation
   * @throws IOException
   */
  public void grabQuota(final HRegion region, final RowMutations rowMutations) throws IOException {
    UserGroupInformation ugi = null;
    TableName table = null;
    try {
      ugi = getUserGroupInformation(region);
      table = region.getTableDesc().getTableName();
      for (Mutation mutation : rowMutations.getMutations()) {
        grabQuota(ugi, table, mutation);
      }
    } catch (Throwable t) {
      this.grabQuotaFailedCount.increment();
      LOG.fatal("Unexpected exception when grab quota after RowMutations, user=" + ugi + ", table=" + table,
        t);
    }
  }

  public long getGrabQuotaFailedCount() {
    return this.grabQuotaFailedCount.get();
  }

  private void checkQuotaSupport() throws IOException {
    if (!isQuotaEnabled()) {
      throw new DoNotRetryIOException(
        new UnsupportedOperationException("quota support disabled"));
    }
  }
  
  public int calculateRequestUnitNum(final Result result) {
    return (int) calculateReadCapacityUnitNum(QuotaUtil.calculateResultSize(result));
  }

  public int calculateRequestUnitNum(final List<Result> results) {
    return (int) calculateReadCapacityUnitNum(QuotaUtil.calculateResultSize(results));
  }

  public int calculateRequestUnitNum(final Mutation mutation) {
    return (int) calculateWriteCapacityUnitNum(QuotaUtil.calculateMutationSize(mutation));
  }

  public long calculateReadCapacityUnitNum(final long size) {
    return (long) Math.ceil(size * 1.0 / this.readCapacityUnit);
  }

  public long calculateWriteCapacityUnitNum(final long size) {
    return (long) Math.ceil(size * 1.0 / this.writeCapacityUnit);
  }

  public long getQuotaReadAvailable(OperationQuota quota) {
    if (quota instanceof AllowExceedOperationQuota) {
      return quota.getReadAvailable() * this.readCapacityUnit;
    }
    return quota.getReadAvailable();
  }

  public long getQuotaWriteAvailable(OperationQuota quota) {
    if (quota instanceof AllowExceedOperationQuota) {
      return quota.getWriteAvailable() * this.writeCapacityUnit;
    }
    return quota.getWriteAvailable();
  }
}
