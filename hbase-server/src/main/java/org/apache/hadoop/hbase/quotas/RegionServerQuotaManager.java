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
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.ipc.RpcScheduler;
import org.apache.hadoop.hbase.ipc.RequestContext;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.zookeeper.RegionQuotaTracker;
import org.apache.hadoop.security.UserGroupInformation;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

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

  private static final int HUGE_OPERATION_QUOTA_THRESHOLD = 1000;
  private final Cache<TableName, Boolean> hugeOperationMap = CacheBuilder.newBuilder()
      .expireAfterWrite(5, TimeUnit.MINUTES).build();

  private final RegionServerServices rsServices;
  // Region quota tracker
  private final RegionQuotaTracker regionQuotaTracker;

  private QuotaCache quotaCache = null;

  private boolean enabled = false;
  private boolean isSimulated = false;

  private LongAdder grabQuotaFailedCount = new LongAdder();

  private final boolean allowExceed;

  private final int readCapacityUnit;
  private final int writeCapacityUnit;
  private final int scanCapacityUnit;

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
    this.scanCapacityUnit = this.conf.getInt(QuotaUtil.SCAN_CAPACITY_UNIT_CONF_KEY,
      QuotaUtil.DEFAULT_SCAN_CAPACITY_UNIT);
    this.regionQuotaTracker = new RegionQuotaTracker(rsServices.getZooKeeper(), rsServices);
    this.regionQuotaTracker.start();
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

    this.grabQuotaFailedCount.reset();
    enabled = true;
  }

  public void stop() {
    if (enabled) {
      quotaCache.stop("shutdown");
    }
  }
  
  public boolean isStopped() {
    if (enabled) {
      return quotaCache.isStopped();
    }
    return false;
  }

  public boolean isQuotaEnabled() {
    return enabled || regionQuotaTracker.getRegionReadLimiters().size() > 0
        || regionQuotaTracker.getRegionWriteLimiters().size() > 0;
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
   * @param region the region where the operation will be executed
   * @return the OperationQuota
   */
  @VisibleForTesting
  protected OperationQuota getQuota(final UserGroupInformation ugi, final HRegion region) {
    OperationQuota regionQuota = getRegionQuota(ugi, region);
    if (regionQuota != null) {
      return regionQuota;
    }
    final TableName table = region.getTableDesc().getTableName();
    if (enabled && !table.isSystemTable()) {
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

  /**
   * Returns the region quota which is a hard limit
   */
  private OperationQuota getRegionQuota(final UserGroupInformation ugi, final HRegion region) {
    TableName table = region.getTableDesc().getTableName();
    if (table.isSystemTable()) {
      return null;
    }
    String regionName = region.getRegionInfo().getEncodedName();
    ConcurrentHashMap<String, QuotaLimiter> regionReadLimiter =
        regionQuotaTracker.getRegionReadLimiters();
    ConcurrentHashMap<String, QuotaLimiter> regionWriteLimiter =
        regionQuotaTracker.getRegionWriteLimiters();
    QuotaLimiter readLimiter = regionReadLimiter.getOrDefault(regionName, null);
    QuotaLimiter writeLimiter = regionWriteLimiter.getOrDefault(regionName, null);
    if (readLimiter != null || writeLimiter != null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Throttle region " + regionName + " by hard limit: read limiter: " + readLimiter
            + ", write limiter: " + writeLimiter);
      }
      if (enabled) {
        // get RS and user table limiter
        QuotaLimiter rsLimiter = quotaCache.getRegionServerLimiter();
        QuotaLimiter userLimiter = quotaCache.getUserQuotaState(ugi).getTableLimiter(table);
        return new RegionOperationQuota(readLimiter, writeLimiter, userLimiter, rsLimiter);
      } else {
        return new RegionOperationQuota(readLimiter, writeLimiter);
      }
    } else {
      return null;
    }
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
    UserGroupInformation ugi = getUserGroupInformation(region);
    return checkQuota(ugi, region, type);
  }

  private OperationQuota checkQuota(final UserGroupInformation ugi, final HRegion region,
      final OperationQuota.OperationType type) throws IOException, ThrottlingException {
    switch (type) {
      case SCAN:
        return checkQuota(ugi, region, 0, 0, 1);
      case GET:
        return checkQuota(ugi, region, 0, 1, 0);
      case MUTATE:
        return checkQuota(ugi, region, 1, 0, 0);
    }
    throw new RuntimeException("Invalid operation type: " + type);
  }

  /**
   * Check quota for one Mutation.
   */
  public void checkQuota(final HRegion region, final Mutation mutation)
      throws IOException, ThrottlingException {
    UserGroupInformation ugi = getUserGroupInformation(region);
    checkQuota(ugi, region, calculateWriteCapacityUnitNum(mutation), 0, 0);
  }

  /**
   * Check quota for RowMutations.
   */
  public void checkQuota(final HRegion region, final RowMutations rowMutations)
      throws IOException {
    UserGroupInformation ugi = getUserGroupInformation(region);
    int numWrites = calculateWriteCapacityUnitNum(rowMutations);
    checkQuota(ugi, region, numWrites, 0, 0);
  }

  /**
   * Check quota for a list of Mutation.
   */
  public void checkQuota(final HRegion region, final List<Mutation> mutations)
      throws IOException, ThrottlingException {
    UserGroupInformation ugi = getUserGroupInformation(region);
    checkQuota(ugi, region, calculateWriteCapacityUnitNum(mutations), 0, 0);
  }

  private OperationQuota checkQuota(final UserGroupInformation ugi, final HRegion region,
      final int numWrites, final int numReads, final int numScans)
      throws IOException, ThrottlingException {
    checkQuotaSupport();
    OperationQuota quota = null;
    TableName table = region.getTableDesc().getTableName();
    try {
      quota = getQuota(ugi, region);
      if (numWrites > HUGE_OPERATION_QUOTA_THRESHOLD || numReads > HUGE_OPERATION_QUOTA_THRESHOLD
          || numScans > HUGE_OPERATION_QUOTA_THRESHOLD) {
        if (!hugeOperationMap.get(table, () -> false)) {
          LOG.warn("HUGE operation for user=" + ugi.getUserName() + " table=" + table
              + ", need quota numWrites=" + numWrites + " numReads=" + numReads + " numScans="
              + numScans);
          hugeOperationMap.put(table, true);
        }
      }
      quota.checkQuota(numWrites, numReads, numScans);
    } catch (ThrottlingException e) {
      // avoid log too much exception when overload
      if (quota.canLogThrottlingException()) {
        LOG.error("Throttling exception for user=" + ugi.getUserName() + " table=" + table
            + " numWrites=" + numWrites + " numReads=" + numReads + " numScans=" + numScans + ": "
            + e.getMessage());
        LOG.info(
          "Quota snapshot for user=" + ugi.getUserName() + " table=" + table + " : " + quota);
      }
      if (!isThrottleSimulated()) {
        throw e;
      }
    } catch (Throwable t) {
      LOG.error("Unexcepted exception when check quota", t);
      throw new IOException("Unexcepted exception when check quota", t);
    }
    return quota;
  }

  /**
   * Grab quota after Get. It will not check quota, just grab by result.
   */
  public void grabQuota(final HRegion region, final Result result) {
    UserGroupInformation ugi = null;
    TableName table = null;
    try {
      ugi = getUserGroupInformation(region);
      table = region.getTableDesc().getTableName();
      grabQuota(ugi, region, 0, calculateReadCapacityUnitNum(result) - 1, 0);
    } catch (Throwable t) {
      this.grabQuotaFailedCount.increment();
      LOG.error("Unexpected exception when grab quota after Get, user=" + ugi + ", table=" + table,
        t);
    }
  }

  /**
   * Grab quota after Scan. It will not check quota, just grab by results.
   */
  public void grabQuota(final HRegion region, final List<Result> results) {
    UserGroupInformation ugi = null;
    TableName table = null;
    try {
      ugi = getUserGroupInformation(region);
      table = region.getTableDesc().getTableName();
      grabQuota(ugi, region, 0, 0, calculateReadCapacityUnitNum(results) - 1);
    } catch (Throwable t) {
      this.grabQuotaFailedCount.increment();
      LOG.error("Unexpected exception when grab quota after Scan, user=" + ugi + ", table=" + table,
        t);
    }
  }

  /**
   * Grab quota after Append.
   */
  public void grabQuota(final HRegion region, final List<Cell> result, final Mutation mutation) {
    UserGroupInformation ugi = null;
    TableName table = null;
    try {
      ugi = getUserGroupInformation(region);
      table = region.getTableDesc().getTableName();
      grabQuota(ugi, region,
        (int) calculateWriteCapacityUnitNum(QuotaUtil.calculateCellsSize(result))
            - calculateWriteCapacityUnitNum(mutation),
        0, 0);
    } catch (Throwable t) {
      this.grabQuotaFailedCount.increment();
      LOG.fatal(
        "Unexpected exception when grab quota after RowMutations, user=" + ugi + ", table=" + table,
        t);
    }
  }

  private void grabQuota(final UserGroupInformation ugi, final HRegion region, final int numWrites,
      final int numReads, final int numScans)
      throws ExecutionException {
    TableName table = region.getTableDesc().getTableName();
    OperationQuota quota = getQuota(ugi, region);
    if (quota instanceof AllowExceedOperationQuota) {
      if (numWrites > HUGE_OPERATION_QUOTA_THRESHOLD || numReads > HUGE_OPERATION_QUOTA_THRESHOLD
          || numScans > HUGE_OPERATION_QUOTA_THRESHOLD) {
        if (!hugeOperationMap.get(table, () -> false)) {
          LOG.warn("HUGE operation for user=" + ugi.getUserName() + " table=" + table
              + ", need quota numWrites=" + numWrites + " numReads=" + numReads + " numScans="
              + numScans);
          hugeOperationMap.put(table, true);
        }
      }
      ((AllowExceedOperationQuota) quota).grabQuota(numWrites, numReads, numScans);
    } else {
      quota.grabQuota(numWrites, numReads, numScans);
    }
  }

  public long getGrabQuotaFailedCount() {
    return this.grabQuotaFailedCount.sum();
  }

  private void checkQuotaSupport() throws IOException {
    if (!isQuotaEnabled()) {
      throw new DoNotRetryIOException(
        new UnsupportedOperationException("quota support disabled"));
    }
  }

  private int calculateReadCapacityUnitNum(final Result result) {
    return (int) calculateReadCapacityUnitNum(QuotaUtil.calculateResultSize(result));
  }

  private int calculateReadCapacityUnitNum(final List<Result> results) {
    return (int) calculateScanCapacityUnitNum(QuotaUtil.calculateResultSize(results));
  }

  private int calculateWriteCapacityUnitNum(final Mutation mutation) {
    return (int) calculateWriteCapacityUnitNum(QuotaUtil.calculateMutationSize(mutation));
  }

  private int calculateWriteCapacityUnitNum(final RowMutations rowMutations) {
    return calculateWriteCapacityUnitNum(rowMutations.getMutations());
  }

  private int calculateWriteCapacityUnitNum(final List<Mutation> mutations) {
    if (mutations.size() > QuotaUtil.HUGE_MUTATIONS_NUMBER) {
      // For huge multi-put, calculate extra quota one by one
      int writeCapacityUnitNum = (int) calculateWriteCapacityUnitNum(
        QuotaUtil.calculateMutationSize(mutations.subList(0, QuotaUtil.HUGE_MUTATIONS_NUMBER)));
      int extraWriteCapacityUnitNum = (int) mutations.stream().skip(QuotaUtil.HUGE_MUTATIONS_NUMBER)
          .mapToLong(
            mutation -> calculateWriteCapacityUnitNum(QuotaUtil.calculateMutationSize(mutation)))
          .sum();
      return writeCapacityUnitNum + extraWriteCapacityUnitNum;
    } else {
      return (int) calculateWriteCapacityUnitNum(QuotaUtil.calculateMutationSize(mutations));
    }
  }

  public long calculateReadCapacityUnitNum(final long size) {
    return (long) Math.ceil(size * 1.0 / this.readCapacityUnit);
  }

  public long calculateWriteCapacityUnitNum(final long size) {
    if (size > QuotaUtil.HUGE_MUTATION_SIZE) {
      // For huge mutation, the quota will multiply by HUGE_MUTATION_QUOTA_MULTIPLIER
      return (long) Math.ceil(size * 1.0 / this.writeCapacityUnit)
          * QuotaUtil.HUGE_MUTATION_QUOTA_MULTIPLIER;
    } else {
      return (long) Math.ceil(size * 1.0 / this.writeCapacityUnit);
    }
  }

  public long calculateScanCapacityUnitNum(final long size) {
    return (long) Math.ceil(size * 1.0 / this.scanCapacityUnit);
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

  @VisibleForTesting
  public RegionQuotaTracker getRegionQuotaTracker() {
    return regionQuotaTracker;
  }
}
