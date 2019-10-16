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
import java.util.Optional;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.ipc.RpcScheduler;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.quotas.OperationQuota.ReadOperationType;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.xiaomi.infra.thirdparty.com.google.common.annotations.VisibleForTesting;

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
public class RegionServerRpcQuotaManager {
  private static final Logger LOG = LoggerFactory.getLogger(RegionServerRpcQuotaManager.class);

  private final RegionServerServices rsServices;

  private QuotaCache quotaCache = null;
  private volatile boolean rpcThrottleEnabled;
  // Storage for quota rpc throttle
  private RpcThrottleStorage rpcThrottleStorage;
  private final long writeCapacityUnit;
  private final long readCapacityUnit;

  public RegionServerRpcQuotaManager(final RegionServerServices rsServices) {
    this.rsServices = rsServices;
    rpcThrottleStorage =
        new RpcThrottleStorage(rsServices.getZooKeeper(), rsServices.getConfiguration());
    this.writeCapacityUnit = rsServices.getConfiguration()
        .getLong(QuotaUtil.WRITE_CAPACITY_UNIT_CONF_KEY, QuotaUtil.DEFAULT_WRITE_CAPACITY_UNIT);
    this.readCapacityUnit = rsServices.getConfiguration()
        .getLong(QuotaUtil.READ_CAPACITY_UNIT_CONF_KEY, QuotaUtil.DEFAULT_READ_CAPACITY_UNIT);
  }

  public void start(final RpcScheduler rpcScheduler) throws IOException {
    if (!QuotaUtil.isQuotaEnabled(rsServices.getConfiguration())) {
      LOG.info("Quota support disabled");
      return;
    }

    LOG.info("Initializing RPC quota support");

    // Initialize quota cache
    quotaCache = new QuotaCache(rsServices);
    quotaCache.start();
    rpcThrottleEnabled = rpcThrottleStorage.isRpcThrottleEnabled();
    LOG.info("Start rpc quota manager and rpc throttle enabled is {}", rpcThrottleEnabled);
  }

  public void stop() {
    if (isQuotaEnabled()) {
      quotaCache.stop("shutdown");
    }
  }

  @VisibleForTesting
  protected boolean isRpcThrottleEnabled() {
    return rpcThrottleEnabled;
  }

  private boolean isQuotaEnabled() {
    return quotaCache != null;
  }

  public void switchRpcThrottle(boolean enable) throws IOException {
    if (isQuotaEnabled()) {
      if (rpcThrottleEnabled != enable) {
        boolean previousEnabled = rpcThrottleEnabled;
        rpcThrottleEnabled = rpcThrottleStorage.isRpcThrottleEnabled();
        LOG.info("Switch rpc throttle from {} to {}", previousEnabled, rpcThrottleEnabled);
      } else {
        LOG.warn(
          "Skip switch rpc throttle because previous value {} is the same as current value {}",
          rpcThrottleEnabled, enable);
      }
    } else {
      LOG.warn("Skip switch rpc throttle to {} because rpc quota is disabled", enable);
    }
  }

  @VisibleForTesting
  QuotaCache getQuotaCache() {
    return quotaCache;
  }

  /**
   * Returns the quota for an operation.
   *
   * @param ugi the user that is executing the operation
   * @param region the region where the operation will be executed
   * @return the OperationQuota
   */
  private OperationQuota getQuota(final UserGroupInformation ugi, final Region region) {
    TableName table = region.getTableDescriptor().getTableName();
    if (isQuotaEnabled() && !table.isSystemTable() && isRpcThrottleEnabled()) {
      UserQuotaState userQuotaState = quotaCache.getUserQuotaState(ugi);
      QuotaLimiter userLimiter = userQuotaState.getTableLimiter(table);
      boolean useNoop = userLimiter.isBypass();
      if (userQuotaState.hasBypassGlobals()) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("get quota for ugi={} table={} userLimiter={}", ugi, table, userLimiter);
        }
        if (!useNoop) {
          return new DefaultOperationQuota(this.rsServices.getConfiguration(), userLimiter);
        }
      } else {
        QuotaLimiter nsLimiter = quotaCache.getNamespaceLimiter(table.getNamespaceAsString());
        QuotaLimiter tableLimiter = quotaCache.getTableLimiter(table);
        QuotaLimiter rsLimiter = quotaCache
            .getRegionServerQuotaLimiter(QuotaTableUtil.QUOTA_REGION_SERVER_ROW_KEY);
        useNoop &= tableLimiter.isBypass() && nsLimiter.isBypass() && rsLimiter.isBypass();
        boolean exceedThrottleQuotaEnabled = quotaCache.isExceedThrottleQuotaEnabled();
        if (LOG.isTraceEnabled()) {
          LOG.trace(
            "get quota for ugi={} table={} userLimiter={} tableLimiter={} nsLimiter={} "
                + "rsLimiter={} exceedThrottleQuotaEnabled={}",
            ugi, table, userLimiter, tableLimiter, nsLimiter, rsLimiter,
            exceedThrottleQuotaEnabled);
        }
        if (!useNoop) {
          if (exceedThrottleQuotaEnabled) {
            return new ExceedOperationQuota(this.rsServices.getConfiguration(), rsLimiter,
                userLimiter, tableLimiter, nsLimiter);
          } else {
            return new DefaultOperationQuota(this.rsServices.getConfiguration(), userLimiter,
                tableLimiter, nsLimiter, rsLimiter);
          }
        }
      }
    }
    return NoopOperationQuota.get();
  }

  /**
   * Check the read quota for the current (rpc-context) user.
   * Returns the OperationQuota used to get the available quota and
   * to report the data/usage of the operation.
   * @param region the region where the read operation will be performed
   * @param type the operation type
   * @return the OperationQuota
   * @throws RpcThrottlingException if the operation cannot be executed due to quota exceeded.
   */
  public OperationQuota checkQuota(final Region region, final ReadOperationType type)
      throws IOException, RpcThrottlingException {
    switch (type) {
      case SCAN:
        return checkReadQuota(region, 0, 1);
      case GET:
        return checkReadQuota(region, 1, 0);
    }
    throw new RuntimeException("Invalid operation type: " + type);
  }

  /**
   * Check the write quota for the current (rpc-context) user.
   * @param region the region where the write operation will be performed
   * @param mutation the mutation
   * @throws RpcThrottlingException if the operation cannot be executed due to quota exceeded.
   */
  public void checkQuota(final Region region, final Mutation mutation)
      throws IOException, RpcThrottlingException {
    long size = QuotaUtil.calculateMutationSize(mutation);
    checkWriteQuota(region, 1, size);
  }

  /**
   * Check the write quota for the current (rpc-context) user.
   * @param region the region where the write operation will be performed
   * @param mutations the mutation list
   * @throws RpcThrottlingException if the operation cannot be executed due to quota exceeded.
   */
  public void checkQuota(final Region region, final List<Mutation> mutations)
      throws IOException, RpcThrottlingException {
    long writeSize =
        mutations.stream().mapToLong(mutation -> QuotaUtil.calculateMutationSize(mutation)).sum();
    checkWriteQuota(region, mutations.size(), writeSize);
  }

  /**
   * Check the read quota for the current (rpc-context) user.
   * Returns the OperationQuota used to get the available quota and
   * to report the data/usage of the operation.
   * @param region the region where the read operation will be performed
   * @param numReads number of short-reads to perform
   * @param numScans number of scan to perform
   * @return the OperationQuota
   * @throws RpcThrottlingException if the operation cannot be executed due to quota exceeded.
   */
  private OperationQuota checkReadQuota(final Region region, final int numReads, final int numScans)
      throws IOException, RpcThrottlingException {
    UserGroupInformation ugi = getRequestUser();
    OperationQuota quota = getQuota(ugi, region);
    try {
      quota.checkReadQuota(numReads, numScans);
    } catch (RpcThrottlingException e) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Throttling exception for user={} table={} numReads={} numScans={}: {}",
          ugi.getUserName(), region.getTableDescriptor().getTableName(), numReads, numScans,
          e.getMessage());
      }
      throw e;
    }
    return quota;
  }

  /**
   * Check the write quota for the current (rpc-context) user.
   * @param region the region where the write operation will be performed
   * @param numWrites number of write to perform
   * @param writeSize data size in bytes of write to perform
   * @throws RpcThrottlingException if the operation cannot be executed due to quota exceeded.
   */
  private void checkWriteQuota(final Region region, final int numWrites,
      final long writeSize) throws IOException, RpcThrottlingException {
    UserGroupInformation ugi = getRequestUser();
    OperationQuota quota = getQuota(ugi, region);
    try {
      quota.checkWriteQuota(numWrites, writeSize);
    } catch (RpcThrottlingException e) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Throttling exception for user={} table={} numWrites={} writeSize={}: {}",
          ugi.getUserName(), region.getTableDescriptor().getTableName(), numWrites, writeSize,
          e.getMessage());
      }
      throw e;
    }
  }

  public long calculateWriteCapacityUnit(final long size) {
    return (long) Math.ceil(size * 1.0 / this.writeCapacityUnit);
  }

  public long calculateReadCapacityUnit(final long size) {
    return (long) Math.ceil(size * 1.0 / this.readCapacityUnit);
  }

  private UserGroupInformation getRequestUser() throws IOException {
    Optional<User> user = RpcServer.getRequestUser();
    UserGroupInformation ugi;
    if (user.isPresent()) {
      ugi = user.get().getUGI();
    } else {
      ugi = User.getCurrent().getUGI();
    }
    return ugi;
  }
}
