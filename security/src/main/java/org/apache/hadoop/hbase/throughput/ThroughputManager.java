/*
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

package org.apache.hadoop.hbase.throughput;

import java.io.IOException;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.RegionStatistics;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

/**
 * A manager class manages in-memory limiter data structures.
 * <p>
 * When the throughput quota limits for a user is specified, a in-memory
 * {@code ThroughputLimiter} object is created and indexed with table name,
 * user name and request type. To simplify the implementation, the quota
 * of each {@code ThroughputLimiter} holds the total quota of the whole
 * cluster instead of the portion of the current region server. To address
 * this problem, a scale factor is applied when acquiring tokens based on
 * the portion of regions assigned on this region server. For example,
 * if table 't1' has 100 active regions and 10 regions on the current region
 * server, then each request will try to acquire 10 tokens.
 * </p>
 */
@ThreadSafe
public class ThroughputManager {
  public static final Log LOG = LogFactory.getLog(ThroughputManager.class);

  /*
   * A process may have multiple ZooKeeperWatcher instances, for example, in standalone mode.
   */
  private static final ConcurrentMap<ZooKeeperWatcher, ThroughputManager> instances =
      new ConcurrentHashMap<ZooKeeperWatcher, ThroughputManager>();

  private final ConcurrentMap<byte[], Map<String, EnumMap<RequestType, ThroughputLimiter>>> limitersByTable =
      new ConcurrentSkipListMap<byte[], Map<String, EnumMap<RequestType, ThroughputLimiter>>>(
          Bytes.BYTES_COMPARATOR);
  private final ZKThrouputQuotaWatcher watcher;

  private ThroughputManager(ZooKeeperWatcher watcher, Configuration conf) {
    this.watcher = new ZKThrouputQuotaWatcher(watcher, this, conf);

    try {
      this.watcher.start();
    } catch (KeeperException ke) {
      LOG.error("ZooKeeper initialization failed", ke);
    }
  }

  public static ThroughputManager get(
      ZooKeeperWatcher watcher, Configuration conf) throws IOException {
    ThroughputManager instance = instances.get(watcher);
    if (instance == null) {
      instance = new ThroughputManager(watcher, conf);
      ThroughputManager prev = instances.putIfAbsent(watcher, instance);
      if (prev != null) {
        instance = prev;
      }
    }
    return instance;
  }

  /**
   * Remove throughput quota limiter.
   */
  public void removeTableLimiters(String tableName) {
    limitersByTable.remove(Bytes.toBytes(tableName));
  }

  private Map<String, EnumMap<RequestType, ThroughputLimiter>> initTableLimiters(
      Map<String, EnumMap<RequestType, Double>> limits) {
    Map<String, EnumMap<RequestType, ThroughputLimiter>> tableLimiters =
        new HashMap<String, EnumMap<RequestType, ThroughputLimiter>>();
    for (Entry<String, EnumMap<RequestType, Double>> e1 : limits.entrySet()) {
      String userName = e1.getKey();
      EnumMap<RequestType, ThroughputLimiter> reqLimiters =
          new EnumMap<RequestType, ThroughputLimiter>(RequestType.class);
      for (Entry<RequestType, Double> e2 : e1.getValue().entrySet()) {
        RequestType requestType = e2.getKey();
        Double limit = e2.getValue();
        if (ThroughputQuota.isValidLimit(limit)) {
          reqLimiters.put(requestType, new ThroughputLimiter(limit));
        }
      }
      if (!reqLimiters.isEmpty()) {
        tableLimiters.put(userName, reqLimiters);
      }
    }
    return tableLimiters;
  }

  /**
   * Update the in-memory throughput limiters.
   */
  public void refreshTableLimiters(String tableName, byte[] data) throws IOException {
    ThroughputQuota newQuota = ThroughputQuota.fromBytes(data);
    Map<String, EnumMap<RequestType, ThroughputLimiter>> newLimiters =
        initTableLimiters(newQuota.getLimits());
    /*
     * Override all the existing quota limiters. ConcurrentMap guarantees there is no leakage of
     * partially constructed objects read by reader threads.
     */
    limitersByTable.put(Bytes.toBytes(tableName), newLimiters);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Refresh table limiters for table '" + tableName + "', new limiters = "
          + newLimiters);
    }
  }

  /**
   * Write throughput quota to zookeeper. This is used to notify other region coprocessors.
   */
  public void writeToZookeeper(ThroughputQuota quota)
      throws IOException {
    byte[] data = ThroughputQuota.toBytes(quota);
    if (data != null) {
      watcher.writeToZookeeper(quota.getTableName(), data);
    }
  }

  /**
   * Remove table quota from zookeeper when the table is dropped.
   */
  public void removeFromZookeeper(String tableName) {
    watcher.removeFromZookeeper(tableName);
  }

  public void acquireQuota(User user, byte[] tableName, RequestType requestType, long timeout,
      TimeUnit timeUnit, RegionStatistics stats) throws ThroughputExceededException {
    String userName = user.getShortName();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Try to acquire throughput quota for (" + userName + ", "
          + Bytes.toString(tableName) + ", " + requestType + ")");
    }

    // check per user quota
    ThroughputLimiter userLimiter = getThroughputLimiter(userName,
      ThroughputQuotaTable.WILDCARD_TABLE_NAME, requestType);
    if (userLimiter != null) {
      double tokens = calcGlobalTokens(stats);
      if (!userLimiter.tryAcquire(tokens, timeout, timeUnit)) {
        throw new ThroughputExceededException("Throughput quota exceeds for user " + userName);
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Acquired per user throughput quota for " + userName);
      }
    }

    // check per (user, table) quota
    ThroughputLimiter userTableLimiter = getThroughputLimiter(userName, tableName,
      requestType);
    if (userTableLimiter != null) {
      double tokens = calcPerTableTokens(stats, tableName);
      if (!userTableLimiter.tryAcquire(tokens, timeout, timeUnit)) {
        throw new ThroughputExceededException("Throughput quota exceeds for user "
            + userName + ", table " + Bytes.toString(tableName));
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Acquired per (user, table) throughput quota for (" + userName + ", "
            + Bytes.toString(tableName) + ")");
      }
    }
  }

  /*
   * Per table limiters is immutable after inserted to table map, so this method is thread-safe
   */
  protected ThroughputLimiter getThroughputLimiter(String userName, byte[] tableName,
      RequestType requestType) {
    Map<String, EnumMap<RequestType, ThroughputLimiter>> tableLimiters =
        limitersByTable.get(tableName);
    ThroughputLimiter limiter = null;
    if (tableLimiters != null) {
      EnumMap<RequestType, ThroughputLimiter> reqLimiters = tableLimiters.get(userName);
      if (reqLimiters != null) {
        limiter = reqLimiters.get(requestType);
      }
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Get throughput quota limiter for (" + userName + ", "
          + Bytes.toString(tableName) + ", "
          + requestType + "), limiter = " + limiter);
    }

    return limiter;
  }

  /*
   * Calculate how many tokens to request for each operation.
   */
  private double calcGlobalTokens(RegionStatistics stats) {
    if (stats == null) {
      LOG.warn("Region statistics is missing when calculating global token counts");
      return 1.0;
    }

    int totalRegions = stats.getRegionCount();
    int localRegions = stats.getRegionCountRS();

    if (LOG.isDebugEnabled()) {
      LOG.debug("Cluster total regions " + totalRegions + ", local regions " + localRegions);
    }
    localRegions = localRegions == 0 ? 1 : localRegions;
    return totalRegions / (double) localRegions;
  }

  private double calcPerTableTokens(RegionStatistics stats, byte[] tableName) {
    if (stats != null) {
      Map<byte[], Pair<Integer, Integer>> c = stats.getRegionCountPerTable();
      if (c != null) {
        Pair<Integer, Integer> p = c.get(tableName);
        if (p != null) {
          int totalRegions = p.getFirst();
          int localRegions = p.getSecond();
          if (LOG.isDebugEnabled()) {
            LOG.debug("Table " + Bytes.toString(tableName) + " total regions " + totalRegions
                + ", local regions " + localRegions);
          }
          localRegions = localRegions == 0 ? 1 : localRegions;
          return totalRegions / (double) localRegions;
        }
      }
    }
    LOG.warn("Region statistics is missing when calculating token counts for table: "
        + Bytes.toString(tableName));
    return 1.0;
  }
}
