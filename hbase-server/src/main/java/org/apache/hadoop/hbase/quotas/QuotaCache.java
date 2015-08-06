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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Chore;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.TimeUnit;
import org.apache.hadoop.hbase.protobuf.generated.QuotaProtos.Throttle;
import org.apache.hadoop.hbase.protobuf.generated.QuotaProtos.TimedQuota;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos.RegionServerReportResponse;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos.TableRegionCount;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.security.UserGroupInformation;

import com.google.common.annotations.VisibleForTesting;

/**
 * Cache that keeps track of the quota settings for the users and tables that
 * are interacting with it.
 *
 * To avoid blocking the operations if the requested quota is not in cache
 * an "empty quota" will be returned and the request to fetch the quota information
 * will be enqueued for the next refresh.
 *
 * TODO: At the moment the Cache has a Chore that will be triggered every 5min
 * or on cache-miss events. Later the Quotas will be pushed using the notification system.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class QuotaCache implements Stoppable {
  private static final Log LOG = LogFactory.getLog(QuotaCache.class);

  public static final String REFRESH_CONF_KEY = "hbase.quota.refresh.period";
  private static final int REFRESH_DEFAULT_PERIOD = 5 * 60000; // 5min
  private static final int EVICT_PERIOD_FACTOR = 5; // N * REFRESH_DEFAULT_PERIOD
  static final String REGION_SERVER_READ_LIMIT_KEY = "hbase.regionserver.read.limit";
  static final int DEFAULT_REGION_SERVER_READ_LIMIT = 3000;
  static final String REGION_SERVER_WRITE_LIMIT_KEY = "hbase.regionserver.write.limit";
  static final int DEFAULT_REGION_SERVER_WRITE_LIMIT = 10000;
  

  // for testing purpose only, enforce the cache to be always refreshed
  static boolean TEST_FORCE_REFRESH = false;

  private final ConcurrentHashMap<String, QuotaState> namespaceQuotaCache =
      new ConcurrentHashMap<String, QuotaState>();
  private final ConcurrentHashMap<TableName, QuotaState> tableQuotaCache =
      new ConcurrentHashMap<TableName, QuotaState>();
  private final ConcurrentHashMap<String, UserQuotaState> userQuotaCache =
      new ConcurrentHashMap<String, UserQuotaState>();
  private final QuotaLimiter rsQuotaLimiter;
  private final RegionServerServices rsServices;
  private Map<TableName, Double> localQuotaFactors = new ConcurrentHashMap<TableName, Double>();
  private QuotaRefresherChore refreshChore;
  private boolean stopped = true;

  public QuotaCache(final RegionServerServices rsServices) {
    this.rsServices = rsServices;
    Configuration conf = getConfiguration();
    rsQuotaLimiter = createRegionServerQuotaLimiter(conf);
  }
  
  /**
   * Returns the QuotaLimiter of regionserver
   * @param conf
   * @return
   */
  public static QuotaLimiter createRegionServerQuotaLimiter(Configuration conf) {
    Throttle.Builder throttle = Throttle.newBuilder();
    TimedQuota.Builder readQuota = TimedQuota.newBuilder();
    readQuota.setSoftLimit(conf.getInt(REGION_SERVER_READ_LIMIT_KEY,
      DEFAULT_REGION_SERVER_READ_LIMIT));
    readQuota.setTimeUnit(TimeUnit.SECONDS);
    TimedQuota.Builder writeQuota = TimedQuota.newBuilder();
    writeQuota.setSoftLimit(conf.getInt(REGION_SERVER_WRITE_LIMIT_KEY,
      DEFAULT_REGION_SERVER_WRITE_LIMIT));
    writeQuota.setTimeUnit(TimeUnit.SECONDS);
    throttle.setReadNum(readQuota);
    throttle.setWriteNum(writeQuota);
    return QuotaLimiterFactory.fromThrottle(throttle.build());
  }

  /**
   * when fetch quota period, need update regionserver quotalimiter, too.
   */
  private void updateRegionServerQuotaLimiter() {
    /** TODO: admin can set regionserver quota by shell
     */
    Configuration conf = getConfiguration();
    QuotaLimiterFactory.update(rsQuotaLimiter, this.createRegionServerQuotaLimiter(conf));
  }
  
  /**
   * Throttling in cluster is implemented by throttling in regionserver.
   * localTableFactors means how many user-table quota can be allocated in this regionserver.
   * when fetch quota, need update local quota factors first.
   * @param regionServerNum
   * @param tableRegionsNumMap
   */
  private void updateLocalQuotaFactors() {
    RegionServerReportResponse.Builder rsrr = RegionServerReportResponse.newBuilder(this.rsServices
        .getRegionServerReportResponse());
    RegionServerReportResponse response = rsrr.build();

    int regionServerNum = response.getServerNum();
    // don't update when no regionserver
    if (regionServerNum == 0) {
      return;
    }

    for (TableRegionCount entry : response.getRegionCountsList()) {
      TableName tableName = ProtobufUtil.toTableName(entry.getTableName());
      
      /** TODO: Case need to handle.
       *  When create new table after this update, it will have no local factor  
       *  so the local quota is equal to the cluster quota
       */
      // if table not on this regionserver, no local factor for this table
      if (!tableQuotaCache.containsKey(tableName)) {
        localQuotaFactors.remove(tableName);
        continue;
      }

      // if table has no regions, no local factor for this table
      int tableRegionsNum = entry.getRegionNum();
      if (tableRegionsNum == 0) {
        localQuotaFactors.remove(tableName);
        continue;
      }

      int localRegionsNum = 0;
      try {
        List<HRegion> localRegions = this.rsServices.getOnlineRegions(tableName);
        if (localRegions != null) {
          localRegionsNum = localRegions.size();
        }
      } catch (IOException e) {
        LOG.info("get online regions of " + tableName + "failed");
      }

      double localFactor = computeLocalFactor(regionServerNum, tableRegionsNum, localRegionsNum);
      LOG.info("For Table : " + tableName + ", localFactor=" + localFactor);
      localQuotaFactors.put(tableName, localFactor);
    }
  }
  
  protected static double computeLocalFactor(int regionServerNum, int tableRegionsNum, int localRegionsNum) {
    /** TODO: Case need to modify better.
     *  the default factor is 1.0, then move a new region to this rs
     *  but the table will get all cluster quota
     */
    // Case default : allocate all cluster quota for this table
    double localFactor = 1.0;
    if (tableRegionsNum == 0 || regionServerNum == 0) {
      return localFactor;
    }
    // Case 1 : table's regions num is smaller than the rs number
    if (tableRegionsNum < regionServerNum) {
      // Case 1.1 : we can average distribute quota by the number of table regions
      if (localRegionsNum >= 1) {
        localFactor = 1.0 / tableRegionsNum;
      }
      // else Case 1.2 is the default case, when table's local regions num is 0, factor is 1.0
    } else {
      // Case 2 : table's regions number is more or equal to the rs number
      // Case 2.1 : we can average distribute quota by the number of regionserver
      if (tableRegionsNum % regionServerNum == 0) {
        localFactor = 1.0 / regionServerNum;
      } else {
        // Case 2.2 : we cann't average distribute quota
        double averageRegionsNum = tableRegionsNum * 1.0 / regionServerNum;
        if (localRegionsNum >= averageRegionsNum) {
          // Case 2.2.1 : distribute more quota than the average
          localFactor = Math.ceil(averageRegionsNum) / tableRegionsNum;
        } else {
          // Case 2.2.2 : distribute fewer quota than the average
          localFactor = Math.floor(averageRegionsNum) / tableRegionsNum;
        }
      }
    }
    LOG.info("regionServerNum=" + regionServerNum + ", tableRegionsNum=" + tableRegionsNum
        + ", localRegionsNum=" + localRegionsNum + ", compute localFactor=" + localFactor);
    return localFactor;
  }
  
  public void start() throws IOException {
    stopped = false;

    // TODO: This will be replaced once we have the notification bus ready.
    Configuration conf = getConfiguration();
    int period = conf.getInt(REFRESH_CONF_KEY, REFRESH_DEFAULT_PERIOD);
    refreshChore = new QuotaRefresherChore(period, this);
    Threads.setDaemonThreadRunning(refreshChore.getThread());
  }

  @Override
  public void stop(final String why) {
    stopped = true;
  }

  @Override
  public boolean isStopped() {
    return stopped;
  }

  /**
   * Returns the limiter associated to the specified user/table.
   *
   * @param ugi the user to limit
   * @param table the table to limit
   * @return the limiter associated to the specified user/table
   */
  public QuotaLimiter getUserLimiter(final UserGroupInformation ugi, final TableName table) {
    if (table.isSystemTable()) {
      return NoopQuotaLimiter.get();
    }
    return getUserQuotaState(ugi).getTableLimiter(table);
  }

  /**
   * Returns the QuotaState associated to the specified user.
   * TODO: Case need to handle. 
   * When get UserQuotaState, need get first, then the ugi will be put to the userQuotaCache
   * In the next update, fetch will make get by the keys of userQuotaCache, then get user quota
   * so the first request cann't be throttle
   * @param ugi the user
   * @return the quota info associated to specified user
   */
  public UserQuotaState getUserQuotaState(final UserGroupInformation ugi) {
    String key = ugi.getShortUserName();
    UserQuotaState quotaInfo = userQuotaCache.get(key);
    if (quotaInfo == null) {
      quotaInfo = new UserQuotaState();
      if (userQuotaCache.putIfAbsent(key, quotaInfo) == null) {
        triggerCacheRefresh();
      }
    }
    return quotaInfo;
  }

  /**
   * Returns the limiter associated to the specified table.
   *
   * @param table the table to limit
   * @return the limiter associated to the specified table
   */
  public QuotaLimiter getTableLimiter(final TableName table) {
    return getQuotaState(this.tableQuotaCache, table).getGlobalLimiter();
  }

  /**
   * Returns the limiter associated to the specified namespace.
   *
   * @param namespace the namespace to limit
   * @return the limiter associated to the specified namespace
   */
  public QuotaLimiter getNamespaceLimiter(final String namespace) {
    return getQuotaState(this.namespaceQuotaCache, namespace).getGlobalLimiter();
  }

  /**
   * Returns the QuotaState requested.
   * If the quota info is not in cache an empty one will be returned
   * and the quota request will be enqueued for the next cache refresh.
   */
  private <K> QuotaState getQuotaState(final ConcurrentHashMap<K, QuotaState> quotasMap,
      final K key) {
    QuotaState quotaInfo = quotasMap.get(key);
    if (quotaInfo == null) {
      quotaInfo = new QuotaState();
      if (quotasMap.putIfAbsent(key, quotaInfo) == null) {
        triggerCacheRefresh();
      }
    }
    return quotaInfo;
  }

  public QuotaLimiter getRegionServerLimiter() {
    return rsQuotaLimiter;
  }

  private Configuration getConfiguration() {
    return rsServices.getConfiguration();
  }

  @VisibleForTesting
  void triggerCacheRefresh() {
    refreshChore.triggerNow();
  }

  @VisibleForTesting
  long getLastUpdate() {
    return refreshChore.lastUpdate;
  }

  @VisibleForTesting
  Map<String, QuotaState> getNamespaceQuotaCache() {
    return namespaceQuotaCache;
  }

  @VisibleForTesting
  Map<TableName, QuotaState> getTableQuotaCache() {
    return tableQuotaCache;
  }

  @VisibleForTesting
  Map<String, UserQuotaState> getUserQuotaCache() {
    return userQuotaCache;
  }

  // TODO: Remove this once we have the notification bus
  private class QuotaRefresherChore extends Chore {
    private long lastUpdate = 0;

    public QuotaRefresherChore(final int period, final Stoppable stoppable) {
      super("QuotaRefresherChore", period, stoppable);
    }

    @Override
    protected void chore() {
      // Prefetch online tables/namespaces
      for (TableName table: QuotaCache.this.rsServices.getOnlineTables()) {
        if (table.isSystemTable()) continue;
        if (!QuotaCache.this.tableQuotaCache.contains(table)) {
          QuotaCache.this.tableQuotaCache.putIfAbsent(table, new QuotaState());
        }
        String ns = table.getNamespaceAsString();
        if (!QuotaCache.this.namespaceQuotaCache.contains(ns)) {
          QuotaCache.this.namespaceQuotaCache.putIfAbsent(ns, new QuotaState());
        }
      }
      
      // first update, then fetch
      updateRegionServerQuotaLimiter();
      updateLocalQuotaFactors();
      fetchNamespaceQuotaState();
      fetchTableQuotaState();
      fetchUserQuotaState();
      lastUpdate = EnvironmentEdgeManager.currentTimeMillis();
      LOG.info("QuotaCache refreshed");
      LOG.info("RegionServer Limiter is " + rsQuotaLimiter);
      for (Map.Entry<String, UserQuotaState> entry : userQuotaCache.entrySet()) {
        LOG.info("For user " + entry.getKey() + " " + entry.getValue());
      }
    }

    private void fetchNamespaceQuotaState() {
      fetch("namespace", QuotaCache.this.namespaceQuotaCache, new Fetcher<String, QuotaState>() {
        @Override
        public Get makeGet(final Map.Entry<String, QuotaState> entry) {
          return QuotaUtil.makeGetForNamespaceQuotas(entry.getKey());
        }

        @Override
        public Map<String, QuotaState> fetchEntries(final List<Get> gets)
            throws IOException {
          return QuotaUtil.fetchNamespaceQuotas(QuotaCache.this.getConfiguration(), gets);
        }
      });
    }

    private void fetchTableQuotaState() {
      fetch("table", QuotaCache.this.tableQuotaCache, new Fetcher<TableName, QuotaState>() {
        @Override
        public Get makeGet(final Map.Entry<TableName, QuotaState> entry) {
          return QuotaUtil.makeGetForTableQuotas(entry.getKey());
        }

        @Override
        public Map<TableName, QuotaState> fetchEntries(final List<Get> gets)
            throws IOException {
          return QuotaUtil.fetchTableQuotas(QuotaCache.this.getConfiguration(), gets);
        }
      });
    }

    private void fetchUserQuotaState() {
      final Set<String> namespaces = QuotaCache.this.namespaceQuotaCache.keySet();
      final Set<TableName> tables = QuotaCache.this.tableQuotaCache.keySet();
      fetch("user", QuotaCache.this.userQuotaCache, new Fetcher<String, UserQuotaState>() {
        @Override
        public Get makeGet(final Map.Entry<String, UserQuotaState> entry) {
          return QuotaUtil.makeGetForUserQuotas(entry.getKey(), tables, namespaces);
        }

        @Override
        public Map<String, UserQuotaState> fetchEntries(final List<Get> gets)
            throws IOException {
          return QuotaUtil.fetchUserQuotas(QuotaCache.this.getConfiguration(), gets, localQuotaFactors);
        }
      });
    }

    private <K, V extends QuotaState> void fetch(final String type,
        final ConcurrentHashMap<K, V> quotasMap, final Fetcher<K, V> fetcher) {
      long now = EnvironmentEdgeManager.currentTimeMillis();
      long refreshPeriod = getPeriod();
      long evictPeriod = refreshPeriod * EVICT_PERIOD_FACTOR;

      // Find the quota entries to update
      List<Get> gets = new ArrayList<Get>();
      List<K> toRemove = new ArrayList<K>();
      for (Map.Entry<K, V> entry: quotasMap.entrySet()) {
        long lastUpdate = entry.getValue().getLastUpdate();
        long lastQuery = entry.getValue().getLastQuery();
        if (lastQuery > 0 && (now - lastQuery) >= evictPeriod) {
          toRemove.add(entry.getKey());
        } else if (TEST_FORCE_REFRESH || (now - lastUpdate) >= refreshPeriod) {
          gets.add(fetcher.makeGet(entry));
        }
      }

      for (final K key: toRemove) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("evict " + type + " key=" + key);
        }
        // long time no query for this user/table/ns, remove it's quota
        quotasMap.remove(key);
      }

      // fetch and update the quota entries
      if (!gets.isEmpty()) {
        try {
          for (Map.Entry<K, V> entry: fetcher.fetchEntries(gets).entrySet()) {
            V quotaInfo = quotasMap.putIfAbsent(entry.getKey(), entry.getValue());
            if (quotaInfo != null) {
              quotaInfo.update(entry.getValue());
            }

            if (LOG.isTraceEnabled()) {
              LOG.trace("refresh " + type + " key=" + entry.getKey() + " quotas=" + quotaInfo);
            }
          }
        } catch (IOException e) {
          LOG.warn("Unable to read " + type + " from quota table", e);
        }
      }
    }
  }
    
  static interface Fetcher<Key, Value> {
    Get makeGet(Map.Entry<Key, Value> entry);
    Map<Key, Value> fetchEntries(List<Get> gets) throws IOException;
  }
}
