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
import java.util.HashSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.catalog.MetaReader;
import org.apache.hadoop.hbase.ipc.RequestContext;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.master.handler.CreateTableHandler;
import org.apache.hadoop.hbase.namespace.NamespaceAuditor;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.TimeUnit;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.ListRegionQuotaRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.ListRegionQuotaResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.SetQuotaRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.SetQuotaResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.SetRegionQuotaRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.SetRegionQuotaResponse;
import org.apache.hadoop.hbase.protobuf.generated.QuotaProtos.Quotas;
import org.apache.hadoop.hbase.protobuf.generated.QuotaProtos.Throttle;
import org.apache.hadoop.hbase.protobuf.generated.QuotaProtos.ThrottleRequest;
import org.apache.hadoop.hbase.protobuf.generated.QuotaProtos.ThrottleType;
import org.apache.hadoop.hbase.protobuf.generated.QuotaProtos.TimedQuota;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.RegionQuotaTracker;
import org.apache.zookeeper.KeeperException;

import com.google.common.annotations.VisibleForTesting;

/**
 * Master Quota Manager.
 * It is responsible for initialize the quota table on the first-run and
 * provide the admin operations to interact with the quota table.
 *
 * TODO: FUTURE: The master will be responsible to notify each RS of quota changes
 * and it will do the "quota aggregation" when the QuotaScope is CLUSTER.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class MasterQuotaManager implements RegionStateListener {
  private static final Log LOG = LogFactory.getLog(MasterQuotaManager.class);

  private final MasterServices masterServices;
  private NamedLock<String> namespaceLocks;
  private NamedLock<TableName> tableLocks;
  private NamedLock<String> userLocks;
  private boolean enabled = false;
  private NamespaceAuditor namespaceQuotaManager;
  private long totalExistedReadLimit = 0;
  private long totalExistedWriteLimit = 0;
  private int regionServerNum;
  public static final String REGION_SERVER_OVERCONSUMPTION_FACTOR = "hbase.regionserver.overconsumption.factor";
  public static final float DEFAULT_REGION_SERVER_OVERCONSUMPTION_FACTOR = 0.7f;
  private int regionServerReadLimit;
  private int regionServerWriteLimit;
  // Tracker for region quotas which is a hard limit
  private RegionQuotaTracker regionQuotaTracker;

  public MasterQuotaManager(final MasterServices masterServices) {
    this.masterServices = masterServices;
    this.regionQuotaTracker = new RegionQuotaTracker(masterServices.getZooKeeper(), masterServices);
    this.regionQuotaTracker.start();
  }

  public void start() throws IOException {
    // If the user doesn't want the quota support skip all the initializations.
    if (!QuotaUtil.isQuotaEnabled(masterServices.getConfiguration())) {
      LOG.info("Quota support disabled");
      return;
    }

    // Create the quota table if missing
    //if (!MetaTableAccessor.tableExists(masterServices.getShortCircuitConnection(),
    //      QuotaUtil.QUOTA_TABLE_NAME)) {
    if (!MetaReader.tableExists(masterServices.getCatalogTracker().getConnection(),
      QuotaUtil.QUOTA_TABLE_NAME)) {
      LOG.info("Quota table not found. Creating...");
      createQuotaTable();
    }

    LOG.info("Initializing quota support");
    namespaceLocks = new NamedLock<String>();
    tableLocks = new NamedLock<TableName>();
    userLocks = new NamedLock<String>();

    computeTotalExistedLimit();

    regionServerReadLimit = getConfiguration().getInt(QuotaCache.REGION_SERVER_READ_LIMIT_KEY,
      QuotaCache.DEFAULT_REGION_SERVER_READ_LIMIT);
    regionServerReadLimit *= getConfiguration().getFloat(REGION_SERVER_OVERCONSUMPTION_FACTOR,
      DEFAULT_REGION_SERVER_OVERCONSUMPTION_FACTOR);
    regionServerWriteLimit = getConfiguration().getInt(
      QuotaCache.REGION_SERVER_WRITE_LIMIT_KEY, QuotaCache.DEFAULT_REGION_SERVER_WRITE_LIMIT);
    regionServerWriteLimit *= getConfiguration().getFloat(REGION_SERVER_OVERCONSUMPTION_FACTOR,
      DEFAULT_REGION_SERVER_OVERCONSUMPTION_FACTOR);

    namespaceQuotaManager = new NamespaceAuditor(masterServices);
    namespaceQuotaManager.start();
    enabled = true;
  }

  public void stop() {
  }

  public boolean isQuotaEnabled() {
    return enabled && namespaceQuotaManager.isInitialized();
  }

  private Configuration getConfiguration() {
    return masterServices.getConfiguration();
  }

  /* ==========================================================================
   *  Admin operations to manage the quota table
   */
  public SetQuotaResponse setQuota(final SetQuotaRequest req)
      throws IOException, InterruptedException {
    checkQuotaSupport();

    if (req.hasUserName()) {
      userLocks.lock(req.getUserName());
      try {
        if (req.hasTableName()) {
          setUserQuota(req.getUserName(), ProtobufUtil.toTableName(req.getTableName()), req);
        } else if (req.hasNamespace()) {
           setUserQuota(req.getUserName(), req.getNamespace(), req);
        } else {
           setUserQuota(req.getUserName(), req);
        }
      } finally {
        userLocks.unlock(req.getUserName());
      }
    } else if (req.hasTableName()) {
      TableName table = ProtobufUtil.toTableName(req.getTableName());
      tableLocks.lock(table);
      try {
        setTableQuota(table, req);
      } finally {
        tableLocks.unlock(table);
      }
    } else if (req.hasNamespace()) {
      namespaceLocks.lock(req.getNamespace());
      try {
        setNamespaceQuota(req.getNamespace(), req);
      } finally {
        namespaceLocks.unlock(req.getNamespace());
      }
    } else {
      throw new DoNotRetryIOException(
        new UnsupportedOperationException("a user, a table or a namespace must be specified"));
    }
    return SetQuotaResponse.newBuilder().build();
  }

  public void setUserQuota(final String userName, final SetQuotaRequest req)
      throws IOException, InterruptedException {
    checkQuotaSupport();
    setQuota(req, new SetQuotaOperations() {
      @Override
      public Quotas fetch() throws IOException {
        return QuotaUtil.getUserQuota(getConfiguration(), userName);
      }
      @Override
      public void update(final Quotas quotas) throws IOException {
        QuotaUtil.addUserQuota(getConfiguration(), userName, quotas);
      }
      @Override
      public void delete() throws IOException {
        QuotaUtil.deleteUserQuota(masterServices.getConfiguration(), userName);
      }
      @Override
      public void preApply(final Quotas quotas) throws IOException {
        masterServices.getCoprocessorHost().preSetUserQuota(userName, quotas);
      }
      @Override
      public void postApply(final Quotas quotas) throws IOException {
        masterServices.getCoprocessorHost().postSetUserQuota(userName, quotas);
      }
    });
  }

  public void setUserQuota(final String userName, final TableName table,
      final SetQuotaRequest req) throws IOException, InterruptedException {
    checkQuotaSupport();
    if (isLegalThrottle(req)) {
      long reqLimit = getSoftLimitFromRequest(req);
      ThrottleType type = req.getThrottle().getType();
      checkNamespaceQuota(userName, table, reqLimit, type);
      checkRegionServerQuota(userName, table, reqLimit, type, computeLocalFactorForTable(table));
    }
    setQuota(req, new SetQuotaOperations() {
      @Override
      public Quotas fetch() throws IOException {
        return QuotaUtil.getUserQuota(getConfiguration(), userName, table);
      }
      @Override
      public void update(final Quotas quotas) throws IOException {
        QuotaUtil.addUserQuota(getConfiguration(), userName, table, quotas);
      }
      @Override
      public void delete() throws IOException {
        QuotaUtil.deleteUserQuota(masterServices.getConfiguration(), userName, table);
      }
      @Override
      public void preApply(final Quotas quotas) throws IOException {
        if (req.hasBypassGlobals()) {
          masterServices.getCoprocessorHost().preBypassUserQuota(userName, table);
        } else {
          masterServices.getCoprocessorHost().preSetUserQuota(userName, table, quotas);
        }
      }
      @Override
      public void postApply(final Quotas quotas) throws IOException {
        masterServices.getCoprocessorHost().postSetUserQuota(userName, table, quotas);
      }
    });
  }

  public void setUserQuota(final String userName, final String namespace,
      final SetQuotaRequest req) throws IOException, InterruptedException {
    checkQuotaSupport();
    setQuota(req, new SetQuotaOperations() {
      @Override
      public Quotas fetch() throws IOException {
        return QuotaUtil.getUserQuota(getConfiguration(), userName, namespace);
      }
      @Override
      public void update(final Quotas quotas) throws IOException {
        QuotaUtil.addUserQuota(getConfiguration(), userName, namespace, quotas);
      }
      @Override
      public void delete() throws IOException {
        QuotaUtil.deleteUserQuota(masterServices.getConfiguration(), userName, namespace);
      }
      @Override
      public void preApply(final Quotas quotas) throws IOException {
        masterServices.getCoprocessorHost().preSetUserQuota(userName, namespace, quotas);
      }
      @Override
      public void postApply(final Quotas quotas) throws IOException {
        masterServices.getCoprocessorHost().postSetUserQuota(userName, namespace, quotas);
      }
    });
  }

  public void setTableQuota(final TableName table, final SetQuotaRequest req)
      throws IOException, InterruptedException {
    checkQuotaSupport();
    setQuota(req, new SetQuotaOperations() {
      @Override
      public Quotas fetch() throws IOException {
        return QuotaUtil.getTableQuota(getConfiguration(), table);
      }
      @Override
      public void update(final Quotas quotas) throws IOException {
        QuotaUtil.addTableQuota(getConfiguration(), table, quotas);
      }
      @Override
      public void delete() throws IOException {
        QuotaUtil.deleteTableQuota(getConfiguration(), table);
      }
      @Override
      public void preApply(final Quotas quotas) throws IOException {
        masterServices.getCoprocessorHost().preSetTableQuota(table, quotas);
      }
      @Override
      public void postApply(final Quotas quotas) throws IOException {
        masterServices.getCoprocessorHost().postSetTableQuota(table, quotas);
      }
    });
  }

  public void setNamespaceQuota(final String namespace, final SetQuotaRequest req)
      throws IOException, InterruptedException {
    checkQuotaSupport();
    if (isLegalThrottle(req)) {
      long reqLimit = getSoftLimitFromRequest(req);
      ThrottleType type = req.getThrottle().getType();
      checkNamespaceQuota(namespace, reqLimit, type);
      int rsNum = masterServices.getServerManager().getOnlineServersList().size();
      checkRegionServerQuota(namespace, getSoftLimitFromRequest(req), rsNum, req.getThrottle()
          .getType());
    }
    setQuota(req, new SetQuotaOperations() {
      @Override
      public Quotas fetch() throws IOException {
        return QuotaUtil.getNamespaceQuota(getConfiguration(), namespace);
      }
      @Override
      public void update(final Quotas quotas) throws IOException {
        QuotaUtil.addNamespaceQuota(getConfiguration(), namespace, quotas);
      }
      @Override
      public void delete() throws IOException {
        QuotaUtil.deleteNamespaceQuota(getConfiguration(), namespace);
      }
      @Override
      public void preApply(final Quotas quotas) throws IOException {
        masterServices.getCoprocessorHost().preSetNamespaceQuota(namespace, quotas);
      }
      @Override
      public void postApply(final Quotas quotas) throws IOException {
        masterServices.getCoprocessorHost().postSetNamespaceQuota(namespace, quotas);
      }
    });
  }

  public void setNamespaceQuota(NamespaceDescriptor desc) throws IOException {
    if (enabled) {
      this.namespaceQuotaManager.addNamespace(desc);
    }
  }

  public void removeNamespaceQuota(String namespace) throws IOException {
    if (enabled) {
      this.namespaceQuotaManager.deleteNamespace(namespace);
    }
  }

  public void removeTableQuota(TableName tableName) throws IOException, InterruptedException {
    if (enabled) {
      // unthrottle table quotas
      setTableQuota(tableName,
        QuotaSettings.buildSetQuotaRequestProto(QuotaSettingsFactory.unthrottleTable(tableName)));

      // unthrottle user table quotas
      QuotaFilter userTableFilter = new QuotaFilter();
      userTableFilter.setUserFilter(".*").setTableFilter(tableName.getNameAsString());
      try (QuotaRetriever scanner = QuotaRetriever.open(this.getConfiguration(), userTableFilter)) {
        for (QuotaSettings settings : scanner) {
          if (settings.getQuotaType() == QuotaType.THROTTLE) {
            String user = settings.getUserName();
            if (user != null) {
              setUserQuota(user, tableName, QuotaSettings
                  .buildSetQuotaRequestProto(QuotaSettingsFactory.unthrottleUser(user, tableName)));
            }
          }
        }
      }
    }
  }

  // we only focus on throttle request with conditions:
  // 1. type = READ_NUMBER or WRITE_NUMBER
  // 2. hasSoftLimit and time unit is TimeUnit.SECONDS
  public static boolean isLegalThrottle(final SetQuotaRequest req) {
    if (req.hasThrottle() && req.getThrottle().hasType() && req.getThrottle().hasTimedQuota()) {
      ThrottleType type = req.getThrottle().getType();
      if (type == ThrottleType.READ_NUMBER || type == ThrottleType.WRITE_NUMBER) {
        TimedQuota quota = req.getThrottle().getTimedQuota();
        if (quota.hasSoftLimit() && quota.hasTimeUnit() && quota.getTimeUnit() == TimeUnit.SECONDS) {
          return true;
        }
        return false;
      }
      return false;
    }
    return false;
  }

  private void setQuota(final SetQuotaRequest req, final SetQuotaOperations quotaOps)
      throws IOException, InterruptedException {
    if (req.hasRemoveAll() && req.getRemoveAll() == true) {
      quotaOps.preApply(null);
      quotaOps.delete();
      quotaOps.postApply(null);
      return;
    }

    // Apply quota changes
    Quotas quotas = quotaOps.fetch();
    quotaOps.preApply(quotas);

    Quotas.Builder builder = (quotas != null) ? quotas.toBuilder() : Quotas.newBuilder();
    if (req.hasThrottle()) applyThrottle(builder, req.getThrottle());
    if (req.hasBypassGlobals()) applyBypassGlobals(builder, req.getBypassGlobals());

    // Submit new changes
    quotas = builder.build();
    if (QuotaUtil.isEmptyQuota(quotas)) {
      quotaOps.delete();
    } else {
      quotaOps.update(quotas);
    }
    quotaOps.postApply(quotas);
  }

  public void checkNamespaceTableAndRegionQuota(TableName tName, int regions) throws IOException {
    if (enabled) {
      namespaceQuotaManager.checkQuotaToCreateTable(tName, regions);
    }
  }

  /**
   * Remove table from namespace quota.
   *
   * @param tName - The table name to update quota usage.
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public void removeTableFromNamespaceQuota(TableName tName) throws IOException {
    if (enabled) {
      namespaceQuotaManager.removeFromNamespaceUsage(tName);
    }
  }

  public NamespaceAuditor getNamespaceQuotaManager() {
    return this.namespaceQuotaManager;
  }

  private static interface SetQuotaOperations {
    Quotas fetch() throws IOException;
    void delete() throws IOException;
    void update(final Quotas quotas) throws IOException;
    void preApply(final Quotas quotas) throws IOException;
    void postApply(final Quotas quotas) throws IOException;
  }

  /* ==========================================================================
   *  Helpers to apply changes to the quotas
   */
  private void applyThrottle(final Quotas.Builder quotas, final ThrottleRequest req)
      throws IOException {
    Throttle.Builder throttle;

    if (req.hasType() && (req.hasTimedQuota() || quotas.hasThrottle())) {
      // Validate timed quota if present
      if (req.hasTimedQuota()) validateTimedQuota(req.getTimedQuota());

      // apply the new settings
      throttle = quotas.hasThrottle() ? quotas.getThrottle().toBuilder() : Throttle.newBuilder();
      switch (req.getType()) {
        case REQUEST_NUMBER:
          if (req.hasTimedQuota()) {
            throttle.setReqNum(req.getTimedQuota());
          } else {
            throttle.clearReqNum();
          }
          break;
        case REQUEST_SIZE:
          if (req.hasTimedQuota()) {
            throttle.setReqSize(req.getTimedQuota());
          } else {
            throttle.clearReqSize();
          }
          break;
        case WRITE_NUMBER:
          if (req.hasTimedQuota()) {
            throttle.setWriteNum(req.getTimedQuota());
          } else {
            throttle.clearWriteNum();
          }
          break;
        case WRITE_SIZE:
          if (req.hasTimedQuota()) {
            throttle.setWriteSize(req.getTimedQuota());
          } else {
            throttle.clearWriteSize();
          }
          break;
        case READ_NUMBER:
          if (req.hasTimedQuota()) {
            throttle.setReadNum(req.getTimedQuota());
          } else {
            throttle.clearReadNum();
          }
          break;
        case READ_SIZE:
          if (req.hasTimedQuota()) {
            throttle.setReadSize(req.getTimedQuota());
          } else {
            throttle.clearReadSize();
          }
          break;
      }
      quotas.setThrottle(throttle.build());
    } else {
      quotas.clearThrottle();
    }
  }

  private void applyBypassGlobals(final Quotas.Builder quotas, boolean bypassGlobals) {
    if (bypassGlobals) {
      quotas.setBypassGlobals(bypassGlobals);
    } else {
      quotas.clearBypassGlobals();
    }
  }

  private void validateTimedQuota(final TimedQuota timedQuota) throws IOException {
    if (timedQuota.getSoftLimit() < 1) {
      throw new DoNotRetryIOException(new IllegalArgumentException(
          "The throttle limit must be greater than 0, got " + timedQuota.getSoftLimit()));
    }
  }

  /* ==========================================================================
   *  Helpers
   */

  private void checkQuotaSupport() throws IOException {
    if (!enabled) {
      throw new DoNotRetryIOException(
        new UnsupportedOperationException("quota support disabled"));
    }
  }

  protected double computeLocalFactorForTable(TableName tableName) {
    int tableRegionsNum = this.masterServices.getAssignmentManager().getRegionStates()
        .getRegionByStateOfTable(tableName).get(RegionState.State.OPEN).size();
    regionServerNum = this.masterServices.getServerManager().getOnlineServersList().size();
    // check by the worst case, so localRegionsNum=tableRegionsNum
    // TODO: verify ?
    double factor = QuotaCache.computeLocalFactor(regionServerNum, tableRegionsNum, tableRegionsNum);
    LOG.info("compute local factor, rsNum=" + regionServerNum + ", tableRegionNum="
        + tableRegionsNum + ", factor=" + factor);
    return factor;
  }
  
  public void checkRegionServerQuota(String user, TableName tableName, long reqLimit,
      ThrottleType type, double localFactor) throws IOException {
    // Because previous set quota may be delete quota and disabled tables may be enabled,
    // so update total existed limit after set quota
    computeTotalExistedLimit();
    reqLimit = (long)(reqLimit * localFactor);
    if (reqLimit < 1) {
      throw new DoNotRetryIOException(
          new IllegalArgumentException("The machine throttle limit must be greater than 0, type="
              + type + ", user=" + user + ", tableName=" + tableName + ", requestLimit=" + reqLimit
              + ", localFactor=" + localFactor));
    }
    long previous = (long)(getSoftLimitForUserAndTable(user, tableName, type) * localFactor);
    long consumed = type == ThrottleType.READ_NUMBER ? totalExistedReadLimit
        : totalExistedWriteLimit;
    long total = type == ThrottleType.READ_NUMBER ? regionServerReadLimit
        : regionServerWriteLimit;
    String logStr = "checkRegionServerQuota, ForTable, type=" + type + ", tableName=" + tableName
        + ", localFactor=" + localFactor;
    checkQuotaUpdate(logStr, reqLimit, previous, consumed, total);
  }
  
  protected long getConsumedSoftLimitForAllNamespaces(ThrottleType type)
      throws IOException {
    return getCumulativeSoftLimit(
      new QuotaFilter().setNamespaceFilter(TableName.VALID_NAMESPACE_REGEX), type);
  }
  
  public void checkRegionServerQuota(final String namespace, long reqLimit, int rsNum,
      ThrottleType type) throws IOException {
    long previous = getSoftLimitForNamespace(namespace, type);
    long consumed = getConsumedSoftLimitForAllNamespaces(type);
    long total = getSoftLimitForCluster(type, rsNum);
    String logStr = "checkRegionServerQuota, ForNamespace, type=" + type + ", namespace="
        + namespace + ", rsNum=" + rsNum;
    checkQuotaUpdate(logStr, reqLimit, previous, consumed, total);
  }
  
  // a generic method to check whether quota update is permitted. This method will check
  // whether (reqLimit - preLimit + consumedLimit) < totalLimit is satisfied
  protected void checkQuotaUpdate(String checkType, long reqLimit, long previousLimit,
      long consumedLimit, long totalLimit) throws IOException {
    if (reqLimit != 0) {
      if (reqLimit < previousLimit) {
        return;
      }
      
      if (reqLimit - previousLimit + consumedLimit > totalLimit) {
        String failed = checkType + " failed, reqLimit=" + reqLimit + ", preLimit=" + previousLimit
            + ", consumedLimit=" + consumedLimit + ", totalLimit=" + totalLimit;
        LOG.warn(failed);
        throw new QuotaExceededException(failed);
      }
    }
  }

  protected void checkThrottleType(ThrottleType type) throws IOException {
    if (type != ThrottleType.READ_NUMBER && type != ThrottleType.WRITE_NUMBER) {
      throw new IOException("throttle type not support, type: " + type);
    }
  }
  
  protected long getSoftLimitFromRequest(final SetQuotaRequest req) throws IOException {
    long reqLimit = 0l;
    ThrottleRequest throttle = req.getThrottle();
    ThrottleType throttleType = throttle.getType();

    checkThrottleType(throttleType);

    if (throttle.hasTimedQuota() && throttle.getTimedQuota().hasSoftLimit()) {
      TimedQuota quota = throttle.getTimedQuota();
      if (quota.hasTimeUnit() && quota.getTimeUnit() != TimeUnit.SECONDS) {
        throw new IOException("time unit must be seconds");
      }
      reqLimit = quota.getSoftLimit();
    }
    return reqLimit;
  }
  
  protected long getSoftLimitForUserAndTable(String user, TableName tableName, ThrottleType type)
      throws IOException {
    QuotaFilter filter = new QuotaFilter();
    filter.setTableFilter(tableName.getNameAsString());
    if (user != null) {
      filter.setUserFilter(user);
    } else {
      setUserFilter(filter);
    }
    ThrottleSettings throttle = getThrottlingSettings(filter, type);
    return throttle == null ? 0 : throttle.getSoftLimit();
  }
  
  protected long getSoftLimitForNamespace(String namespace, ThrottleType type) throws IOException {
    ThrottleSettings namespaceThrottle = getThrottlingSettings(
      new QuotaFilter().setNamespaceFilter(namespace), type);
    return namespaceThrottle == null ? 0 : namespaceThrottle.getSoftLimit();
  }
  
  protected long getSoftLimitForCluster(ThrottleType type, int rsNum) {
    long result = rsNum
        * (type == ThrottleType.READ_NUMBER ? regionServerReadLimit : regionServerWriteLimit);
    LOG.info("get soft limit for cluster, rsNum=" + rsNum + ", type=" + type + ", rsReadLimit="
        + regionServerReadLimit + ", rsWriteLimit=" + regionServerWriteLimit);
    return result;
  }
  
  protected long getConsumedSoftLimitForNamespace(String namespace, ThrottleType type)
      throws IOException {
    String pattern = null;
    if (namespace == null || namespace.equals(NamespaceDescriptor.DEFAULT_NAMESPACE_NAME_STR)) {
      pattern = TableName.VALID_TABLE_QUALIFIER_REGEX;
    } else {
      pattern = namespace + "\\" + TableName.NAMESPACE_DELIM + TableName.VALID_TABLE_QUALIFIER_REGEX;
    }
    QuotaFilter filter = new QuotaFilter().setUserFilter("(.+)").setTableFilter(pattern);
    return getCumulativeSoftLimit(filter, type);
  }

  public void checkNamespaceQuota(String user, TableName tableName, long reqLimit, ThrottleType type)
      throws IOException {
    long previous = getSoftLimitForUserAndTable(user, tableName, type);
    long consumed = getConsumedSoftLimitForNamespace(tableName.getNamespaceAsString(), type);
    long total = getSoftLimitForNamespace(tableName.getNamespaceAsString(), type);
    String logStr = "checkNamespaceQuota,type=" + type + ", tableName=" + tableName;
    checkQuotaUpdate(logStr, reqLimit, previous, consumed, total);
  }
  
  public void checkNamespaceQuota(final String namespace, long reqLimit, ThrottleType type)
      throws IOException {
    long consumed = getConsumedSoftLimitForNamespace(namespace, type);
    if (reqLimit < consumed) {
      throw new DoNotRetryIOException("can not set namespace limit to: " + reqLimit
          + ", because it has consumed: " + consumed);
    }
  }
  
  private void computeTotalExistedLimit() {
    LOG.info("Start to compute total existed limit...");
    long totalReadLimit = 0;
    long totalWriteLimit = 0;
    try {
      regionServerNum = this.masterServices.getServerManager().getOnlineServersList().size();

      QuotaRetriever scanner = QuotaRetriever.open(this.getConfiguration());
      for (QuotaSettings settings : scanner) {
        switch (settings.getQuotaType()) {
        case THROTTLE:
          ThrottleSettings throttle = (ThrottleSettings) settings;
          // just compute the (user,table) quota, which timeunit is seconds
          if (throttle.getUserName() != null && throttle.getTableName() != null
              && throttle.getTimeUnit() == ProtobufUtil.toTimeUnit(TimeUnit.SECONDS)) {
            TableName tableName = throttle.getTableName();
            int tableRegionsNum = this.masterServices.getAssignmentManager().getRegionStates()
                .getRegionByStateOfTable(tableName).get(RegionState.State.OPEN).size();
            if (tableRegionsNum == 0
                && this.masterServices.getAssignmentManager().getZKTable() != null
                && this.masterServices.getAssignmentManager().getZKTable()
                    .isDisabledTable(tableName)) {
              continue;
            }
            long maxLocalThrottleLimit = (long) (throttle.getSoftLimit() * QuotaCache
                .computeLocalFactor(regionServerNum, tableRegionsNum, tableRegionsNum));
            if (throttle.getThrottleType() == ProtobufUtil.toThrottleType(ThrottleType.READ_NUMBER)) {
              totalReadLimit += maxLocalThrottleLimit;
            } else if (throttle.getThrottleType() == ProtobufUtil
                .toThrottleType(ThrottleType.WRITE_NUMBER)) {
              totalWriteLimit += maxLocalThrottleLimit;
            }
          }
          break;
        case GLOBAL_BYPASS:
          break;
        default:
          break;
        }
      }
      scanner.close();
    } catch (Throwable t) {
      LOG.error("fail to update total existed limit", t);
    }
    totalExistedReadLimit = totalReadLimit;
    totalExistedWriteLimit = totalWriteLimit;
    LOG.info("After compute total existed limit for RegionServer, totalExistedReadlimit="
        + totalExistedReadLimit + ", totalExistedWriteLimit=" + totalExistedWriteLimit);
  }

  private QuotaFilter setUserFilter(QuotaFilter filter) throws IOException {
    String user = "";
    if (RequestContext.isInRequestContext()) {
      user = RequestContext.getRequestUser().getShortName();
    } else {
      user = User.getCurrent().getShortName();
    }
    filter.setUserFilter(user);
    return filter;
  }
  
  private ThrottleSettings getThrottlingSettings(QuotaFilter filter, ThrottleType throttleType)
      throws IOException {
    ThrottleSettings throttle = null;
    QuotaRetriever scanner = QuotaRetriever.open(this.getConfiguration(), filter);
    try {
      for (QuotaSettings settings : scanner) {
        if (settings.getQuotaType() == QuotaType.THROTTLE) {
          throttle = (ThrottleSettings) settings;
          if (throttle.getThrottleType() == ProtobufUtil.toThrottleType(throttleType)) {
            break;
          }
        }
      }
    } finally {
      LOG.info("Get throttle settings, filter " + filter + ", " + throttle);
      scanner.close();
    }
    return throttle;
  }
  
  protected long getCumulativeSoftLimit(QuotaFilter filter, ThrottleType throttleType)
      throws IOException {
    long softLimit = 0l;
    QuotaRetriever scanner = QuotaRetriever.open(this.getConfiguration(), filter);
    try {
      for (QuotaSettings settings : scanner) {
        if (settings.getQuotaType() == QuotaType.THROTTLE) {
          ThrottleSettings throttle = (ThrottleSettings) settings;
          if (throttleType == null
              || throttle.getThrottleType() == ProtobufUtil.toThrottleType(throttleType)) {
            softLimit += throttle.getSoftLimit();
          }
        }
      }
    } finally {
      LOG.info("Get throttle settings, filter: " + filter + ", throttleType: " + throttleType);
      scanner.close();
    }
    return softLimit;
  }
  
  private void createQuotaTable() throws IOException {
    HRegionInfo newRegions[] = new HRegionInfo[] {
      new HRegionInfo(QuotaUtil.QUOTA_TABLE_NAME)
    };

    masterServices.getExecutorService()
      .submit(new CreateTableHandler(masterServices,
        masterServices.getMasterFileSystem(),
        QuotaUtil.QUOTA_TABLE_DESC,
        masterServices.getConfiguration(),
        newRegions,
        masterServices)
          .prepare());
  }

  // This method is for strictly testing purpose only
  @VisibleForTesting
  public long getTotalExistedReadLimit() {
    computeTotalExistedLimit();
    return totalExistedReadLimit;
  }

  @VisibleForTesting
  public long getTotalExistedWriteLimit() {
    computeTotalExistedLimit();
    return totalExistedWriteLimit;
  }

  private class NamedLock<T> {
    private HashSet<T> locks = new HashSet<T>();

    public void lock(final T name) throws InterruptedException {
      synchronized (locks) {
        while (locks.contains(name)) {
          locks.wait();
        }
        locks.add(name);
      }
    }

    public void unlock(final T name) {
      synchronized (locks) {
        locks.remove(name);
        locks.notifyAll();
      }
    }
  }
  
  public void checkAndUpdateNamespaceRegionQuota(TableName tName, int regions) throws IOException {
    if (enabled) {
      namespaceQuotaManager.checkQuotaToUpdateRegion(tName, regions);
    }
  }

  public void onRegionMerged(HRegionInfo hri) throws IOException {
    if (enabled) {
      namespaceQuotaManager.updateQuotaForRegionMerge(hri);
    }
  }

  public void onRegionSplit(HRegionInfo hri) throws IOException {
    if (enabled) {
      namespaceQuotaManager.checkQuotaToSplitRegion(hri);
    }
  }

  @Override
  public void onRegionSplitReverted(HRegionInfo hri) throws IOException {
    if (enabled) {
      this.namespaceQuotaManager.removeRegionFromNamespaceUsage(hri);
    }
  }

  public SetRegionQuotaResponse setRegionQuota(final SetRegionQuotaRequest req) throws IOException {
    String regionName = Bytes.toString(req.getRegionQuota().getRegion().toByteArray());
    try {
      if (req.getRegionQuota().hasThrottle()) {
        ThrottleRequest throttle = req.getRegionQuota().getThrottle();
        org.apache.hadoop.hbase.quotas.ThrottleType type =
            ProtobufUtil.toThrottleType(throttle.getType());
        if (throttle.hasTimedQuota()) {
          regionQuotaTracker.setRegionQuota(regionName, type, throttle);
        } else {
          regionQuotaTracker.removeRegionQuota(regionName, type);
        }
      } else {
        regionQuotaTracker.removeRegionQuota(regionName);
      }
    } catch (KeeperException e) {
      throw new IOException(e);
    }
    return SetRegionQuotaResponse.getDefaultInstance();
  }

  public ListRegionQuotaResponse listRegionQuota(final ListRegionQuotaRequest req)
      throws IOException {
    ListRegionQuotaResponse.Builder builder = ListRegionQuotaResponse.newBuilder();
    try {
      for (RegionQuotaSettings regionQuota : regionQuotaTracker.listRegionQuotas()) {
        builder.addRegionQuota(
          ProtobufUtil.createRegionQuota(regionQuota.getRegionName(), regionQuota.getThrottle()));
      }
    } catch (KeeperException e) {
      throw new IOException(e);
    }
    return builder.build();
  }
}
