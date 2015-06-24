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
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
//import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.catalog.MetaReader; 
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.coprocessor.BaseMasterObserver;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.ipc.RequestContext;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.master.handler.CreateTableHandler;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.SetQuotaRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.SetQuotaResponse;
import org.apache.hadoop.hbase.protobuf.generated.QuotaProtos.Quotas;
import org.apache.hadoop.hbase.protobuf.generated.QuotaProtos.Throttle;
import org.apache.hadoop.hbase.protobuf.generated.QuotaProtos.TimedQuota;
import org.apache.hadoop.hbase.protobuf.generated.QuotaProtos.ThrottleRequest;
import org.apache.hadoop.hbase.protobuf.generated.QuotaProtos.ThrottleType;
import org.apache.hadoop.hbase.protobuf.generated.QuotaProtos.QuotaScope;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.TimeUnit;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.security.UserGroupInformation;

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
public class MasterQuotaManager {
  private static final Log LOG = LogFactory.getLog(MasterQuotaManager.class);

  private final MasterServices masterServices;
  private NamedLock<String> namespaceLocks;
  private NamedLock<TableName> tableLocks;
  private NamedLock<String> userLocks;
  private boolean enabled = false;
  private long totalExistedReadLimit = 0;
  private long totalExistedWriteLimit = 0;
  private int regionServerNum;
  public static final String REGION_SERVER_OVERCONSUMPTION_FACTOR = "hbase.regionserver.overconsumption.factor";
  public static final float DEFAULT_REGION_SERVER_OVERCONSUMPTION_FACTOR = 0.7f;

  public MasterQuotaManager(final MasterServices masterServices) {
    this.masterServices = masterServices;
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
    if (!MetaReader.tableExists(masterServices.getCatalogTracker(), QuotaUtil.QUOTA_TABLE_NAME)) {   
      LOG.info("Quota table not found. Creating...");
      createQuotaTable();
    }

    LOG.info("Initializing quota support");
    namespaceLocks = new NamedLock<String>();
    tableLocks = new NamedLock<TableName>();
    userLocks = new NamedLock<String>();

    try {
      computeTotalExistedLimit();
    } catch (IOException e) {
      LOG.info("fail to update total existed limit");
    }

    enabled = true;
  }

  public void stop() {
  }

  public boolean isQuotaEnabled() {
    return enabled;
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
    checkRegionServerQuota(table, req);
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
        masterServices.getCoprocessorHost().preSetUserQuota(userName, table, quotas);
      }
      @Override
      public void postApply(final Quotas quotas) throws IOException {
        masterServices.getCoprocessorHost().postSetUserQuota(userName, table, quotas);
      }
    });
  }

  public void setUserQuota(final String userName, final String namespace,
      final SetQuotaRequest req) throws IOException, InterruptedException {
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

    // Because set quota may be delete quota, so update total existed limit after set quota
    try {
      computeTotalExistedLimit();
    } catch (IOException e) {
      LOG.info("fail to update total existed limit");
    }
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
            throttle.clearReqNum();
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
      throw new DoNotRetryIOException(new UnsupportedOperationException(
          "The throttle limit must be greater then 0, got " + timedQuota.getSoftLimit()));
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

  /*
   * check if set quota exceed regionserver limit * overconsumption factor ?
   */
  private void checkRegionServerQuota(TableName tableName, final SetQuotaRequest req)
      throws IOException {
    int regionServerReadLimit = getConfiguration().getInt(QuotaCache.REGION_SERVER_READ_LIMIT_KEY,
      QuotaCache.DEFAULT_REGION_SERVER_READ_LIMIT);
    regionServerReadLimit *= getConfiguration().getFloat(REGION_SERVER_OVERCONSUMPTION_FACTOR,
      DEFAULT_REGION_SERVER_OVERCONSUMPTION_FACTOR);
    int regionServerWriteLimit = getConfiguration().getInt(
      QuotaCache.REGION_SERVER_WRITE_LIMIT_KEY, QuotaCache.DEFAULT_REGION_SERVER_WRITE_LIMIT);
    regionServerWriteLimit *= getConfiguration().getFloat(REGION_SERVER_OVERCONSUMPTION_FACTOR,
      DEFAULT_REGION_SERVER_OVERCONSUMPTION_FACTOR);

    int tableRegionsNum = this.masterServices.getAssignmentManager().getRegionStates()
        .getRegionByStateOfTable(tableName).get(RegionState.State.OPEN).size();

    // check by the worst case, so localRegionsNum=tableRegionsNum
    double localFactor = QuotaCache.computeLocalFactor(regionServerNum,
      tableRegionsNum, tableRegionsNum);

    if (req.getThrottle().getType() == ThrottleType.READ_NUMBER) {
      long readReqLimit = computeReqLimitByLocalFactor(tableName, req, localFactor);
      if ((readReqLimit > 0) && ((readReqLimit + totalExistedReadLimit) > regionServerReadLimit)) {
        throw new QuotaExceededException("Failed to set read quota for table=" + tableName
            + ", because quota is exceed the region server read limit");
      }
    }

    if (req.getThrottle().getType() == ThrottleType.WRITE_NUMBER) {
      long writeReqLimit = computeReqLimitByLocalFactor(tableName, req, localFactor);
      if ((writeReqLimit > 0) && ((writeReqLimit + totalExistedWriteLimit) > regionServerWriteLimit)) {
        throw new QuotaExceededException("Failed to set write quota for table=" + tableName
            + ", because quota is exceed the region server read limit");
      }
    }
  }

  /*
   * Compute req limit in reginserver. Need to handle add quota or update quota, so compute the
   * reqLimitGap between existed limit
   */
  public long computeReqLimitByLocalFactor(TableName tableName, final SetQuotaRequest req,
      double localFactor) {
    long reqLimit = 0;
    if (req.hasThrottle() && req.getThrottle().hasTimedQuota()) {
      TimedQuota quota = req.getThrottle().getTimedQuota();
      // just check the quota which timeunit is seconds
      if (quota.hasSoftLimit() && quota.hasTimeUnit() && quota.getTimeUnit() == TimeUnit.SECONDS) {
        reqLimit = quota.getSoftLimit();
      }
    }

    long reqLimitGap = 0;

    if (req.getThrottle().getType() == ThrottleType.READ_NUMBER) {
      reqLimitGap = reqLimit - getTableExistedReadLimit(tableName);
    }
    if (req.getThrottle().getType() == ThrottleType.WRITE_NUMBER) {
      reqLimitGap = reqLimit - getTableExistedWriteLimit(tableName);
    }

    return (long) (reqLimitGap * localFactor);
  }

  private void computeTotalExistedLimit() throws IOException {
    LOG.info("Start to compute total existed limit...");
    long totalReadLimit = 0;
    long totalWriteLimit = 0;
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

    totalExistedReadLimit = totalReadLimit;
    totalExistedWriteLimit = totalWriteLimit;
    LOG.info("After compute total existed limit for RegionServer, totalExistedReadlimit="
        + totalExistedReadLimit + ", totalExistedWriteLimit=" + totalExistedWriteLimit);
  }

  private ThrottleSettings getTableThrottlingSettings(TableName table, ThrottleType throttleType)
      throws IOException {
    String user = "";
    if (RequestContext.isInRequestContext()) {
      user = RequestContext.getRequestUser().getName();
    } else {
      user = User.getCurrent().getName();
    }
    ThrottleSettings throttle = null;
    QuotaFilter filter = new QuotaFilter().setUserFilter(user).setTableFilter(
      table.getNameAsString());
    QuotaRetriever scanner = QuotaRetriever.open(this.getConfiguration(), filter);
    for (QuotaSettings settings : scanner) {
      if (settings.getQuotaType() == QuotaType.THROTTLE) {
        throttle = (ThrottleSettings) settings;
        if (throttle.getThrottleType() == ProtobufUtil.toThrottleType(throttleType)) {
          break;
        }
      }
    }
    scanner.close();
    return throttle;
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
    return totalExistedReadLimit;
  }

  @VisibleForTesting
  public long getTotalExistedWriteLimit() {
    return totalExistedWriteLimit;
  }

  @VisibleForTesting
  public long getTableExistedReadLimit(TableName table) {
    try {
      ThrottleSettings throttle = getTableThrottlingSettings(table, ThrottleType.READ_NUMBER);
      if (throttle != null) {
        return throttle.getSoftLimit();
      }
    } catch (IOException e) {
    }
    return 0;
  }

  @VisibleForTesting
  public long getTableExistedWriteLimit(TableName table) {
    try {
      ThrottleSettings throttle = getTableThrottlingSettings(table, ThrottleType.WRITE_NUMBER);
      if (throttle != null) {
        return throttle.getSoftLimit();
      }
    } catch (IOException e) {
    }
    return 0;
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
}
