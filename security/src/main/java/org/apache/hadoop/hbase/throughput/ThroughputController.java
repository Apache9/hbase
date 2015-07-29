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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.RegionStatistics;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.MasterObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.WritableByteArrayComparable;
import org.apache.hadoop.hbase.ipc.HBaseRPC;
import org.apache.hadoop.hbase.ipc.ProtocolSignature;
import org.apache.hadoop.hbase.ipc.RequestContext;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Provides basic throughput quota checks for client requests.
 * <p>
 * {@code ThroughputController} performs request throughput limit checks for HBase operations based
 * on the identity of the user performing the operation. The limit can be set for a single table or
 * globally. If the throughput quota limit is reached, a {@link ThroughputExceededException} will be
 * thrown for the operation. For scan operation, the operation may not fail directly but slowed down
 * when the scan has been started.
 * </p>
 * <p>
 * To perform throughput limits checks, {@code ThroughputController} relies on the
 * {@link org.apache.hadoop.hbase.ipc.SecureRpcEngine} being loaded to provide the user identities
 * for remote requests.
 * </p>
 * <p>
 * The throughput quota limits can be manipulated via the exposed
 * {@link ThroughputControllerProtocol} implementation, and the associated {@code get_limit} and
 * {@code set_limit} HBase shell commands.
 * </p>
 */
public class ThroughputController extends BaseRegionObserver implements
    MasterObserver, ThroughputControllerProtocol {
  public static final Log LOG = LogFactory.getLog(ThroughputController.class);
  // Version number for ThroughputQuotaController
  private static final long PROTOCOL_VERSION = 1L;
  private ThroughputManager quotaManager;
  // flags if we are running on a region of the throughput quota meta table
  private boolean isQuotaRegion = false;
  // Defined only for Endpoint implementation, so it can have way to access region services.
  private RegionCoprocessorEnvironment regionEnv;
  private static final long DEFAULT_TOKEN_WAIT_TIMEOUT = 1000;
  private long tokenWaitTimeoutMillis;

  /*
   * Returns the active user to which throughput quota checks should be applied. If we are in the
   * context of a RPC call, the remote user is used, otherwise the currently logged in user is used.
   */
  private User getActiveUser() throws IOException {
    User user = RequestContext.getRequestUser();
    if (!RequestContext.isInRequestContext()) {
      // for non-rpc handling, fallback to system user
      user = User.getCurrent();
    }

    return user;
  }

  private Set<byte[]> parseTableNames(final Map<byte[], List<KeyValue>> familyMap) {
    Set<byte[]> tableSet = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
    List<KeyValue> kvs = familyMap.get(ThroughputQuotaTable.THROUGHPUT_QUOTA_FAMILY);
    for (KeyValue kv : kvs) {
      tableSet.add(kv.getRow());
    }
    return tableSet;
  }

  private void acquireQuota(ObserverContext<RegionCoprocessorEnvironment> c,
      RequestType requestType) throws ThroughputExceededException, IOException {
    acquireQuota(c, requestType, 0);
  }

  private void acquireQuota(ObserverContext<RegionCoprocessorEnvironment> c,
      RequestType requestType, long timeoutMillis)
      throws ThroughputExceededException, IOException {
    User user = getActiveUser();
    if (user != null) {
      RegionStatistics stats = c.getEnvironment().getRegionServerServices().getRegionStats();
      HRegion region = c.getEnvironment().getRegion();
      byte[] tableName = region.getRegionInfo().getTableName();
      if (Bytes.compareTo(tableName, HConstants.ROOT_TABLE_NAME) == 0 ||
          Bytes.compareTo(tableName, HConstants.META_TABLE_NAME) == 0) {
        return;
      }
      this.quotaManager.acquireQuota(user, tableName, requestType, timeoutMillis,
        TimeUnit.MILLISECONDS, stats);
    } else {
      LOG.warn("Throughput quota limit is enabled, but user is unknown, context = " + c
          + ", request type =" + requestType);
    }
  }

  /*
   * RegionObserver/MasterObserver implementation(non-Javadoc)
   */
  public void start(CoprocessorEnvironment env) throws IOException {
    // if running at region
    if (env instanceof RegionCoprocessorEnvironment) {
      this.regionEnv = (RegionCoprocessorEnvironment) env;
      Configuration conf = this.regionEnv.getConfiguration();
      this.tokenWaitTimeoutMillis = conf.getLong(
        Constants.CONF_TOKEN_WAIT_TIMEOUT_MILLIS, DEFAULT_TOKEN_WAIT_TIMEOUT);
    }
  }

  public void stop(CoprocessorEnvironment env) {
  }

  /*
   * RegionObserver implementation
   */
  @Override
  public void postOpen(ObserverContext<RegionCoprocessorEnvironment> c) {
    RegionCoprocessorEnvironment e = c.getEnvironment();
    final HRegion region = e.getRegion();
    if (region == null) {
      LOG.error("NULL region from RegionCoprocessorEnvironment in postOpen()");
      return;
    }

    try {
      // One manager instance per region server process
      this.quotaManager = ThroughputManager.get(
        e.getRegionServerServices().getZooKeeper(),
        e.getConfiguration());
    } catch (IOException ioe) {
      // Pass along as a RuntimeException, so that the coprocessor is unloaded
      throw new RuntimeException("Error obtaining ThroughputQuotaManager", ioe);
    }

    if (ThroughputQuotaTable.isQuotaRegion(region)) {
      this.isQuotaRegion = true;
      try {
        // Load all quota limits on this region
        List<ThroughputQuota> all = ThroughputQuotaTable.loadAll(region);
        // For each table, write out it's limits to the respective znode for that table.
        for (ThroughputQuota quota : all) {
          this.quotaManager.writeToZookeeper(quota);
        }
      } catch (IOException ex) {
        // It's better to fail than perform checks incorrectly
        throw new RuntimeException("Failed to initialize zk nodes", ex);
      }
    }

    if (LOG.isDebugEnabled() && this.isQuotaRegion) {
      LOG.debug("Openning throughput quota region: " + region);
    }
  }

  @Override
  public void preGetClosestRowBefore(ObserverContext<RegionCoprocessorEnvironment> c, byte[] row,
      byte[] family, Result result) throws IOException {
    acquireQuota(c, RequestType.READ);
  }

  @Override
  public void preGet(ObserverContext<RegionCoprocessorEnvironment> c, Get get, List<KeyValue> result)
      throws IOException {
    acquireQuota(c, RequestType.READ);
  }

  @Override
  public boolean preExists(ObserverContext<RegionCoprocessorEnvironment> c, Get get, boolean exists)
      throws IOException {
    acquireQuota(c, RequestType.READ);
    return exists;
  }

  @Override
  public void prePut(ObserverContext<RegionCoprocessorEnvironment> c, Put put, WALEdit edit,
      boolean writeToWAL) throws IOException {
    acquireQuota(c, RequestType.WRITE);
  }

  @Override
  public void postPut(final ObserverContext<RegionCoprocessorEnvironment> e,
      final Put put, final WALEdit edit, final boolean writeToWAL) throws IOException {
    if (this.isQuotaRegion) {
      Configuration conf = e.getEnvironment().getConfiguration();
      Set<byte[]> tables = parseTableNames(put.getFamilyMap());
      for (byte[] table : tables) {
        String tableName = Bytes.toString(table);
        try {
          ThroughputQuota quota = ThroughputQuotaTable.loadThroughputQuota(conf, table);
          this.quotaManager.writeToZookeeper(quota);
        } catch (IOException ex) {
          LOG.error("Failed updating throuput quota mirror for '" + tableName + "'", ex);
        }
      }
    }
  }

  @Override
  public void preDelete(ObserverContext<RegionCoprocessorEnvironment> c, Delete delete,
      WALEdit edit, boolean writeToWAL) throws IOException {
    acquireQuota(c, RequestType.WRITE);
  }

  @Override
  public void postDelete(final ObserverContext<RegionCoprocessorEnvironment> e,
      final Delete delete, final WALEdit edit, final boolean writeToWAL) throws IOException {
    if (this.isQuotaRegion) {
      Set<byte[]> tables = parseTableNames(delete.getFamilyMap());
      for (byte[] table : tables) {
        this.quotaManager.removeFromZookeeper(Bytes.toString(table));
      }
    }
  }

  @Override
  public boolean preCheckAndPut(ObserverContext<RegionCoprocessorEnvironment> c, byte[] row,
      byte[] family, byte[] qualifier, CompareOp compareOp, WritableByteArrayComparable comparator,
      Put put, boolean result) throws IOException {
    acquireQuota(c, RequestType.READ);
    acquireQuota(c, RequestType.WRITE);
    return result;
  }

  @Override
  public boolean preCheckAndDelete(ObserverContext<RegionCoprocessorEnvironment> c, byte[] row,
      byte[] family, byte[] qualifier, CompareOp compareOp, WritableByteArrayComparable comparator,
      Delete delete, boolean result) throws IOException {
    acquireQuota(c, RequestType.READ);
    acquireQuota(c, RequestType.WRITE);
    return result;
  }

  @Override
  public long preIncrementColumnValue(ObserverContext<RegionCoprocessorEnvironment> c, byte[] row,
      byte[] family, byte[] qualifier, long amount, boolean writeToWAL) throws IOException {
    acquireQuota(c, RequestType.READ);
    acquireQuota(c, RequestType.WRITE);
    return -1;
  }

  @Override
  public Result preAppend(ObserverContext<RegionCoprocessorEnvironment> c, Append append)
      throws IOException {
    acquireQuota(c, RequestType.READ);
    acquireQuota(c, RequestType.WRITE);
    return null;
  }

  @Override
  public Result preIncrement(ObserverContext<RegionCoprocessorEnvironment> c, Increment increment)
      throws IOException {
    acquireQuota(c, RequestType.READ);
    acquireQuota(c, RequestType.WRITE);
    return null;
  }

  @Override
  public RegionScanner preScannerOpen(ObserverContext<RegionCoprocessorEnvironment> c, Scan scan,
      RegionScanner s) throws IOException {
    acquireQuota(c, RequestType.READ);
    return s;
  }

  @Override
  public boolean preScannerNext(ObserverContext<RegionCoprocessorEnvironment> c, InternalScanner s,
      List<Result> result, int limit, boolean hasNext) throws IOException {
    /*
     * Slow down the scanning instead of fail directly which will abort the whole scan. Is it better
     * to request less than 1 token for each next call?
     */
    acquireQuota(c, RequestType.READ, this.tokenWaitTimeoutMillis);
    return hasNext;
  }

  /*
   * MasterObserver implementation
   */

  /*
   * Non-empty implementations
   */
  @Override
  public void postDeleteTable(ObserverContext<MasterCoprocessorEnvironment> ctx, byte[] tableName,
      boolean preserveACL) throws IOException {
    if (!preserveACL) { // reuse acl preserve flag
      ThroughputQuotaTable.removeThroughputQuota(
        ctx.getEnvironment().getConfiguration(), tableName);
    }
  }

  /*
   * Trivial empty implementations, just ignore
   */
  @Override
  public void preCreateTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
      HTableDescriptor desc, HRegionInfo[] regions) throws IOException {
  }

  @Override
  public void postCreateTable(ObserverContext<MasterCoprocessorEnvironment> ctx,
      HTableDescriptor desc, HRegionInfo[] regions) throws IOException {
  }

  @Override
  public void preDeleteTable(ObserverContext<MasterCoprocessorEnvironment> ctx, byte[] tableName,
      boolean preserveACL) throws IOException {
  }

  @Override
  public void preModifyTable(ObserverContext<MasterCoprocessorEnvironment> ctx, byte[] tableName,
      HTableDescriptor htd) throws IOException {
  }

  @Override
  public void postModifyTable(ObserverContext<MasterCoprocessorEnvironment> ctx, byte[] tableName,
      HTableDescriptor htd) throws IOException {
  }

  @Override
  public void preAddColumn(ObserverContext<MasterCoprocessorEnvironment> ctx, byte[] tableName,
      HColumnDescriptor column) throws IOException {
  }

  @Override
  public void postAddColumn(ObserverContext<MasterCoprocessorEnvironment> ctx, byte[] tableName,
      HColumnDescriptor column) throws IOException {
  }

  @Override
  public void preModifyColumn(ObserverContext<MasterCoprocessorEnvironment> ctx, byte[] tableName,
      HColumnDescriptor descriptor) throws IOException {
  }

  @Override
  public void postModifyColumn(ObserverContext<MasterCoprocessorEnvironment> ctx, byte[] tableName,
      HColumnDescriptor descriptor) throws IOException {
  }

  @Override
  public void preDeleteColumn(ObserverContext<MasterCoprocessorEnvironment> ctx, byte[] tableName,
      byte[] c) throws IOException {
  }

  @Override
  public void postDeleteColumn(ObserverContext<MasterCoprocessorEnvironment> ctx, byte[] tableName,
      byte[] c) throws IOException {
  }

  @Override
  public void preEnableTable(ObserverContext<MasterCoprocessorEnvironment> ctx, byte[] tableName)
      throws IOException {
  }

  @Override
  public void postEnableTable(ObserverContext<MasterCoprocessorEnvironment> ctx, byte[] tableName)
      throws IOException {
  }

  @Override
  public void preDisableTable(ObserverContext<MasterCoprocessorEnvironment> c, byte[] tableName)
      throws IOException {
    // should not allow user to disable the meta table, we can rely on ACL externally
  }

  @Override
  public void postDisableTable(ObserverContext<MasterCoprocessorEnvironment> ctx, byte[] tableName)
      throws IOException {
  }

  @Override
  public void preMove(ObserverContext<MasterCoprocessorEnvironment> ctx, HRegionInfo region,
      ServerName srcServer, ServerName destServer) throws IOException {
  }

  @Override
  public void postMove(ObserverContext<MasterCoprocessorEnvironment> ctx, HRegionInfo region,
      ServerName srcServer, ServerName destServer) throws IOException {
  }

  @Override
  public void preAssign(ObserverContext<MasterCoprocessorEnvironment> ctx, HRegionInfo regionInfo)
      throws IOException {
  }

  @Override
  public void postAssign(ObserverContext<MasterCoprocessorEnvironment> ctx, HRegionInfo regionInfo)
      throws IOException {
  }

  @Override
  public void preUnassign(ObserverContext<MasterCoprocessorEnvironment> ctx,
      HRegionInfo regionInfo, boolean force) throws IOException {
  }

  @Override
  public void postUnassign(ObserverContext<MasterCoprocessorEnvironment> ctx,
      HRegionInfo regionInfo, boolean force) throws IOException {
  }

  @Override
  public void preBalance(ObserverContext<MasterCoprocessorEnvironment> ctx) throws IOException {
  }

  @Override
  public void postBalance(ObserverContext<MasterCoprocessorEnvironment> ctx) throws IOException {
  }

  @Override
  public boolean preBalanceSwitch(ObserverContext<MasterCoprocessorEnvironment> ctx,
      boolean newValue) throws IOException {
    return false;
  }

  @Override
  public void postBalanceSwitch(ObserverContext<MasterCoprocessorEnvironment> ctx,
      boolean oldValue, boolean newValue) throws IOException {
  }

  @Override
  public void preShutdown(ObserverContext<MasterCoprocessorEnvironment> ctx) throws IOException {
  }

  @Override
  public void preStopMaster(ObserverContext<MasterCoprocessorEnvironment> ctx) throws IOException {
  }

  @Override
  public void postStartMaster(ObserverContext<MasterCoprocessorEnvironment> ctx) throws IOException {
    // create throughput quota meta table
    ThroughputQuotaTable.init(ctx.getEnvironment().getMasterServices());
  }

  /*
   * Endpoint implementation
   */
  @Override
  public long getProtocolVersion(String protocol, long clientVersion) throws IOException {
    return getProtocolVersion(protocol, clientVersion, "unkown");
  }

  @Override
  public long getProtocolVersion(String protocol, long clientVersion, String versionReport)
      throws IOException {
    LOG.info("User " + RequestContext.getRequestUserName() + " from client :"
        + RequestContext.get().getRemoteAddress() + " connect to server with version: "
        + versionReport);
    return PROTOCOL_VERSION;
  }

  @Override
  public ProtocolSignature getProtocolSignature(String protocol,
      long clientVersion, int clientMethodsHash) throws IOException {
    if (ThroughputControllerProtocol.class.getName().equals(protocol)) {
      return new ProtocolSignature(PROTOCOL_VERSION, null);
    }
    throw new HBaseRPC.UnknownProtocolException(
        "Unexpected protocol requested: " + protocol);
  }

  @Override
  public void setThroughputLimit(ThroughputQuota quota) throws IOException {
    if (quota == null || quota.getTableName() == null || quota.getTableName().isEmpty()) {
      throw new IOException("Invalid throughput quota");
    }
    ThroughputQuotaTable.saveThroughputQuota(this.regionEnv.getConfiguration(), quota);
  }

  @Override
  public ThroughputQuota getThroughputLimit(byte[] tableName) throws IOException {
    if (tableName == null || tableName.length == 0) {
      throw new IOException("Invalid table name");
    }
    return ThroughputQuotaTable.loadThroughputQuota(this.regionEnv.getConfiguration(), tableName);
  }

  @Override
  public void preSnapshot(ObserverContext<MasterCoprocessorEnvironment> ctx,
      SnapshotDescription snapshot, HTableDescriptor hTableDescriptor)
      throws IOException {
  }

  @Override
  public void postSnapshot(ObserverContext<MasterCoprocessorEnvironment> ctx,
      SnapshotDescription snapshot, HTableDescriptor hTableDescriptor)
      throws IOException {
  }

  @Override
  public void preCloneSnapshot(
      ObserverContext<MasterCoprocessorEnvironment> ctx,
      SnapshotDescription snapshot, HTableDescriptor hTableDescriptor)
      throws IOException {
  }

  @Override
  public void postCloneSnapshot(
      ObserverContext<MasterCoprocessorEnvironment> ctx,
      SnapshotDescription snapshot, HTableDescriptor hTableDescriptor)
      throws IOException {
  }

  @Override
  public void preRestoreSnapshot(
      ObserverContext<MasterCoprocessorEnvironment> ctx,
      SnapshotDescription snapshot, HTableDescriptor hTableDescriptor)
      throws IOException {
  }

  @Override
  public void postRestoreSnapshot(
      ObserverContext<MasterCoprocessorEnvironment> ctx,
      SnapshotDescription snapshot, HTableDescriptor hTableDescriptor)
      throws IOException {
  }

  @Override
  public void preDeleteSnapshot(
      ObserverContext<MasterCoprocessorEnvironment> ctx,
      SnapshotDescription snapshot) throws IOException {
  }

  @Override
  public void postDeleteSnapshot(
      ObserverContext<MasterCoprocessorEnvironment> ctx,
      SnapshotDescription snapshot) throws IOException {
  }

  @Override
  public void preGetTableDescriptors(
      ObserverContext<MasterCoprocessorEnvironment> ctx,
      List<String> tableNamesList, List<HTableDescriptor> descriptors)
      throws IOException {
  }

  @Override
  public void postGetTableDescriptors(
      ObserverContext<MasterCoprocessorEnvironment> ctx,
      List<HTableDescriptor> descriptors) throws IOException {
  }
}
