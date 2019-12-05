/**
 *
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
package org.apache.hadoop.hbase.client;

import com.google.protobuf.ByteString;
import com.google.protobuf.ServiceException;
import com.xiaomi.infra.hbase.salted.KeySalter;
import com.xiaomi.infra.hbase.salted.SaltedHTable;

import java.io.Closeable;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.RegionException;
import org.apache.hadoop.hbase.RegionLoad;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotDisabledException;
import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.UnknownRegionException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.catalog.MetaReader;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.client.MetaScanner.MetaScannerVisitor;
import org.apache.hadoop.hbase.client.MetaScanner.MetaScannerVisitorBase;
import org.apache.hadoop.hbase.client.replication.ReplicationSerDeHelper;
import org.apache.hadoop.hbase.client.replication.ReplicationUtils;
import org.apache.hadoop.hbase.client.replication.TableCFs;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.hadoop.hbase.ipc.HBaseRpcControllerImpl;
import org.apache.hadoop.hbase.ipc.MasterCoprocessorRpcChannel;
import org.apache.hadoop.hbase.ipc.RegionServerCoprocessorRpcChannel;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.RequestConverter;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.AdminService;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.CloseRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.CloseRegionResponse;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.CompactRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.CompactionEnableRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.CompactionEnableResponse;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.FlushRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetRegionInfoRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetRegionInfoResponse;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetRegionInfoResponse.CompactionState;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.RollWALWriterRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.RollWALWriterResponse;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.StopServerRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ClientService;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ScanRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ScanResponse;
import org.apache.hadoop.hbase.protobuf.generated.ClusterStatusProtos;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.NameStringPair;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.ProcedureDescription;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.TableSchema;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.AddColumnRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.AssignRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.CreateNamespaceRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.CreateTableRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.DeleteColumnRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.DeleteNamespaceRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.DeleteSnapshotRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.DeleteTableRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.DisableTableRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.DispatchMergingRegionsRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.EnableCatalogJanitorRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.EnableTableRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.ExecProcedureRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.ExecProcedureResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.GetCompletedSnapshotsRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.GetNamespaceDescriptorRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.GetSchemaAlterStatusRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.GetSchemaAlterStatusResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.GetTableDescriptorsRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.GetTableDescriptorsResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.IsProcedureDoneRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.IsRestoreSnapshotDoneRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.IsSnapshotDoneRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.IsSplitOrMergeEnabledRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.ListNamespaceDescriptorsRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.ListRegionQuotaRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.ListRegionQuotaResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.ListTableDescriptorsByNamespaceRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.ListTableNamesByNamespaceRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.MasterService;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.MasterService.BlockingInterface;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.MasterSwitchType;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.ModifyColumnRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.ModifyNamespaceRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.ModifyTableRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.MoveRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.OfflineRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.RestoreSnapshotRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.RestoreSnapshotResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.SetBalancerRunningRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.SetQuotaRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.SetRegionQuotaRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.SetSplitOrMergeEnabledRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.ShutdownRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.SnapshotRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.SnapshotResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.StopMasterRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.SwitchThrottleRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.SwitchThrottleResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.TruncateTableRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.UnassignRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.ReplicationProtos;
import org.apache.hadoop.hbase.protobuf.generated.ReplicationProtos.GetReplicationPeerConfigResponse;
import org.apache.hadoop.hbase.protobuf.generated.SnapshotProtos.SnapshotDescription;
import org.apache.hadoop.hbase.quotas.QuotaFilter;
import org.apache.hadoop.hbase.quotas.QuotaRetriever;
import org.apache.hadoop.hbase.quotas.QuotaSettings;
import org.apache.hadoop.hbase.quotas.RegionQuotaSettings;
import org.apache.hadoop.hbase.quotas.ThrottleState;
import org.apache.hadoop.hbase.quotas.ThrottleType;
import org.apache.hadoop.hbase.regionserver.wal.FailedLogCloseException;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationLoadSource;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.ReplicationPeerDescription;
import org.apache.hadoop.hbase.snapshot.ClientSnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.HBaseSnapshotException;
import org.apache.hadoop.hbase.snapshot.RestoreSnapshotException;
import org.apache.hadoop.hbase.snapshot.SnapshotCreationException;
import org.apache.hadoop.hbase.snapshot.UnknownSnapshotException;
import org.apache.hadoop.hbase.util.Addressing;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.util.StringUtils;

/**
 * Provides an interface to manage HBase database table metadata + general administrative functions.
 * Use HBaseAdmin to create, drop, list, enable and disable tables. Use it also to add and drop
 * table column families.
 * <p>
 * See {@link HTable} to add, update, and delete data from an individual table.
 * <p>
 * Currently HBaseAdmin instances are not expected to be long-lived. For example, an HBaseAdmin
 * instance will not ride over a Master restart.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class HBaseAdmin implements Abortable, Closeable {
  private static final Log LOG = LogFactory.getLog(HBaseAdmin.class);

  // We use the implementation class rather then the interface because we
  // need the package protected functions to get the connection to master
  private HConnection connection;

  private volatile Configuration conf;
  private final long pause;
  private final int numRetries;
  // Some operations can take a long time such as disable of big table.
  // numRetries is for 'normal' stuff... Multiply by this factor when
  // want to wait a long time.
  private final int retryLongerMultiplier;
  private boolean aborted;
  private boolean cleanupConnectionOnClose = false; // close the connection in close()
  private boolean closed = false;
  private RpcRetryingCallerFactory rpcCallerFactory;

  /**
   * Constructor. See {@link #HBaseAdmin(HConnection connection)}
   * @param c Configuration object. Copied internally.
   */
  public HBaseAdmin(Configuration c)
      throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
    // Will not leak connections, as the new implementation of the constructor
    // does not throw exceptions anymore.
    this(HConnectionManager.getConnection(new Configuration(c)));
    this.cleanupConnectionOnClose = true;
  }

  /**
   * Constructor for externally managed HConnections. The connection to master will be created when
   * required by admin functions.
   * @param connection The HConnection instance to use
   * @throws MasterNotRunningException, ZooKeeperConnectionException are not thrown anymore but kept
   *           into the interface for backward api compatibility
   */
  public HBaseAdmin(HConnection connection)
      throws MasterNotRunningException, ZooKeeperConnectionException {
    this.conf = connection.getConfiguration();
    this.connection = connection;

    this.pause =
        this.conf.getLong(HConstants.HBASE_CLIENT_PAUSE, HConstants.DEFAULT_HBASE_CLIENT_PAUSE);
    this.numRetries = this.conf.getInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER,
      HConstants.DEFAULT_HBASE_CLIENT_RETRIES_NUMBER);
    this.retryLongerMultiplier = this.conf.getInt("hbase.client.retries.longer.multiplier", 10);
    this.rpcCallerFactory = connection.getRpcRetryingCallerFactory();
  }

  @Override
  public void abort(String why, Throwable e) {
    // Currently does nothing but throw the passed message and exception
    this.aborted = true;
    throw new RuntimeException(why, e);
  }

  @Override
  public boolean isAborted() {
    return this.aborted;
  }

  /** @return HConnection used by this object. */
  public HConnection getConnection() {
    return connection;
  }

  /**
   * @return - true if the master server is running. Throws an exception otherwise.
   * @throws ZooKeeperConnectionException
   * @throws MasterNotRunningException
   */
  public boolean isMasterRunning() throws IOException {
    return connection.isMasterRunning();
  }

  /**
   * @param tableName Table to check.
   * @return True if table exists already.
   * @throws IOException
   */
  public boolean tableExists(TableName tableName) throws IOException {
    return MetaReader.tableExists(connection, tableName);
  }

  public boolean tableExists(final byte[] tableName) throws IOException {
    return tableExists(TableName.valueOf(tableName));
  }

  public boolean tableExists(final String tableName) throws IOException {
    return tableExists(TableName.valueOf(tableName));
  }

  /**
   * List all the userspace tables. In other words, scan the hbase:meta table. If we wanted this to
   * be really fast, we could implement a special catalog table that just contains table names and
   * their descriptors. Right now, it only exists as part of the hbase:meta table's region info.
   * @return - returns an array of HTableDescriptors
   * @throws IOException if a remote or network exception occurs
   */
  public HTableDescriptor[] listTables() throws IOException {
    return this.connection.listTables();
  }

  /**
   * List all the userspace tables matching the given pattern.
   * @param pattern The compiled regular expression to match against
   * @return - returns an array of HTableDescriptors
   * @throws IOException if a remote or network exception occurs
   * @see #listTables()
   */
  public HTableDescriptor[] listTables(Pattern pattern) throws IOException {
    return this.connection.listTables(pattern.toString());
  }

  /**
   * List all the userspace tables matching the given regular expression.
   * @param regex The regular expression to match against
   * @return - returns an array of HTableDescriptors
   * @throws IOException if a remote or network exception occurs
   * @see #listTables(java.util.regex.Pattern)
   */
  public HTableDescriptor[] listTables(String regex) throws IOException {
    return listTables(Pattern.compile(regex));
  }

  /**
   * List all of the names of userspace tables.
   * @return String[] table names
   * @throws IOException if a remote or network exception occurs
   */
  @Deprecated
  public String[] getTableNames() throws IOException {
    return this.connection.getTableNames();
  }

  /**
   * List all of the names of userspace tables matching the given regular expression.
   * @param pattern The regular expression to match against
   * @return String[] table names
   * @throws IOException if a remote or network exception occurs
   */
  @Deprecated
  public String[] getTableNames(Pattern pattern) throws IOException {
    List<String> matched = new ArrayList<String>();
    for (String name : this.connection.getTableNames()) {
      if (pattern.matcher(name).matches()) {
        matched.add(name);
      }
    }
    return matched.toArray(new String[matched.size()]);
  }

  /**
   * List all of the names of userspace tables matching the given regular expression.
   * @param regex The regular expression to match against
   * @return String[] table names
   * @throws IOException if a remote or network exception occurs
   */
  @Deprecated
  public String[] getTableNames(String regex) throws IOException {
    return getTableNames(Pattern.compile(regex));
  }

  /**
   * List all of the names of userspace tables.
   * @return TableName[] table names
   * @throws IOException if a remote or network exception occurs
   */
  public TableName[] listTableNames() throws IOException {
    return this.connection.listTableNames();
  }

  /**
   * Method for getting the tableDescriptor
   * @param tableName as a byte []
   * @return the tableDescriptor
   * @throws TableNotFoundException
   * @throws IOException if a remote or network exception occurs
   */
  public HTableDescriptor getTableDescriptor(final TableName tableName)
      throws TableNotFoundException, IOException {
    return this.connection.getHTableDescriptor(tableName);
  }

  public HTableDescriptor getTableDescriptor(final byte[] tableName)
      throws TableNotFoundException, IOException {
    return getTableDescriptor(TableName.valueOf(tableName));
  }

  private long getPauseTime(int tries) {
    int triesCount = tries;
    if (triesCount >= HConstants.RETRY_BACKOFF.length) {
      triesCount = HConstants.RETRY_BACKOFF.length - 1;
    }
    return this.pause * HConstants.RETRY_BACKOFF[triesCount];
  }

  /**
   * Creates a new table. Synchronous operation.
   * @param desc table descriptor for table
   * @throws IllegalArgumentException if the table name is reserved
   * @throws MasterNotRunningException if master is not running
   * @throws TableExistsException if table already exists (If concurrent threads, the table may have
   *           been created between test-for-existence and attempt-at-creation).
   * @throws IOException if a remote or network exception occurs
   */
  public void createTable(HTableDescriptor desc) throws IOException {
    createTable(desc, null);
  }

  /**
   * Creates a new table with the specified number of regions. The start key specified will become
   * the end key of the first region of the table, and the end key specified will become the start
   * key of the last region of the table (the first region has a null start key and the last region
   * has a null end key). BigInteger math will be used to divide the key range specified into enough
   * segments to make the required number of total regions. Synchronous operation.
   * @param desc table descriptor for table
   * @param startKey beginning of key range
   * @param endKey end of key range
   * @param numRegions the total number of regions to create
   * @throws IllegalArgumentException if the table name is reserved
   * @throws MasterNotRunningException if master is not running
   * @throws org.apache.hadoop.hbase.TableExistsException if table already exists (If concurrent
   *           threads, the table may have been created between test-for-existence and
   *           attempt-at-creation).
   * @throws IOException
   */
  public void createTable(HTableDescriptor desc, byte[] startKey, byte[] endKey, int numRegions)
      throws IOException {
    if (numRegions < 3) {
      throw new IllegalArgumentException("Must create at least three regions");
    } else if (Bytes.compareTo(startKey, endKey) >= 0) {
      throw new IllegalArgumentException("Start key must be smaller than end key");
    }
    if (numRegions == 3) {
      createTable(desc, new byte[][] { startKey, endKey });
      return;
    }
    byte[][] splitKeys = Bytes.split(startKey, endKey, numRegions - 3);
    if (splitKeys == null || splitKeys.length != numRegions - 1) {
      throw new IllegalArgumentException("Unable to split key range into enough regions");
    }
    createTable(desc, splitKeys);
  }

  /**
   * Creates a new table with an initial set of empty regions defined by the specified split keys.
   * The total number of regions created will be the number of split keys plus one. Synchronous
   * operation. Note : Avoid passing empty split key.
   * @param desc table descriptor for table
   * @param splitKeys array of split keys for the initial regions of the table
   * @throws IllegalArgumentException if the table name is reserved, if the split keys are repeated
   *           and if the split key has empty byte array.
   * @throws MasterNotRunningException if master is not running
   * @throws org.apache.hadoop.hbase.TableExistsException if table already exists (If concurrent
   *           threads, the table may have been created between test-for-existence and
   *           attempt-at-creation).
   * @throws IOException
   */
  public void createTable(final HTableDescriptor desc, byte[][] splitKeys) throws IOException {

    // use slots to pre-split table if splitKeys is not set and the table is salted
    // there is no change to set splitKeys in coprocessor of server-side so that we reset here
    if (desc.getSlotsCount() == null && desc.getKeySalter() != null) {
      throw new IOException("must specify SLOTS_COUNT when KEY_SALTER is set");
    }
    if (splitKeys == null && desc.isSalted()) {
      KeySalter salter = SaltedHTable.createKeySalter(desc.getKeySalter(), desc.getSlotsCount());
      if (salter.getAllSalts().length > 1) {
        splitKeys = new byte[salter.getAllSalts().length - 1][];
        // there won't be rowkey smaller than the first slot after salted
        for (int i = 0; i < splitKeys.length; ++i) {
          splitKeys[i] = salter.getAllSalts()[i + 1];
        }
      }
    }

    try {
      createTableAsync(desc, splitKeys);
    } catch (SocketTimeoutException ste) {
      LOG.warn("Creating " + desc.getTableName() + " took too long", ste);
    }
    int numRegs = splitKeys == null ? 1 : splitKeys.length + 1;
    int prevRegCount = 0;
    boolean doneWithMetaScan = false;
    for (int tries = 0; tries < this.numRetries * this.retryLongerMultiplier; ++tries) {
      if (!doneWithMetaScan) {
        // Wait for new table to come on-line
        final AtomicInteger actualRegCount = new AtomicInteger(0);
        MetaScannerVisitor visitor = new MetaScannerVisitorBase() {
          @Override
          public boolean processRow(Result rowResult) throws IOException {
            HRegionInfo info = HRegionInfo.getHRegionInfo(rowResult);
            if (info == null) {
              LOG.warn("No serialized HRegionInfo in " + rowResult);
              return true;
            }
            if (!info.getTable().equals(desc.getTableName())) {
              return false;
            }
            ServerName serverName = HRegionInfo.getServerName(rowResult);
            // Make sure that regions are assigned to server
            if (!(info.isOffline() || info.isSplit()) && serverName != null &&
                serverName.getHostAndPort() != null) {
              actualRegCount.incrementAndGet();
            }
            return true;
          }
        };
        MetaScanner.metaScan(conf, connection, visitor, desc.getTableName());

        // if the server side enable IGNORE_SPLITS_WHEN_CREATE_TABLE option,
        if (actualRegCount.get() > 0) {
          HTableDescriptor htdFromMaster = getTableDescriptor(desc.getName());
          if (htdFromMaster.getValue(HTableDescriptor.IGNORE_SPLITS_WHEN_CREATING) != null &&
              Boolean.parseBoolean(
                htdFromMaster.getValue(HTableDescriptor.IGNORE_SPLITS_WHEN_CREATING))) {
            numRegs = 1;
          }
        }

        if (actualRegCount.get() < numRegs) {
          if (tries == this.numRetries * this.retryLongerMultiplier - 1) {
            throw new RegionOfflineException("Only " + actualRegCount.get() + " of " + numRegs +
                " regions are online; retries exhausted.");
          }
          try { // Sleep
            Thread.sleep(getPauseTime(tries));
          } catch (InterruptedException e) {
            throw new InterruptedIOException("Interrupted when opening" + " regions; " +
                actualRegCount.get() + " of " + numRegs + " regions processed so far");
          }
          if (actualRegCount.get() > prevRegCount) { // Making progress
            prevRegCount = actualRegCount.get();
            tries = -1;
          }
        } else {
          doneWithMetaScan = true;
          tries = -1;
        }
      } else if (isTableEnabled(desc.getTableName())) {
        // if creating table successfully in source table, then make sure that the table will be
        // created in sink replicated clusters.
        createTableForPeers(conf, desc, splitKeys);
        return;
      } else {
        try { // Sleep
          Thread.sleep(getPauseTime(tries));
        } catch (InterruptedException e) {
          throw new InterruptedIOException(
              "Interrupted when waiting" + " for table to be enabled; meta scan was done");
        }
      }
    }
    throw new TableNotEnabledException("Retries exhausted while still waiting for table: " +
        desc.getTableName() + " to be enabled");
  }

  private boolean shouldSyncTableSchema(Configuration conf) {
    return conf.getBoolean(HConstants.REPLICATION_SYNC_TABLE_SCHEMA,
      HConstants.REPLICATION_SYNC_TABLE_SCHEMA_DEFAULT)
        && conf.getBoolean(HConstants.REPLICATION_ENABLE_KEY,
          HConstants.REPLICATION_ENABLE_DEFAULT);
  }

  private void createTableForPeers(Configuration conf, final HTableDescriptor desc,
      byte[][] splitKeys) throws IOException {
    if (!shouldSyncTableSchema(conf)) {
      return;
    }
    boolean noReplicateCol = Arrays.stream(desc.getColumnFamilies())
        .allMatch(c -> c.getScope() == HConstants.REPLICATION_SCOPE_LOCAL);
    if (noReplicateCol) {
      LOG.warn("Table " + desc.getTableName()
          + "has no column family which need to replicate, so skip create table for peers.");
      return;
    }
    List<ReplicationPeerDescription> peers = listReplicationPeers();
    TableName tableName = desc.getTableName();
    for (ReplicationPeerDescription peerDesc : peers) {
      if (peerDesc.getPeerConfig().needToReplicate(tableName)) {
        Configuration peerConf = ReplicationUtils.getPeerClusterConfiguration(peerDesc, conf);
        try (HConnection connection = HConnectionManager.createConnection(peerConf);
            HBaseAdmin repHBaseAdmin = new HBaseAdmin(connection)) {
          if (!repHBaseAdmin.tableExists(tableName)) {
            repHBaseAdmin.createTable(desc, splitKeys);
          }
        }
      }
    }
  }

  /**
   * Creates a new table but does not block and wait for it to come online. Asynchronous operation.
   * To check if the table exists, use {@link #isTableAvailable} -- it is not safe to create an
   * HTable instance to this table before it is available. Note : Avoid passing empty split key.
   * @param desc table descriptor for table
   * @throws IllegalArgumentException Bad table name, if the split keys are repeated and if the
   *           split key has empty byte array.
   * @throws MasterNotRunningException if master is not running
   * @throws org.apache.hadoop.hbase.TableExistsException if table already exists (If concurrent
   *           threads, the table may have been created between test-for-existence and
   *           attempt-at-creation).
   * @throws IOException
   */
  public void createTableAsync(HTableDescriptor desc, byte[][] splitKeys) throws IOException {
    if (desc.getTableName() == null) {
      throw new IllegalArgumentException("TableName cannot be null");
    }
    if (splitKeys != null && splitKeys.length > 0) {
      Arrays.sort(splitKeys, Bytes.BYTES_COMPARATOR);
      // Verify there are no duplicate split keys
      byte[] lastKey = null;
      for (byte[] splitKey : splitKeys) {
        if (Bytes.compareTo(splitKey, HConstants.EMPTY_BYTE_ARRAY) == 0) {
          throw new IllegalArgumentException(
              "Empty split key must not be passed in the split keys.");
        }
        if (lastKey != null && Bytes.equals(splitKey, lastKey)) {
          throw new IllegalArgumentException(
              "All split keys must be unique, " + "found duplicate: " +
                  Bytes.toStringBinary(splitKey) + ", " + Bytes.toStringBinary(lastKey));
        }
        lastKey = splitKey;
      }
    }
    final CreateTableRequest req = RequestConverter.buildCreateTableRequest(desc, splitKeys);
    executeCallable(new MasterCallable<Void>(getConnection()) {

      @Override
      protected Void rpcCall(MasterService.BlockingInterface master, HBaseRpcController controller)
          throws ServiceException {
        master.createTable(controller, req);
        return null;
      }
    });
  }

  public void deleteTable(final String tableName) throws IOException {
    deleteTable(TableName.valueOf(tableName));
  }

  public void deleteTable(final byte[] tableName) throws IOException {
    deleteTable(TableName.valueOf(tableName));
  }

  /**
   * Deletes a table. Synchronous operation.
   * @param tableName name of table to delete
   * @throws IOException if a remote or network exception occurs
   */
  public void deleteTable(TableName tableName) throws IOException {
    boolean tableExists = true;
    final DeleteTableRequest deleteReq = RequestConverter.buildDeleteTableRequest(tableName);
    executeCallable(new MasterCallable<Void>(getConnection(), tableName) {

      @Override
      protected Void rpcCall(MasterService.BlockingInterface master, HBaseRpcController controller)
          throws ServiceException {
        master.deleteTable(controller, deleteReq);
        return null;
      }
    });

    int failures = 0;
    // Wait until all regions deleted
    for (int tries = 0; tries < (this.numRetries * this.retryLongerMultiplier); tries++) {
      try {
        HRegionLocation firstMetaServer = getFirstMetaServerForTable(tableName);
        Scan scan = MetaReader.getScanForTableName(tableName);
        scan.addColumn(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER);
        ScanRequest request = RequestConverter
            .buildScanRequest(firstMetaServer.getRegionInfo().getRegionName(), scan, 1, true);
        Result[] values = null;
        // Get a batch at a time.
        ClientService.BlockingInterface server =
            connection.getClient(firstMetaServer.getServerName());
        HBaseRpcController controller = new HBaseRpcControllerImpl();
        try {
          controller.setPriority(tableName);
          ScanResponse response = server.scan(controller, request);
          values = ResponseConverter.getResults(controller.cellScanner(), response);
        } catch (ServiceException se) {
          throw ProtobufUtil.getRemoteException(se);
        }

        // let us wait until hbase:meta table is updated and
        // HMaster removes the table from its HTableDescriptors
        if (values == null || values.length == 0) {
          tableExists = false;
          GetTableDescriptorsResponse htds;
          MasterKeepAliveConnection master = connection.getKeepAliveMasterService();
          try {
            GetTableDescriptorsRequest req =
                RequestConverter.buildGetTableDescriptorsRequest(tableName);
            htds = master.getTableDescriptors(null, req);
          } catch (ServiceException se) {
            throw ProtobufUtil.getRemoteException(se);
          } finally {
            master.close();
          }
          tableExists = !htds.getTableSchemaList().isEmpty();
          if (!tableExists) {
            break;
          }
        }
      } catch (IOException ex) {
        failures++;
        if (failures == numRetries - 1) { // no more tries left
          if (ex instanceof RemoteException) {
            throw ((RemoteException) ex).unwrapRemoteException();
          } else {
            throw ex;
          }
        }
      }
      try {
        Thread.sleep(getPauseTime(tries));
      } catch (InterruptedException e) {
        // continue
      }
    }

    if (tableExists) {
      throw new IOException("Retries exhausted, it took too long to wait" + " for the table " +
          tableName + " to be deleted.");
    }
    // Delete cached information to prevent clients from using old locations
    this.connection.clearRegionCache(tableName);
    LOG.info("Deleted " + tableName);
  }

  /**
   * Deletes tables matching the passed in pattern and wait on completion. Warning: Use this method
   * carefully, there is no prompting and the effect is immediate. Consider using
   * {@link #listTables(java.lang.String)} and {@link #deleteTable(byte[])}
   * @param regex The regular expression to match table names against
   * @return Table descriptors for tables that couldn't be deleted
   * @throws IOException
   * @see #deleteTables(java.util.regex.Pattern)
   * @see #deleteTable(java.lang.String)
   */
  public HTableDescriptor[] deleteTables(String regex) throws IOException {
    return deleteTables(Pattern.compile(regex));
  }

  /**
   * Delete tables matching the passed in pattern and wait on completion. Warning: Use this method
   * carefully, there is no prompting and the effect is immediate. Consider using
   * {@link #listTables(java.util.regex.Pattern) } and {@link #deleteTable(byte[])}
   * @param pattern The pattern to match table names against
   * @return Table descriptors for tables that couldn't be deleted
   * @throws IOException
   */
  public HTableDescriptor[] deleteTables(Pattern pattern) throws IOException {
    List<HTableDescriptor> failed = new LinkedList<HTableDescriptor>();
    for (HTableDescriptor table : listTables(pattern)) {
      try {
        deleteTable(table.getTableName());
      } catch (IOException ex) {
        LOG.info("Failed to delete table " + table.getTableName(), ex);
        failed.add(table);
      }
    }
    return failed.toArray(new HTableDescriptor[failed.size()]);
  }

  /**
   * Enable a table. May timeout. Use {@link #enableTableAsync(byte[])} and
   * {@link #isTableEnabled(byte[])} instead. The table has to be in disabled state for it to be
   * enabled.
   * @param tableName name of the table
   * @throws IOException if a remote or network exception occurs There could be couple types of
   *           IOException TableNotFoundException means the table doesn't exist.
   *           TableNotDisabledException means the table isn't in disabled state.
   * @see #isTableEnabled(byte[])
   * @see #disableTable(byte[])
   * @see #enableTableAsync(byte[])
   */
  public void enableTable(final TableName tableName) throws IOException {
    enableTableAsync(tableName);

    // Wait until all regions are enabled
    waitUntilTableIsEnabled(tableName);

    LOG.info("Enabled table " + tableName);
  }

  public void enableTable(final byte[] tableName) throws IOException {
    enableTable(TableName.valueOf(tableName));
  }

  public void enableTable(final String tableName) throws IOException {
    enableTable(TableName.valueOf(tableName));
  }

  /**
   * Wait for the table to be enabled and available If enabling the table exceeds the retry period,
   * an exception is thrown.
   * @param tableName name of the table
   * @throws IOException if a remote or network exception occurs or table is not enabled after the
   *           retries period.
   */
  private void waitUntilTableIsEnabled(final TableName tableName) throws IOException {
    boolean enabled = false;
    long start = EnvironmentEdgeManager.currentTimeMillis();
    for (int tries = 0; tries < (this.numRetries * this.retryLongerMultiplier); tries++) {
      try {
        enabled = isTableEnabled(tableName);
      } catch (TableNotFoundException tnfe) {
        // wait for table to be created
        enabled = false;
      }
      enabled = enabled && isTableAvailable(tableName);
      if (enabled) {
        break;
      }
      long sleep = getPauseTime(tries);
      if (LOG.isDebugEnabled()) {
        LOG.debug(
          "Sleeping= " + sleep + "ms, waiting for all regions to be " + "enabled in " + tableName);
      }
      try {
        Thread.sleep(sleep);
      } catch (InterruptedException e) {
        // Do this conversion rather than let it out because do not want to
        // change the method signature.
        throw (InterruptedIOException) new InterruptedIOException("Interrupted").initCause(e);
      }
    }
    if (!enabled) {
      long msec = EnvironmentEdgeManager.currentTimeMillis() - start;
      throw new IOException("Table '" + tableName + "' not yet enabled, after " + msec + "ms.");
    }
  }

  /**
   * Brings a table on-line (enables it). Method returns immediately though enable of table may take
   * some time to complete, especially if the table is large (All regions are opened as part of
   * enabling process). Check {@link #isTableEnabled(byte[])} to learn when table is fully online.
   * If table is taking too long to online, check server logs.
   * @param tableName
   * @throws IOException
   * @since 0.90.0
   */
  public void enableTableAsync(final TableName tableName) throws IOException {
    TableName.isLegalFullyQualifiedTableName(tableName.getName());
    final EnableTableRequest req = RequestConverter.buildEnableTableRequest(tableName);
    executeCallable(new MasterCallable<Void>(getConnection(), tableName) {

      @Override
      protected Void rpcCall(MasterService.BlockingInterface master, HBaseRpcController controller)
          throws ServiceException {
        LOG.info("Started enable of " + tableName);
        master.enableTable(controller, req);
        return null;
      }
    });
  }

  public void enableTableAsync(final byte[] tableName) throws IOException {
    enableTable(TableName.valueOf(tableName));
  }

  public void enableTableAsync(final String tableName) throws IOException {
    enableTableAsync(TableName.valueOf(tableName));
  }

  /**
   * Enable tables matching the passed in pattern and wait on completion. Warning: Use this method
   * carefully, there is no prompting and the effect is immediate. Consider using
   * {@link #listTables(java.lang.String)} and {@link #enableTable(byte[])}
   * @param regex The regular expression to match table names against
   * @throws IOException
   * @see #enableTables(java.util.regex.Pattern)
   * @see #enableTable(java.lang.String)
   */
  public HTableDescriptor[] enableTables(String regex) throws IOException {
    return enableTables(Pattern.compile(regex));
  }

  /**
   * Enable tables matching the passed in pattern and wait on completion. Warning: Use this method
   * carefully, there is no prompting and the effect is immediate. Consider using
   * {@link #listTables(java.util.regex.Pattern) } and {@link #enableTable(byte[])}
   * @param pattern The pattern to match table names against
   * @throws IOException
   */
  public HTableDescriptor[] enableTables(Pattern pattern) throws IOException {
    List<HTableDescriptor> failed = new LinkedList<HTableDescriptor>();
    for (HTableDescriptor table : listTables(pattern)) {
      if (isTableDisabled(table.getTableName())) {
        try {
          enableTable(table.getTableName());
        } catch (IOException ex) {
          LOG.info("Failed to enable table " + table.getTableName(), ex);
          failed.add(table);
        }
      }
    }
    return failed.toArray(new HTableDescriptor[failed.size()]);
  }

  /**
   * Starts the disable of a table. If it is being served, the master will tell the servers to stop
   * serving it. This method returns immediately. The disable of a table can take some time if the
   * table is large (all regions are closed as part of table disable operation). Call
   * {@link #isTableDisabled(byte[])} to check for when disable completes. If table is taking too
   * long to online, check server logs.
   * @param tableName name of table
   * @throws IOException if a remote or network exception occurs
   * @see #isTableDisabled(byte[])
   * @see #isTableEnabled(byte[])
   * @since 0.90.0
   */
  public void disableTableAsync(final TableName tableName) throws IOException {
    TableName.isLegalFullyQualifiedTableName(tableName.getName());
    final DisableTableRequest req = RequestConverter.buildDisableTableRequest(tableName);
    executeCallable(new MasterCallable<Void>(getConnection(), tableName) {

      @Override
      protected Void rpcCall(MasterService.BlockingInterface master, HBaseRpcController controller)
          throws ServiceException {
        LOG.info("Started disable of " + tableName);
        master.disableTable(controller, req);
        return null;
      }
    });
  }

  public void disableTableAsync(final byte[] tableName) throws IOException {
    disableTableAsync(TableName.valueOf(tableName));
  }

  public void disableTableAsync(final String tableName) throws IOException {
    disableTableAsync(TableName.valueOf(tableName));
  }

  /**
   * Disable table and wait on completion. May timeout eventually. Use
   * {@link #disableTableAsync(byte[])} and {@link #isTableDisabled(String)} instead. The table has
   * to be in enabled state for it to be disabled.
   * @param tableName
   * @throws IOException There could be couple types of IOException TableNotFoundException means the
   *           table doesn't exist. TableNotEnabledException means the table isn't in enabled state.
   */
  public void disableTable(final TableName tableName) throws IOException {
    disableTableAsync(tableName);
    // Wait until table is disabled
    boolean disabled = false;
    for (int tries = 0; tries < (this.numRetries * this.retryLongerMultiplier); tries++) {
      disabled = isTableDisabled(tableName);
      if (disabled) {
        break;
      }
      long sleep = getPauseTime(tries);
      if (LOG.isDebugEnabled()) {
        LOG.debug(
          "Sleeping= " + sleep + "ms, waiting for all regions to be " + "disabled in " + tableName);
      }
      try {
        Thread.sleep(sleep);
      } catch (InterruptedException e) {
        // Do this conversion rather than let it out because do not want to
        // change the method signature.
        throw (InterruptedIOException) new InterruptedIOException("Interrupted").initCause(e);
      }
    }
    if (!disabled) {
      throw new RegionException("Retries exhausted, it took too long to wait" + " for the table " +
          tableName + " to be disabled.");
    }
    LOG.info("Disabled " + tableName);
  }

  public void disableTable(final byte[] tableName) throws IOException {
    disableTable(TableName.valueOf(tableName));
  }

  public void disableTable(final String tableName) throws IOException {
    disableTable(TableName.valueOf(tableName));
  }

  /**
   * Disable tables matching the passed in pattern and wait on completion. Warning: Use this method
   * carefully, there is no prompting and the effect is immediate. Consider using
   * {@link #listTables(java.lang.String)} and {@link #disableTable(byte[])}
   * @param regex The regular expression to match table names against
   * @return Table descriptors for tables that couldn't be disabled
   * @throws IOException
   * @see #disableTables(java.util.regex.Pattern)
   * @see #disableTable(java.lang.String)
   */
  public HTableDescriptor[] disableTables(String regex) throws IOException {
    return disableTables(Pattern.compile(regex));
  }

  /**
   * Disable tables matching the passed in pattern and wait on completion. Warning: Use this method
   * carefully, there is no prompting and the effect is immediate. Consider using
   * {@link #listTables(java.util.regex.Pattern) } and {@link #disableTable(byte[])}
   * @param pattern The pattern to match table names against
   * @return Table descriptors for tables that couldn't be disabled
   * @throws IOException
   */
  public HTableDescriptor[] disableTables(Pattern pattern) throws IOException {
    List<HTableDescriptor> failed = new LinkedList<HTableDescriptor>();
    for (HTableDescriptor table : listTables(pattern)) {
      if (isTableEnabled(table.getTableName())) {
        try {
          disableTable(table.getTableName());
        } catch (IOException ex) {
          LOG.info("Failed to disable table " + table.getTableName(), ex);
          failed.add(table);
        }
      }
    }
    return failed.toArray(new HTableDescriptor[failed.size()]);
  }

  /*
   * Checks whether table exists. If not, throws TableNotFoundException
   * @param tableName
   */
  private void checkTableExistence(TableName tableName) throws IOException {
    if (!tableExists(tableName)) {
      throw new TableNotFoundException(tableName);
    }
  }

  /**
   * @param tableName name of table to check
   * @return true if table is on-line
   * @throws IOException if a remote or network exception occurs
   */
  public boolean isTableEnabled(TableName tableName) throws IOException {
    checkTableExistence(tableName);
    return connection.isTableEnabled(tableName);
  }

  public boolean isTableEnabled(byte[] tableName) throws IOException {
    return isTableEnabled(TableName.valueOf(tableName));
  }

  public boolean isTableEnabled(String tableName) throws IOException {
    return isTableEnabled(TableName.valueOf(tableName));
  }

  /**
   * @param tableName name of table to check
   * @return true if table is off-line
   * @throws IOException if a remote or network exception occurs
   */
  public boolean isTableDisabled(TableName tableName) throws IOException {
    checkTableExistence(tableName);
    return connection.isTableDisabled(tableName);
  }

  public boolean isTableDisabled(byte[] tableName) throws IOException {
    return isTableDisabled(TableName.valueOf(tableName));
  }

  public boolean isTableDisabled(String tableName) throws IOException {
    return isTableDisabled(TableName.valueOf(tableName));
  }

  /**
   * @param tableName name of table to check
   * @return true if all regions of the table are available
   * @throws IOException if a remote or network exception occurs
   */
  public boolean isTableAvailable(TableName tableName) throws IOException {
    return connection.isTableAvailable(tableName);
  }

  public boolean isTableAvailable(byte[] tableName) throws IOException {
    return isTableAvailable(TableName.valueOf(tableName));
  }

  public boolean isTableAvailable(String tableName) throws IOException {
    return isTableAvailable(TableName.valueOf(tableName));
  }

  /**
   * Use this api to check if the table has been created with the specified number of splitkeys
   * which was used while creating the given table. Note : If this api is used after a table's
   * region gets splitted, the api may return false.
   * @param tableName name of table to check
   * @param splitKeys keys to check if the table has been created with all split keys
   * @throws IOException if a remote or network excpetion occurs
   */
  public boolean isTableAvailable(TableName tableName, byte[][] splitKeys) throws IOException {
    return connection.isTableAvailable(tableName, splitKeys);
  }

  public boolean isTableAvailable(byte[] tableName, byte[][] splitKeys) throws IOException {
    return isTableAvailable(TableName.valueOf(tableName), splitKeys);
  }

  public boolean isTableAvailable(String tableName, byte[][] splitKeys) throws IOException {
    return isTableAvailable(TableName.valueOf(tableName), splitKeys);
  }

  /**
   * Get the status of alter command - indicates how many regions have received the updated schema
   * Asynchronous operation.
   * @param tableName TableName instance
   * @return Pair indicating the number of regions updated Pair.getFirst() is the regions that are
   *         yet to be updated Pair.getSecond() is the total number of regions of the table
   * @throws IOException if a remote or network exception occurs
   */
  public Pair<Integer, Integer> getAlterStatus(TableName tableName) throws IOException {
    final GetSchemaAlterStatusRequest req =
        RequestConverter.buildGetSchemaAlterStatusRequest(tableName);
    return executeCallable(new MasterCallable<Pair<Integer, Integer>>(getConnection(), tableName) {

      @Override
      protected Pair<Integer, Integer> rpcCall(MasterService.BlockingInterface master,
          HBaseRpcController controller) throws ServiceException {
        GetSchemaAlterStatusResponse ret = master.getSchemaAlterStatus(controller, req);
        Pair<Integer, Integer> pair = new Pair<Integer, Integer>(
            Integer.valueOf(ret.getYetToUpdateRegions()), Integer.valueOf(ret.getTotalRegions()));
        return pair;
      }
    });
  }

  /**
   * Get the status of alter command - indicates how many regions have received the updated schema
   * Asynchronous operation.
   * @param tableName name of the table to get the status of
   * @return Pair indicating the number of regions updated Pair.getFirst() is the regions that are
   *         yet to be updated Pair.getSecond() is the total number of regions of the table
   * @throws IOException if a remote or network exception occurs
   */
  public Pair<Integer, Integer> getAlterStatus(final byte[] tableName) throws IOException {
    return getAlterStatus(TableName.valueOf(tableName));
  }

  /**
   * Add a column to an existing table. Asynchronous operation.
   * @param tableName name of the table to add column to
   * @param column column descriptor of column to be added
   * @throws IOException if a remote or network exception occurs
   */
  public void addColumn(final byte[] tableName, HColumnDescriptor column) throws IOException {
    addColumn(TableName.valueOf(tableName), column);
  }

  /**
   * Add a column to an existing table. Asynchronous operation.
   * @param tableName name of the table to add column to
   * @param column column descriptor of column to be added
   * @throws IOException if a remote or network exception occurs
   */
  public void addColumn(final String tableName, HColumnDescriptor column) throws IOException {
    addColumn(TableName.valueOf(tableName), column);
  }

  /**
   * Add a column to an existing table. Asynchronous operation.
   * @param tableName name of the table to add column to
   * @param column column descriptor of column to be added
   * @throws IOException if a remote or network exception occurs
   */
  public void addColumn(TableName tableName, HColumnDescriptor column) throws IOException {
    final AddColumnRequest req = RequestConverter.buildAddColumnRequest(tableName, column);
    executeCallable(new MasterCallable<Void>(getConnection(), tableName) {

      @Override
      protected Void rpcCall(MasterService.BlockingInterface master, HBaseRpcController controller)
          throws ServiceException {
        master.addColumn(controller, req);
        return null;
      }
    });
    // sync table table to peer cluster.
    addColumnForPeers(conf, tableName, column);
  }

  private void addColumnForPeers(Configuration conf, TableName tableName, HColumnDescriptor column)
      throws IOException {
    if (!shouldSyncTableSchema(conf)) {
      return;
    }
    List<ReplicationPeerDescription> peers = listReplicationPeers();
    for (ReplicationPeerDescription peerDesc : peers) {
      if (peerDesc.getPeerConfig().needToReplicate(tableName)) {
        Configuration peerConf = ReplicationUtils.getPeerClusterConfiguration(peerDesc, conf);
        try (HConnection connection = HConnectionManager.createConnection(peerConf);
            HBaseAdmin repHBaseAdmin = new HBaseAdmin(connection)) {
          if (repHBaseAdmin.tableExists(tableName)) {
            HTableDescriptor htd = repHBaseAdmin.getTableDescriptor(tableName);
            // to avoid replication loop.
            if (htd.getFamily(column.getName()) == null) {
              repHBaseAdmin.addColumn(tableName, column);
            }
          }
        }
      }
    }
  }

  /**
   * Delete a column from a table. Asynchronous operation.
   * @param tableName name of table
   * @param columnName name of column to be deleted
   * @throws IOException if a remote or network exception occurs
   */
  public void deleteColumn(byte[] tableName, String columnName) throws IOException {
    deleteColumn(TableName.valueOf(tableName), Bytes.toBytes(columnName));
  }

  /**
   * Delete a column from a table. Asynchronous operation.
   * @param tableName name of table
   * @param columnName name of column to be deleted
   * @throws IOException if a remote or network exception occurs
   */
  public void deleteColumn(String tableName, String columnName) throws IOException {
    deleteColumn(TableName.valueOf(tableName), Bytes.toBytes(columnName));
  }

  /**
   * Delete a column from a table. Asynchronous operation.
   * @param tableName name of table
   * @param columnName name of column to be deleted
   * @throws IOException if a remote or network exception occurs
   */
  public void deleteColumn(TableName tableName, byte[] columnName) throws IOException {
    final DeleteColumnRequest req =
        RequestConverter.buildDeleteColumnRequest(tableName, columnName);
    executeCallable(new MasterCallable<Void>(getConnection()) {

      @Override
      protected Void rpcCall(MasterService.BlockingInterface master, HBaseRpcController controller)
          throws ServiceException {
        master.deleteColumn(controller, req);
        return null;
      }
    });
    deleteColumnForPeers(conf, tableName, columnName);
  }

  private void deleteColumnForPeers(Configuration conf, TableName tableName, byte[] column)
      throws IOException {
    if (!shouldSyncTableSchema(conf)) {
      return;
    }
    List<ReplicationPeerDescription> peers = listReplicationPeers();
    for (ReplicationPeerDescription peerDesc : peers) {
      if (peerDesc.getPeerConfig().needToReplicate(tableName)) {
        Configuration peerConf = ReplicationUtils.getPeerClusterConfiguration(peerDesc, conf);
        try (HConnection connection = HConnectionManager.createConnection(peerConf);
            HBaseAdmin repHBaseAdmin = new HBaseAdmin(connection)) {
          if (repHBaseAdmin.tableExists(tableName)) {
            HTableDescriptor htd = repHBaseAdmin.getTableDescriptor(tableName);
            // to avoid replication loop.
            if (htd.getFamily(column) != null) {
              repHBaseAdmin.deleteColumn(tableName, column);
            }
          }
        }
      }
    }
  }

  /**
   * Modify an existing column family on a table. Asynchronous operation.
   * @param tableName name of table
   * @param descriptor new column descriptor to use
   * @throws IOException if a remote or network exception occurs
   */
  public void modifyColumn(String tableName, HColumnDescriptor descriptor) throws IOException {
    modifyColumn(TableName.valueOf(tableName), descriptor);
  }

  /**
   * Modify an existing column family on a table. Asynchronous operation.
   * @param tableName name of table
   * @param descriptor new column descriptor to use
   * @throws IOException if a remote or network exception occurs
   */
  public void modifyColumn(byte[] tableName, HColumnDescriptor descriptor) throws IOException {
    modifyColumn(TableName.valueOf(tableName), descriptor);
  }

  /**
   * Modify an existing column family on a table. Asynchronous operation.
   * @param tableName name of table
   * @param descriptor new column descriptor to use
   * @throws IOException if a remote or network exception occurs
   */
  public void modifyColumn(TableName tableName, HColumnDescriptor descriptor) throws IOException {
    final ModifyColumnRequest req =
        RequestConverter.buildModifyColumnRequest(tableName, descriptor);
    executeCallable(new MasterCallable<Void>(getConnection(), tableName) {

      @Override
      protected Void rpcCall(MasterService.BlockingInterface master, HBaseRpcController controller)
          throws ServiceException {
        master.modifyColumn(controller, req);
        return null;
      }
    });
  }

  /**
   * Close a region. For expert-admins. Runs close on the regionserver. The master will not be
   * informed of the close.
   * @param regionname region name to close
   * @param serverName If supplied, we'll use this location rather than the one currently in
   *          <code>hbase:meta</code>
   * @throws IOException if a remote or network exception occurs
   */
  public void closeRegion(final String regionname, final String serverName) throws IOException {
    closeRegion(Bytes.toBytes(regionname), serverName);
  }

  /**
   * Close a region. For expert-admins Runs close on the regionserver. The master will not be
   * informed of the close.
   * @param regionname region name to close
   * @param serverName The servername of the regionserver. If passed null we will use servername
   *          found in the hbase:meta table. A server name is made of host, port and startcode. Here
   *          is an example: <code> host187.example.com,60020,1289493121758</code>
   * @throws IOException if a remote or network exception occurs
   */
  public void closeRegion(final byte[] regionname, final String serverName) throws IOException {
    if (serverName != null) {
      Pair<HRegionInfo, ServerName> pair = MetaReader.getRegion(connection, regionname);
      if (pair == null || pair.getFirst() == null) {
        throw new UnknownRegionException(Bytes.toStringBinary(regionname));
      } else {
        closeRegion(ServerName.valueOf(serverName), pair.getFirst());
      }
    } else {
      Pair<HRegionInfo, ServerName> pair = MetaReader.getRegion(connection, regionname);
      if (pair == null) {
        throw new UnknownRegionException(Bytes.toStringBinary(regionname));
      } else if (pair.getSecond() == null) {
        throw new NoServerForRegionException(Bytes.toStringBinary(regionname));
      } else {
        closeRegion(pair.getSecond(), pair.getFirst());
      }
    }
  }

  /**
   * For expert-admins. Runs close on the regionserver. Closes a region based on the encoded region
   * name. The region server name is mandatory. If the servername is provided then based on the
   * online regions in the specified regionserver the specified region will be closed. The master
   * will not be informed of the close. Note that the regionname is the encoded regionname.
   * @param encodedRegionName The encoded region name; i.e. the hash that makes up the region name
   *          suffix: e.g. if regionname is
   *          <code>TestTable,0094429456,1289497600452.527db22f95c8a9e0116f0cc13c680396.</code> ,
   *          then the encoded region name is: <code>527db22f95c8a9e0116f0cc13c680396</code>.
   * @param serverName The servername of the regionserver. A server name is made of host, port and
   *          startcode. This is mandatory. Here is an example:
   *          <code> host187.example.com,60020,1289493121758</code>
   * @return true if the region was closed, false if not.
   * @throws IOException if a remote or network exception occurs
   */
  public boolean closeRegionWithEncodedRegionName(final String encodedRegionName,
      final String serverName) throws IOException {
    if (null == serverName || ("").equals(serverName.trim())) {
      throw new IllegalArgumentException("The servername cannot be null or empty.");
    }
    ServerName sn = ServerName.valueOf(serverName);
    AdminService.BlockingInterface admin = this.connection.getAdmin(sn);
    // Close the region without updating zk state.
    CloseRegionRequest request =
        RequestConverter.buildCloseRegionRequest(sn, encodedRegionName, false);
    try {
      CloseRegionResponse response = admin.closeRegion(null, request);
      boolean isRegionClosed = response.getClosed();
      if (false == isRegionClosed) {
        LOG.error("Not able to close the region " + encodedRegionName + ".");
      }
      return isRegionClosed;
    } catch (ServiceException se) {
      throw ProtobufUtil.getRemoteException(se);
    }
  }

  /**
   * Close a region. For expert-admins Runs close on the regionserver. The master will not be
   * informed of the close.
   * @param sn
   * @param hri
   * @throws IOException
   */
  public void closeRegion(final ServerName sn, final HRegionInfo hri) throws IOException {
    AdminService.BlockingInterface admin = this.connection.getAdmin(sn);
    // Close the region without updating zk state.
    ProtobufUtil.closeRegion(admin, sn, hri.getRegionName(), false);
  }

  /**
   * Get all the online regions on a region server.
   */
  public List<HRegionInfo> getOnlineRegions(final ServerName sn) throws IOException {
    AdminService.BlockingInterface admin = this.connection.getAdmin(sn);
    return ProtobufUtil.getOnlineRegions(admin);
  }

  /**
   * Flush a table or an individual region. Synchronous operation.
   * @param tableNameOrRegionName table or region to flush
   * @throws IOException if a remote or network exception occurs
   * @throws InterruptedException
   */
  public void flush(final String tableNameOrRegionName) throws IOException, InterruptedException {
    flush(Bytes.toBytes(tableNameOrRegionName));
  }

  /**
   * Flush a table or an individual region. Synchronous operation.
   * @param tableNameOrRegionName table or region to flush
   * @throws IOException if a remote or network exception occurs
   * @throws InterruptedException
   */
  public void flush(final byte[] tableNameOrRegionName) throws IOException, InterruptedException {
    Pair<HRegionInfo, ServerName> regionServerPair = getRegion(tableNameOrRegionName);
    if (regionServerPair != null) {
      if (regionServerPair.getSecond() == null) {
        throw new NoServerForRegionException(Bytes.toStringBinary(tableNameOrRegionName));
      } else {
        flush(regionServerPair.getSecond(), regionServerPair.getFirst());
      }
    } else {
      TableName tableName = checkTableExists(TableName.valueOf(tableNameOrRegionName));
      for (HRegionLocation loc : connection.locateRegions(tableName, false, false)) {
        if (loc.getServerName() == null || loc.getRegionInfo().isOffline()) {
          continue;
        }
        try {
          flush(loc.getServerName(), loc.getRegionInfo());
        } catch (NotServingRegionException e) {
          if (LOG.isDebugEnabled()) {
            LOG.debug(
              "Trying to flush " + loc.getRegionInfo() + ": " + StringUtils.stringifyException(e));
          }
        }
      }
    }
  }

  private void flush(final ServerName sn, final HRegionInfo hri) throws IOException {
    AdminService.BlockingInterface admin = this.connection.getAdmin(sn);
    FlushRegionRequest request = RequestConverter.buildFlushRegionRequest(hri.getRegionName());
    try {
      admin.flushRegion(null, request);
    } catch (ServiceException se) {
      throw ProtobufUtil.getRemoteException(se);
    }
  }

  /**
   * Compact a table or an individual region. Asynchronous operation.
   * @param tableNameOrRegionName table or region to compact
   * @throws IOException if a remote or network exception occurs
   * @throws InterruptedException
   */
  public void compact(final String tableNameOrRegionName) throws IOException, InterruptedException {
    compact(Bytes.toBytes(tableNameOrRegionName));
  }

  /**
   * Compact a table or an individual region. Asynchronous operation.
   * @param tableNameOrRegionName table or region to compact
   * @throws IOException if a remote or network exception occurs
   * @throws InterruptedException
   */
  public void compact(final byte[] tableNameOrRegionName) throws IOException, InterruptedException {
    compact(tableNameOrRegionName, null, false);
  }

  /**
   * Compact a column family within a table or region. Asynchronous operation.
   * @param tableOrRegionName table or region to compact
   * @param columnFamily column family within a table or region
   * @throws IOException if a remote or network exception occurs
   * @throws InterruptedException
   */
  public void compact(String tableOrRegionName, String columnFamily)
      throws IOException, InterruptedException {
    compact(Bytes.toBytes(tableOrRegionName), Bytes.toBytes(columnFamily));
  }

  /**
   * Compact a column family within a table or region. Asynchronous operation.
   * @param tableNameOrRegionName table or region to compact
   * @param columnFamily column family within a table or region
   * @throws IOException if a remote or network exception occurs
   * @throws InterruptedException
   */
  public void compact(final byte[] tableNameOrRegionName, final byte[] columnFamily)
      throws IOException, InterruptedException {
    compact(tableNameOrRegionName, columnFamily, false);
  }

  /**
   * Major compact a table or an individual region. Asynchronous operation.
   * @param tableNameOrRegionName table or region to major compact
   * @throws IOException if a remote or network exception occurs
   * @throws InterruptedException
   */
  public void majorCompact(final String tableNameOrRegionName)
      throws IOException, InterruptedException {
    majorCompact(Bytes.toBytes(tableNameOrRegionName));
  }

  /**
   * Major compact a table or an individual region. Asynchronous operation.
   * @param tableNameOrRegionName table or region to major compact
   * @throws IOException if a remote or network exception occurs
   * @throws InterruptedException
   */
  public void majorCompact(final byte[] tableNameOrRegionName)
      throws IOException, InterruptedException {
    compact(tableNameOrRegionName, null, true);
  }

  /**
   * Major compact a column family within a table or region. Asynchronous operation.
   * @param tableNameOrRegionName table or region to major compact
   * @param columnFamily column family within a table or region
   * @throws IOException if a remote or network exception occurs
   * @throws InterruptedException
   */
  public void majorCompact(final String tableNameOrRegionName, final String columnFamily)
      throws IOException, InterruptedException {
    majorCompact(Bytes.toBytes(tableNameOrRegionName), Bytes.toBytes(columnFamily));
  }

  /**
   * Major compact a column family within a table or region. Asynchronous operation.
   * @param tableNameOrRegionName table or region to major compact
   * @param columnFamily column family within a table or region
   * @throws IOException if a remote or network exception occurs
   * @throws InterruptedException
   */
  public void majorCompact(final byte[] tableNameOrRegionName, final byte[] columnFamily)
      throws IOException, InterruptedException {
    compact(tableNameOrRegionName, columnFamily, true);
  }

  /**
   * Compact a table or an individual region. Asynchronous operation.
   * @param tableNameOrRegionName table or region to compact
   * @param columnFamily column family within a table or region
   * @param major True if we are to do a major compaction.
   * @throws IOException if a remote or network exception occurs
   * @throws InterruptedException
   */
  private void compact(final byte[] tableNameOrRegionName, final byte[] columnFamily,
      final boolean major) throws IOException, InterruptedException {
    Pair<HRegionInfo, ServerName> regionServerPair = getRegion(tableNameOrRegionName);
    if (regionServerPair != null) {
      if (regionServerPair.getSecond() == null) {
        throw new NoServerForRegionException(Bytes.toStringBinary(tableNameOrRegionName));
      } else {
        compact(regionServerPair.getSecond(), regionServerPair.getFirst(), major, columnFamily);
      }
    } else {
      final TableName tableName = checkTableExists(TableName.valueOf(tableNameOrRegionName));
      for (HRegionLocation loc : connection.locateRegions(tableName, false, false)) {
        if (loc.getServerName() == null || loc.getRegionInfo().isOffline()) {
          continue;
        }
        try {
          compact(loc.getServerName(), loc.getRegionInfo(), major, columnFamily);
        } catch (NotServingRegionException e) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Trying to" + (major ? " major" : "") + " compact " + loc.getRegionInfo() +
              ": " + StringUtils.stringifyException(e));
          }
        }
      }
    }
  }

  private void compact(final ServerName sn, final HRegionInfo hri, final boolean major,
      final byte[] family) throws IOException {
    AdminService.BlockingInterface admin = this.connection.getAdmin(sn);
    CompactRegionRequest request =
        RequestConverter.buildCompactRegionRequest(hri.getRegionName(), major, family);
    try {
      admin.compactRegion(null, request);
    } catch (ServiceException se) {
      throw ProtobufUtil.getRemoteException(se);
    }
  }

  /**
   * Compact all regions on the region server
   * @param sn the region server name
   * @param major if it's major compaction
   * @throws IOException
   */
  public void compactRegionServer(final ServerName sn, boolean major) throws IOException {
    for (HRegionInfo region : getOnlineRegions(sn)) {
      compact(sn, region, major, null);
    }
  }

  /**
   * Move the region <code>r</code> to <code>dest</code>.
   * @param encodedRegionName The encoded region name; i.e. the hash that makes up the region name
   *          suffix: e.g. if regionname is
   *          <code>TestTable,0094429456,1289497600452.527db22f95c8a9e0116f0cc13c680396.</code>,
   *          then the encoded region name is: <code>527db22f95c8a9e0116f0cc13c680396</code>.
   * @param destServerName The servername of the destination regionserver. If passed the empty byte
   *          array we'll assign to a random server. A server name is made of host, port and
   *          startcode. Here is an example: <code> host187.example.com,60020,1289493121758</code>
   * @throws IOException
   */
  public void move(final byte[] encodedRegionName, final byte[] destServerName) throws IOException {
    final MoveRegionRequest req =
        RequestConverter.buildMoveRegionRequest(encodedRegionName, destServerName);
    executeCallable(new MasterCallable<Void>(getConnection(), encodedRegionName) {

      @Override
      protected Void rpcCall(MasterService.BlockingInterface master, HBaseRpcController controller)
          throws ServiceException {
        master.moveRegion(controller, req);
        return null;
      }
    });
  }

  /**
   * @param regionName Region name to assign.
   * @throws IOException
   */
  public void assign(final byte[] regionName) throws IOException {
    byte[] toBeAssigned = getRegionName(regionName);
    final AssignRegionRequest req = RequestConverter.buildAssignRegionRequest(toBeAssigned);
    executeCallable(new MasterCallable<Void>(getConnection(), toBeAssigned) {

      @Override
      protected Void rpcCall(MasterService.BlockingInterface master, HBaseRpcController controller)
          throws ServiceException {
        master.assignRegion(controller, req);
        return null;
      }
    });
  }

  /**
   * Unassign a region from current hosting regionserver. Region will then be assigned to a
   * regionserver chosen at random. Region could be reassigned back to the same server. Use
   * {@link #move(byte[], byte[])} if you want to control the region movement.
   * @param regionName Region to unassign. Will clear any existing RegionPlan if one found.
   * @param force If true, force unassign (Will remove region from regions-in-transition too if
   *          present. If results in double assignment use hbck -fix to resolve. To be used by
   *          experts).
   * @throws IOException
   */
  public void unassign(final byte[] regionName, final boolean force) throws IOException {
    byte[] toBeUnassigned = getRegionName(regionName);
    final UnassignRegionRequest request =
        RequestConverter.buildUnassignRegionRequest(toBeUnassigned, force);
    executeCallable(new MasterCallable<Void>(getConnection(), toBeUnassigned) {
      @Override
      protected Void rpcCall(MasterService.BlockingInterface master, HBaseRpcController controller)
          throws ServiceException {
        master.unassignRegion(controller, request);
        return null;
      }
    });
  }

  /**
   * Offline specified region from master's in-memory state. It will not attempt to reassign the
   * region as in unassign. This API can be used when a region not served by any region server and
   * still online as per Master's in memory state. If this API is incorrectly used on active region
   * then master will loose track of that region. This is a special method that should be used by
   * experts or hbck.
   * @param regionName Region to offline.
   * @throws IOException
   */
  public void offline(final byte[] regionName) throws IOException {
    final OfflineRegionRequest req = RequestConverter.buildOfflineRegionRequest(regionName);
    executeCallable(new MasterCallable<Void>(getConnection(), regionName) {

      @Override
      protected Void rpcCall(MasterService.BlockingInterface master, HBaseRpcController controller)
          throws ServiceException {
        master.offlineRegion(controller, req);
        return null;
      }
    });
  }

  /**
   * Turn the load balancer on or off.
   * @param on If true, enable balancer. If false, disable balancer.
   * @param synchronous If true, it waits until current balance() call, if outstanding, to return.
   * @return Previous balancer value
   */
  public boolean setBalancerRunning(boolean on, boolean synchronous) throws IOException {
    final SetBalancerRunningRequest req =
        RequestConverter.buildSetBalancerRunningRequest(on, synchronous);
    return executeCallable(new MasterCallable<Boolean>(getConnection()) {

      @Override
      protected Boolean rpcCall(MasterService.BlockingInterface master,
          HBaseRpcController controller) throws ServiceException {
        return master.setBalancerRunning(controller, req).getPrevBalanceValue();
      }
    });
  }

  /**
   * Turn the compaction switch in global instance on or off.
   * @param b If false, disable minor&major compaction in all RS.
   * @return Previous value
   * @throws IOException
   */
  public boolean setCompactionEnable(final boolean b) throws IOException {
    boolean ret = false;
    Collection<ServerName> servers =
        getClusterStatus(EnumSet.of(ClusterStatus.Option.SERVERS_NAME)).getServers();
    for (ServerName sn : servers) {
      AdminService.BlockingInterface admin = this.connection.getAdmin(sn);
      CompactionEnableRequest.Builder builder = CompactionEnableRequest.newBuilder();
      builder.setEnable(b);
      try {
        CompactionEnableResponse response = admin.switchCompaction(null, builder.build());
        ret |= response.getEnable();
      } catch (ServiceException se) {
        throw ProtobufUtil.getRemoteException(se);
      }
    }
    return ret;
  }

  /**
   * Invoke the balancer. Will run the balancer and if regions to move, it will go ahead and do the
   * reassignments. Can NOT run for various reasons. Check logs.
   * @return True if balancer ran, false otherwise.
   */
  public boolean balancer() throws IOException {
    return executeCallable(new MasterCallable<Boolean>(getConnection()) {

      @Override
      protected Boolean rpcCall(MasterService.BlockingInterface master,
          HBaseRpcController controller) throws ServiceException {
        return master.balance(controller, RequestConverter.buildBalanceRequest()).getBalancerRan();
      }
    });
  }

  /**
   * Enable/Disable the catalog janitor
   * @param enable if true enables the catalog janitor
   * @return the previous state
   * @throws IOException
   */
  public boolean enableCatalogJanitor(boolean enable) throws IOException {
    final EnableCatalogJanitorRequest req =
        RequestConverter.buildEnableCatalogJanitorRequest(enable);
    return executeCallable(new MasterCallable<Boolean>(getConnection()) {

      @Override
      protected Boolean rpcCall(MasterService.BlockingInterface master,
          HBaseRpcController controller) throws ServiceException {
        return master.enableCatalogJanitor(controller, req).getPrevValue();
      }
    });
  }

  /**
   * Ask for a scan of the catalog table
   * @return the number of entries cleaned
   * @throws IOException
   */
  public int runCatalogScan() throws IOException {
    return executeCallable(new MasterCallable<Integer>(getConnection()) {

      @Override
      protected Integer rpcCall(MasterService.BlockingInterface master,
          HBaseRpcController controller) throws ServiceException {
        return master.runCatalogScan(controller, RequestConverter.buildCatalogScanRequest())
            .getScanResult();
      }
    });
  }

  /**
   * Query on the catalog janitor state (Enabled/Disabled?)
   * @throws IOException
   */
  public boolean isCatalogJanitorEnabled() throws IOException {
    return executeCallable(new MasterCallable<Boolean>(getConnection()) {

      @Override
      protected Boolean rpcCall(MasterService.BlockingInterface master,
          HBaseRpcController controller) throws ServiceException {
        return master.isCatalogJanitorEnabled(controller,
          RequestConverter.buildIsCatalogJanitorEnabledRequest()).getValue();
      }
    });
  }

  /**
   * Merge two regions. Asynchronous operation.
   * @param encodedNameOfRegionA encoded name of region a
   * @param encodedNameOfRegionB encoded name of region b
   * @param forcible true if do a compulsory merge, otherwise we will only merge two adjacent
   *          regions
   * @throws IOException
   */
  public void mergeRegions(byte[] encodedNameOfRegionA, byte[] encodedNameOfRegionB,
      boolean forcible) throws IOException {
    final DispatchMergingRegionsRequest request = RequestConverter
        .buildDispatchMergingRegionsRequest(encodedNameOfRegionA, encodedNameOfRegionB, forcible);
    executeCallable(new MasterCallable<Void>(getConnection(), encodedNameOfRegionA) {

      @Override
      protected Void rpcCall(MasterService.BlockingInterface master, HBaseRpcController controller)
          throws ServiceException {
        master.dispatchMergingRegions(controller, request);
        return null;
      }
    });
  }

  /**
   * Split a table or an individual region. Asynchronous operation.
   * @param tableNameOrRegionName table or region to split
   * @throws IOException if a remote or network exception occurs
   * @throws InterruptedException
   */
  public void split(final String tableNameOrRegionName) throws IOException, InterruptedException {
    split(Bytes.toBytes(tableNameOrRegionName));
  }

  /**
   * Split a table or an individual region. Implicitly finds an optimal split point. Asynchronous
   * operation.
   * @param tableNameOrRegionName table to region to split
   * @throws IOException if a remote or network exception occurs
   * @throws InterruptedException
   */
  public void split(final byte[] tableNameOrRegionName) throws IOException, InterruptedException {
    split(tableNameOrRegionName, null);
  }

  public void split(final String tableNameOrRegionName, final String splitPoint)
      throws IOException, InterruptedException {
    split(Bytes.toBytes(tableNameOrRegionName), Bytes.toBytes(splitPoint));
  }

  /**
   * Split a table or an individual region. Asynchronous operation.
   * @param tableNameOrRegionName table to region to split
   * @param splitPoint the explicit position to split on
   * @throws IOException if a remote or network exception occurs
   * @throws InterruptedException interrupt exception occurred
   */
  public void split(byte[] tableNameOrRegionName, byte[] splitPoint)
      throws IOException, InterruptedException {
    Pair<HRegionInfo, ServerName> regionServerPair = getRegion(tableNameOrRegionName);
    if (regionServerPair != null) {
      if (regionServerPair.getSecond() == null) {
        throw new NoServerForRegionException(Bytes.toStringBinary(tableNameOrRegionName));
      } else {
        split(regionServerPair.getSecond(), regionServerPair.getFirst(), splitPoint);
      }
    } else {
      TableName tableName = checkTableExists(TableName.valueOf(tableNameOrRegionName));
      for (HRegionLocation loc : connection.locateRegions(tableName, false, false)) {
        if (loc.getServerName() == null) {
          continue;
        }
        HRegionInfo r = loc.getRegionInfo();
        if (r.isOffline() || r.isSplitParent()) {
          continue;
        }
        if (splitPoint != null && !r.containsRow(splitPoint)) {
          continue;
        }
        split(loc.getServerName(),loc.getRegionInfo(), splitPoint);
      }
    }
  }

  private void split(final ServerName sn, final HRegionInfo hri, byte[] splitPoint)
      throws IOException {
    if (hri.getStartKey() != null && splitPoint != null &&
        Bytes.compareTo(hri.getStartKey(), splitPoint) == 0) {
      throw new IOException("should not give a splitkey which equals to startkey!");
    }
    AdminService.BlockingInterface admin = this.connection.getAdmin(sn);
    ProtobufUtil.split(admin, hri, splitPoint);
  }

  /**
   * Modify an existing table, more IRB friendly version. Asynchronous operation. This means that it
   * may be a while before your schema change is updated across all of the table.
   * @param tableName name of table.
   * @param htd modified description of the table
   * @throws IOException if a remote or network exception occurs
   */
  public void modifyTable(TableName tableName, HTableDescriptor htd) throws IOException {
    if (!tableName.equals(htd.getTableName())) {
      throw new IllegalArgumentException("the specified table name '" + tableName +
          "' doesn't match with the HTD one: " + htd.getTableName());
    }

    // check KeySalter not modified
    checkSaltedAttributeUnModified(tableName, htd);
    final ModifyTableRequest req = RequestConverter.buildModifyTableRequest(tableName, htd);
    executeCallable(new MasterCallable<Void>(getConnection(), tableName) {
      @Override
      public Void rpcCall(MasterService.BlockingInterface master, HBaseRpcController controller)
          throws ServiceException {
        master.modifyTable(null, req);
        return null;
      }
    });
  }

  private void checkSaltedAttributeUnModified(TableName tableName, HTableDescriptor modifiedHtd)
      throws IOException {
    HTableDescriptor htd = this.getTableDescriptor(tableName);
    boolean saltedAttributeUnModified = false;
    if (htd.isSalted() != modifiedHtd.isSalted()) {
      saltedAttributeUnModified = true;
    }
    if (htd.isSalted()) {
      if (!htd.getSlotsCount().equals(modifiedHtd.getSlotsCount()) ||
          !htd.getKeySalter().equals(modifiedHtd.getKeySalter())) {
        saltedAttributeUnModified = true;
      }
    }

    if (saltedAttributeUnModified) {
      throw new IOException("can not modify the salted attribute of table : " + tableName);
    }
  }

  public void modifyTable(final byte[] tableName, final HTableDescriptor htd) throws IOException {
    modifyTable(TableName.valueOf(tableName), htd);
  }

  public void modifyTable(final String tableName, final HTableDescriptor htd) throws IOException {
    modifyTable(TableName.valueOf(tableName), htd);
  }

  /**
   * @param tableNameOrRegionName Name of a table or name of a region.
   * @return a pair of HRegionInfo and ServerName if <code>tableNameOrRegionName</code> is a
   *         verified region name (we call {@link MetaReader#getRegion(byte[])} else null. Throw an
   *         exception if <code>tableNameOrRegionName</code> is null.
   * @throws IOException
   */
  Pair<HRegionInfo, ServerName> getRegion(byte[] tableNameOrRegionName) throws IOException {
    if (tableNameOrRegionName == null) {
      throw new IllegalArgumentException("Pass a table name or region name");
    }
    Pair<HRegionInfo, ServerName> pair = MetaReader.getRegion(connection, tableNameOrRegionName);
    if (pair == null) {
      final AtomicReference<Pair<HRegionInfo, ServerName>> result =
        new AtomicReference<Pair<HRegionInfo, ServerName>>(null);
      final String encodedName = Bytes.toString(tableNameOrRegionName);
      MetaScannerVisitor visitor = new MetaScannerVisitorBase() {
        @Override
        public boolean processRow(Result data) throws IOException {
          HRegionInfo info = HRegionInfo.getHRegionInfo(data);
          if (info == null) {
            LOG.warn("No serialized HRegionInfo in " + data);
            return true;
          }
          if (!encodedName.equals(info.getEncodedName())) return true;
          ServerName sn = HRegionInfo.getServerName(data);
          result.set(new Pair<HRegionInfo, ServerName>(info, sn));
          return false; // found the region, stop
        }
      };

      MetaScanner.metaScan(conf, connection, visitor, null);
      pair = result.get();
    }
    return pair;
  }

  /**
   * If the input is a region name, it is returned as is. If it's an encoded region name, the
   * corresponding region is found from meta and its region name is returned. If we can't find any
   * region in meta matching the input as either region name or encoded region name, the input is
   * returned as is. We don't throw unknown region exception.
   */
  private byte[] getRegionName(final byte[] regionNameOrEncodedRegionName) throws IOException {
    if (Bytes.equals(regionNameOrEncodedRegionName,
      HRegionInfo.FIRST_META_REGIONINFO.getRegionName()) ||
      Bytes.equals(regionNameOrEncodedRegionName,
        HRegionInfo.FIRST_META_REGIONINFO.getEncodedNameAsBytes())) {
      return HRegionInfo.FIRST_META_REGIONINFO.getRegionName();
    }
    byte[] tmp = regionNameOrEncodedRegionName;
    Pair<HRegionInfo, ServerName> regionServerPair = getRegion(regionNameOrEncodedRegionName);
    if (regionServerPair != null && regionServerPair.getFirst() != null) {
      tmp = regionServerPair.getFirst().getRegionName();
    }
    return tmp;
  }

  /**
   * Check if table exists or not
   * @param tableName Name of a table.
   * @return tableName instance
   * @throws IOException if a remote or network exception occurs.
   * @throws TableNotFoundException if table does not exist.
   */
  // TODO rename this method
  private TableName checkTableExists(final TableName tableName) throws IOException {
    if (!MetaReader.tableExists(connection, tableName)) {
      throw new TableNotFoundException(tableName);
    }
    return tableName;
  }

  /**
   * Shuts down the HBase cluster
   * @throws IOException if a remote or network exception occurs
   */
  public synchronized void shutdown() throws IOException {
    executeCallable(new MasterCallable<Void>(getConnection()) {
      @Override
      public Void rpcCall(MasterService.BlockingInterface master, HBaseRpcController controller)
          throws ServiceException {
        master.shutdown(controller, ShutdownRequest.getDefaultInstance());
        return null;
      }
    });
  }

  /**
   * Shuts down the current HBase master only. Does not shutdown the cluster.
   * @see #shutdown()
   * @throws IOException if a remote or network exception occurs
   */
  public synchronized void stopMaster() throws IOException {
    executeCallable(new MasterCallable<Void>(getConnection()) {
      @Override
      public Void rpcCall(MasterService.BlockingInterface master, HBaseRpcController controller)
          throws ServiceException {
        master.stopMaster(null, StopMasterRequest.getDefaultInstance());
        return null;
      }
    });
  }

  /**
   * Stop the designated regionserver
   * @param hostnamePort Hostname and port delimited by a <code>:</code> as in
   *          <code>example.org:1234</code>
   * @throws IOException if a remote or network exception occurs
   */
  public synchronized void stopRegionServer(final String hostnamePort) throws IOException {
    String hostname = Addressing.parseHostname(hostnamePort);
    int port = Addressing.parsePort(hostnamePort);
    AdminService.BlockingInterface admin =
        this.connection.getAdmin(ServerName.valueOf(hostname, port, 0));
    StopServerRequest request = RequestConverter
        .buildStopServerRequest("Called by admin client " + this.connection.toString());
    try {
      admin.stopServer(null, request);
    } catch (ServiceException se) {
      throw ProtobufUtil.getRemoteException(se);
    }
  }

  /**
   * @return cluster status
   * @throws IOException if a remote or network exception occurs
   */
  public ClusterStatus getClusterStatus() throws IOException {
    return getClusterStatus(EnumSet.allOf(ClusterStatus.Option.class));
  }

  public ClusterStatus getClusterStatus(EnumSet<ClusterStatus.Option> options) throws IOException {
    return executeCallable(new MasterCallable<ClusterStatus>(getConnection()) {

      @Override
      protected ClusterStatus rpcCall(MasterService.BlockingInterface master,
          HBaseRpcController controller) throws ServiceException {
        return ClusterStatus.convert(
            master.getClusterStatus(controller, RequestConverter.buildGetClusterStatusRequest(options))
                .getClusterStatus());
      }
    });
  }

  private HRegionLocation getFirstMetaServerForTable(final TableName tableName) throws IOException {
    return connection.locateRegion(TableName.META_TABLE_NAME,
      HRegionInfo.createRegionName(tableName, null, HConstants.NINES, false));
  }

  /**
   * @return Configuration used by the instance.
   */
  public Configuration getConfiguration() {
    return this.conf;
  }

  /**
   * Create a new namespace
   * @param descriptor descriptor which describes the new namespace
   * @throws IOException
   */
  public void createNamespace(NamespaceDescriptor descriptor) throws IOException {
    final CreateNamespaceRequest req = RequestConverter.buildCreateNamespaceRequest(descriptor);
    executeCallable(new MasterCallable<Void>(getConnection()) {

      @Override
      protected Void rpcCall(MasterService.BlockingInterface master, HBaseRpcController controller)
          throws Exception {
        master.createNamespace(controller, req);
        return null;
      }
    });
  }

  /**
   * Modify an existing namespace
   * @param descriptor descriptor which describes the new namespace
   * @throws IOException
   */
  public void modifyNamespace(NamespaceDescriptor descriptor) throws IOException {
    final ModifyNamespaceRequest req = RequestConverter.buildModifyNamespaceRequest(descriptor);
    executeCallable(new MasterCallable<Void>(getConnection()) {

      @Override
      protected Void rpcCall(MasterService.BlockingInterface master, HBaseRpcController controller)
          throws Exception {
        master.modifyNamespace(controller, req);
        return null;
      }
    });
  }

  /**
   * Delete an existing namespace. Only empty namespaces (no tables) can be removed.
   * @param name namespace name
   * @throws IOException
   */
  public void deleteNamespace(String name) throws IOException {
    final DeleteNamespaceRequest req = RequestConverter.buildDeleteNamespaceRequest(name);
    executeCallable(new MasterCallable<Void>(getConnection()) {

      @Override
      protected Void rpcCall(MasterService.BlockingInterface master, HBaseRpcController controller)
          throws Exception {
        master.deleteNamespace(controller, req);
        return null;
      }
    });
  }

  /**
   * Get a namespace descriptor by name
   * @param name name of namespace descriptor
   * @return A descriptor
   * @throws IOException
   */
  public NamespaceDescriptor getNamespaceDescriptor(String name) throws IOException {
    final GetNamespaceDescriptorRequest req =
        RequestConverter.buildGetNamespaceDescriptorRequest(name);
    return executeCallable(new MasterCallable<NamespaceDescriptor>(getConnection()) {

      @Override
      protected NamespaceDescriptor rpcCall(MasterService.BlockingInterface master,
          HBaseRpcController controller) throws Exception {
        return ProtobufUtil.toNamespaceDescriptor(
          master.getNamespaceDescriptor(controller, req).getNamespaceDescriptor());
      }
    });
  }

  /**
   * List available namespace descriptors
   * @return List of descriptors
   * @throws IOException
   */
  public NamespaceDescriptor[] listNamespaceDescriptors() throws IOException {
    return executeCallable(new MasterCallable<NamespaceDescriptor[]>(getConnection()) {

      @Override
      protected NamespaceDescriptor[] rpcCall(MasterService.BlockingInterface master,
          HBaseRpcController controller) throws Exception {
        List<HBaseProtos.NamespaceDescriptor> list =
            master
                .listNamespaceDescriptors(controller,
                  ListNamespaceDescriptorsRequest.getDefaultInstance())
                .getNamespaceDescriptorList();
        NamespaceDescriptor[] res = new NamespaceDescriptor[list.size()];
        for (int i = 0; i < list.size(); i++) {
          res[i] = ProtobufUtil.toNamespaceDescriptor(list.get(i));
        }
        return res;
      }
    });
  }

  /**
   * Get list of table descriptors by namespace
   * @param name namespace name
   * @return A descriptor
   * @throws IOException
   */
  public HTableDescriptor[] listTableDescriptorsByNamespace(String name) throws IOException {
    final ListTableDescriptorsByNamespaceRequest req =
        RequestConverter.buildListTableDescriptorsByNamespaceRequest(name);
    return executeCallable(new MasterCallable<HTableDescriptor[]>(getConnection()) {
      @Override
      protected HTableDescriptor[] rpcCall(MasterService.BlockingInterface master,
          HBaseRpcController controller) throws Exception {
        List<TableSchema> list =
            master.listTableDescriptorsByNamespace(controller, req).getTableSchemaList();
        HTableDescriptor[] res = new HTableDescriptor[list.size()];
        for (int i = 0; i < list.size(); i++) {

          res[i] = HTableDescriptor.convert(list.get(i));
        }
        return res;
      }
    });
  }

  /**
   * Get list of table names by namespace
   * @param name namespace name
   * @return The list of table names in the namespace
   * @throws IOException
   */
  public TableName[] listTableNamesByNamespace(String name) throws IOException {
    final ListTableNamesByNamespaceRequest req =
        RequestConverter.buildListTableNamesByNamespaceRequest(name);
    return executeCallable(new MasterCallable<TableName[]>(getConnection()) {

      @Override
      protected TableName[] rpcCall(MasterService.BlockingInterface master,
          HBaseRpcController controller) throws Exception {
        List<HBaseProtos.TableName> tableNames =
            master.listTableNamesByNamespace(controller, req).getTableNameList();
        TableName[] result = new TableName[tableNames.size()];
        for (int i = 0; i < tableNames.size(); i++) {
          result[i] = ProtobufUtil.toTableName(tableNames.get(i));
        }
        return result;
      }
    });
  }

  /**
   * Check to see if HBase is running. Throw an exception if not. We consider that HBase is running
   * if ZooKeeper and Master are running.
   * @param conf system configuration
   * @throws MasterNotRunningException if the master is not running
   * @throws ZooKeeperConnectionException if unable to connect to zookeeper
   */
  public static void checkHBaseAvailable(Configuration conf) throws MasterNotRunningException,
      ZooKeeperConnectionException, ServiceException, IOException {
    Configuration copyOfConf = HBaseConfiguration.create(conf);

    // We set it to make it fail as soon as possible if HBase is not available
    copyOfConf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 1);
    copyOfConf.setInt(HConstants.ZK_RECOVERY_RETRY, 0);

    HConnectionImplementation connection =
      (HConnectionImplementation) HConnectionManager.getConnection(copyOfConf);

    try {
      // Check Master
      connection.isMasterRunning();
    } finally {
      connection.close();
    }
  }

  /**
   * get the regions of a given table.
   * @param tableName the name of the table
   * @return Ordered list of {@link HRegionInfo}.
   * @throws IOException
   */
  public List<HRegionInfo> getTableRegions(final TableName tableName) throws IOException {
    return connection.locateRegions(tableName).stream().map(HRegionLocation::getRegionInfo)
        .collect(Collectors.toList());
  }

  public List<HRegionInfo> getTableRegions(final byte[] tableName) throws IOException {
    return getTableRegions(TableName.valueOf(tableName));
  }

  @Override
  public synchronized void close() throws IOException {
    if (cleanupConnectionOnClose && this.connection != null && !this.closed) {
      this.connection.close();
      this.closed = true;
    }
  }

  /**
   * Get tableDescriptors
   * @param tableNames List of table names
   * @return HTD[] the tableDescriptor
   * @throws IOException if a remote or network exception occurs
   */
  public HTableDescriptor[] getTableDescriptorsByTableName(List<TableName> tableNames)
      throws IOException {
    return this.connection.getHTableDescriptorsByTableName(tableNames);
  }

  /**
   * Get tableDescriptors
   * @param names List of table names
   * @return HTD[] the tableDescriptor
   * @throws IOException if a remote or network exception occurs
   */
  public HTableDescriptor[] getTableDescriptors(List<String> names) throws IOException {
    List<TableName> tableNames = new ArrayList<TableName>(names.size());
    for (String name : names) {
      tableNames.add(TableName.valueOf(name));
    }
    return getTableDescriptorsByTableName(tableNames);
  }

  /**
   * Roll the log writer. That is, start writing log messages to a new file.
   * @param serverName The servername of the regionserver. A server name is made of host, port and
   *          startcode. This is mandatory. Here is an example:
   *          <code> host187.example.com,60020,1289493121758</code>
   * @return If lots of logs, flush the returned regions so next time through we can clean logs.
   *         Returns null if nothing to flush. Names are actual region names as returned by
   *         {@link HRegionInfo#getEncodedName()}
   * @throws IOException if a remote or network exception occurs
   * @throws FailedLogCloseException
   */
  public synchronized byte[][] rollHLogWriter(String serverName)
      throws IOException, FailedLogCloseException {
    ServerName sn = ServerName.valueOf(serverName);
    AdminService.BlockingInterface admin = this.connection.getAdmin(sn);
    RollWALWriterRequest request = RequestConverter.buildRollWALWriterRequest();
    try {
      RollWALWriterResponse response = admin.rollWALWriter(null, request);
      int regionCount = response.getRegionToFlushCount();
      byte[][] regionsToFlush = new byte[regionCount][];
      for (int i = 0; i < regionCount; i++) {
        ByteString region = response.getRegionToFlush(i);
        regionsToFlush[i] = region.toByteArray();
      }
      return regionsToFlush;
    } catch (ServiceException se) {
      throw ProtobufUtil.getRemoteException(se);
    }
  }

  public String[] getMasterCoprocessors() {
    try {
      return getClusterStatus(EnumSet.of(ClusterStatus.Option.MASTER_COPROCESSORS))
          .getMasterCoprocessors();
    } catch (IOException e) {
      LOG.error("Could not getClusterStatus()", e);
      return null;
    }
  }

  /**
   * Get the current compaction state of a table or region. It could be in a major compaction, a
   * minor compaction, both, or none.
   * @param tableNameOrRegionName table or region to major compact
   * @throws IOException if a remote or network exception occurs
   * @throws InterruptedException
   * @return the current compaction state
   */
  public CompactionState getCompactionState(final String tableNameOrRegionName)
      throws IOException, InterruptedException {
    return getCompactionState(Bytes.toBytes(tableNameOrRegionName));
  }

  /**
   * Get the current compaction state of a table or region. It could be in a major compaction, a
   * minor compaction, both, or none.
   * @param tableNameOrRegionName table or region to major compact
   * @throws IOException if a remote or network exception occurs
   * @throws InterruptedException
   * @return the current compaction state
   */
  public CompactionState getCompactionState(final byte[] tableNameOrRegionName)
      throws IOException, InterruptedException {
    CompactionState state = CompactionState.NONE;
    try {
      Pair<HRegionInfo, ServerName> regionServerPair = getRegion(tableNameOrRegionName);
      if (regionServerPair != null) {
        if (regionServerPair.getSecond() == null) {
          throw new NoServerForRegionException(Bytes.toStringBinary(tableNameOrRegionName));
        } else {
          ServerName sn = regionServerPair.getSecond();
          AdminService.BlockingInterface admin = this.connection.getAdmin(sn);
          GetRegionInfoRequest request = RequestConverter
              .buildGetRegionInfoRequest(regionServerPair.getFirst().getRegionName(), true);
          GetRegionInfoResponse response = admin.getRegionInfo(null, request);
          return response.getCompactionState();
        }
      } else {
        TableName tableName = checkTableExists(TableName.valueOf(tableNameOrRegionName));
        for (HRegionLocation loc : connection.locateRegions(tableName, false, false)) {
          if (loc.getServerName() == null || loc.getRegionInfo().isOffline()) {
            continue;
          }
          try {
            ServerName sn = loc.getServerName();
            AdminService.BlockingInterface admin = this.connection.getAdmin(sn);
            GetRegionInfoRequest request =
              RequestConverter.buildGetRegionInfoRequest(loc.getRegionInfo().getRegionName(), true);
            GetRegionInfoResponse response = admin.getRegionInfo(null, request);
            switch (response.getCompactionState()) {
              case MAJOR_AND_MINOR:
                return CompactionState.MAJOR_AND_MINOR;
              case MAJOR:
                if (state == CompactionState.MINOR) {
                  return CompactionState.MAJOR_AND_MINOR;
                }
                state = CompactionState.MAJOR;
                break;
              case MINOR:
                if (state == CompactionState.MAJOR) {
                  return CompactionState.MAJOR_AND_MINOR;
                }
                state = CompactionState.MINOR;
                break;
              case NONE:
              default: // nothing, continue
            }
          } catch (NotServingRegionException e) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Trying to get compaction state of " + loc.getRegionInfo() + ": " +
                StringUtils.stringifyException(e));
            }
          } catch (RemoteException e) {
            if (e.getMessage().indexOf(NotServingRegionException.class.getName()) >= 0) {
              if (LOG.isDebugEnabled()) {
                LOG.debug("Trying to get compaction state of " + loc.getRegionInfo() + ": " +
                  StringUtils.stringifyException(e));
              }
            } else {
              throw e;
            }
          }
        }
      }
    } catch (ServiceException se) {
      throw ProtobufUtil.getRemoteException(se);
    }
    return state;
  }

  /**
   * Take a snapshot for the given table. If the table is enabled, a FLUSH-type snapshot will be
   * taken. If the table is disabled, an offline snapshot is taken.
   * <p>
   * Snapshots are considered unique based on <b>the name of the snapshot</b>. Attempts to take a
   * snapshot with the same name (even a different type or with different parameters) will fail with
   * a {@link SnapshotCreationException} indicating the duplicate naming.
   * <p>
   * Snapshot names follow the same naming constraints as tables in HBase. See
   * {@link org.apache.hadoop.hbase.TableName#isLegalFullyQualifiedTableName(byte[])}.
   * @param snapshotName name of the snapshot to be created
   * @param tableName name of the table for which snapshot is created
   * @throws IOException if a remote or network exception occurs
   * @throws SnapshotCreationException if snapshot creation failed
   * @throws IllegalArgumentException if the snapshot request is formatted incorrectly
   */
  public void snapshot(final String snapshotName, final TableName tableName)
      throws IOException, SnapshotCreationException, IllegalArgumentException {
    snapshot(snapshotName, tableName, SnapshotDescription.Type.FLUSH);
  }

  public void snapshot(final String snapshotName, final String tableName)
      throws IOException, SnapshotCreationException, IllegalArgumentException {
    snapshot(snapshotName, TableName.valueOf(tableName), SnapshotDescription.Type.FLUSH);
  }

  /**
   * Create snapshot for the given table of given flush type.
   * <p>
   * Snapshots are considered unique based on <b>the name of the snapshot</b>. Attempts to take a
   * snapshot with the same name (even a different type or with different parameters) will fail with
   * a {@link SnapshotCreationException} indicating the duplicate naming.
   * <p>
   * Snapshot names follow the same naming constraints as tables in HBase.
   * @param snapshotName name of the snapshot to be created
   * @param tableName name of the table for which snapshot is created
   * @param flushType if the snapshot should be taken without flush memstore first
   * @throws IOException if a remote or network exception occurs
   * @throws SnapshotCreationException if snapshot creation failed
   * @throws IllegalArgumentException if the snapshot request is formatted incorrectly
   */
  public void snapshot(final byte[] snapshotName, final byte[] tableName,
      final SnapshotDescription.Type flushType)
      throws IOException, SnapshotCreationException, IllegalArgumentException {
    snapshot(Bytes.toString(snapshotName), Bytes.toString(tableName), flushType);
  }

  /**
   * public void snapshot(final String snapshotName, Create a timestamp consistent snapshot for the
   * given table. final byte[] tableName) throws IOException,
   * <p>
   * Snapshots are considered unique based on <b>the name of the snapshot</b>. Attempts to take a
   * snapshot with the same name (even a different type or with different parameters) will fail with
   * a {@link SnapshotCreationException} indicating the duplicate naming.
   * <p>
   * Snapshot names follow the same naming constraints as tables in HBase.
   * @param snapshotName name of the snapshot to be created
   * @param tableName name of the table for which snapshot is created
   * @throws IOException if a remote or network exception occurs
   * @throws SnapshotCreationException if snapshot creation failed
   * @throws IllegalArgumentException if the snapshot request is formatted incorrectly
   */
  public void snapshot(final byte[] snapshotName, final TableName tableName)
      throws IOException, SnapshotCreationException, IllegalArgumentException {
    snapshot(Bytes.toString(snapshotName), tableName, SnapshotDescription.Type.FLUSH);
  }

  public void snapshot(final byte[] snapshotName, final byte[] tableName)
      throws IOException, SnapshotCreationException, IllegalArgumentException {
    snapshot(Bytes.toString(snapshotName), TableName.valueOf(tableName),
      SnapshotDescription.Type.FLUSH);
  }

  /**
   * Create typed snapshot of the table.
   * <p>
   * Snapshots are considered unique based on <b>the name of the snapshot</b>. Attempts to take a
   * snapshot with the same name (even a different type or with different parameters) will fail with
   * a {@link SnapshotCreationException} indicating the duplicate naming.
   * <p>
   * Snapshot names follow the same naming constraints as tables in HBase. See
   * {@link org.apache.hadoop.hbase.TableName#isLegalFullyQualifiedTableName(byte[])}.
   * <p>
   * @param snapshotName name to give the snapshot on the filesystem. Must be unique from all other
   *          snapshots stored on the cluster
   * @param tableName name of the table to snapshot
   * @param type type of snapshot to take
   * @throws IOException we fail to reach the master
   * @throws SnapshotCreationException if snapshot creation failed
   * @throws IllegalArgumentException if the snapshot request is formatted incorrectly
   */
  public void snapshot(String snapshotName, TableName tableName, SnapshotDescription.Type type)
      throws IOException, SnapshotCreationException, IllegalArgumentException {
    snapshot(SnapshotDescription.newBuilder().setTable(tableName.getNameAsString())
        .setName(snapshotName).setType(type).build());
  }

  public void snapshot(final String snapshotName, final String tableName,
      SnapshotDescription.Type type)
      throws IOException, SnapshotCreationException, IllegalArgumentException {
    snapshot(snapshotName, TableName.valueOf(tableName), type);
  }

  public void snapshot(final String snapshotName, final byte[] tableName,
      SnapshotDescription.Type type)
      throws IOException, SnapshotCreationException, IllegalArgumentException {
    snapshot(snapshotName, TableName.valueOf(tableName), type);
  }

  /**
   * Take a snapshot and wait for the server to complete that snapshot (blocking).
   * <p>
   * Only a single snapshot should be taken at a time for an instance of HBase, or results may be
   * undefined (you can tell multiple HBase clusters to snapshot at the same time, but only one at a
   * time for a single cluster).
   * <p>
   * Snapshots are considered unique based on <b>the name of the snapshot</b>. Attempts to take a
   * snapshot with the same name (even a different type or with different parameters) will fail with
   * a {@link SnapshotCreationException} indicating the duplicate naming.
   * <p>
   * Snapshot names follow the same naming constraints as tables in HBase. See
   * {@link org.apache.hadoop.hbase.TableName#isLegalFullyQualifiedTableName(byte[])}.
   * <p>
   * You should probably use {@link #snapshot(String, String)} or {@link #snapshot(byte[], byte[])}
   * unless you are sure about the type of snapshot that you want to take.
   * @param snapshot snapshot to take
   * @throws IOException or we lose contact with the master.
   * @throws SnapshotCreationException if snapshot failed to be taken
   * @throws IllegalArgumentException if the snapshot request is formatted incorrectly
   */
  public void snapshot(SnapshotDescription snapshot)
      throws IOException, SnapshotCreationException, IllegalArgumentException {
    // actually take the snapshot
    SnapshotResponse response = takeSnapshotAsync(snapshot);
    final IsSnapshotDoneRequest req = RequestConverter.buildIsSnapshotDoneRequest(snapshot);
    IsSnapshotDoneRequest.newBuilder().setSnapshot(snapshot).build();
    boolean done = false;
    long start = EnvironmentEdgeManager.currentTimeMillis();
    long max = response.getExpectedTimeout();
    long maxPauseTime = max / this.numRetries;
    int tries = 0;
    LOG.debug("Waiting a max of " + max + " ms for snapshot '" +
        ClientSnapshotDescriptionUtils.toString(snapshot) + "'' to complete. (max " + maxPauseTime +
        " ms per retry)");
    while (tries == 0 || ((EnvironmentEdgeManager.currentTimeMillis() - start) < max && !done)) {
      try {
        // sleep a backoff <= pauseTime amount
        long sleep = getPauseTime(tries++);
        sleep = sleep > maxPauseTime ? maxPauseTime : sleep;
        LOG.debug(
          "(#" + tries + ") Sleeping: " + sleep + "ms while waiting for snapshot completion.");
        Thread.sleep(sleep);

      } catch (InterruptedException e) {
        LOG.debug("Interrupted while waiting for snapshot " + snapshot + " to complete");
        Thread.currentThread().interrupt();
      }
      LOG.debug("Getting current status of snapshot from master...");
      done = executeCallable(
        new MasterCallable<Boolean>(getConnection(), TableName.valueOf(snapshot.getTable())) {

          @Override
          protected Boolean rpcCall(MasterService.BlockingInterface master,
              HBaseRpcController controller) throws ServiceException {
            return master.isSnapshotDone(controller, req).getDone();
          }
        });
    }
    ;
    if (!done) {
      throw new SnapshotCreationException(
          "Snapshot '" + snapshot.getName() + "' wasn't completed in expectedTime:" + max + " ms",
          ProtobufUtil.createSnapshotDesc(snapshot));
    }
  }

  /**
   * Take a snapshot without waiting for the server to complete that snapshot (asynchronous)
   * <p>
   * Only a single snapshot should be taken at a time, or results may be undefined.
   * @param snapshot snapshot to take
   * @return response from the server indicating the max time to wait for the snapshot
   * @throws IOException if the snapshot did not succeed or we lose contact with the master.
   * @throws SnapshotCreationException if snapshot creation failed
   * @throws IllegalArgumentException if the snapshot request is formatted incorrectly
   */
  public SnapshotResponse takeSnapshotAsync(SnapshotDescription snapshot)
      throws IOException, SnapshotCreationException {
    ClientSnapshotDescriptionUtils.assertSnapshotRequestIsValid(snapshot);
    final SnapshotRequest req = RequestConverter.buildSnapshotRequest(snapshot);
    // run the snapshot on the master
    return executeCallable(new MasterCallable<SnapshotResponse>(getConnection(),
        TableName.valueOf(snapshot.getTable())) {

      @Override
      protected SnapshotResponse rpcCall(MasterService.BlockingInterface master,
          HBaseRpcController controller) throws ServiceException {
        return master.snapshot(null, req);
      }
    });
  }

  /**
   * Check the current state of the passed snapshot.
   * <p>
   * There are three possible states:
   * <ol>
   * <li>running - returns <tt>false</tt></li>
   * <li>finished - returns <tt>true</tt></li>
   * <li>finished with error - throws the exception that caused the snapshot to fail</li>
   * </ol>
   * <p>
   * The cluster only knows about the most recent snapshot. Therefore, if another snapshot has been
   * run/started since the snapshot your are checking, you will recieve an
   * {@link UnknownSnapshotException}.
   * @param snapshot description of the snapshot to check
   * @return <tt>true</tt> if the snapshot is completed, <tt>false</tt> if the snapshot is still
   *         running
   * @throws IOException if we have a network issue
   * @throws HBaseSnapshotException if the snapshot failed
   * @throws UnknownSnapshotException if the requested snapshot is unknown
   */
  public boolean isSnapshotFinished(SnapshotDescription snapshot)
      throws IOException, HBaseSnapshotException, UnknownSnapshotException {
    final IsSnapshotDoneRequest req = RequestConverter.buildIsSnapshotDoneRequest(snapshot);
    return executeCallable(new MasterCallable<Boolean>(getConnection()) {

      @Override
      protected Boolean rpcCall(MasterService.BlockingInterface master,
          HBaseRpcController controller) throws ServiceException {
        return master.isSnapshotDone(controller, req).getDone();
      }
    });
  }

  /**
   * Restore the specified snapshot on the original table. (The table must be disabled) If the
   * "hbase.snapshot.restore.take.failsafe.snapshot" configuration property is set to true, a
   * snapshot of the current table is taken before executing the restore operation. In case of
   * restore failure, the failsafe snapshot will be restored. If the restore completes without
   * problem the failsafe snapshot is deleted.
   * @param snapshotName name of the snapshot to restore
   * @throws IOException if a remote or network exception occurs
   * @throws RestoreSnapshotException if snapshot failed to be restored
   * @throws IllegalArgumentException if the restore request is formatted incorrectly
   */
  public void restoreSnapshot(final byte[] snapshotName)
      throws IOException, RestoreSnapshotException {
    restoreSnapshot(Bytes.toString(snapshotName));
  }

  /**
   * Restore the specified snapshot on the original table. (The table must be disabled) If the
   * "hbase.snapshot.restore.take.failsafe.snapshot" configuration property is set to true, a
   * snapshot of the current table is taken before executing the restore operation. In case of
   * restore failure, the failsafe snapshot will be restored. If the restore completes without
   * problem the failsafe snapshot is deleted.
   * @param snapshotName name of the snapshot to restore
   * @throws IOException if a remote or network exception occurs
   * @throws RestoreSnapshotException if snapshot failed to be restored
   * @throws IllegalArgumentException if the restore request is formatted incorrectly
   */
  public void restoreSnapshot(final String snapshotName)
      throws IOException, RestoreSnapshotException {
    boolean takeFailSafeSnapshot =
        conf.getBoolean("hbase.snapshot.restore.take.failsafe.snapshot", false);
    restoreSnapshot(snapshotName, takeFailSafeSnapshot);
  }

  /**
   * Restore the specified snapshot on the original table. (The table must be disabled) If
   * 'takeFailSafeSnapshot' is set to true, a snapshot of the current table is taken before
   * executing the restore operation. In case of restore failure, the failsafe snapshot will be
   * restored. If the restore completes without problem the failsafe snapshot is deleted. The
   * failsafe snapshot name is configurable by using the property
   * "hbase.snapshot.restore.failsafe.name".
   * @param snapshotName name of the snapshot to restore
   * @param takeFailSafeSnapshot true if the failsafe snapshot should be taken
   * @throws IOException if a remote or network exception occurs
   * @throws RestoreSnapshotException if snapshot failed to be restored
   * @throws IllegalArgumentException if the restore request is formatted incorrectly
   */
  public void restoreSnapshot(final byte[] snapshotName, final boolean takeFailSafeSnapshot)
      throws IOException, RestoreSnapshotException {
    restoreSnapshot(Bytes.toString(snapshotName), takeFailSafeSnapshot);
  }

  /**
   * Restore the specified snapshot on the original table. (The table must be disabled) If
   * 'takeFailSafeSnapshot' is set to true, a snapshot of the current table is taken before
   * executing the restore operation. In case of restore failure, the failsafe snapshot will be
   * restored. If the restore completes without problem the failsafe snapshot is deleted. The
   * failsafe snapshot name is configurable by using the property
   * "hbase.snapshot.restore.failsafe.name".
   * @param snapshotName name of the snapshot to restore
   * @param takeFailSafeSnapshot true if the failsafe snapshot should be taken
   * @throws IOException if a remote or network exception occurs
   * @throws RestoreSnapshotException if snapshot failed to be restored
   * @throws IllegalArgumentException if the restore request is formatted incorrectly
   */
  public void restoreSnapshot(final String snapshotName, boolean takeFailSafeSnapshot)
      throws IOException, RestoreSnapshotException {
    restoreSnapshot(snapshotName, takeFailSafeSnapshot, false);
  }

  /**
   * Restore the specified snapshot on the original table. (The table must be disabled) If
   * 'takeFailSafeSnapshot' is set to true, a snapshot of the current table is taken before
   * executing the restore operation. In case of restore failure, the failsafe snapshot will be
   * restored. If the restore completes without problem the failsafe snapshot is deleted. The
   * failsafe snapshot name is configurable by using the property
   * "hbase.snapshot.restore.failsafe.name".
   * @param snapshotName name of the snapshot to restore
   * @param takeFailSafeSnapshot true if the failsafe snapshot should be taken
   * @param restoreAcl true to restore all acl of snapshot into the table.
   * @throws IOException if a remote or network exception occurs
   * @throws RestoreSnapshotException if snapshot failed to be restored
   * @throws IllegalArgumentException if the restore request is formatted incorrectly
   */
  public void restoreSnapshot(final String snapshotName, boolean takeFailSafeSnapshot,
      boolean restoreAcl) throws IOException, RestoreSnapshotException {
    TableName tableName = null;
    for (SnapshotDescription snapshotInfo : listSnapshots()) {
      if (snapshotInfo.getName().equals(snapshotName)) {
        tableName = TableName.valueOf(snapshotInfo.getTable());
        break;
      }
    }

    if (tableName == null) {
      throw new RestoreSnapshotException(
          "Unable to find the table name for snapshot=" + snapshotName);
    }

    // The table does not exists, switch to clone.
    if (!tableExists(tableName)) {
      try {
        cloneSnapshot(snapshotName, tableName, restoreAcl);
      } catch (InterruptedException e) {
        throw new InterruptedIOException(
            "Interrupted when restoring a nonexistent table: " + e.getMessage());
      }
      return;
    }

    // Check if the table is disabled
    if (!isTableDisabled(tableName)) {
      throw new TableNotDisabledException(tableName);
    }

    // Take a snapshot of the current state
    String failSafeSnapshotSnapshotName = null;
    if (takeFailSafeSnapshot) {
      failSafeSnapshotSnapshotName = conf.get("hbase.snapshot.restore.failsafe.name",
        "hbase-failsafe-{snapshot.name}-{restore.timestamp}");
      failSafeSnapshotSnapshotName =
          failSafeSnapshotSnapshotName.replace("{snapshot.name}", snapshotName)
              .replace("{table.name}", tableName.toString().replace(TableName.NAMESPACE_DELIM, '.'))
              .replace("{restore.timestamp}",
                String.valueOf(EnvironmentEdgeManager.currentTimeMillis()));
      LOG.info("Taking restore-failsafe snapshot: " + failSafeSnapshotSnapshotName);
      snapshot(failSafeSnapshotSnapshotName, tableName);
    }

    try {
      // Restore snapshot
      internalRestoreSnapshot(snapshotName, tableName, restoreAcl);
    } catch (IOException e) {
      // Somthing went wrong during the restore...
      // if the pre-restore snapshot is available try to rollback
      if (takeFailSafeSnapshot) {
        try {
          internalRestoreSnapshot(failSafeSnapshotSnapshotName, tableName, restoreAcl);
          String msg = "Restore snapshot=" + snapshotName + " failed. Rollback to snapshot=" +
              failSafeSnapshotSnapshotName + " succeeded.";
          LOG.error(msg, e);
          throw new RestoreSnapshotException(msg, e);
        } catch (IOException ex) {
          String msg = "Failed to restore and rollback to snapshot=" + failSafeSnapshotSnapshotName;
          LOG.error(msg, ex);
          throw new RestoreSnapshotException(msg, e);
        }
      } else {
        throw new RestoreSnapshotException("Failed to restore snapshot=" + snapshotName, e);
      }
    }

    // If the restore is succeeded, delete the pre-restore snapshot
    if (takeFailSafeSnapshot) {
      try {
        LOG.info("Deleting restore-failsafe snapshot: " + failSafeSnapshotSnapshotName);
        deleteSnapshot(failSafeSnapshotSnapshotName);
      } catch (IOException e) {
        LOG.error("Unable to remove the failsafe snapshot: " + failSafeSnapshotSnapshotName, e);
      }
    }
  }

  /**
   * Create a new table by cloning the snapshot content.
   * @param snapshotName name of the snapshot to be cloned
   * @param tableName name of the table where the snapshot will be restored
   * @throws IOException if a remote or network exception occurs
   * @throws TableExistsException if table to be created already exists
   * @throws RestoreSnapshotException if snapshot failed to be cloned
   * @throws IllegalArgumentException if the specified table has not a valid name
   */
  public void cloneSnapshot(final byte[] snapshotName, final byte[] tableName)
      throws IOException, TableExistsException, RestoreSnapshotException, InterruptedException {
    cloneSnapshot(Bytes.toString(snapshotName), TableName.valueOf(tableName), false);
  }

  /**
   * Create a new table by cloning the snapshot content.
   * @param snapshotName name of the snapshot to be cloned
   * @param tableName name of the table where the snapshot will be restored
   * @throws IOException if a remote or network exception occurs
   * @throws TableExistsException if table to be created already exists
   * @throws RestoreSnapshotException if snapshot failed to be cloned
   * @throws IllegalArgumentException if the specified table has not a valid name
   */
  public void cloneSnapshot(final byte[] snapshotName, final TableName tableName)
      throws IOException, TableExistsException, RestoreSnapshotException, InterruptedException {
    cloneSnapshot(Bytes.toString(snapshotName), tableName, false);
  }

  /**
   * Create a new table by cloning the snapshot content.
   * @param snapshotName name of the snapshot to be cloned
   * @param tableName name of the table where the snapshot will be restored
   * @throws IOException if a remote or network exception occurs
   * @throws TableExistsException if table to be created already exists
   * @throws RestoreSnapshotException if snapshot failed to be cloned
   * @throws IllegalArgumentException if the specified table has not a valid name
   */
  public void cloneSnapshot(final String snapshotName, final String tableName)
      throws IOException, TableExistsException, RestoreSnapshotException, InterruptedException {
    cloneSnapshot(snapshotName, TableName.valueOf(tableName), false);
  }

  public void cloneSnapshot(final String snapshotName, final TableName tableName)
      throws IOException, TableExistsException, RestoreSnapshotException, InterruptedException {
    cloneSnapshot(snapshotName, tableName, false);
  }

  /**
   * Create a new table by cloning the snapshot content.
   * @param snapshotName name of the snapshot to be cloned
   * @param tableName name of the table where the snapshot will be restored
   * @param restoreACL true to clone acl into newly created table
   * @throws IOException if a remote or network exception occurs
   * @throws TableExistsException if table to be created already exists
   * @throws RestoreSnapshotException if snapshot failed to be cloned
   * @throws IllegalArgumentException if the specified table has not a valid name
   */
  public void cloneSnapshot(final String snapshotName, final TableName tableName,
      boolean restoreACL)
      throws IOException, TableExistsException, RestoreSnapshotException, InterruptedException {
    if (tableExists(tableName)) {
      throw new TableExistsException(tableName);
    }
    internalRestoreSnapshot(snapshotName, tableName, restoreACL);
    waitUntilTableIsEnabled(tableName);
  }

  /**
   * Execute a distributed procedure on a cluster.
   * @param signature A distributed procedure is uniquely identified by its signature (default the
   *          root ZK node name of the procedure).
   * @param instance The instance name of the procedure. For some procedures, this parameter is
   *          optional.
   * @param props Property/Value pairs of properties passing to the procedure
   */
  public void execProcedure(String signature, String instance, Map<String, String> props)
      throws IOException {
    ProcedureDescription.Builder builder = ProcedureDescription.newBuilder();
    builder.setSignature(signature).setInstance(instance);
    for (String key : props.keySet()) {
      NameStringPair pair =
          NameStringPair.newBuilder().setName(key).setValue(props.get(key)).build();
      builder.addConfiguration(pair);
    }

    final ExecProcedureRequest req =
        RequestConverter.buildExecProcedureRequest(signature, instance, props);
    // run the procedure on the master
    ExecProcedureResponse response =
        executeCallable(new MasterCallable<ExecProcedureResponse>(getConnection()) {
          @Override
          protected ExecProcedureResponse rpcCall(MasterService.BlockingInterface master,
              HBaseRpcController controller) throws ServiceException {
            return master.execProcedure(controller, req);
          }
        });

    long start = EnvironmentEdgeManager.currentTimeMillis();
    long max = response.getExpectedTimeout();
    long maxPauseTime = max / this.numRetries;
    int tries = 0;
    LOG.debug("Waiting a max of " + max + " ms for procedure '" + signature + " : " + instance +
        "'' to complete. (max " + maxPauseTime + " ms per retry)");
    boolean done = false;
    while (tries == 0 || ((EnvironmentEdgeManager.currentTimeMillis() - start) < max && !done)) {
      try {
        // sleep a backoff <= pauseTime amount
        long sleep = getPauseTime(tries++);
        sleep = sleep > maxPauseTime ? maxPauseTime : sleep;
        LOG.debug(
          "(#" + tries + ") Sleeping: " + sleep + "ms while waiting for procedure completion.");
        Thread.sleep(sleep);

      } catch (InterruptedException e) {
        LOG.debug("Interrupted while waiting for procedure " + signature + " to complete");
        Thread.currentThread().interrupt();
      }
      LOG.debug("Getting current status of procedure from master...");
      done = isProcedureFinished(signature, instance, props);
    }
    if (!done) {
      throw new IOException("Procedure '" + signature + " : " + instance +
          "' wasn't completed in expectedTime:" + max + " ms");
    }
  }

  /**
   * Check the current state of the specified procedure.
   * <p>
   * There are three possible states:
   * <ol>
   * <li>running - returns <tt>false</tt></li>
   * <li>finished - returns <tt>true</tt></li>
   * <li>finished with error - throws the exception that caused the procedure to fail</li>
   * </ol>
   * <p>
   * @param signature The signature that uniquely identifies a procedure
   * @param instance The instance name of the procedure
   * @param props Property/Value pairs of properties passing to the procedure
   * @return true if the specified procedure is finished successfully, false if it is still running
   * @throws IOException if the specified procedure finished with error
   */
  public boolean isProcedureFinished(String signature, String instance, Map<String, String> props)
      throws IOException {
    final IsProcedureDoneRequest req =
        RequestConverter.buildIsProcedureDoneRequest(signature, instance, props);
    return executeCallable(new MasterCallable<Boolean>(getConnection()) {

      @Override
      protected Boolean rpcCall(MasterService.BlockingInterface master,
          HBaseRpcController controller) throws ServiceException {
        return master.isProcedureDone(controller, req).getDone();
      }
    });
  }

  /**
   * Execute Restore/Clone snapshot and wait for the server to complete (blocking). To check if the
   * cloned table exists, use {@link #isTableAvailable} -- it is not safe to create an HTable
   * instance to this table before it is available.
   * @param snapshotName snapshot to restore
   * @param tableName table name to restore the snapshot on
   * @throws IOException if a remote or network exception occurs
   * @throws RestoreSnapshotException if snapshot failed to be restored
   * @throws IllegalArgumentException if the restore request is formatted incorrectly
   */
  private void internalRestoreSnapshot(String snapshotName, TableName tableName, boolean restoreACL)
      throws IOException, RestoreSnapshotException {
    SnapshotDescription snapshot = SnapshotDescription.newBuilder().setName(snapshotName)
        .setTable(tableName.getNameAsString()).build();
    // actually restore the snapshot
    internalRestoreSnapshotAsync(snapshot, restoreACL);

    final IsRestoreSnapshotDoneRequest req =
        RequestConverter.buildIsRestoreSnapshotDoneRequest(snapshot);
    boolean done;
    final long maxPauseTime = 5000;
    int tries = 0;
    do {
      try {
        // sleep a backoff <= pauseTime amount
        long sleep = getPauseTime(tries++);
        sleep = sleep > maxPauseTime ? maxPauseTime : sleep;
        LOG.debug(
          tries + ") Sleeping: " + sleep + " ms while we wait for snapshot restore to complete.");
        Thread.sleep(sleep);
      } catch (InterruptedException e) {
        LOG.debug("Interrupted while waiting for snapshot " + snapshot + " restore to complete");
        Thread.currentThread().interrupt();
      }
      LOG.debug("Getting current status of snapshot restore from master...");
      done = executeCallable(new MasterCallable<Boolean>(getConnection(), tableName) {

        @Override
        protected Boolean rpcCall(MasterService.BlockingInterface master,
            HBaseRpcController controller) throws ServiceException {
          return master.isRestoreSnapshotDone(controller, req).getDone();
        }
      });
    } while (!done);
    if (!done) {
      throw new RestoreSnapshotException("Snapshot '" + snapshot.getName() + "' wasn't restored.");
    }
  }

  /**
   * Execute Restore/Clone snapshot and wait for the server to complete (asynchronous)
   * <p>
   * Only a single snapshot should be restored at a time, or results may be undefined.
   * @param snapshot snapshot to restore
   * @return response from the server indicating the max time to wait for the snapshot
   * @throws IOException if a remote or network exception occurs
   * @throws RestoreSnapshotException if snapshot failed to be restored
   * @throws IllegalArgumentException if the restore request is formatted incorrectly
   */
  private RestoreSnapshotResponse internalRestoreSnapshotAsync(SnapshotDescription snapshot,
      boolean restoreACL) throws IOException, RestoreSnapshotException {
    ClientSnapshotDescriptionUtils.assertSnapshotRequestIsValid(snapshot);
    final RestoreSnapshotRequest req =
        RequestConverter.buildRestoreSnapshotRequest(snapshot, restoreACL);
    // run the snapshot restore on the master
    return executeCallable(new MasterCallable<RestoreSnapshotResponse>(getConnection(),
        TableName.valueOf(snapshot.getTable())) {
      @Override
      protected RestoreSnapshotResponse rpcCall(MasterService.BlockingInterface master,
          HBaseRpcController controller) throws ServiceException {
        return master.restoreSnapshot(controller, req);
      }
    });
  }

  /**
   * List completed snapshots.
   * @return a list of snapshot descriptors for completed snapshots
   * @throws IOException if a network error occurs
   */
  public List<SnapshotDescription> listSnapshots() throws IOException {
    return executeCallable(new MasterCallable<List<SnapshotDescription>>(getConnection()) {

      @Override
      protected List<SnapshotDescription> rpcCall(MasterService.BlockingInterface master,
          HBaseRpcController controller) throws ServiceException {
        return master
            .getCompletedSnapshots(controller, GetCompletedSnapshotsRequest.getDefaultInstance())
            .getSnapshotsList();
      }
    });
  }

  /**
   * List all the completed snapshots matching the given regular expression.
   * @param regex The regular expression to match against
   * @return - returns a List of SnapshotDescription
   * @throws IOException if a remote or network exception occurs
   */
  public List<SnapshotDescription> listSnapshots(String regex) throws IOException {
    return listSnapshots(Pattern.compile(regex));
  }

  /**
   * List all the completed snapshots matching the given pattern.
   * @param pattern The compiled regular expression to match against
   * @return - returns a List of SnapshotDescription
   * @throws IOException if a remote or network exception occurs
   */
  public List<SnapshotDescription> listSnapshots(Pattern pattern) throws IOException {
    List<SnapshotDescription> matched = new LinkedList<SnapshotDescription>();
    List<SnapshotDescription> snapshots = listSnapshots();
    for (SnapshotDescription snapshot : snapshots) {
      if (pattern.matcher(snapshot.getName()).matches()) {
        matched.add(snapshot);
      }
    }
    return matched;
  }

  /**
   * Delete an existing snapshot.
   * @param snapshotName name of the snapshot
   * @throws IOException if a remote or network exception occurs
   */
  public void deleteSnapshot(byte[] snapshotName) throws IOException {
    deleteSnapshot(Bytes.toString(snapshotName));
  }

  /**
   * Delete an existing snapshot.
   * @param snapshotName name of the snapshot
   * @throws IOException if a remote or network exception occurs
   */
  public void deleteSnapshot(String snapshotName) throws IOException {
    // make sure the snapshot is possibly valid
    TableName.isLegalFullyQualifiedTableName(Bytes.toBytes(snapshotName));
    internalDeleteSnapshot(SnapshotDescription.newBuilder().setName(snapshotName).build());
  }

  /**
   * Delete existing snapshots whose names match the pattern passed.
   * @param regex The regular expression to match against
   * @throws IOException if a remote or network exception occurs
   */
  public void deleteSnapshots(final String regex) throws IOException {
    deleteSnapshots(Pattern.compile(regex));
  }

  /**
   * Delete existing snapshots whose names match the pattern passed.
   * @param pattern pattern for names of the snapshot to match
   * @throws IOException if a remote or network exception occurs
   */
  public void deleteSnapshots(final Pattern pattern) throws IOException {
    List<SnapshotDescription> snapshots = listSnapshots(pattern);
    for (final SnapshotDescription snapshot : snapshots) {
      try {
        internalDeleteSnapshot(snapshot);
      } catch (IOException ex) {
        LOG.info(
          "Failed to delete snapshot " + snapshot.getName() + " for table " + snapshot.getTable(),
          ex);
      }
    }
  }

  private void internalDeleteSnapshot(SnapshotDescription snapshot) throws IOException {
    final DeleteSnapshotRequest req = RequestConverter.buildDeleteSnapshotRequest(snapshot);
    executeCallable(new MasterCallable<Void>(getConnection()) {

      @Override
      protected Void rpcCall(MasterService.BlockingInterface master, HBaseRpcController controller)
          throws ServiceException {
        master.deleteSnapshot(controller, req);
        return null;
      }
    });
  }

  /**
   * Apply the new quota settings.
   * @param quota the quota settings
   * @throws IOException if a remote or network exception occurs
   */
  public void setQuota(final QuotaSettings quota) throws IOException {
    final SetQuotaRequest req = QuotaSettings.buildSetQuotaRequestProto(quota);
    executeCallable(new MasterCallable<Void>(getConnection()) {

      @Override
      protected Void rpcCall(MasterService.BlockingInterface master, HBaseRpcController controller)
          throws ServiceException {
        if (quota.getTableName() != null) {
          controller.setPriority(quota.getTableName());
        }
        master.setQuota(controller, req);
        return null;
      }
    });
  }

  /**
   * Return a Quota Scanner to list the quotas based on the filter.
   * @param filter the quota settings filter
   * @return the quota scanner
   * @throws IOException if a remote or network exception occurs
   */
  public QuotaRetriever getQuotaRetriever(final QuotaFilter filter) throws IOException {
    return QuotaRetriever.open(conf, filter);
  }

  private <V> V executeCallable(MasterCallable<V> callable) throws IOException {
    try {
      return rpcCallerFactory.<V> newCaller().callWithRetries(callable);
    } finally {
      callable.close();
    }
  }

  /**
   * Creates and returns a {@link com.google.protobuf.RpcChannel} instance connected to the active
   * master.
   * <p>
   * The obtained {@link com.google.protobuf.RpcChannel} instance can be used to access a published
   * coprocessor {@link com.google.protobuf.Service} using standard protobuf service invocations:
   * </p>
   * <div style="background-color: #cccccc; padding: 2px"> <blockquote>
   * 
   * <pre>
   * CoprocessorRpcChannel channel = myAdmin.coprocessorService();
   * MyService.BlockingInterface service = MyService.newBlockingStub(channel);
   * MyCallRequest request = MyCallRequest.newBuilder()
   *     ...
   *     .build();
   * MyCallResponse response = service.myCall(null, request);
   * </pre>
   * 
   * </blockquote></div>
   * @return A MasterCoprocessorRpcChannel instance
   */
  public CoprocessorRpcChannel coprocessorService() {
    return new MasterCoprocessorRpcChannel(connection);
  }

  /**
   * Creates and returns a {@link com.google.protobuf.RpcChannel} instance connected to the passed
   * region server.
   * <p>
   * The obtained {@link com.google.protobuf.RpcChannel} instance can be used to access a published
   * coprocessor {@link com.google.protobuf.Service} using standard protobuf service invocations:
   * </p>
   * <div style="background-color: #cccccc; padding: 2px"> <blockquote>
   * 
   * <pre>
   * CoprocessorRpcChannel channel = myAdmin.coprocessorService(serverName);
   * MyService.BlockingInterface service = MyService.newBlockingStub(channel);
   * MyCallRequest request = MyCallRequest.newBuilder()
   *     ...
   *     .build();
   * MyCallResponse response = service.myCall(null, request);
   * </pre>
   * 
   * </blockquote></div>
   * @param sn the server name to which the endpoint call is made
   * @return A RegionServerCoprocessorRpcChannel instance
   */
  public CoprocessorRpcChannel coprocessorService(ServerName sn) {
    return new RegionServerCoprocessorRpcChannel(connection, sn);
  }

  /**
   * Truncate a table. Synchronous operation.
   * @param tableName name of table to truncate
   * @param preserveSplits True if the splits should be preserved
   * @throws IOException if a remote or network exception occurs
   */
  public void truncateTable(final TableName tableName, boolean preserveSplits) throws IOException {
    final TruncateTableRequest req =
        RequestConverter.buildTruncateTableRequest(tableName, preserveSplits);
    executeCallable(new MasterCallable<Void>(getConnection(), tableName) {

      @Override
      protected Void rpcCall(MasterService.BlockingInterface master, HBaseRpcController controller)
          throws ServiceException {
        LOG.info("Started truncate of " + tableName);
        master.truncateTable(null, req);
        return null;
      }
    });
  }

  public ThrottleState switchThrottle(ThrottleState state) throws IOException {
    final SwitchThrottleRequest req = RequestConverter.buildSwitchThrottleRequest(state);
    return executeCallable(new MasterCallable<ThrottleState>(getConnection()) {

      @Override
      protected ThrottleState rpcCall(BlockingInterface master, HBaseRpcController controller)
          throws Exception {
        SwitchThrottleResponse resp = master.switchThrottle(controller, req);
        return resp.hasPrevThrottleState()
            ? ProtobufUtil.toThrottleState(resp.getPrevThrottleState()) : null;
      }
    });
  }

  public void addReplicationPeer(String peerId, ReplicationPeerConfig peerConfig)
      throws IOException {
    this.addReplicationPeer(peerId, peerConfig, true);
  }

  public void addReplicationPeer(String peerId, ReplicationPeerConfig peerConfig, boolean enabled)
      throws IOException {
    executeCallable(new MasterCallable<Void>(getConnection()) {
      @Override
      protected Void rpcCall(BlockingInterface master, HBaseRpcController controller)
          throws Exception {
        master.addReplicationPeer(controller,
          RequestConverter.buildAddReplicationPeerRequest(peerId, peerConfig, enabled));
        return null;
      }
    });
  }

  public void addReplicationPeerForBranch2(String peerId, ReplicationPeerConfig peerConfig,
      boolean enabled) throws IOException {
    executeCallable(new MasterCallable<Void>(getConnection()) {
      @Override
      protected Void rpcCall(BlockingInterface master, HBaseRpcController controller)
          throws Exception {
        master.addReplicationPeerForBranch2(controller,
          RequestConverter.buildNewAddReplicationPeerRequest(peerId, peerConfig, enabled));
        return null;
      }
    });
  }

  public void removeReplicationPeer(String peerId) throws IOException {
    executeCallable(new MasterCallable<Void>(getConnection()) {
      @Override
      protected Void rpcCall(BlockingInterface master, HBaseRpcController controller) throws Exception {
        master.removeReplicationPeer(controller,
          RequestConverter.buildRemoveReplicationPeerRequest(peerId));
        return null;
      }
    });
  }

  public void enableReplicationPeer(final String peerId) throws IOException {
    executeCallable(new MasterCallable<Void>(getConnection()) {
      @Override
      protected Void rpcCall(BlockingInterface master, HBaseRpcController controller) throws Exception {
        master.enableReplicationPeer(controller,
          RequestConverter.buildEnableReplicationPeerRequest(peerId));
        return null;
      }
    });
  }

  public void disableReplicationPeer(final String peerId) throws IOException {
    executeCallable(new MasterCallable<Void>(getConnection()) {
      @Override
      protected Void rpcCall(BlockingInterface master, HBaseRpcController controller) throws Exception {
        master.disableReplicationPeer(controller,
          RequestConverter.buildDisableReplicationPeerRequest(peerId));
        return null;
      }
    });
  }

  public ReplicationPeerConfig getReplicationPeerConfig(final String peerId) throws IOException {
    return executeCallable(new MasterCallable<ReplicationPeerConfig>(getConnection()) {
      @Override
      protected ReplicationPeerConfig rpcCall(BlockingInterface master, HBaseRpcController controller) throws Exception {
        GetReplicationPeerConfigResponse response = master.getReplicationPeerConfig(
          controller, RequestConverter.buildGetReplicationPeerConfigRequest(peerId));
        return ReplicationSerDeHelper.convert(response.getPeerConfig());
      }
    });
  }

  public void updateReplicationPeerConfig(final String peerId,
      final ReplicationPeerConfig peerConfig) throws IOException {
    executeCallable(new MasterCallable<Void>(getConnection()) {
      @Override
      protected Void rpcCall(BlockingInterface master, HBaseRpcController controller) throws Exception {
        master.updateReplicationPeerConfig(controller,
          RequestConverter.buildUpdateReplicationPeerConfigRequest(peerId, peerConfig));
        return null;
      }
    });
  }

  public void updateReplicationPeerConfigForBranch2(final String peerId,
      final ReplicationPeerConfig peerConfig) throws IOException {
    executeCallable(new MasterCallable<Void>(getConnection()) {
      @Override
      protected Void rpcCall(BlockingInterface master, HBaseRpcController controller)
          throws Exception {
        master.updateReplicationPeerConfigForBranch2(controller,
          RequestConverter.buildNewUpdateReplicationPeerConfigRequest(peerId, peerConfig));
        return null;
      }
    });
  }

  public void appendReplicationPeerTableCFs(String id,
      Map<TableName, ? extends Collection<String>> tableCfs) throws ReplicationException,
      IOException {
    if (tableCfs == null) {
      throw new ReplicationException("tableCfs is null");
    }
    ReplicationPeerConfig peerConfig = getReplicationPeerConfig(id);
    ReplicationSerDeHelper.appendTableCFsToReplicationPeerConfig(tableCfs, peerConfig);
    updateReplicationPeerConfig(id, peerConfig);
  }

  public void removeReplicationPeerTableCFs(String id,
      Map<TableName, ? extends Collection<String>> tableCfs) throws ReplicationException,
      IOException {
    if (tableCfs == null) {
      throw new ReplicationException("tableCfs is null");
    }
    ReplicationPeerConfig peerConfig = getReplicationPeerConfig(id);
    ReplicationSerDeHelper.removeTableCFsFromReplicationPeerConfig(tableCfs, peerConfig, id);
    updateReplicationPeerConfig(id, peerConfig);
  }

  public List<ReplicationPeerDescription> listReplicationPeers() throws IOException {
    return listReplicationPeers((Pattern)null);
  }

  public List<ReplicationPeerDescription> listReplicationPeers(String regex) throws IOException {
    return listReplicationPeers(Pattern.compile(regex));
  }

  public List<ReplicationPeerDescription> listReplicationPeers(Pattern pattern)
      throws IOException {
    return executeCallable(new MasterCallable<List<ReplicationPeerDescription>>(getConnection()) {
      @Override
      protected List<ReplicationPeerDescription> rpcCall(BlockingInterface master, HBaseRpcController controller) throws Exception {
        List<ReplicationProtos.ReplicationPeerDescription> peersList = master.listReplicationPeers(
          controller, RequestConverter.buildListReplicationPeersRequest(pattern))
            .getPeerDescList();
        List<ReplicationPeerDescription> result = new ArrayList<>(peersList.size());
        for (ReplicationProtos.ReplicationPeerDescription peer : peersList) {
          result.add(ReplicationSerDeHelper.toReplicationPeerDescription(peer));
        }
        return result;
      }
    });
  }

  public List<ReplicationPeerDescription> listReplicationPeersForBranch2() throws IOException {
    Pattern pattern = null;
    return executeCallable(new MasterCallable<List<ReplicationPeerDescription>>(getConnection()) {
      @Override
      protected List<ReplicationPeerDescription> rpcCall(BlockingInterface master,
          HBaseRpcController controller) throws Exception {
        List<ReplicationProtos.NewReplicationPeerDescription> peersList =
            master.listReplicationPeersForBranch2(controller,
              RequestConverter.buildListReplicationPeersRequest(pattern)).getPeerDescList();
        List<ReplicationPeerDescription> result = new ArrayList<>(peersList.size());
        for (ReplicationProtos.NewReplicationPeerDescription peer : peersList) {
          result.add(ReplicationSerDeHelper.toNewReplicationPeerDescription(peer));
        }
        return result;
      }
    });
  }

  public List<TableCFs> listReplicatedTableCFs() throws IOException {
    List<TableCFs> replicatedTableCFs = new ArrayList<>();
    HTableDescriptor[] tables = listTables();
    for (HTableDescriptor table : tables) {
      HColumnDescriptor[] columns = table.getColumnFamilies();
      Map<String, Integer> cfs = new HashMap<>();
      for (HColumnDescriptor column : columns) {
        if (column.getScope() != HConstants.REPLICATION_SCOPE_LOCAL) {
          cfs.put(column.getNameAsString(), column.getScope());
        }
      }
      if (!cfs.isEmpty()) {
        replicatedTableCFs.add(new TableCFs(table.getTableName(), cfs));
      }
    }
    return replicatedTableCFs;
  }

  public void enableTableReplication(final TableName tableName) throws IOException {
    if (tableName == null) {
      throw new IllegalArgumentException("Table name cannot be null");
    }
    if (!tableExists(tableName)) {
      throw new TableNotFoundException("Table '" + tableName.getNameAsString()
          + "' does not exists.");
    }
    byte[][] splits = getTableSplitRowKeys(tableName);
    checkAndSyncTableDescToPeers(tableName, splits);
    setTableRep(tableName, true);
  }

  public void disableTableReplication(final TableName tableName) throws IOException {
    if (tableName == null) {
      throw new IllegalArgumentException("Table name is null");
    }
    if (!tableExists(tableName)) {
      throw new TableNotFoundException("Table '" + tableName.getNamespaceAsString()
          + "' does not exists.");
    }
    setTableRep(tableName, false);
  }

  /**
   * Set the table's replication switch if the table's replication switch is already not set.
   * @param tableName name of the table
   * @param enableRep is replication switch enable or disable
   * @throws IOException if a remote or network exception occurs
   */
  private void setTableRep(final TableName tableName, boolean enableRep) throws IOException {
    HTableDescriptor htd = new HTableDescriptor(getTableDescriptor(tableName));
    ReplicationState currentReplicationState = getTableReplicationState(htd);
    if (enableRep && currentReplicationState != ReplicationState.ENABLED
        || !enableRep && currentReplicationState != ReplicationState.DISABLED) {
      for (HColumnDescriptor hcd : htd.getFamilies()) {
        hcd.setScope(enableRep ? HConstants.REPLICATION_SCOPE_GLOBAL
            : HConstants.REPLICATION_SCOPE_LOCAL);
      }
      modifyTable(tableName, htd);
    }
  }

  /**
   * This enum indicates the current state of the replication for a given table.
   */
  private enum ReplicationState {
    ENABLED, // all column families enabled
    MIXED, // some column families enabled, some disabled
    DISABLED // all column families disabled
  }

  /**
   * @param htd table descriptor details for the table to check
   * @return ReplicationState the current state of the table.
   */
  private ReplicationState getTableReplicationState(HTableDescriptor htd) {
    boolean hasEnabled = false;
    boolean hasDisabled = false;

    for (HColumnDescriptor hcd : htd.getFamilies()) {
      if (hcd.getScope() != HConstants.REPLICATION_SCOPE_GLOBAL
          && hcd.getScope() != HConstants.REPLICATION_SCOPE_SERIAL) {
        hasDisabled = true;
      } else {
        hasEnabled = true;
      }
    }

    if (hasEnabled && hasDisabled) return ReplicationState.MIXED;
    if (hasEnabled) return ReplicationState.ENABLED;
    return ReplicationState.DISABLED;
  }

  /**
   * Get the split row keys of table
   * @param tableName table name
   * @return array of split row keys
   * @throws IOException
   */
  private byte[][] getTableSplitRowKeys(TableName tableName) throws IOException {
    HTable table = null;
    try {
      table = new HTable(this.connection.getConfiguration(), tableName);
      byte[][] startKeys = table.getStartKeys();
      if (startKeys.length == 1) {
        return null;
      }
      byte[][] splits = new byte[startKeys.length - 1][];
      for (int i = 1; i < startKeys.length; i++) {
        splits[i - 1] = startKeys[i];
      }
      return splits;
    } finally {
      if (table != null) {
        try {
          table.close();
        } catch (IOException e) {
          LOG.warn("Unable to close table");
        }
      }
    }
  }

  /**
   * Connect to peer and check the table descriptor on peer:
   * <ol>
   * <li>Create the same table on peer when not exist.</li>
   * <li>Throw exception if the table exists on peer cluster but descriptors are not same.</li>
   * </ol>
   * @param tableName name of the table to sync to the peer
   * @param splits table split keys
   * @throws IOException
   */
  private void checkAndSyncTableDescToPeers(final TableName tableName, final byte[][] splits)
      throws IOException {
    List<ReplicationPeerDescription> peers = listReplicationPeers();
    if (peers == null || peers.size() <= 0) {
      throw new IllegalArgumentException("Found no peer cluster for replication.");
    }
    for (ReplicationPeerDescription peerDesc : peers) {
      if (peerDesc.getPeerConfig().needToReplicate(tableName)) {
        Configuration peerConf = ReplicationUtils.getPeerClusterConfiguration(peerDesc, conf);
        try (HConnection connection = HConnectionManager.createConnection(peerConf);
            HBaseAdmin repHBaseAdmin = new HBaseAdmin(connection)) {
          HTableDescriptor localHtd = getTableDescriptor(tableName);
          HTableDescriptor peerHtd = null;
          if (!repHBaseAdmin.tableExists(tableName)) {
            repHBaseAdmin.createTable(localHtd, splits);
          } else {
            peerHtd = repHBaseAdmin.getTableDescriptor(tableName);
            if (peerHtd == null) {
              throw new IllegalArgumentException("Failed to get table descriptor for table "
                  + tableName.getNameAsString() + " from peer cluster " + peerDesc.getPeerId());
            }
            if (!compareForReplication(peerHtd, localHtd)) {
              throw new IllegalArgumentException("Table " + tableName.getNameAsString()
                  + " exists in peer cluster " + peerDesc.getPeerId()
                  + ", but the table descriptors are not same when compared with source cluster."
                  + " Thus can not enable the table's replication switch.");
            }
          }
        }
      }
    }
  }

  /**
   * Compare the contents of the descriptor with another one passed as a parameter for replication
   * purpose. The REPLICATION_SCOPE field is ignored during comparison.
   * @param peerHtd descriptor on peer cluster
   * @param localHtd descriptor on source cluster which needs to be replicated.
   * @return true if the contents of the two descriptors match (ignoring just REPLICATION_SCOPE).
   * @see java.lang.Object#equals(java.lang.Object)
   */
  private boolean compareForReplication(HTableDescriptor peerHtd, HTableDescriptor localHtd) {
    if (peerHtd == localHtd) {
      return true;
    }
    if (peerHtd == null) {
      return false;
    }
    boolean result = false;

    // Create a copy of peer HTD as we need to change its replication
    // scope to match with the local HTD.
    HTableDescriptor peerHtdCopy = new HTableDescriptor(peerHtd);

    result = copyReplicationScope(peerHtdCopy, localHtd);

    // If copy was successful, compare the two tables now.
    if (result) {
      result = (peerHtdCopy.compareTo(localHtd) == 0);
    }

    return result;
  }

  /**
   * Copies the REPLICATION_SCOPE of table descriptor passed as an argument. Before copy, the method
   * ensures that the name of table and column-families should match.
   * @param peerHtd descriptor on peer cluster
   * @param localHtd - The HTableDescriptor of table from source cluster.
   * @return true If the name of table and column families match and REPLICATION_SCOPE copied
   *         successfully. false If there is any mismatch in the names.
   */
  private boolean copyReplicationScope(final HTableDescriptor peerHtd,
      final HTableDescriptor localHtd) {
    // Copy the REPLICATION_SCOPE only when table names and the names of
    // Column-Families are same.
    int result = peerHtd.getTableName().compareTo(localHtd.getTableName());

    if (result == 0) {
      Iterator<HColumnDescriptor> remoteHCDIter = peerHtd.getFamilies().iterator();
      Iterator<HColumnDescriptor> localHCDIter = localHtd.getFamilies().iterator();

      while (remoteHCDIter.hasNext() && localHCDIter.hasNext()) {
        HColumnDescriptor remoteHCD = remoteHCDIter.next();
        HColumnDescriptor localHCD = localHCDIter.next();

        String remoteHCDName = remoteHCD.getNameAsString();
        String localHCDName = localHCD.getNameAsString();

        if (remoteHCDName.equals(localHCDName)) {
          remoteHCD.setScope(localHCD.getScope());
        } else {
          result = -1;
          break;
        }
      }
      if (remoteHCDIter.hasNext() || localHCDIter.hasNext()) {
        return false;
      }
    }

    return result == 0;
  }

  public ReplicationLoadSource getPeerMaxReplicationLoad(String peerId) throws IOException {
    return executeCallable(new MasterCallable<ReplicationLoadSource>(getConnection()) {
      @Override
      protected ReplicationLoadSource rpcCall(BlockingInterface master,
          HBaseRpcController controller) throws Exception {
        ClusterStatusProtos.ReplicationLoadSource heaviestLoad =
            master
                .getPeerMaxReplicationLoad(controller,
                  RequestConverter
                      .buildGetPeerMaxReplicationLoadRequest(Optional.ofNullable(peerId)))
                .getReplicationLoadSource();
        ReplicationLoadSource result = ProtobufUtil.toReplicationLoadSource(heaviestLoad);
        return result;
      }
    });
  }

  /**
   * Get {@link RegionLoad} of all regions hosted on a regionserver.
   *
   * @param serverName region server from which {@link RegionLoad} is required.
   * @return a {@link RegionLoad} list of all regions hosted on a region server
   * @throws IOException if a remote or network exception occurs
   */
  public List<RegionLoad> getRegionLoads(ServerName serverName) throws IOException {
    return getRegionLoads(serverName, null);
  }

  /**
   * Get {@link RegionLoad} of all regions hosted on a regionserver for a table.
   *
   * @param serverName region server from which {@link RegionLoad} is required.
   * @param tableName get {@link RegionLoad} of regions belonging to the table
   * @return a {@link RegionLoad} list of all regions of a table hosted on a region server
   * @throws IOException if a remote or network exception occurs
   */
  public List<RegionLoad> getRegionLoads(ServerName serverName, TableName tableName) throws IOException {
    AdminService.BlockingInterface admin = this.connection.getAdmin(serverName);
    AdminProtos.GetRegionLoadRequest request =
        RequestConverter.buildGetRegionLoadRequest(tableName);
    try {
      return admin.getRegionLoad(null, request).getRegionLoadsList().stream().map(RegionLoad::new)
          .collect(Collectors.toList());
    } catch (ServiceException se) {
      throw ProtobufUtil.getRemoteException(se);
    }
  }

  /**
   * Turn the split switch on or off.
   * @param enabled enabled or not
   * @param synchronous If <code>true</code>, it waits until current split() call, if outstanding,
   *          to return.
   * @return Previous switch value
   * @throws IOException if a remote or network exception occurs
   */
  public boolean splitSwitch(boolean enabled, boolean synchronous) throws IOException {
    return setSplitOrMergeOn(enabled, synchronous, MasterSwitchType.SPLIT);
  }

  /**
   * Turn the merge switch on or off.
   * @param enabled enabled or not
   * @param synchronous If <code>true</code>, it waits until current merge() call, if outstanding,
   *          to return.
   * @return Previous switch value
   * @throws IOException if a remote or network exception occurs
   */
  public boolean mergeSwitch(boolean enabled, boolean synchronous) throws IOException {
    return setSplitOrMergeOn(enabled, synchronous, MasterSwitchType.MERGE);
  }

  private boolean setSplitOrMergeOn(boolean enabled, boolean synchronous,
      MasterSwitchType switchType) throws IOException {
    SetSplitOrMergeEnabledRequest request =
        RequestConverter.buildSetSplitOrMergeEnabledRequest(enabled, synchronous, switchType);
    return executeCallable(new MasterCallable<Boolean>(getConnection()) {
      @Override
      protected Boolean rpcCall(MasterService.BlockingInterface master,
          HBaseRpcController controller) throws ServiceException {
        return master.setSplitOrMergeEnabled(controller, request).getPrevValueList().get(0);
      }
    });
  }

  /**
   * Query the current state of the split switch.
   * @return <code>true</code> if the switch is enabled, <code>false</code> otherwise.
   * @throws IOException if a remote or network exception occurs
   */
  public boolean isSplitEnabled() throws IOException {
    IsSplitOrMergeEnabledRequest request = RequestConverter
        .buildIsSplitOrMergeEnabledRequest(org.apache.hadoop.hbase.client.MasterSwitchType.SPLIT);
    return executeCallable(new MasterCallable<Boolean>(getConnection()) {
      @Override
      protected Boolean rpcCall(MasterService.BlockingInterface master,
          HBaseRpcController controller) throws ServiceException {
        return master.isSplitOrMergeEnabled(controller, request).getEnabled();
      }
    });
  }

  /**
   * Query the current state of the merge switch.
   * @return <code>true</code> if the switch is enabled, <code>false</code> otherwise.
   * @throws IOException if a remote or network exception occurs
   */
  public boolean isMergeEnabled() throws IOException {
    IsSplitOrMergeEnabledRequest request = RequestConverter
        .buildIsSplitOrMergeEnabledRequest(org.apache.hadoop.hbase.client.MasterSwitchType.MERGE);
    return executeCallable(new MasterCallable<Boolean>(getConnection()) {
      @Override
      protected Boolean rpcCall(MasterService.BlockingInterface master,
          HBaseRpcController controller) throws ServiceException {
        return master.isSplitOrMergeEnabled(controller, request).getEnabled();
      }
    });
  }

  /**
   * Set region quota which is a hard limit
   * @param regionName thd encoded region name
   * @param type throttle type
   * @param limit throttle limit
   * @param timeUnit time unit
   * @throws IOException if a remote or network exception occurs
   */
  public void setRegionQuota(byte[] regionName, ThrottleType type, long limit, TimeUnit timeUnit)
      throws IOException {
    final SetRegionQuotaRequest req =
        RequestConverter.buildSetRegionQuotaRequest(regionName, type, limit, timeUnit);
    executeCallable(new MasterCallable<Void>(getConnection()) {
      @Override
      protected Void rpcCall(BlockingInterface master, HBaseRpcController controller)
          throws Exception {
        master.setRegionQuota(controller, req);
        return null;
      }
    });
  }

  /**
   * Remove region quota of the specified throttle type
   * @param regionName the encoded region name
   * @param type throttle type
   * @throws IOException if a remote or network exception occurs
   */
  public void removeRegionQuota(byte[] regionName, ThrottleType type) throws IOException {
    final SetRegionQuotaRequest req =
        RequestConverter.buildSetRegionQuotaRequest(regionName, type, -1, null);
    executeCallable(new MasterCallable<Void>(getConnection()) {
      @Override
      protected Void rpcCall(BlockingInterface master, HBaseRpcController controller)
          throws Exception {
        master.setRegionQuota(controller, req);
        return null;
      }
    });
  }

  /**
   * Remove region quota of all throttle types
   * @param regionName the encoded region name
   * @throws IOException if a remote or network exception occurs
   */
  public void removeRegionQuota(byte[] regionName) throws IOException {
    final SetRegionQuotaRequest req =
        RequestConverter.buildSetRegionQuotaRequest(regionName, null, -1, null);
    executeCallable(new MasterCallable<Void>(getConnection()) {
      @Override
      protected Void rpcCall(BlockingInterface master, HBaseRpcController controller)
          throws Exception {
        master.setRegionQuota(controller, req);
        return null;
      }
    });
  }

  /**
   * List region quotas which is a hard limit
   * @return region quotas
   * @throws IOException if a remote or network exception occurs
   */
  public List<RegionQuotaSettings> listRegionQuota() throws IOException {
    final ListRegionQuotaRequest req = ListRegionQuotaRequest.getDefaultInstance();
    ListRegionQuotaResponse response =
        executeCallable(new MasterCallable<ListRegionQuotaResponse>(getConnection()) {
          @Override
          protected ListRegionQuotaResponse rpcCall(BlockingInterface master,
              HBaseRpcController controller) throws Exception {
            return master.listRegionQuota(controller, req);
          }
        });
    return response.getRegionQuotaList().stream()
        .map(regionQuota -> new RegionQuotaSettings(
            Bytes.toString(regionQuota.getRegion().toByteArray()), regionQuota.getThrottle()))
        .collect(Collectors.toList());
  }
}
