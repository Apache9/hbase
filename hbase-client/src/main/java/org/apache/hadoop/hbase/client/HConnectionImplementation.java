package org.apache.hadoop.hbase.client;

import static org.apache.hadoop.hbase.client.NonceGenerator.CLIENT_NONCES_ENABLED_KEY;

import com.google.common.base.Throwables;
import com.google.common.hash.Hashing;
import com.google.protobuf.BlockingRpcChannel;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import com.xiaomi.infra.hbase.salted.KeySalter;
import com.xiaomi.infra.hbase.salted.SaltedHTable;

import java.io.Closeable;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Chore;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.MultiActionResultTooLarge;
import org.apache.hadoop.hbase.RegionTooBusyException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.TooManyRegionScannersException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.catalog.MetaReader;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.MetaScanner.MetaScannerVisitor;
import org.apache.hadoop.hbase.client.MetaScanner.MetaScannerVisitorBase;
import org.apache.hadoop.hbase.client.backoff.ClientBackoffPolicy;
import org.apache.hadoop.hbase.client.backoff.ClientBackoffPolicyFactory;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.exceptions.ClientExceptionsUtil;
import org.apache.hadoop.hbase.exceptions.RegionMovedException;
import org.apache.hadoop.hbase.exceptions.RegionOpeningException;
import org.apache.hadoop.hbase.ipc.RpcClient;
import org.apache.hadoop.hbase.ipc.RpcClientFactory;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.RequestConverter;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.AdminService;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ClientService;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.CoprocessorServiceRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.CoprocessorServiceResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.AddColumnRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.AddColumnResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.AssignRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.AssignRegionResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.BalanceRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.BalanceResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.CreateNamespaceRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.CreateNamespaceResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.CreateTableRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.CreateTableResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.DeleteColumnRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.DeleteColumnResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.DeleteNamespaceRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.DeleteNamespaceResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.DeleteSnapshotRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.DeleteSnapshotResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.DeleteTableRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.DeleteTableResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.DisableTableRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.DisableTableResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.DispatchMergingRegionsRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.DispatchMergingRegionsResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.EnableCatalogJanitorRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.EnableCatalogJanitorResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.EnableTableRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.EnableTableResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.ExecProcedureRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.ExecProcedureResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.GetClusterStatusRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.GetClusterStatusResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.GetCompletedSnapshotsRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.GetCompletedSnapshotsResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.GetNamespaceDescriptorRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.GetNamespaceDescriptorResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.GetSchemaAlterStatusRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.GetSchemaAlterStatusResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.GetTableDescriptorsRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.GetTableDescriptorsResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.GetTableNamesRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.GetTableNamesResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.IsCatalogJanitorEnabledRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.IsCatalogJanitorEnabledResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.IsMasterRunningRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.IsMasterRunningResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.IsProcedureDoneRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.IsProcedureDoneResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.IsRestoreSnapshotDoneRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.IsRestoreSnapshotDoneResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.IsSnapshotDoneRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.IsSnapshotDoneResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.IsSplitOrMergeEnabledRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.IsSplitOrMergeEnabledResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.ListNamespaceDescriptorsRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.ListNamespaceDescriptorsResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.ListTableDescriptorsByNamespaceRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.ListTableDescriptorsByNamespaceResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.ListTableNamesByNamespaceRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.ListTableNamesByNamespaceResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.MasterService;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.ModifyColumnRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.ModifyColumnResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.ModifyNamespaceRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.ModifyNamespaceResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.ModifyTableRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.ModifyTableResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.MoveRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.MoveRegionResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.OfflineRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.OfflineRegionResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.RestoreSnapshotRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.RestoreSnapshotResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.RunCatalogScanRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.RunCatalogScanResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.SetBalancerRunningRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.SetBalancerRunningResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.SetQuotaRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.SetQuotaResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.SetSplitOrMergeEnabledRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.SetSplitOrMergeEnabledResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.ShutdownRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.ShutdownResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.SnapshotRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.SnapshotResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.StopMasterRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.StopMasterResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.SwitchThrottleRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.SwitchThrottleResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.TruncateTableRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.TruncateTableResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.UnassignRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.UnassignRegionResponse;
import org.apache.hadoop.hbase.protobuf.generated.ReplicationProtos;
import org.apache.hadoop.hbase.protobuf.generated.ReplicationProtos.AddReplicationPeerRequest;
import org.apache.hadoop.hbase.protobuf.generated.ReplicationProtos.AddReplicationPeerResponse;
import org.apache.hadoop.hbase.protobuf.generated.ReplicationProtos.DisableReplicationPeerRequest;
import org.apache.hadoop.hbase.protobuf.generated.ReplicationProtos.DisableReplicationPeerResponse;
import org.apache.hadoop.hbase.protobuf.generated.ReplicationProtos.EnableReplicationPeerRequest;
import org.apache.hadoop.hbase.protobuf.generated.ReplicationProtos.EnableReplicationPeerResponse;
import org.apache.hadoop.hbase.protobuf.generated.ReplicationProtos.GetReplicationPeerConfigRequest;
import org.apache.hadoop.hbase.protobuf.generated.ReplicationProtos.GetReplicationPeerConfigResponse;
import org.apache.hadoop.hbase.protobuf.generated.ReplicationProtos.ListReplicationPeersRequest;
import org.apache.hadoop.hbase.protobuf.generated.ReplicationProtos.ListReplicationPeersResponse;
import org.apache.hadoop.hbase.protobuf.generated.ReplicationProtos.RemoveReplicationPeerRequest;
import org.apache.hadoop.hbase.protobuf.generated.ReplicationProtos.RemoveReplicationPeerResponse;
import org.apache.hadoop.hbase.protobuf.generated.ReplicationProtos.UpdateReplicationPeerConfigRequest;
import org.apache.hadoop.hbase.protobuf.generated.ReplicationProtos.UpdateReplicationPeerConfigResponse;
import org.apache.hadoop.hbase.quotas.ThrottlingException;
import org.apache.hadoop.hbase.regionserver.RegionServerStoppedException;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CompletedFuture;
import org.apache.hadoop.hbase.util.ExceptionUtil;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.zookeeper.MasterAddressTracker;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.zookeeper.KeeperException;

/** Encapsulates connection to zookeeper and regionservers. */
@edu.umd.cs.findbugs.annotations.SuppressWarnings(
    value = "AT_OPERATION_SEQUENCE_ON_CONCURRENT_ABSTRACTION",
    justification = "Access to the conncurrent hash map is under a lock so should be fine.")
@InterfaceAudience.Private
public class HConnectionImplementation implements HConnection, Closeable {

  private static final Log LOG = LogFactory.getLog(HConnectionImplementation.class);

  static final String REGION_LOCATE_MAX_CONCURRENT = "hbase.client.locate.max.concurrent";

  private static final long keepAlive = 5 * 60 * 1000;

  private final long pause;
  private final int numTries;
  final int rpcTimeout;
  NonceGenerator nonceGenerator = null;
  private final boolean usePrefetch;
  private final int prefetchRegionLimit;

  private volatile boolean closed;
  private volatile boolean aborted;

  // package protected for the tests
  ClusterStatusListener clusterStatusListener;

  private final Object metaRegionLock = new Object();

  private final Object userRegionLock = new Object();

  // We have a single lock for master & zk to prevent deadlocks. Having
  // one lock for ZK and one lock for master is not possible:
  // When creating a connection to master, we need a connection to ZK to get
  // its address. But another thread could have taken the ZK lock, and could
  // be waiting for the master lock => deadlock.
  private final Object masterLock = new Object();

  private final HConnectionImplementation.DelayedClosing delayedClosing =
      DelayedClosing.createAndStart(this);

  // thread executor shared by all HTableInterface instances created
  // by this connection
  private volatile ExecutorService batchPool = null;
  private volatile boolean cleanupPool = false;

  private final Configuration conf;

  // cache the configuration value for tables so that we can avoid calling
  // the expensive Configuration to fetch the value multiple times.
  private final TableConfiguration tableConfig;

  // Client rpc instance.
  private RpcClient rpcClient;

  /**
   * Map of table to table {@link HRegionLocation}s.
   */
  private final ConcurrentMap<TableName, ConcurrentSkipListMap<byte[], HRegionLocation>> cachedRegionLocations =
      new ConcurrentHashMap<TableName, ConcurrentSkipListMap<byte[], HRegionLocation>>();

  // The presence of a server in the map implies it's likely that there is an
  // entry in cachedRegionLocations that map to this server; but the absence
  // of a server in this map guarentees that there is no entry in cache that
  // maps to the absent server.
  // The access to this attribute must be protected by a lock on cachedRegionLocations
  private final Set<ServerName> cachedServers = new ConcurrentSkipListSet<ServerName>();

  // region cache prefetch is enabled by default. this set contains all
  // tables whose region cache prefetch are disabled.
  private final Set<Integer> regionCachePrefetchDisabledTables = new CopyOnWriteArraySet<Integer>();

  private int refCount;

  // indicates whether this connection's life cycle is managed (by us)
  private boolean managed;

  private User user;

  private final RpcRetryingCallerFactory rpcCallerFactory;

  private final RpcControllerFactory rpcControllerFactory;

  // single tracker per connection
  private final ServerStatisticTracker stats;

  private final ClientBackoffPolicy backoffPolicy;

  /**
   * Cluster registry of basic info such as clusterid and meta region location.
   */
  AsyncRegistry registry;

  private final ExecutorService locateMetaExecutor;

  private final ExecutorService[] locateRegionExecutors;

  HConnectionImplementation(Configuration conf, boolean managed) throws IOException {
    this(conf, managed, null, null);
  }

  /**
   * constructor
   * @param conf Configuration object
   * @param managed If true, does not do full shutdown on close; i.e. cleanup of connection to zk
   *          and shutdown of all services; we just close down the resources this connection was
   *          responsible for and decrement usage counters. It is up to the caller to do the full
   *          cleanup. It is set when we want have connection sharing going on -- reuse of zk
   *          connection, and cached region locations, established regionserver connections, etc.
   *          When connections are shared, we have reference counting going on and will only do full
   *          cleanup when no more users of an HConnectionImplementation instance.
   */
  HConnectionImplementation(Configuration conf, boolean managed, ExecutorService pool, User user)
      throws IOException {
    this.conf = conf;
    this.user = user;
    this.batchPool = pool;
    this.managed = managed;

    this.tableConfig = new TableConfiguration(conf);
    this.closed = false;
    this.pause = conf.getLong(HConstants.HBASE_CLIENT_PAUSE, HConstants.DEFAULT_HBASE_CLIENT_PAUSE);
    this.numTries = tableConfig.getRetriesNumber();
    this.rpcTimeout =
        conf.getInt(HConstants.HBASE_RPC_TIMEOUT_KEY, HConstants.DEFAULT_HBASE_RPC_TIMEOUT);
    if (conf.getBoolean(CLIENT_NONCES_ENABLED_KEY, true)) {
      this.nonceGenerator = PerClientRandomNonceGenerator.get();
    } else {
      this.nonceGenerator = ConnectionUtils.NO_NONCE_GENERATOR;
    }

    this.stats = ServerStatisticTracker.create(conf);
    this.usePrefetch =
        conf.getBoolean(HConstants.HBASE_CLIENT_PREFETCH, HConstants.DEFAULT_HBASE_CLIENT_PREFETCH);
    this.prefetchRegionLimit = conf.getInt(HConstants.HBASE_CLIENT_PREFETCH_LIMIT,
      HConstants.DEFAULT_HBASE_CLIENT_PREFETCH_LIMIT);
    this.rpcControllerFactory = RpcControllerFactory.instantiate(conf);
    this.rpcCallerFactory = RpcRetryingCallerFactory.instantiate(conf, this.stats);
    this.backoffPolicy = ClientBackoffPolicyFactory.create(conf);

    this.locateMetaExecutor =
        Executors.newSingleThreadExecutor(Threads.newDaemonThreadFactory("Locate-Meta"));
    int regionLocateMaxConcurrent = Math.max(1, conf.getInt(REGION_LOCATE_MAX_CONCURRENT, 16));
    ThreadFactory locateRegionThreadFactory = Threads.newDaemonThreadFactory("Locate-Region");
    locateRegionExecutors = new ExecutorService[regionLocateMaxConcurrent];
    for (int i = 0; i < regionLocateMaxConcurrent; i++) {
      locateRegionExecutors[i] = Executors.newSingleThreadExecutor(locateRegionThreadFactory);
    }

    // Do we publish the status?
    boolean shouldListen =
        conf.getBoolean(HConstants.STATUS_PUBLISHED, HConstants.STATUS_PUBLISHED_DEFAULT);
    Class<? extends ClusterStatusListener.Listener> listenerClass = conf.getClass(
      ClusterStatusListener.STATUS_LISTENER_CLASS,
      ClusterStatusListener.DEFAULT_STATUS_LISTENER_CLASS, ClusterStatusListener.Listener.class);

    try {
      this.registry = AsyncRegistryFactory.getRegistry(conf);
      retrieveClusterId();
      this.rpcClient = RpcClientFactory.createClient(this.conf, this.clusterId);
      if (shouldListen) {
        if (listenerClass == null) {
          LOG.warn(HConstants.STATUS_PUBLISHED + " is true, but "
              + ClusterStatusListener.STATUS_LISTENER_CLASS + " is not set - not listening status");
        } else {
          clusterStatusListener =
              new ClusterStatusListener(new ClusterStatusListener.DeadServerHandler() {
                @Override
                public void newDead(ServerName sn) {
                  clearCaches(sn);
                  rpcClient.cancelConnections(sn);
                }
              }, conf, listenerClass);
        }
      }

    } catch (Throwable e) {
      // avoid leaks: registry, rpcClient, ...
      LOG.debug("connection construction failed", e);
      close();
      throw e;
    }
  }

  @Override
  public HTableInterface getTable(String tableName) throws IOException {
    return getTable(TableName.valueOf(tableName));
  }

  @Override
  public HTableInterface getTable(byte[] tableName) throws IOException {
    return getTable(TableName.valueOf(tableName));
  }

  @Override
  public HTableInterface getTable(TableName tableName) throws IOException {
    return getTable(tableName, getBatchPool());
  }

  @Override
  public HTableInterface getTable(String tableName, ExecutorService pool) throws IOException {
    return getTable(TableName.valueOf(tableName), pool);
  }

  @Override
  public HTableInterface getTable(byte[] tableName, ExecutorService pool) throws IOException {
    return getTable(TableName.valueOf(tableName), pool);
  }

  @Override
  public HTableInterface getTable(TableName tableName, ExecutorService pool) throws IOException {
    if (managed) {
      throw new IOException("The connection has to be unmanaged.");
    }

    HTableInterface table = new HTable(tableName, this, tableConfig, pool);
    KeySalter salter = SaltedHTable.getKeySalter(table);
    if (salter == null) {
      return table;
    } else {
      return new SaltedHTable(table, salter);
    }
  }

  private ExecutorService getBatchPool() {
    if (batchPool == null) {
      // shared HTable thread executor not yet initialized
      synchronized (this) {
        if (batchPool == null) {
          int maxThreads = conf.getInt("hbase.hconnection.threads.max", 256);
          int coreThreads = conf.getInt("hbase.hconnection.threads.core", 256);
          if (maxThreads == 0) {
            maxThreads = Runtime.getRuntime().availableProcessors() * 8;
          }
          if (coreThreads == 0) {
            coreThreads = Runtime.getRuntime().availableProcessors() * 8;
          }
          long keepAliveTime = conf.getLong("hbase.hconnection.threads.keepalivetime", 60);
          LinkedBlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<Runnable>(
              maxThreads * conf.getInt(HConstants.HBASE_CLIENT_MAX_TOTAL_TASKS,
                HConstants.DEFAULT_HBASE_CLIENT_MAX_TOTAL_TASKS));
          ThreadPoolExecutor tpe = new ThreadPoolExecutor(coreThreads, maxThreads, keepAliveTime,
              TimeUnit.SECONDS, workQueue, Threads.newDaemonThreadFactory(toString() + "-shared-"));
          tpe.allowCoreThreadTimeOut(true);
          this.batchPool = tpe;
        }
        this.cleanupPool = true;
      }
    }
    return this.batchPool;
  }

  protected ExecutorService getCurrentBatchPool() {
    return batchPool;
  }

  /**
   * For tests only.
   * @param rpcClient Client we should use instead.
   * @return Previous rpcClient
   */
  RpcClient setRpcClient(final RpcClient rpcClient) {
    RpcClient oldRpcClient = this.rpcClient;
    this.rpcClient = rpcClient;
    return oldRpcClient;
  }

  /**
   * An identifier that will remain the same for a given connection.
   * @return
   */
  public String toString() {
    return "hconnection-0x" + Integer.toHexString(hashCode());
  }

  protected String clusterId = null;

  void retrieveClusterId() {
    if (clusterId != null) {
      return;
    }
    try {
      this.clusterId = this.registry.getClusterId().get();
    } catch (Exception e) {
      LOG.warn("Retrieve cluster id failed", e);
    }
    if (clusterId == null) {
      clusterId = HConstants.CLUSTER_ID_DEFAULT;
      LOG.debug("clusterid came back null, using default " + clusterId);
    }
  }

  @Override
  public Configuration getConfiguration() {
    return this.conf;
  }

  /**
   * @return true if the master is running, throws an exception otherwise
   * @throws MasterNotRunningException - if the master is not running
   * @throws ZooKeeperConnectionException
   */
  @Override
  public boolean isMasterRunning() throws IOException {
    // When getting the master connection, we check it's running,
    // so if there is no exception, it means we've been able to get a
    // connection on a running master
    MasterKeepAliveConnection m = getKeepAliveMasterService();
    m.close();
    return true;
  }

  @Override
  public HRegionLocation getRegionLocation(final TableName tableName, final byte[] row,
      boolean reload) throws IOException {
    return reload ? relocateRegion(tableName, row) : locateRegion(tableName, row);
  }

  @Override
  public HRegionLocation getRegionLocation(byte[] tableName, byte[] row, boolean reload)
      throws IOException {
    return getRegionLocation(TableName.valueOf(tableName), row, reload);
  }

  private ExecutorService getExecutor(TableName tableName, byte[] row) {
    if (tableName.equals(TableName.META_TABLE_NAME)) {
      return locateMetaExecutor;
    }
    // Use the first two bytes to select executor
    byte[] rowPrefix = row.length < 2 ? row : Arrays.copyOf(row, 2);
    int hash = Math.abs(Hashing.murmur3_32().newHasher().putBytes(tableName.getName())
        .putBytes(rowPrefix).hash().asInt());
    // if hash is Integer.MIN_VALUE then abs is still less than zero
    int index = hash < 0 ? 0 : hash % locateRegionExecutors.length;
    return locateRegionExecutors[index];
  }

  @Override
  public Future<HRegionLocation> getRegionLocationAsync(TableName tableName, byte[] row,
      boolean reload) {
    ExecutorService executor = getExecutor(tableName, row);
    if (reload) {
      return executor.submit(new LocateRegionCallable(tableName, row) {

        @Override
        public HRegionLocation call() throws Exception {
          return relocateRegion(tableName, row);
        }
      });
    }
    HRegionLocation cachedLoc = getCachedLocation(tableName, row);
    if (cachedLoc != null) {
      return new CompletedFuture<HRegionLocation>(cachedLoc);
    } else {
      return executor.submit(new LocateRegionCallable(tableName, row) {

        @Override
        public HRegionLocation call() throws Exception {
          return locateRegion(tableName, row);
        }

      });
    }
  }

  private static <T> T get(CompletableFuture<T> future) throws IOException {
    try {
      return future.get();
    } catch (InterruptedException e) {
      throw (IOException) new InterruptedIOException().initCause(e);
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      Throwables.propagateIfPossible(cause, IOException.class);
      throw new IOException(cause);
    }
  }

  private TableState getTableState(TableName tableName) throws IOException {
    TableState state = MetaReader.getTableState(this, tableName);
    // assume enabled if not exists
    return state != null ? state : new TableState(tableName, TableState.State.ENABLED);
  }

  @Override
  public boolean isTableEnabled(TableName tableName) throws IOException {
    return getTableState(tableName).inStates(TableState.State.ENABLED);
  }

  @Override
  public boolean isTableEnabled(byte[] tableName) throws IOException {
    return isTableEnabled(TableName.valueOf(tableName));
  }

  @Override
  public boolean isTableDisabled(TableName tableName) throws IOException {
    return getTableState(tableName).inStates(TableState.State.DISABLED);
  }

  @Override
  public boolean isTableDisabled(byte[] tableName) throws IOException {
    return isTableDisabled(TableName.valueOf(tableName));
  }

  @Override
  public boolean isTableAvailable(final TableName tableName) throws IOException {
    final AtomicBoolean available = new AtomicBoolean(true);
    final AtomicInteger regionCount = new AtomicInteger(0);
    MetaScannerVisitor visitor = new MetaScannerVisitorBase() {
      @Override
      public boolean processRow(Result row) throws IOException {
        HRegionInfo info = MetaScanner.getHRegionInfo(row);
        if (info != null && !info.isSplitParent()) {
          if (tableName.equals(info.getTable())) {
            ServerName server = HRegionInfo.getServerName(row);
            if (server == null) {
              available.set(false);
              return false;
            }
            regionCount.incrementAndGet();
          } else if (tableName.compareTo(info.getTable()) < 0) {
            // Return if we are done with the current table
            return false;
          }
        }
        return true;
      }
    };
    MetaScanner.metaScan(conf, this, visitor, tableName);
    return available.get() && (regionCount.get() > 0);
  }

  @Override
  public boolean isTableAvailable(final byte[] tableName) throws IOException {
    return isTableAvailable(TableName.valueOf(tableName));
  }

  @Override
  public boolean isTableAvailable(final TableName tableName, final byte[][] splitKeys)
      throws IOException {
    final AtomicBoolean available = new AtomicBoolean(true);
    final AtomicInteger regionCount = new AtomicInteger(0);
    MetaScannerVisitor visitor = new MetaScannerVisitorBase() {
      @Override
      public boolean processRow(Result row) throws IOException {
        HRegionInfo info = MetaScanner.getHRegionInfo(row);
        if (info != null && !info.isSplitParent()) {
          if (tableName.equals(info.getTable())) {
            ServerName server = HRegionInfo.getServerName(row);
            if (server == null) {
              available.set(false);
              return false;
            }
            if (!Bytes.equals(info.getStartKey(), HConstants.EMPTY_BYTE_ARRAY)) {
              for (byte[] splitKey : splitKeys) {
                // Just check if the splitkey is available
                if (Bytes.equals(info.getStartKey(), splitKey)) {
                  regionCount.incrementAndGet();
                  break;
                }
              }
            } else {
              // Always empty start row should be counted
              regionCount.incrementAndGet();
            }
          } else if (tableName.compareTo(info.getTable()) < 0) {
            // Return if we are done with the current table
            return false;
          }
        }
        return true;
      }
    };
    MetaScanner.metaScan(conf, this, visitor, tableName);
    // +1 needs to be added so that the empty start row is also taken into account
    return available.get() && (regionCount.get() == splitKeys.length + 1);
  }

  @Override
  public boolean isTableAvailable(final byte[] tableName, final byte[][] splitKeys)
      throws IOException {
    return isTableAvailable(TableName.valueOf(tableName), splitKeys);
  }

  @Override
  public HRegionLocation locateRegion(final byte[] regionName) throws IOException {
    return locateRegion(HRegionInfo.getTable(regionName), HRegionInfo.getStartKey(regionName),
      false, true);
  }

  @Override
  public boolean isDeadServer(ServerName sn) {
    if (clusterStatusListener == null) {
      return false;
    } else {
      return clusterStatusListener.isDeadServer(sn);
    }
  }

  @Override
  public List<HRegionLocation> locateRegions(final TableName tableName) throws IOException {
    return locateRegions(tableName, false, true);
  }

  @Override
  public List<HRegionLocation> locateRegions(final byte[] tableName) throws IOException {
    return locateRegions(TableName.valueOf(tableName));
  }

  @Override
  public List<HRegionLocation> locateRegions(final TableName tableName, final boolean useCache,
      final boolean offlined) throws IOException {
    if (TableName.META_TABLE_NAME.equals(tableName)) {
      return Collections.singletonList(get(registry.getMetaRegionLocation()));
    } else {
      NavigableMap<HRegionInfo, ServerName> regions =
        MetaScanner.allTableRegions(conf, this, tableName, offlined);
      final List<HRegionLocation> locations = new ArrayList<HRegionLocation>();
      for (HRegionInfo regionInfo : regions.keySet()) {
        locations.add(locateRegion(tableName, regionInfo.getStartKey(), useCache, true));
      }
      return locations;
    }
  }

  @Override
  public List<HRegionLocation> locateRegions(final byte[] tableName, final boolean useCache,
      final boolean offlined) throws IOException {
    return locateRegions(TableName.valueOf(tableName), useCache, offlined);
  }

  @Override
  public HRegionLocation locateRegion(final TableName tableName, final byte[] row)
      throws IOException {
    return locateRegion(tableName, row, true, true);
  }

  @Override
  public HRegionLocation locateRegion(final byte[] tableName, final byte[] row) throws IOException {
    return locateRegion(TableName.valueOf(tableName), row);
  }

  @Override
  public HRegionLocation relocateRegion(final TableName tableName, final byte[] row)
      throws IOException {
    // Since this is an explicit request not to use any caching, finding
    // disabled tables should not be desirable. This will ensure that an exception is thrown when
    // the first time a disabled table is interacted with.
    if (isTableDisabled(tableName)) {
      throw new TableNotEnabledException(tableName.getNameAsString() + " is disabled.");
    }

    return locateRegion(tableName, row, false, true);
  }

  @Override
  public HRegionLocation relocateRegion(byte[] tableName, byte[] row) throws IOException {
    return relocateRegion(TableName.valueOf(tableName), row);
  }

  private HRegionLocation locateRegion(final TableName tableName, final byte[] row,
      boolean useCache, boolean retry) throws IOException {
    if (this.closed) {
      throw new IOException(toString() + " closed");
    }
    if (tableName == null || tableName.getName().length == 0) {
      throw new IllegalArgumentException("table name cannot be null or zero length");
    }

    if (tableName.equals(TableName.META_TABLE_NAME)) {
      return locateMeta(useCache);
    } else {
      // Region not in the cache - have to go to the meta RS
      return locateRegionInMeta(tableName, row, useCache, userRegionLock, retry);
    }
  }

  /*
   * Search hbase:meta for the HRegionLocation info that contains the table and row we're seeking.
   * It will prefetch certain number of regions info and save them to the global region cache.
   */
  private void prefetchRegionCache(final TableName tableName, final byte[] row) {
    // Implement a new visitor for MetaScanner, and use it to walk through
    // the hbase:meta
    MetaScannerVisitor visitor = new MetaScannerVisitorBase() {
      public boolean processRow(Result result) throws IOException {
        try {
          HRegionInfo regionInfo = MetaScanner.getHRegionInfo(result);
          if (regionInfo == null) {
            return true;
          }

          // possible we got a region of a different table...
          if (!regionInfo.getTable().equals(tableName)) {
            return false; // stop scanning
          }
          if (regionInfo.isOffline()) {
            // don't cache offline regions
            return true;
          }

          ServerName serverName = HRegionInfo.getServerName(result);
          if (serverName == null) {
            return true; // don't cache it
          }
          // instantiate the location
          long seqNum = HRegionInfo.getSeqNumDuringOpen(result);
          HRegionLocation loc = new HRegionLocation(regionInfo, serverName, seqNum);
          // cache this meta entry
          cacheLocation(tableName, null, loc);
          return true;
        } catch (RuntimeException e) {
          throw new IOException(e);
        }
      }
    };
    try {
      // pre-fetch certain number of regions info at region cache.
      long startTime = System.currentTimeMillis();
      MetaScanner.metaScan(conf, this, visitor, tableName, row, this.prefetchRegionLimit,
        TableName.META_TABLE_NAME);
      LOG.info("Prefetch for table=" + tableName + ", row=" + Bytes.toString(row)
          + ", prefetchRegionLimit=" + this.prefetchRegionLimit + ", time consume="
          + (System.currentTimeMillis() - startTime));
    } catch (IOException e) {
      LOG.warn("Encountered problems when prefetch META table: ", e);
      if (ExceptionUtil.isInterrupt(e)) {
        Thread.currentThread().interrupt();
      }
    }
  }

  private HRegionLocation locateMeta(boolean useCache) throws IOException {
    // HBASE-10785: We cache the location of the META itself, so that we are not overloading
    // zookeeper with one request for every region lookup. We cache the META with empty row
    // key in MetaCache.
    byte[] metaCacheKey = HConstants.EMPTY_START_ROW; // use byte[0] as the row for meta
    HRegionLocation location;
    if (useCache) {
      location = getCachedLocation(TableName.META_TABLE_NAME, metaCacheKey);
      if (location != null) {
        return location;
      }
    }
    // only one thread should do the lookup.
    synchronized (metaRegionLock) {
      // Check the cache again for a hit in case some other thread made the
      // same query while we were waiting on the lock.
      if (useCache) {
        location = getCachedLocation(TableName.META_TABLE_NAME, metaCacheKey);
        if (location != null) {
          return location;
        }
      }
      // Look up from zookeeper
      location = get(this.registry.getMetaRegionLocation());
      if (location != null) {
        cacheLocation(TableName.META_TABLE_NAME, null, location);
      }
    }
    return location;
  }

  /*
   * Search the hbase:meta table for the HRegionLocation info that contains the table and row we're
   * seeking.
   */
  private HRegionLocation locateRegionInMeta(final TableName tableName, final byte[] row,
      boolean useCache, Object regionLockObject, boolean retry) throws IOException {
    HRegionLocation location;
    // If we are supposed to be using the cache, look in the cache to see if
    // we already have the region.
    if (useCache) {
      location = getCachedLocation(tableName, row);
      if (location != null) {
        return location;
      }
    }

    Throwable lastCause = null;
    int maxAttempts = retry ? numTries : 1;
    // build the key of the meta region we should be looking for.
    // the extra 9's on the end are necessary to allow "exact" matches
    // without knowing the precise region names.
    byte[] metaKey = HRegionInfo.createRegionName(tableName, row, HConstants.NINES, false);
    Scan s = new Scan();
    s.setReversed(true);
    s.withStartRow(metaKey);
    s.addFamily(HConstants.CATALOG_FAMILY);
    s.setOneRowLimit();
    for (int tries = 0;; tries++) {
      if (tries >= maxAttempts) {
        throw new NoServerForRegionException("Unable to find region for "
            + Bytes.toStringBinary(row) + " after " + tries + " tries.", lastCause);
      }

      // This block guards against two threads trying to load the meta
      // region at the same time. The first will load the meta region and
      // the second will use the value that the first one found.
      if (useCache) {
        if (usePrefetch && getRegionCachePrefetch(tableName)) {
          synchronized (regionLockObject) {
            // Check the cache again for a hit in case some other thread made the
            // same query while we were waiting on the lock.
            location = getCachedLocation(tableName, row);
            if (location != null) {
              return location;
            }
            // If the parent table is META, we may want to pre-fetch some
            // region info into the global region cache for this table.
            prefetchRegionCache(tableName, row);
          }
        }
        location = getCachedLocation(tableName, row);
        if (location != null) {
          return location;
        }
      } else {
        // If we are not supposed to be using the cache, delete any existing cached location
        // so it won't interfere.
        forceDeleteCachedLocation(tableName, row);
      }
      try {
        Result regionInfoRow = null;
        s.resetMvccReadPoint();
        try (ReversedClientScanner rcs =
            new ReversedClientScanner(conf, s, TableName.META_TABLE_NAME, this)) {
          regionInfoRow = rcs.next();
        }
        if (regionInfoRow == null) {
          throw new TableNotFoundException(tableName);
        }
        // convert the row result into the HRegionLocation we need!
        HRegionInfo regionInfo = MetaScanner.getHRegionInfo(regionInfoRow);
        if (regionInfo == null) {
          throw new IOException("HRegionInfo was null or empty in " + TableName.META_TABLE_NAME
              + ", row=" + regionInfoRow);
        }

        // possible we got a region of a different table...
        if (!regionInfo.getTable().equals(tableName)) {
          throw new TableNotFoundException(
              "Table '" + tableName + "' was not found, got: " + regionInfo.getTable() + ".");
        }
        if (regionInfo.isSplit()) {
          throw new RegionOfflineException(
              "the only available region for" + " the required row is a split parent,"
                  + " the daughters should be online soon: " + regionInfo.getRegionNameAsString());
        }
        if (regionInfo.isOffline()) {
          throw new RegionOfflineException("the region is offline, could"
              + " be caused by a disable table call: " + regionInfo.getRegionNameAsString());
        }

        ServerName serverName = HRegionInfo.getServerName(regionInfoRow);
        if (serverName == null) {
          throw new NoServerForRegionException("No server address listed " + "in "
              + TableName.META_TABLE_NAME + " for region " + regionInfo.getRegionNameAsString()
              + " containing row " + Bytes.toStringBinary(row));
        }

        if (isDeadServer(serverName)) {
          throw new RegionServerStoppedException(
              "hbase:meta says the region " + regionInfo.getRegionNameAsString()
                  + " is managed by the server " + serverName + ", but it is dead.");
        }

        // Instantiate the location
        location = new HRegionLocation(regionInfo, serverName,
            HRegionInfo.getSeqNumDuringOpen(regionInfoRow));
        cacheLocation(tableName, null, location);
        return location;
      } catch (TableNotFoundException e) {
        // if we got this error, probably means the table just plain doesn't
        // exist. rethrow the error immediately. this should always be coming
        // from the HTable constructor.
        throw e;
      } catch (IOException e) {
        lastCause = e;
        ExceptionUtil.rethrowIfInterrupt(e);

        if (e instanceof RemoteException) {
          e = ((RemoteException) e).unwrapRemoteException();
        }
        if (tries < maxAttempts - 1) {
          LOG.warn("locateRegionInMeta in " + TableName.META_TABLE_NAME + ", attempt=" + tries
              + " of " + maxAttempts + " failed; retrying after sleep of "
              + ConnectionUtils.getPauseTime(this.pause, tries),
            e);
        } else {
          throw e;
        }
        // Only relocate the parent region if necessary
        if (!(e instanceof RegionOfflineException || e instanceof NoServerForRegionException)) {
          relocateRegion(TableName.META_TABLE_NAME, metaKey);
        }
      }
      try {
        Thread.sleep(ConnectionUtils.getPauseTime(this.pause, tries));
      } catch (InterruptedException e) {
        throw new InterruptedIOException(
            "Giving up trying to location region in " + "meta: thread is interrupted.");
      }
    }
  }

  /*
   * Search the cache for a location that fits our table and row key. Return null if no suitable
   * region is located.
   * @param tableName
   * @param row
   * @return Null or region location found in cache.
   */
  HRegionLocation getCachedLocation(final TableName tableName, final byte[] row) {
    ConcurrentSkipListMap<byte[], HRegionLocation> tableLocations = getTableLocations(tableName);

    Entry<byte[], HRegionLocation> e = tableLocations.floorEntry(row);
    if (e == null) {
      return null;
    }
    HRegionLocation possibleRegion = e.getValue();

    // make sure that the end key is greater than the row we're looking
    // for, otherwise the row actually belongs in the next region, not
    // this one. the exception case is when the endkey is
    // HConstants.EMPTY_END_ROW, signifying that the region we're
    // checking is actually the last region in the table.
    byte[] endKey = possibleRegion.getRegionInfo().getEndKey();
    if (Bytes.equals(endKey, HConstants.EMPTY_END_ROW) || tableName.getRowComparator()
        .compareRows(endKey, 0, endKey.length, row, 0, row.length) > 0) {
      return possibleRegion;
    }

    // Passed all the way through, so we got nothing - complete cache miss
    return null;
  }

  /**
   * Delete a cached location, no matter what it is. Called when we were told to not use cache.
   * @param tableName tableName
   * @param row
   */
  void forceDeleteCachedLocation(final TableName tableName, final byte[] row) {
    HRegionLocation rl = null;
    Map<byte[], HRegionLocation> tableLocations = getTableLocations(tableName);
    // start to examine the cache. we can only do cache actions
    // if there's something in the cache for this table.
    rl = getCachedLocation(tableName, row);
    if (rl != null) {
      tableLocations.remove(rl.getRegionInfo().getStartKey());
    }
    if ((rl != null) && LOG.isDebugEnabled()) {
      LOG.debug("Removed " + rl.getHostname() + ":" + rl.getPort() + " as a location of "
          + rl.getRegionInfo().getRegionNameAsString() + " for tableName=" + tableName
          + " from cache");
    }
  }

  /*
   * Delete all cached entries of a table that maps to a specific location.
   */
  @Override
  public void clearCaches(final ServerName serverName) {
    if (!this.cachedServers.contains(serverName)) {
      return;
    }

    boolean deletedSomething = false;
    synchronized (this.cachedServers) {
      // We block here, because if there is an error on a server, it's likely that multiple
      // threads will get the error simultaneously. If there are hundreds of thousand of
      // region location to check, it's better to do this only once. A better pattern would
      // be to check if the server is dead when we get the region location.
      if (!this.cachedServers.contains(serverName)) {
        return;
      }
      for (Map<byte[], HRegionLocation> tableLocations : cachedRegionLocations.values()) {
        for (Entry<byte[], HRegionLocation> e : tableLocations.entrySet()) {
          HRegionLocation value = e.getValue();
          if (value != null && serverName.equals(value.getServerName())) {
            tableLocations.remove(e.getKey());
            deletedSomething = true;
          }
        }
      }
      this.cachedServers.remove(serverName);
    }
    if (deletedSomething && LOG.isDebugEnabled()) {
      LOG.debug("Removed all cached region locations that map to " + serverName);
    }
  }

  /*
   * @param tableName
   * @return Map of cached locations for passed <code>tableName</code>
   */
  private ConcurrentSkipListMap<byte[], HRegionLocation> getTableLocations(
      final TableName tableName) {
    // find the map of cached locations for this table
    ConcurrentSkipListMap<byte[], HRegionLocation> result;
    result = this.cachedRegionLocations.get(tableName);
    // if tableLocations for this table isn't built yet, make one
    if (result == null) {
      result = new ConcurrentSkipListMap<byte[], HRegionLocation>(Bytes.BYTES_COMPARATOR);
      ConcurrentSkipListMap<byte[], HRegionLocation> old =
          this.cachedRegionLocations.putIfAbsent(tableName, result);
      if (old != null) {
        return old;
      }
    }
    return result;
  }

  @Override
  public void clearRegionCache() {
    this.cachedRegionLocations.clear();
    this.cachedServers.clear();
  }

  @Override
  public void clearRegionCache(final TableName tableName) {
    this.cachedRegionLocations.remove(tableName);
  }

  @Override
  public void clearRegionCache(final byte[] tableName) {
    clearRegionCache(TableName.valueOf(tableName));
  }

  /**
   * Put a newly discovered HRegionLocation into the cache.
   * @param tableName The table name.
   * @param source the source of the new location, if it's not coming from meta
   * @param location the new location
   */
  private void cacheLocation(final TableName tableName, final HRegionLocation source,
      final HRegionLocation location) {
    boolean isFromMeta = (source == null);
    byte[] startKey = location.getRegionInfo().getStartKey();
    ConcurrentMap<byte[], HRegionLocation> tableLocations = getTableLocations(tableName);
    HRegionLocation oldLocation = tableLocations.putIfAbsent(startKey, location);
    boolean isNewCacheEntry = (oldLocation == null);
    if (isNewCacheEntry) {
      cachedServers.add(location.getServerName());
      return;
    }
    boolean updateCache;
    // If the server in cache sends us a redirect, assume it's always valid.
    if (oldLocation.equals(source)) {
      updateCache = true;
    } else {
      long newLocationSeqNum = location.getSeqNum();
      // Meta record is stale - some (probably the same) server has closed the region
      // with later seqNum and told us about the new location.
      boolean isStaleMetaRecord = isFromMeta && (oldLocation.getSeqNum() > newLocationSeqNum);
      // Same as above for redirect. However, in this case, if the number is equal to previous
      // record, the most common case is that first the region was closed with seqNum, and then
      // opened with the same seqNum; hence we will ignore the redirect.
      // There are so many corner cases with various combinations of opens and closes that
      // an additional counter on top of seqNum would be necessary to handle them all.
      boolean isStaleRedirect = !isFromMeta && (oldLocation.getSeqNum() >= newLocationSeqNum);
      boolean isStaleUpdate = (isStaleMetaRecord || isStaleRedirect);
      updateCache = (!isStaleUpdate);
    }
    if (updateCache) {
      tableLocations.replace(startKey, oldLocation, location);
      cachedServers.add(location.getServerName());
    }
  }

  // Map keyed by service name + regionserver to service stub implementation
  private final ConcurrentHashMap<String, Object> stubs = new ConcurrentHashMap<String, Object>();
  // Map of locks used creating service stubs per regionserver.
  private final ConcurrentHashMap<String, String> connectionLock =
      new ConcurrentHashMap<String, String>();

  /**
   * State of the MasterService connection/setup.
   */
  static class MasterServiceState {
    HConnection connection;
    MasterService.BlockingInterface stub;
    int userCount;
    long keepAliveUntil = Long.MAX_VALUE;

    MasterServiceState(final HConnection connection) {
      super();
      this.connection = connection;
    }

    @Override
    public String toString() {
      return "MasterService";
    }

    Object getStub() {
      return this.stub;
    }

    void clearStub() {
      this.stub = null;
    }

    boolean isMasterRunning() throws ServiceException {
      IsMasterRunningResponse response =
          this.stub.isMasterRunning(null, RequestConverter.buildIsMasterRunningRequest());
      return response != null ? response.getIsMasterRunning() : false;
    }
  }

  /**
   * Makes a client-side stub for master services. Sub-class to specialize. Depends on hosting class
   * so not static. Exists so we avoid duplicating a bunch of code when setting up the
   * MasterMonitorService and MasterAdminService.
   */
  abstract class StubMaker {
    /**
     * Returns the name of the service stub being created.
     */
    protected abstract String getServiceName();

    /**
     * Make stub and cache it internal so can be used later doing the isMasterRunning call.
     * @param channel
     */
    protected abstract Object makeStub(final BlockingRpcChannel channel);

    /**
     * Once setup, check it works by doing isMasterRunning check.
     * @throws ServiceException
     */
    protected abstract void isMasterRunning() throws IOException;

    /**
     * Create a stub. Try once only. It is not typed because there is no common type to protobuf
     * services nor their interfaces. Let the caller do appropriate casting.
     * @return A stub for master services.
     * @throws IOException
     * @throws KeeperException
     * @throws ServiceException
     */
    private Object makeStubNoRetries() throws IOException, KeeperException {
      ServerName sn = get(registry.getMasterAddress());
      if (sn == null) {
        String msg = "ZooKeeper available but no active master location found";
        LOG.info(msg);
        throw new MasterNotRunningException(msg);
      }
      if (isDeadServer(sn)) {
        throw new MasterNotRunningException(sn + " is dead.");
      }
      // Use the security info interface name as our stub key
      String key = getStubKey(getServiceName(), sn.getHostAndPort());
      connectionLock.putIfAbsent(key, key);
      Object stub = null;
      synchronized (connectionLock.get(key)) {
        stub = stubs.get(key);
        if (stub == null) {
          BlockingRpcChannel channel = rpcClient.createBlockingRpcChannel(sn, user, rpcTimeout);
          stub = makeStub(channel);
          isMasterRunning();
          stubs.put(key, stub);
        }
      }
      return stub;
    }

    /**
     * Create a stub against the master. Retry if necessary.
     * @return A stub to do <code>intf</code> against the master
     * @throws MasterNotRunningException
     */
    Object makeStub() throws IOException {
      // The lock must be at the beginning to prevent multiple master creations
      // (and leaks) in a multithread context
      synchronized (masterLock) {
        Exception exceptionCaught = null;
        if (!closed) {
          try {
            return makeStubNoRetries();
          } catch (IOException e) {
            exceptionCaught = e;
          } catch (KeeperException e) {
            exceptionCaught = e;
          }
          throw new MasterNotRunningException(exceptionCaught);
        } else {
          throw new DoNotRetryIOException("Connection was closed while trying to get master");
        }
      }
    }
  }

  /**
   * Class to make a MasterServiceStubMaker stub.
   */
  class MasterServiceStubMaker extends StubMaker {
    private MasterService.BlockingInterface stub;

    @Override
    protected String getServiceName() {
      return MasterService.getDescriptor().getName();
    }

    @Override
    MasterService.BlockingInterface makeStub() throws IOException {
      return (MasterService.BlockingInterface) super.makeStub();
    }

    @Override
    protected Object makeStub(BlockingRpcChannel channel) {
      this.stub = MasterService.newBlockingStub(channel);
      return this.stub;
    }

    @Override
    protected void isMasterRunning() throws IOException {
      try {
        this.stub.isMasterRunning(null, RequestConverter.buildIsMasterRunningRequest());
      } catch (ServiceException e) {
        ProtobufUtil.getRemoteException(e);
      }
    }
  }

  @Override
  public AdminService.BlockingInterface getAdmin(final ServerName serverName) throws IOException {
    return getAdmin(serverName, false);
  }

  @Override
  // Nothing is done w/ the 'master' parameter. It is ignored.
  public AdminService.BlockingInterface getAdmin(final ServerName serverName, final boolean master)
      throws IOException {
    if (isDeadServer(serverName)) {
      throw new RegionServerStoppedException(serverName + " is dead.");
    }
    String key =
        getStubKey(AdminService.BlockingInterface.class.getName(), serverName.getHostAndPort());
    this.connectionLock.putIfAbsent(key, key);
    AdminService.BlockingInterface stub = null;
    synchronized (this.connectionLock.get(key)) {
      stub = (AdminService.BlockingInterface) this.stubs.get(key);
      if (stub == null) {
        BlockingRpcChannel channel =
            this.rpcClient.createBlockingRpcChannel(serverName, user, this.rpcTimeout);
        stub = AdminService.newBlockingStub(channel);
        this.stubs.put(key, stub);
      }
    }
    return stub;
  }

  @Override
  public ClientService.BlockingInterface getClient(final ServerName sn) throws IOException {
    if (isDeadServer(sn)) {
      throw new RegionServerStoppedException(sn + " is dead.");
    }
    String key = getStubKey(ClientService.BlockingInterface.class.getName(), sn.getHostAndPort());
    this.connectionLock.putIfAbsent(key, key);
    ClientService.BlockingInterface stub = null;
    synchronized (this.connectionLock.get(key)) {
      stub = (ClientService.BlockingInterface) this.stubs.get(key);
      if (stub == null) {
        long startTime = System.currentTimeMillis();
        BlockingRpcChannel channel =
            this.rpcClient.createBlockingRpcChannel(sn, user, this.rpcTimeout);
        stub = ClientService.newBlockingStub(channel);
        LOG.info("create stub region server:" + sn + ", rpcTimeout=" + this.rpcTimeout
            + ", time consume=" + (System.currentTimeMillis() - startTime));
        // In old days, after getting stub/proxy, we'd make a call. We are not doing that here.
        // Just fail on first actual call rather than in here on setup.
        this.stubs.put(key, stub);
      }
    }
    return stub;
  }

  static String getStubKey(final String serviceName, final String rsHostnamePort) {
    return serviceName + "@" + rsHostnamePort;
  }

  /**
   * Creates a Chore thread to check the connections to master & zookeeper and close them when they
   * reach their closing time ( {@link MasterServiceState#keepAliveUntil} and
   * {@link #keepZooKeeperWatcherAliveUntil}). Keep alive time is managed by the release functions
   * and the variable {@link #keepAlive}
   */
  private static class DelayedClosing extends Chore implements Stoppable {
    private HConnectionImplementation hci;
    Stoppable stoppable;

    private DelayedClosing(HConnectionImplementation hci, Stoppable stoppable) {
      super("ZooKeeperWatcher and Master delayed closing for connection " + hci, 60 * 1000, // We
                                                                                            // check
                                                                                            // every
                                                                                            // minutes
          stoppable);
      this.hci = hci;
      this.stoppable = stoppable;
    }

    static HConnectionImplementation.DelayedClosing createAndStart(HConnectionImplementation hci) {
      Stoppable stoppable = new Stoppable() {
        private volatile boolean isStopped = false;

        @Override
        public void stop(String why) {
          isStopped = true;
        }

        @Override
        public boolean isStopped() {
          return isStopped;
        }
      };

      return new DelayedClosing(hci, stoppable);
    }

    protected void closeMasterProtocol(HConnectionImplementation.MasterServiceState protocolState) {
      if (System.currentTimeMillis() > protocolState.keepAliveUntil) {
        hci.closeMasterService(protocolState);
        protocolState.keepAliveUntil = Long.MAX_VALUE;
      }
    }

    @Override
    protected void chore() {
      synchronized (hci.masterLock) {
        closeMasterProtocol(hci.masterServiceState);
        closeMasterProtocol(hci.masterServiceState);
      }
    }

    @Override
    public void stop(String why) {
      stoppable.stop(why);
    }

    @Override
    public boolean isStopped() {
      return stoppable.isStopped();
    }
  }

  final HConnectionImplementation.MasterServiceState masterServiceState =
      new MasterServiceState(this);

  @Override
  public MasterService.BlockingInterface getMaster() throws IOException {
    return getKeepAliveMasterService();
  }

  private void resetMasterServiceState(final HConnectionImplementation.MasterServiceState mss) {
    mss.userCount++;
    mss.keepAliveUntil = Long.MAX_VALUE;
  }

  @Override
  public MasterKeepAliveConnection getKeepAliveMasterService() throws IOException {
    synchronized (masterLock) {
      if (!isKeepAliveMasterConnectedAndRunning(this.masterServiceState)) {
        MasterServiceStubMaker stubMaker = new MasterServiceStubMaker();
        this.masterServiceState.stub = stubMaker.makeStub();
      }
      resetMasterServiceState(this.masterServiceState);
    }
    // Ugly delegation just so we can add in a Close method.
    final MasterService.BlockingInterface stub = this.masterServiceState.stub;
    return new MasterKeepAliveConnection() {
      HConnectionImplementation.MasterServiceState mss = masterServiceState;

      @Override
      public AddColumnResponse addColumn(RpcController controller, AddColumnRequest request)
          throws ServiceException {
        return stub.addColumn(controller, request);
      }

      @Override
      public DeleteColumnResponse deleteColumn(RpcController controller,
          DeleteColumnRequest request) throws ServiceException {
        return stub.deleteColumn(controller, request);
      }

      @Override
      public ModifyColumnResponse modifyColumn(RpcController controller,
          ModifyColumnRequest request) throws ServiceException {
        return stub.modifyColumn(controller, request);
      }

      @Override
      public MoveRegionResponse moveRegion(RpcController controller, MoveRegionRequest request)
          throws ServiceException {
        return stub.moveRegion(controller, request);
      }

      @Override
      public DispatchMergingRegionsResponse dispatchMergingRegions(RpcController controller,
          DispatchMergingRegionsRequest request) throws ServiceException {
        return stub.dispatchMergingRegions(controller, request);
      }

      @Override
      public AssignRegionResponse assignRegion(RpcController controller,
          AssignRegionRequest request) throws ServiceException {
        return stub.assignRegion(controller, request);
      }

      @Override
      public UnassignRegionResponse unassignRegion(RpcController controller,
          UnassignRegionRequest request) throws ServiceException {
        return stub.unassignRegion(controller, request);
      }

      @Override
      public OfflineRegionResponse offlineRegion(RpcController controller,
          OfflineRegionRequest request) throws ServiceException {
        return stub.offlineRegion(controller, request);
      }

      @Override
      public DeleteTableResponse deleteTable(RpcController controller, DeleteTableRequest request)
          throws ServiceException {
        return stub.deleteTable(controller, request);
      }

      @Override
      public EnableTableResponse enableTable(RpcController controller, EnableTableRequest request)
          throws ServiceException {
        return stub.enableTable(controller, request);
      }

      @Override
      public DisableTableResponse disableTable(RpcController controller,
          DisableTableRequest request) throws ServiceException {
        return stub.disableTable(controller, request);
      }

      @Override
      public ModifyTableResponse modifyTable(RpcController controller, ModifyTableRequest request)
          throws ServiceException {
        return stub.modifyTable(controller, request);
      }

      @Override
      public CreateTableResponse createTable(RpcController controller, CreateTableRequest request)
          throws ServiceException {
        return stub.createTable(controller, request);
      }

      @Override
      public ShutdownResponse shutdown(RpcController controller, ShutdownRequest request)
          throws ServiceException {
        return stub.shutdown(controller, request);
      }

      @Override
      public StopMasterResponse stopMaster(RpcController controller, StopMasterRequest request)
          throws ServiceException {
        return stub.stopMaster(controller, request);
      }

      @Override
      public BalanceResponse balance(RpcController controller, BalanceRequest request)
          throws ServiceException {
        return stub.balance(controller, request);
      }

      @Override
      public SetBalancerRunningResponse setBalancerRunning(RpcController controller,
          SetBalancerRunningRequest request) throws ServiceException {
        return stub.setBalancerRunning(controller, request);
      }

      @Override
      public RunCatalogScanResponse runCatalogScan(RpcController controller,
          RunCatalogScanRequest request) throws ServiceException {
        return stub.runCatalogScan(controller, request);
      }

      @Override
      public EnableCatalogJanitorResponse enableCatalogJanitor(RpcController controller,
          EnableCatalogJanitorRequest request) throws ServiceException {
        return stub.enableCatalogJanitor(controller, request);
      }

      @Override
      public IsCatalogJanitorEnabledResponse isCatalogJanitorEnabled(RpcController controller,
          IsCatalogJanitorEnabledRequest request) throws ServiceException {
        return stub.isCatalogJanitorEnabled(controller, request);
      }

      @Override
      public CoprocessorServiceResponse execMasterService(RpcController controller,
          CoprocessorServiceRequest request) throws ServiceException {
        return stub.execMasterService(controller, request);
      }

      @Override
      public SnapshotResponse snapshot(RpcController controller, SnapshotRequest request)
          throws ServiceException {
        return stub.snapshot(controller, request);
      }

      @Override
      public GetCompletedSnapshotsResponse getCompletedSnapshots(RpcController controller,
          GetCompletedSnapshotsRequest request) throws ServiceException {
        return stub.getCompletedSnapshots(controller, request);
      }

      @Override
      public DeleteSnapshotResponse deleteSnapshot(RpcController controller,
          DeleteSnapshotRequest request) throws ServiceException {
        return stub.deleteSnapshot(controller, request);
      }

      @Override
      public IsSnapshotDoneResponse isSnapshotDone(RpcController controller,
          IsSnapshotDoneRequest request) throws ServiceException {
        return stub.isSnapshotDone(controller, request);
      }

      @Override
      public RestoreSnapshotResponse restoreSnapshot(RpcController controller,
          RestoreSnapshotRequest request) throws ServiceException {
        return stub.restoreSnapshot(controller, request);
      }

      @Override
      public IsRestoreSnapshotDoneResponse isRestoreSnapshotDone(RpcController controller,
          IsRestoreSnapshotDoneRequest request) throws ServiceException {
        return stub.isRestoreSnapshotDone(controller, request);
      }

      @Override
      public ExecProcedureResponse execProcedure(RpcController controller,
          ExecProcedureRequest request) throws ServiceException {
        return stub.execProcedure(controller, request);
      }

      @Override
      public IsProcedureDoneResponse isProcedureDone(RpcController controller,
          IsProcedureDoneRequest request) throws ServiceException {
        return stub.isProcedureDone(controller, request);
      }

      @Override
      public IsMasterRunningResponse isMasterRunning(RpcController controller,
          IsMasterRunningRequest request) throws ServiceException {
        return stub.isMasterRunning(controller, request);
      }

      @Override
      public ModifyNamespaceResponse modifyNamespace(RpcController controller,
          ModifyNamespaceRequest request) throws ServiceException {
        return stub.modifyNamespace(controller, request);
      }

      @Override
      public CreateNamespaceResponse createNamespace(RpcController controller,
          CreateNamespaceRequest request) throws ServiceException {
        return stub.createNamespace(controller, request);
      }

      @Override
      public DeleteNamespaceResponse deleteNamespace(RpcController controller,
          DeleteNamespaceRequest request) throws ServiceException {
        return stub.deleteNamespace(controller, request);
      }

      @Override
      public GetNamespaceDescriptorResponse getNamespaceDescriptor(RpcController controller,
          GetNamespaceDescriptorRequest request) throws ServiceException {
        return stub.getNamespaceDescriptor(controller, request);
      }

      @Override
      public ListNamespaceDescriptorsResponse listNamespaceDescriptors(RpcController controller,
          ListNamespaceDescriptorsRequest request) throws ServiceException {
        return stub.listNamespaceDescriptors(controller, request);
      }

      @Override
      public ListTableDescriptorsByNamespaceResponse listTableDescriptorsByNamespace(
          RpcController controller, ListTableDescriptorsByNamespaceRequest request)
          throws ServiceException {
        return stub.listTableDescriptorsByNamespace(controller, request);
      }

      @Override
      public ListTableNamesByNamespaceResponse listTableNamesByNamespace(RpcController controller,
          ListTableNamesByNamespaceRequest request) throws ServiceException {
        return stub.listTableNamesByNamespace(controller, request);
      }

      @Override
      public void close() {
        release(this.mss);
      }

      @Override
      public GetSchemaAlterStatusResponse getSchemaAlterStatus(RpcController controller,
          GetSchemaAlterStatusRequest request) throws ServiceException {
        return stub.getSchemaAlterStatus(controller, request);
      }

      @Override
      public GetTableDescriptorsResponse getTableDescriptors(RpcController controller,
          GetTableDescriptorsRequest request) throws ServiceException {
        return stub.getTableDescriptors(controller, request);
      }

      @Override
      public GetTableNamesResponse getTableNames(RpcController controller,
          GetTableNamesRequest request) throws ServiceException {
        return stub.getTableNames(controller, request);
      }

      @Override
      public GetClusterStatusResponse getClusterStatus(RpcController controller,
          GetClusterStatusRequest request) throws ServiceException {
        return stub.getClusterStatus(controller, request);
      }

      @Override
      public TruncateTableResponse truncateTable(RpcController controller,
          TruncateTableRequest request) throws ServiceException {
        return stub.truncateTable(controller, request);
      }

      @Override
      public SetQuotaResponse setQuota(RpcController controller, SetQuotaRequest request)
          throws ServiceException {
        return stub.setQuota(controller, request);
      }

      @Override
      public SwitchThrottleResponse switchThrottle(RpcController controller,
          SwitchThrottleRequest request) throws ServiceException {
        return stub.switchThrottle(controller, request);
      }

      @Override
      public AddReplicationPeerResponse addReplicationPeer(RpcController controller,
          AddReplicationPeerRequest request) throws ServiceException {
        return stub.addReplicationPeer(controller, request);
      }

      @Override
      public RemoveReplicationPeerResponse removeReplicationPeer(RpcController controller,
          RemoveReplicationPeerRequest request) throws ServiceException {
        return stub.removeReplicationPeer(controller, request);
      }

      @Override
      public EnableReplicationPeerResponse enableReplicationPeer(RpcController controller,
          EnableReplicationPeerRequest request) throws ServiceException {
        return stub.enableReplicationPeer(controller, request);
      }

      @Override
      public DisableReplicationPeerResponse disableReplicationPeer(RpcController controller,
          DisableReplicationPeerRequest request) throws ServiceException {
        return stub.disableReplicationPeer(controller, request);
      }

      @Override
      public GetReplicationPeerConfigResponse getReplicationPeerConfig(RpcController controller,
          GetReplicationPeerConfigRequest request) throws ServiceException {
        return stub.getReplicationPeerConfig(controller, request);
      }

      @Override
      public UpdateReplicationPeerConfigResponse updateReplicationPeerConfig(
          RpcController controller, UpdateReplicationPeerConfigRequest request)
          throws ServiceException {
        return stub.updateReplicationPeerConfig(controller, request);
      }

      @Override
      public ListReplicationPeersResponse listReplicationPeers(RpcController controller,
          ListReplicationPeersRequest request) throws ServiceException {
        return stub.listReplicationPeers(controller, request);
      }

      @Override
      public ReplicationProtos.GetPeerMaxReplicationLoadResponse getPeerMaxReplicationLoad(
          RpcController controller, ReplicationProtos.GetPeerMaxReplicationLoadRequest request)
          throws ServiceException {
        return stub.getPeerMaxReplicationLoad(controller, request);
      }

      @Override
      public ReplicationProtos.NewListReplicationPeersResponse listReplicationPeersForBranch2(
          RpcController controller, ListReplicationPeersRequest request) throws ServiceException {
        return stub.listReplicationPeersForBranch2(controller, request);
      }

      @Override
      public AddReplicationPeerResponse addReplicationPeerForBranch2(RpcController controller,
          ReplicationProtos.NewAddReplicationPeerRequest request) throws ServiceException {
        return stub.addReplicationPeerForBranch2(controller, request);
      }

      @Override
      public UpdateReplicationPeerConfigResponse updateReplicationPeerConfigForBranch2(
          RpcController controller, ReplicationProtos.NewUpdateReplicationPeerConfigRequest request)
          throws ServiceException {
        return stub.updateReplicationPeerConfigForBranch2(controller, request);
      }

      @Override
      public SetSplitOrMergeEnabledResponse setSplitOrMergeEnabled(RpcController controller,
          SetSplitOrMergeEnabledRequest request) throws ServiceException {
        return stub.setSplitOrMergeEnabled(controller, request);
      }

      @Override
      public IsSplitOrMergeEnabledResponse isSplitOrMergeEnabled(RpcController controller,
          IsSplitOrMergeEnabledRequest request) throws ServiceException {
        return stub.isSplitOrMergeEnabled(controller, request);
      }
    };
  }

  private static void release(HConnectionImplementation.MasterServiceState mss) {
    if (mss != null && mss.connection != null) {
      ((HConnectionImplementation) mss.connection).releaseMaster(mss);
    }
  }

  private boolean isKeepAliveMasterConnectedAndRunning(
      HConnectionImplementation.MasterServiceState mss) {
    if (mss.getStub() == null) {
      return false;
    }
    try {
      return mss.isMasterRunning();
    } catch (UndeclaredThrowableException e) {
      // It's somehow messy, but we can receive exceptions such as
      // java.net.ConnectException but they're not declared. So we catch it...
      LOG.info("Master connection is not running anymore", e.getUndeclaredThrowable());
      return false;
    } catch (ServiceException se) {
      LOG.warn("Checking master connection", se);
      return false;
    }
  }

  void releaseMaster(HConnectionImplementation.MasterServiceState mss) {
    if (mss.getStub() == null) return;
    synchronized (masterLock) {
      --mss.userCount;
      if (mss.userCount <= 0) {
        mss.keepAliveUntil = System.currentTimeMillis() + keepAlive;
      }
    }
  }

  private void closeMasterService(HConnectionImplementation.MasterServiceState mss) {
    if (mss.getStub() != null) {
      LOG.info("Closing master protocol: " + mss);
      mss.clearStub();
    }
    mss.userCount = 0;
  }

  /**
   * Immediate close of the shared master. Can be by the delayed close or when closing the
   * connection itself.
   */
  private void closeMaster() {
    synchronized (masterLock) {
      closeMasterService(masterServiceState);
    }
  }

  void updateCachedLocation(HRegionInfo hri, HRegionLocation source, ServerName serverName,
      long seqNum) {
    HRegionLocation newHrl = new HRegionLocation(hri, serverName, seqNum);
    cacheLocation(hri.getTable(), source, newHrl);
  }

  /**
   * Deletes the cached location of the region if necessary, based on some error from source.
   * @param hri The region in question.
   * @param source The source of the error that prompts us to invalidate cache.
   */
  void deleteCachedLocation(HRegionInfo hri, HRegionLocation source) {
    ConcurrentMap<byte[], HRegionLocation> tableLocations = getTableLocations(hri.getTable());
    tableLocations.remove(hri.getStartKey(), source);
  }

  @Override
  public void deleteCachedRegionLocation(final HRegionLocation location) {
    if (location == null) {
      return;
    }

    HRegionLocation removedLocation;
    TableName tableName = location.getRegionInfo().getTable();
    Map<byte[], HRegionLocation> tableLocations = getTableLocations(tableName);
    removedLocation = tableLocations.remove(location.getRegionInfo().getStartKey());
    if (LOG.isDebugEnabled() && removedLocation != null) {
      LOG.debug("Removed " + location.getRegionInfo().getRegionNameAsString() + " for tableName="
          + tableName + " from cache");
    }
  }

  /**
   * Update the location with the new value (if the exception is a RegionMovedException) or delete
   * it from the cache. Does nothing if we can be sure from the exception that the location is still
   * accurate, or if the cache has already been updated.
   * @param exception an object (to simplify user code) on which we will try to find a nested or
   *          wrapped or both RegionMovedException
   * @param source server that is the source of the location update.
   */
  @Override
  public void updateCachedLocations(final TableName tableName, byte[] rowkey,
      final Object exception, final HRegionLocation source) {
    if (rowkey == null || tableName == null) {
      LOG.warn("Coding error, see method javadoc. row=" + (rowkey == null ? "null" : rowkey)
          + ", tableName=" + (tableName == null ? "null" : tableName));
      return;
    }

    if (source == null || source.getServerName() == null) {
      // This should not happen, but let's secure ourselves.
      return;
    }

    // Is it something we have already updated?
    final HRegionLocation oldLocation = getCachedLocation(tableName, rowkey);
    if (oldLocation == null || !source.getServerName().equals(oldLocation.getServerName())) {
      // There is no such location in the cache (it's been removed already) or
      // the cache has already been refreshed with a different location. => nothing to do
      return;
    }

    HRegionInfo regionInfo = oldLocation.getRegionInfo();
    Throwable cause = ClientExceptionsUtil.findException(exception);
    if (cause != null) {
      if (ClientExceptionsUtil.regionDefinitelyOnTheRegionServerException(cause)) {
        // We know that the region is still on this region server
        return;
      }

      if (cause instanceof RegionMovedException) {
        RegionMovedException rme = (RegionMovedException) cause;
        if (LOG.isTraceEnabled()) {
          LOG.trace(
            "Region " + regionInfo.getRegionNameAsString() + " moved to " + rme.getHostname() + ":"
                + rme.getPort() + " according to " + source.getHostnamePort());
        }
        // We know that the region is not anymore on this region server, but we know
        // the new location.
        updateCachedLocation(regionInfo, source, rme.getServerName(), rme.getLocationSeqNum());
        return;
      }
    }

    // If we're here, it means that can cannot be sure about the location, so we remove it from
    // the cache.
    deleteCachedLocation(regionInfo, source);
  }

  @Override
  public void updateCachedLocations(final byte[] tableName, byte[] rowkey, final Object exception,
      final HRegionLocation source) {
    updateCachedLocations(TableName.valueOf(tableName), rowkey, exception, source);
  }

  @Override
  @Deprecated
  public void processBatch(List<? extends Row> list, final TableName tableName,
      ExecutorService pool, Object[] results) throws IOException, InterruptedException {
    // This belongs in HTable!!! Not in here. St.Ack

    // results must be the same size as list
    if (results.length != list.size()) {
      throw new IllegalArgumentException("argument results must be the same size as argument list");
    }
    processBatchCallback(list, tableName, pool, results, null);
  }

  @Override
  @Deprecated
  public void processBatch(List<? extends Row> list, final byte[] tableName, ExecutorService pool,
      Object[] results) throws IOException, InterruptedException {
    processBatch(list, TableName.valueOf(tableName), pool, results);
  }

  /**
   * Send the queries in parallel on the different region servers. Retries on failures. If the
   * method returns it means that there is no error, and the 'results' array will contain no
   * exception. On error, an exception is thrown, and the 'results' array will contain results and
   * exceptions.
   * @deprecated since 0.96 - Use {@link HTable#processBatchCallback} instead
   */
  @Override
  @Deprecated
  public <R> void processBatchCallback(List<? extends Row> list, TableName tableName,
      ExecutorService pool, Object[] results, Batch.Callback<R> callback)
      throws IOException, InterruptedException {

    // To fulfill the original contract, we have a special callback. This callback
    // will set the results in the Object array.
    HConnectionImplementation.ObjectResultFiller<R> cb =
        new HConnectionImplementation.ObjectResultFiller<R>(results, callback);
    AsyncProcess<?> asyncProcess = createAsyncProcess(tableName, pool, cb, conf);

    // We're doing a submit all. This way, the originalIndex will match the initial list.
    asyncProcess.submitAll(list);
    asyncProcess.waitUntilDone();

    if (asyncProcess.hasError()) {
      throw asyncProcess.getErrors();
    }
  }

  @Override
  @Deprecated
  public <R> void processBatchCallback(List<? extends Row> list, byte[] tableName,
      ExecutorService pool, Object[] results, Batch.Callback<R> callback)
      throws IOException, InterruptedException {
    processBatchCallback(list, TableName.valueOf(tableName), pool, results, callback);
  }

  // For tests.
  protected <R> AsyncProcess<R> createAsyncProcess(TableName tableName, ExecutorService pool,
      AsyncProcess.AsyncProcessCallback<R> callback, Configuration conf) {
    return new AsyncProcess<R>(this, tableName, pool, callback, conf);
  }

  /**
   * Fill the result array for the interfaces using it.
   */
  private static class ObjectResultFiller<Res> implements AsyncProcess.AsyncProcessCallback<Res> {

    private final Object[] results;
    private Batch.Callback<Res> callback;

    ObjectResultFiller(Object[] results, Batch.Callback<Res> callback) {
      this.results = results;
      this.callback = callback;
    }

    @Override
    public void success(int pos, byte[] region, Row row, Res result) {
      assert pos < results.length;
      results[pos] = result;
      if (callback != null) {
        callback.update(region, row.getRow(), result);
      }
    }

    @Override
    public boolean failure(int pos, byte[] region, Row row, Throwable t) {
      assert pos < results.length;
      results[pos] = t;
      // Batch.Callback<Res> was not called on failure in 0.94. We keep this.
      return true; // we want to have this failure in the failures list.
    }

    @Override
    public boolean retriableFailure(int originalIndex, Row row, byte[] region,
        Throwable exception) {
      return true; // we retry
    }
  }

  @Override
  public ServerStatisticTracker getStatisticsTracker() {
    return this.stats;
  }

  @Override
  public ClientBackoffPolicy getBackoffPolicy() {
    return this.backoffPolicy;
  }

  /*
   * Return the number of cached region for a table. It will only be called from a unit test.
   */
  int getNumberOfCachedRegionLocations(final TableName tableName) {
    Map<byte[], HRegionLocation> tableLocs = this.cachedRegionLocations.get(tableName);
    if (tableLocs == null) {
      return 0;
    }
    return tableLocs.values().size();
  }

  /**
   * Check the region cache to see whether a region is cached yet or not. Called by unit tests.
   * @param tableName tableName
   * @param row row
   * @return Region cached or not.
   */
  boolean isRegionCached(TableName tableName, final byte[] row) {
    HRegionLocation location = getCachedLocation(tableName, row);
    return location != null;
  }

  @Override
  public void setRegionCachePrefetch(final TableName tableName, final boolean enable) {
    if (!enable) {
      regionCachePrefetchDisabledTables.add(Bytes.mapKey(tableName.getName()));
    } else {
      regionCachePrefetchDisabledTables.remove(Bytes.mapKey(tableName.getName()));
    }
  }

  @Override
  public void setRegionCachePrefetch(final byte[] tableName, final boolean enable) {
    setRegionCachePrefetch(TableName.valueOf(tableName), enable);
  }

  @Override
  public boolean getRegionCachePrefetch(TableName tableName) {
    return usePrefetch
        && !regionCachePrefetchDisabledTables.contains(Bytes.mapKey(tableName.getName()));
  }

  @Override
  public boolean getRegionCachePrefetch(byte[] tableName) {
    return getRegionCachePrefetch(TableName.valueOf(tableName));
  }

  @Override
  public void abort(final String msg, Throwable t) {
    if (t != null) {
      LOG.fatal(msg, t);
    } else {
      LOG.fatal(msg);
    }
    this.aborted = true;
    close();
    this.closed = true;
  }

  @Override
  public boolean isClosed() {
    return this.closed;
  }

  @Override
  public boolean isAborted() {
    return this.aborted;
  }

  @Override
  public int getCurrentNrHRS() throws IOException {
    return get(this.registry.getCurrentNrHRS());
  }

  /**
   * Increment this client's reference count.
   */
  void incCount() {
    ++refCount;
  }

  /**
   * Decrement this client's reference count.
   */
  void decCount() {
    if (refCount > 0) {
      --refCount;
    }
  }

  /**
   * Return if this client has no reference
   * @return true if this client has no reference; false otherwise
   */
  boolean isZeroReference() {
    return refCount == 0;
  }

  void shutdownExecutorService(ExecutorService service) {
    if (service != null && !service.isShutdown()) {
      service.shutdown();
      try {
        if (!service.awaitTermination(10, TimeUnit.SECONDS)) {
          service.shutdownNow();
        }
      } catch (InterruptedException e) {
        service.shutdownNow();
      }
    }
  }

  void internalClose() {
    if (this.closed) {
      return;
    }
    delayedClosing.stop("Closing connection");
    closeMaster();
    // close batch pool
    if (this.cleanupPool) {
      shutdownExecutorService(this.batchPool);
    }
    // close locateMetaExecutor
    shutdownExecutorService(this.locateMetaExecutor);
    // close locateRegionExecutors
    if (this.locateRegionExecutors != null) {
      for (int i = 0; i < this.locateRegionExecutors.length; i++) {
        shutdownExecutorService(this.locateRegionExecutors[i]);
      }
    }
    this.closed = true;
    if (registry != null) {
      registry.close();
    }
    this.stubs.clear();
    if (clusterStatusListener != null) {
      clusterStatusListener.close();
    }
    if (rpcClient != null) {
      rpcClient.close();
    }
  }

  @Override
  public void close() {
    if (managed) {
      if (aborted) {
        HConnectionManager.deleteStaleConnection(this);
      } else {
        HConnectionManager.deleteConnection(this, false);
      }
    } else {
      internalClose();
    }
  }

  /**
   * Close the connection for good, regardless of what the current value of {@link #refCount} is.
   * Ideally, {@link #refCount} should be zero at this point, which would be the case if all of its
   * consumers close the connection. However, on the off chance that someone is unable to close the
   * connection, perhaps because it bailed out prematurely, the method below will ensure that this
   * {@link HConnection} instance is cleaned up. Caveat: The JVM may take an unknown amount of time
   * to call finalize on an unreachable object, so our hope is that every consumer cleans up after
   * itself, like any good citizen.
   */
  @Override
  protected void finalize() throws Throwable {
    super.finalize();
    // Pretend as if we are about to release the last remaining reference
    refCount = 1;
    close();
  }

  @Override
  public HTableDescriptor[] listTables() throws IOException {
    MasterKeepAliveConnection master = getKeepAliveMasterService();
    try {
      GetTableDescriptorsRequest req =
          RequestConverter.buildGetTableDescriptorsRequest((List<TableName>) null);
      return ProtobufUtil.getHTableDescriptorArray(master.getTableDescriptors(null, req));
    } catch (ServiceException se) {
      throw ProtobufUtil.getRemoteException(se);
    } finally {
      master.close();
    }
  }

  @Override
  public HTableDescriptor[] listTables(String regex) throws IOException {
    MasterKeepAliveConnection master = getKeepAliveMasterService();
    try {
      GetTableDescriptorsRequest req = RequestConverter.buildGetTableDescriptorsRequest(regex);
      return ProtobufUtil.getHTableDescriptorArray(master.getTableDescriptors(null, req));
    } catch (ServiceException se) {
      throw ProtobufUtil.getRemoteException(se);
    } finally {
      master.close();
    }
  }

  @Override
  public String[] getTableNames() throws IOException {
    TableName[] tableNames = listTableNames();
    String result[] = new String[tableNames.length];
    for (int i = 0; i < tableNames.length; i++) {
      result[i] = tableNames[i].getNameAsString();
    }
    return result;
  }

  @Override
  public TableName[] listTableNames() throws IOException {
    MasterKeepAliveConnection master = getKeepAliveMasterService();
    try {
      return ProtobufUtil.getTableNameArray(
        master.getTableNames(null, GetTableNamesRequest.newBuilder().build()).getTableNamesList());
    } catch (ServiceException se) {
      throw ProtobufUtil.getRemoteException(se);
    } finally {
      master.close();
    }
  }

  @Override
  public HTableDescriptor[] getHTableDescriptorsByTableName(List<TableName> tableNames)
      throws IOException {
    if (tableNames == null || tableNames.isEmpty()) return new HTableDescriptor[0];
    MasterKeepAliveConnection master = getKeepAliveMasterService();
    try {
      GetTableDescriptorsRequest req = RequestConverter.buildGetTableDescriptorsRequest(tableNames);
      return ProtobufUtil.getHTableDescriptorArray(master.getTableDescriptors(null, req));
    } catch (ServiceException se) {
      throw ProtobufUtil.getRemoteException(se);
    } finally {
      master.close();
    }
  }

  @Override
  public HTableDescriptor[] getHTableDescriptors(List<String> names) throws IOException {
    List<TableName> tableNames = new ArrayList<TableName>(names.size());
    for (String name : names) {
      tableNames.add(TableName.valueOf(name));
    }

    return getHTableDescriptorsByTableName(tableNames);
  }

  @Override
  public NonceGenerator getNonceGenerator() {
    return this.nonceGenerator;
  }

  /**
   * Connects to the master to get the table descriptor.
   * @param tableName table name
   * @return
   * @throws IOException if the connection to master fails or if the table is not found.
   */
  @Override
  public HTableDescriptor getHTableDescriptor(final TableName tableName) throws IOException {
    if (tableName == null) return null;
    MasterKeepAliveConnection master = getKeepAliveMasterService();
    GetTableDescriptorsResponse htds;
    try {
      GetTableDescriptorsRequest req = RequestConverter.buildGetTableDescriptorsRequest(tableName);
      htds = master.getTableDescriptors(null, req);
    } catch (ServiceException se) {
      throw ProtobufUtil.getRemoteException(se);
    } finally {
      master.close();
    }
    if (!htds.getTableSchemaList().isEmpty()) {
      return HTableDescriptor.convert(htds.getTableSchemaList().get(0));
    }
    throw new TableNotFoundException(tableName.getNameAsString());
  }

  @Override
  public HTableDescriptor getHTableDescriptor(final byte[] tableName) throws IOException {
    return getHTableDescriptor(TableName.valueOf(tableName));
  }

  @Override
  public RpcRetryingCallerFactory getRpcRetryingCallerFactory() {
    return rpcCallerFactory;
  }

  @Override
  public RpcControllerFactory getRpcControllerFactory() {
    return rpcControllerFactory;
  }
}
