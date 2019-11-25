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
package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.lang.reflect.Constructor;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import javax.management.ObjectName;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.SplitOrMergeTracker;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScannable;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.Chore;
import org.apache.hadoop.hbase.ClockOutOfSyncException;
import org.apache.hadoop.hbase.CompatibilitySingletonFactory;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.DroppedSnapshotException;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.HealthCheckChore;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.MultiActionResultTooLarge;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.RemoteExceptionHandler;
import org.apache.hadoop.hbase.ServerLoad;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.TableDescriptors;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TooManyRegionScannersException;
import org.apache.hadoop.hbase.UnknownScannerException;
import org.apache.hadoop.hbase.YouAreDeadException;
import org.apache.hadoop.hbase.ZNodeClearer;
import org.apache.hadoop.hbase.catalog.CatalogTracker;
import org.apache.hadoop.hbase.catalog.MetaEditor;
import org.apache.hadoop.hbase.catalog.MetaReader;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.MasterSwitchType;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.exceptions.FailedSanityCheckException;
import org.apache.hadoop.hbase.exceptions.OutOfOrderScannerNextException;
import org.apache.hadoop.hbase.exceptions.RegionMovedException;
import org.apache.hadoop.hbase.exceptions.RegionOpeningException;
import org.apache.hadoop.hbase.exceptions.UnknownProtocolException;
import org.apache.hadoop.hbase.executor.ExecutorService;
import org.apache.hadoop.hbase.executor.ExecutorType;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.fs.HFileSystem;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.ipc.CallerDisconnectedException;
import org.apache.hadoop.hbase.ipc.HBaseRPCErrorHandler;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.hadoop.hbase.ipc.PriorityFunction;
import org.apache.hadoop.hbase.ipc.RpcCallContext;
import org.apache.hadoop.hbase.ipc.RpcClient;
import org.apache.hadoop.hbase.ipc.RpcClientFactory;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.ipc.RpcServer.BlockingServiceAndInterface;
import org.apache.hadoop.hbase.ipc.RpcServerInterface;
import org.apache.hadoop.hbase.ipc.ServerNotRunningYetException;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.master.RegionState.State;
import org.apache.hadoop.hbase.master.SplitLogManager;
import org.apache.hadoop.hbase.master.TableLockManager;
import org.apache.hadoop.hbase.metrics.MBeanSource;
import org.apache.hadoop.hbase.procedure.RegionServerProcedureManagerHost;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.RequestConverter;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.CloseRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.CloseRegionResponse;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.CompactRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.CompactRegionResponse;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.CompactionEnableRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.CompactionEnableResponse;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.FlushRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.FlushRegionResponse;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetOnlineRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetOnlineRegionResponse;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetRegionInfoRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetRegionInfoResponse;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetRegionLoadRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetRegionLoadResponse;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetServerInfoRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetServerInfoResponse;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetStoreFileRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetStoreFileResponse;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.MergeRegionsRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.MergeRegionsResponse;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.OpenRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.OpenRegionRequest.RegionOpenInfo;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.OpenRegionResponse;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.OpenRegionResponse.RegionOpeningState;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.ReplicateWALEntryRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.ReplicateWALEntryResponse;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.RollWALWriterRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.RollWALWriterResponse;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.SplitRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.SplitRegionResponse;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.StopServerRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.StopServerResponse;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.UpdateFavoredNodesRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.UpdateFavoredNodesResponse;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.WALEntry;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.BulkLoadHFileRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.BulkLoadHFileRequest.FamilyPath;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.BulkLoadHFileResponse;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.Condition;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.CoprocessorServiceCall;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.CoprocessorServiceRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.CoprocessorServiceResponse;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.GetRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.GetResponse;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MultiRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MultiResponse;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutateRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutateResponse;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto.MutationType;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.RegionAction;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.RegionActionResult;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ResultOrException;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ScanRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ScanResponse;
import org.apache.hadoop.hbase.protobuf.generated.ClusterStatusProtos;
import org.apache.hadoop.hbase.protobuf.generated.ClusterStatusProtos.RegionLoad;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.Coprocessor;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.NameStringPair;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.RegionServerInfo;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.RegionSpecifier;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.RegionSpecifier.RegionSpecifierType;
import org.apache.hadoop.hbase.protobuf.generated.RPCProtos.RequestHeader;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos.GetLastFlushedSequenceIdRequest;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos.RegionServerReportRequest;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos.RegionServerReportResponse;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos.RegionServerStartupRequest;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos.RegionServerStartupResponse;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos.RegionServerStatusService;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos.RegionStateTransition;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos.RegionStateTransition.TransitionCode;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos.ReportRSFatalErrorRequest;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos.ReportRegionStateTransitionRequest;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos.ReportRegionStateTransitionResponse;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos.CompactionDescriptor;
import org.apache.hadoop.hbase.quotas.OperationQuota;
import org.apache.hadoop.hbase.quotas.OperationQuota.OperationType;
import org.apache.hadoop.hbase.quotas.RegionServerQuotaManager;
import org.apache.hadoop.hbase.quotas.ThrottleState;
import org.apache.hadoop.hbase.quotas.ThrottlingException;
import org.apache.hadoop.hbase.regionserver.HRegion.Operation;
import org.apache.hadoop.hbase.regionserver.Leases.LeaseStillHeldException;
import org.apache.hadoop.hbase.regionserver.ScannerContext.LimitScope;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionProgress;
import org.apache.hadoop.hbase.regionserver.handler.CloseMetaHandler;
import org.apache.hadoop.hbase.regionserver.handler.CloseRegionHandler;
import org.apache.hadoop.hbase.regionserver.handler.OpenMetaHandler;
import org.apache.hadoop.hbase.regionserver.handler.OpenRegionHandler;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.wal.HLogFactory;
import org.apache.hadoop.hbase.regionserver.wal.HLogKey;
import org.apache.hadoop.hbase.regionserver.wal.HLogSplitter;
import org.apache.hadoop.hbase.regionserver.wal.HLogUtil;
import org.apache.hadoop.hbase.regionserver.wal.WALActionsListener;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.replication.regionserver.MetricsSource;
import org.apache.hadoop.hbase.replication.regionserver.ReplicationLoad;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.trace.SpanReceiverHost;
import org.apache.hadoop.hbase.util.ByteStringer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CompressionTest;
import org.apache.hadoop.hbase.util.ConfigUtil;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSTableDescriptors;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.InfoServer;
import org.apache.hadoop.hbase.util.JvmPauseMonitor;
import org.apache.hadoop.hbase.util.JvmThreadMonitor;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.QueueCounter;
import org.apache.hadoop.hbase.util.Sleeper;
import org.apache.hadoop.hbase.util.Strings;
import org.apache.hadoop.hbase.util.ThreadInfoUtils;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.util.VersionInfo;
import org.apache.hadoop.hbase.zookeeper.ClusterStatusTracker;
import org.apache.hadoop.hbase.zookeeper.MasterAddressTracker;
import org.apache.hadoop.hbase.zookeeper.MetaRegionTracker;
import org.apache.hadoop.hbase.zookeeper.RecoveringRegionWatcher;
import org.apache.hadoop.hbase.zookeeper.ThrottleStateTracker;
import org.apache.hadoop.hbase.zookeeper.ZKClusterId;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperNodeTracker;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.metrics.util.MBeanUtil;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;
import org.apache.htrace.Trace;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.data.Stat;
import org.cliffc.high_scale_lib.Counter;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Maps;
import com.google.protobuf.BlockingRpcChannel;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import com.google.protobuf.ServiceException;
import com.google.protobuf.TextFormat;
import com.xiaomi.infra.crypto.KeyCenterKeyProvider;
import com.xiaomi.infra.hbase.util.MailUtils;

/**
 * HRegionServer makes a set of HRegions available to clients. It checks in with
 * the HMaster. There are many HRegionServers in a single HBase deployment.
 */
@InterfaceAudience.Private
@SuppressWarnings("deprecation")
public class HRegionServer implements ClientProtos.ClientService.BlockingInterface,
  AdminProtos.AdminService.BlockingInterface, Runnable, RegionServerServices,
  HBaseRPCErrorHandler, LastSequenceId {

  public static final Log LOG = LogFactory.getLog(HRegionServer.class);

  private final Random rand;

  private ScannerIdGenerator scannerIdGenerator;

  /*
   * Strings to be used in forming the exception message for
   * RegionsAlreadyInTransitionException.
   */
  protected static final String OPEN = "OPEN";
  protected static final String CLOSE = "CLOSE";

  //RegionName vs current action in progress
  //true - if open region action in progress
  //false - if close region action in progress
  protected final ConcurrentMap<byte[], Boolean> regionsInTransitionInRS =
    new ConcurrentSkipListMap<byte[], Boolean>(Bytes.BYTES_COMPARATOR);

  /** RPC scheduler to use for the region server. */
  public static final String REGION_SERVER_RPC_SCHEDULER_FACTORY_CLASS =
      "hbase.region.server.rpc.scheduler.factory.class";

  protected long maxScannerResultSize;

  // Cache flushing
  protected MemStoreFlusher cacheFlusher;

  protected HeapMemoryManager hMemManager;

  // catalog tracker
  protected CatalogTracker catalogTracker;

  // Watch if a region is out of recovering state from ZooKeeper
  @SuppressWarnings("unused")
  private RecoveringRegionWatcher recoveringRegionWatcher;

  /**
   * Go here to get table descriptors.
   */
  protected TableDescriptors tableDescriptors;

  // Replication services. If no replication, this handler will be null.
  protected ReplicationSourceService replicationSourceHandler;
  protected ReplicationSinkService replicationSinkHandler;

  // Compactions
  public CompactSplitThread compactSplitThread;

  final ConcurrentHashMap<String, RegionScannerHolder> scanners =
      new ConcurrentHashMap<String, RegionScannerHolder>();
  // Hold the name of a closed scanner for a while. This is used to keep compatible for old clients
  // which may send next or close request to a region scanner which has already been exhausted. The
  // entries will be removed automatically after scannerLeaseTimeoutPeriod.
  private final Cache<String, String> closedScanners;

  /**
   * Map of regions currently being served by this region server. Key is the
   * encoded region name.  All access should be synchronized.
   */
  protected final Map<String, HRegion> onlineRegions =
    new ConcurrentHashMap<String, HRegion>();

  /**
   * Map of encoded region names to the DataNode locations they should be hosted on
   * We store the value as InetSocketAddress since this is used only in HDFS
   * API (create() that takes favored nodes as hints for placing file blocks).
   * We could have used ServerName here as the value class, but we'd need to
   * convert it to InetSocketAddress at some point before the HDFS API call, and
   * it seems a bit weird to store ServerName since ServerName refers to RegionServers
   * and here we really mean DataNode locations.
   */
  protected final Map<String, InetSocketAddress[]> regionFavoredNodesMap =
      new ConcurrentHashMap<String, InetSocketAddress[]>();

  /**
   * Set of regions currently being in recovering state which means it can accept writes(edits from
   * previous failed region server) but not reads. A recovering region is also an online region.
   */
  protected final Map<String, HRegion> recoveringRegions = Collections
      .synchronizedMap(new HashMap<String, HRegion>());

  // Leases
  protected Leases leases;

  // Instance of the hbase executor service.
  protected ExecutorService service;

  // Request counter. (Includes requests that are not serviced by regions.)
  final Counter requestCount = new Counter();

  // If false, the file system has become unavailable
  protected volatile boolean fsOk;
  protected HFileSystem fs;

  // Set when a report to the master comes back with a message asking us to
  // shutdown. Also set by call to stop when debugging or running unit tests
  // of HRegionServer in isolation.
  protected volatile boolean stopped = false;

  // Go down hard. Used if file system becomes unavailable and also in
  // debugging and unit tests.
  protected volatile boolean abortRequested;

  // region server static info like info port
  private RegionServerInfo.Builder rsInfo;

  ConcurrentMap<String, Integer> rowlocks = new ConcurrentHashMap<String, Integer>();

  // A state before we go into stopped state.  At this stage we're closing user
  // space regions.
  private boolean stopping = false;

  private volatile boolean killed = false;

  private volatile boolean queueFullDetected = false;

  protected final Configuration conf;

  private Path rootDir;

  protected final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  final int numRetries;
  protected final int threadWakeFrequency;
  private final int msgInterval;

  protected final int numRegionsToReport;

  // Stub to do region server status calls against the master.
  private volatile RegionServerStatusService.BlockingInterface rssStub;
  // RPC client. Used to make the stub above that does region server status checking.
  RpcClient rpcClient;

  // Server to handle client requests. Default access so can be accessed by
  // unit tests.
  RpcServerInterface rpcServer;

  private final InetSocketAddress isa;
  private UncaughtExceptionHandler uncaughtExceptionHandler;

  // Info server. Default access so can be used by unit tests. REGIONSERVER
  // is name of the webapp and the attribute name used stuffing this instance
  // into web context.
  InfoServer infoServer;
  private JvmPauseMonitor pauseMonitor;
  private JvmThreadMonitor jvmThreadMonitor;
  
  /** region server process name */
  public static final String REGIONSERVER = "regionserver";

  /** region server configuration name */
  public static final String REGIONSERVER_CONF = "regionserver_conf";

  private MetricsRegionServer metricsRegionServer;

  private ServerLoad serverLoad;

  private SpanReceiverHost spanReceiverHost;

  private AccessCounter accessCounter;

  /*
   * Check for compactions requests.
   */
  Chore compactionChecker;

  /*
   * Check for flushes
   */
  Chore periodicFlusher;

  /*
   * Queue full detection
   */
  Chore queueFullDetector;

  /*
   * region compactor by locality
   */
  Chore regionCompactor;

  // HLog and HLog roller. log is protected rather than private to avoid
  // eclipse warning when accessed by inner classes
  protected volatile HLog hlog;
  // The meta updates are written to a different hlog. If this
  // regionserver holds meta regions, then this field will be non-null.
  protected volatile HLog hlogForMeta;

  LogRoller hlogRoller;
  LogRoller metaHLogRoller;

  // flag set after we're done setting up server threads (used for testing)
  protected volatile boolean isOnline;

  // zookeeper connection and watcher
  private ZooKeeperWatcher zooKeeper;

  // master address tracker
  private MasterAddressTracker masterAddressTracker;

  // Cluster Status Tracker
  private ClusterStatusTracker clusterStatusTracker;

  // Throttle state tracker
  private ThrottleStateTracker throttleStateTracker;

  // Tracker for split and merge state
  private SplitOrMergeTracker splitOrMergeTracker;

  // Log Splitting Worker
  private SplitLogWorker splitLogWorker;

  // A sleeper that sleeps for msgInterval.
  private final Sleeper sleeper;

  private final RegionServerAccounting regionServerAccounting;

  // Cache configuration and block cache reference
  final CacheConfig cacheConfig;

  /** The health check chore. */
  private HealthCheckChore healthCheckChore;

  /** The nonce manager chore. */
  private Chore nonceManagerChore;

  private Map<String, Service> coprocessorServiceHandlers = Maps.newHashMap();

  /**
   * The server name the Master sees us as.  Its made from the hostname the
   * master passes us, port, and server startcode. Gets set after registration
   * against  Master.  The hostname can differ from the hostname in {@link #isa}
   * but usually doesn't if both servers resolve .
   */
  private ServerName serverNameFromMasterPOV;

  /**
   * This servers startcode.
   */
  private final long startcode;

  /**
   * Unique identifier for the cluster we are a part of.
   */
  private String clusterId;

  /**
   * MX Bean for RegionServerInfo
   */
  private ObjectName mxBean = null;

  /**
   * Chore to clean periodically the moved region list
   */
  private MovedRegionsCleaner movedRegionsCleaner;

  /**
   * Minimum allowable time limit delta (in milliseconds) that can be enforced during scans. This
   * configuration exists to prevent the scenario where a time limit is specified to be so
   * restrictive that the time limit is reached immediately (before any cells are scanned).
   */
  private static final String REGION_SERVER_RPC_MINIMUM_SCAN_TIME_LIMIT_DELTA =
            "hbase.region.server.rpc.minimum.scan.time.limit.delta";
  /**
   * Default value of {@link HRegionServer#REGION_SERVER_RPC_MINIMUM_SCAN_TIME_LIMIT_DELTA}
   */
  private static final long DEFAULT_REGION_SERVER_RPC_MINIMUM_SCAN_TIME_LIMIT_DELTA = 10;

  /**
   * Minimum allowable time limit delta (in milliseconds) that can be enforced during scans. This
   * configuration exists to prevent the scenario where a time limit is specified to be so
   * restrictive that the time limit is reached immediately (before any cells are scanned).
   */
  static final String REGION_SERVER_RPC_MAXIMUM_SCAN_TIME_LIMIT_DELTA =
            "hbase.region.server.rpc.maximum.scan.time.limit.delta";
  /**
   * Default value of {@link HRegionServer#REGION_SERVER_RPC_MINIMUM_SCAN_TIME_LIMIT_DELTA}
   */
  private static final long DEFAULT_REGION_SERVER_RPC_MAXIMUM_SCAN_TIME_LIMIT_DELTA = -1L;
  
  /**
   * Conf key that specifies the maximum number of opened region scanners on a region server
   */
  private static final String REGION_SERVER_MAXIMUM_OPENED_REGION_SCANNER_LIMIT =
      "hbase.regionserver.maximum.opened.region.scanner.limit";
  /**
   * default value of{@link REGION_SERVER_MAXIMUM_OPENED_REGION_SCANNER_LIMIT}
   */
  private static final int DEFAULT_REGION_SERVER_MAXIMUM_OPENED_REGION_SCANNER_LIMIT = -1;

  /**
   * The lease timeout period for client scanners (milliseconds).
   */
  private final int scannerLeaseTimeoutPeriod;

  /**
   * The RPC timeout period (milliseconds)
   */
  private final int rpcTimeout;

  /**
   * The minimum allowable delta to use for the scan limit
   */
  private final long minimumScanTimeLimitDelta;

  /**
   * The maximum allowable delta to use for the scan limit
   */
  private final long maximumScanTimeLimitDelta;
  
  /**
   * The maximum opened region scanner limit on a regionserver
   */
  private final int maxOpenedRegionScannerLimit;

  /**
   * The reference to the priority extraction function
   */
  private final PriorityFunction priority;

  private RegionServerCoprocessorHost rsHost;

  private RegionServerProcedureManagerHost rspmHost;

  private RegionServerQuotaManager rsQuotaManager;

  // Table level lock manager for locking for region operations
  private TableLockManager tableLockManager;

  private final boolean useZKForAssignment;

  // Used for 11059
  private ServerName serverName;

  /**
   * Nonce manager. Nonces are used to make operations like increment and append idempotent
   * in the case where client doesn't receive the response from a successful operation and
   * retries. We track the successful ops for some time via a nonce sent by client and handle
   * duplicate operations (currently, by failing them; in future we might use MVCC to return
   * result). Nonces are also recovered from WAL during, recovery; however, the caveats (from
   * HBASE-3787) are:
   * - WAL recovery is optimized, and under high load we won't read nearly nonce-timeout worth
   *   of past records. If we don't read the records, we don't read and recover the nonces.
   *   Some WALs within nonce-timeout at recovery may not even be present due to rolling/cleanup.
   * - There's no WAL recovery during normal region move, so nonces will not be transfered.
   * We can have separate additional "Nonce WAL". It will just contain bunch of numbers and
   * won't be flushed on main path - because WAL itself also contains nonces, if we only flush
   * it before memstore flush, for a given nonce we will either see it in the WAL (if it was
   * never flushed to disk, it will be part of recovery), or we'll see it as part of the nonce
   * log (or both occasionally, which doesn't matter). Nonce log file can be deleted after the
   * latest nonce in it expired. It can also be recovered during move.
   */
  private final ServerNonceManager nonceManager;

  private UserProvider userProvider;
  
  // When rs report to master, master response back some contents 
  private RegionServerReportResponse reportResponse;

  /** RS instance flag to indicate whether allows compaction or not*/
  private volatile boolean enableCompact = true;

  private String clusterName;
  
  /**
   * Starts a HRegionServer at the default location
   *
   * @param conf
   * @throws IOException
   * @throws InterruptedException
   */
  public HRegionServer(Configuration conf)
  throws IOException, InterruptedException {
    this.fsOk = true;
    this.conf = conf;
    this.isOnline = false;
    checkCodecs(this.conf);
    this.userProvider = UserProvider.instantiate(conf);

    FSUtils.setupShortCircuitRead(this.conf);

    // Config'ed params
    this.numRetries = this.conf.getInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER,
        HConstants.DEFAULT_HBASE_CLIENT_RETRIES_NUMBER);
    this.threadWakeFrequency = conf.getInt(HConstants.THREAD_WAKE_FREQUENCY, 10 * 1000);
    this.msgInterval = conf.getInt("hbase.regionserver.msginterval", 3 * 1000);

    this.sleeper = new Sleeper(this.msgInterval, this);

    boolean isNoncesEnabled = conf.getBoolean(HConstants.HBASE_RS_NONCES_ENABLED, true);
    this.nonceManager = isNoncesEnabled ? new ServerNonceManager(this.conf) : null;

    this.maxScannerResultSize = conf.getLong(
      HConstants.HBASE_CLIENT_SCANNER_MAX_RESULT_SIZE_KEY,
      HConstants.DEFAULT_HBASE_CLIENT_SCANNER_MAX_RESULT_SIZE);

    this.numRegionsToReport = conf.getInt(
      "hbase.regionserver.numregionstoreport", 10);

    this.rpcTimeout = conf.getInt(
      HConstants.HBASE_RPC_SHORTOPERATION_TIMEOUT_KEY,
      HConstants.DEFAULT_HBASE_RPC_SHORTOPERATION_TIMEOUT);

    minimumScanTimeLimitDelta = conf.getLong(REGION_SERVER_RPC_MINIMUM_SCAN_TIME_LIMIT_DELTA,
      DEFAULT_REGION_SERVER_RPC_MINIMUM_SCAN_TIME_LIMIT_DELTA);
    maximumScanTimeLimitDelta = conf.getLong(REGION_SERVER_RPC_MAXIMUM_SCAN_TIME_LIMIT_DELTA,
      DEFAULT_REGION_SERVER_RPC_MAXIMUM_SCAN_TIME_LIMIT_DELTA);

    this.maxOpenedRegionScannerLimit = conf.getInt(
        REGION_SERVER_MAXIMUM_OPENED_REGION_SCANNER_LIMIT,
        DEFAULT_REGION_SERVER_MAXIMUM_OPENED_REGION_SCANNER_LIMIT);

    this.abortRequested = false;
    this.stopped = false;

    this.scannerLeaseTimeoutPeriod = HBaseConfiguration.getInt(conf,
      HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD,
      HConstants.HBASE_REGIONSERVER_LEASE_PERIOD_KEY,
      HConstants.DEFAULT_HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD);

    // Server to handle client requests.
    String hostname = getHostname(conf);
    boolean mode =
        conf.getBoolean(HConstants.CLUSTER_DISTRIBUTED, HConstants.DEFAULT_CLUSTER_DISTRIBUTED);
    if (mode == HConstants.CLUSTER_IS_DISTRIBUTED && hostname.equals(HConstants.LOCALHOST)) {
      String msg =
          "The hostname of regionserver cannot be set to localhost "
              + "in a fully-distributed setup because it won't be reachable. "
              + "See \"Getting Started\" for more information.";
      LOG.fatal(msg);
      throw new IOException(msg);
    }
    int port = conf.getInt(HConstants.REGIONSERVER_PORT,
      HConstants.DEFAULT_REGIONSERVER_PORT);
    // Creation of a HSA will force a resolve.
    InetSocketAddress initialIsa = new InetSocketAddress(hostname, port);
    if (initialIsa.getAddress() == null) {
      throw new IllegalArgumentException("Failed resolve of " + initialIsa);
    }
    this.rand = new Random(initialIsa.hashCode());
    String name = "regionserver/" + initialIsa.toString();
    // Set how many times to retry talking to another server over HConnection.
    HConnectionManager.setServerSideHConnectionRetries(this.conf, name, LOG);
    this.priority = new AnnotationReadingPriorityFunction(this);
    RpcSchedulerFactory rpcSchedulerFactory;
    try {
      Class<?> rpcSchedulerFactoryClass = conf.getClass(
          REGION_SERVER_RPC_SCHEDULER_FACTORY_CLASS,
          SimpleRpcSchedulerFactory.class);
      rpcSchedulerFactory = ((RpcSchedulerFactory) rpcSchedulerFactoryClass.newInstance());
    } catch (InstantiationException e) {
      throw new IllegalArgumentException(e);
    } catch (IllegalAccessException e) {
      throw new IllegalArgumentException(e);
    }

    this.rpcServer = new RpcServer(this, name, getServices(),
      /*HBaseRPCErrorHandler.class, OnlineRegions.class},*/
      initialIsa, // BindAddress is IP we got for this server.
      conf,
      rpcSchedulerFactory.create(conf, this));

    // Set our address.
    this.isa = this.rpcServer.getListenerAddress();

    this.rpcServer.setErrorHandler(this);
    this.startcode = System.currentTimeMillis();
    serverName = ServerName.valueOf(isa.getHostName(), isa.getPort(), startcode);
    this.scannerIdGenerator = new ScannerIdGenerator(serverName);
    useZKForAssignment = ConfigUtil.useZKForAssignment(conf);

    // login the zookeeper client principal (if using security)
    ZKUtil.loginClient(this.conf, "hbase.zookeeper.client.keytab.file",
      "hbase.zookeeper.client.kerberos.principal", this.isa.getHostName());

    // login the server principal (if using secure Hadoop)
    userProvider.login("hbase.regionserver.keytab.file",
      "hbase.regionserver.kerberos.principal", this.isa.getHostName());
    regionServerAccounting = new RegionServerAccounting();
    cacheConfig = new CacheConfig(conf);
    uncaughtExceptionHandler = new UncaughtExceptionHandler() {
      @Override
      public void uncaughtException(Thread t, Throwable e) {
        abort("Uncaught exception in service thread " + t.getName(), e);
      }
    };

    this.rsInfo = RegionServerInfo.newBuilder();
    // Put up the webui. Webui may come up on port other than configured if
    // that port is occupied. Adjust serverInfo if this is the case.
    this.rsInfo.setInfoPort(putUpWebUI());
    this.rsInfo.setVersionInfo(ProtobufUtil.getVersionInfo());

    closedScanners = CacheBuilder.newBuilder()
        .expireAfterAccess(scannerLeaseTimeoutPeriod, TimeUnit.MILLISECONDS).build();
    clusterName = conf.get(HConstants.CLUSTER_NAME, "");
  }

  public static String getHostname(Configuration conf) throws UnknownHostException {
    return conf.get("hbase.regionserver.ipc.address",
        Strings.domainNamePointerToHostName(DNS.getDefaultHost(
            conf.get("hbase.regionserver.dns.interface", "default"),
            conf.get("hbase.regionserver.dns.nameserver", "default"))));
  }

  @Override
  public boolean registerService(Service instance) {
    /*
     * No stacking of instances is allowed for a single service name
     */
    Descriptors.ServiceDescriptor serviceDesc = instance.getDescriptorForType();
    if (coprocessorServiceHandlers.containsKey(serviceDesc.getFullName())) {
      LOG.error("Coprocessor service " + serviceDesc.getFullName()
          + " already registered, rejecting request from " + instance);
      return false;
    }

    coprocessorServiceHandlers.put(serviceDesc.getFullName(), instance);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Registered regionserver coprocessor service: service=" + serviceDesc.getFullName());
    }
    return true;
  }

  /**
   * @return list of blocking services and their security info classes that this server supports
   */
  private List<BlockingServiceAndInterface> getServices() {
    List<BlockingServiceAndInterface> bssi = new ArrayList<BlockingServiceAndInterface>(2);
    bssi.add(new BlockingServiceAndInterface(
        ClientProtos.ClientService.newReflectiveBlockingService(this),
        ClientProtos.ClientService.BlockingInterface.class));
    bssi.add(new BlockingServiceAndInterface(
        AdminProtos.AdminService.newReflectiveBlockingService(this),
        AdminProtos.AdminService.BlockingInterface.class));
    return bssi;
  }

  /**
   * Run test on configured codecs to make sure supporting libs are in place.
   * @param c
   * @throws IOException
   */
  private static void checkCodecs(final Configuration c) throws IOException {
    // check to see if the codec list is available:
    String [] codecs = c.getStrings("hbase.regionserver.codecs", (String[])null);
    if (codecs == null) return;
    for (String codec : codecs) {
      if (!CompressionTest.testCompression(codec)) {
        throw new IOException("Compression codec " + codec +
          " not supported, aborting RS construction");
      }
    }
  }

  String getClusterId() {
    return this.clusterId;
  }

  @Override
  public int getPriority(RequestHeader header, Message param) {
    return priority.getPriority(header, param);
  }

  @Retention(RetentionPolicy.RUNTIME)
  protected @interface QosPriority {
    int priority() default 0;
  }

  PriorityFunction getPriority() {
    return priority;
  }

  @VisibleForTesting
  public int getScannersCount() {
    return scanners.size();
  }

  RegionScanner getScanner(long scannerId) {
    String scannerIdString = Long.toString(scannerId);
    RegionScannerHolder scannerHolder = scanners.get(scannerIdString);
    if (scannerHolder != null) {
      return scannerHolder.s;
    }
    return null;
  }

  /**
   * All initialization needed before we go register with Master.
   *
   * @throws IOException
   * @throws InterruptedException
   */
  private void preRegistrationInitialization(){
    try {
      initializeZooKeeper();
      initializeThreads();
      registerMBean();
    } catch (Throwable t) {
      // Call stop if error or process will stick around for ever since server
      // puts up non-daemon threads.
      this.rpcServer.stop();
      abort("Initialization of RS failed.  Hence aborting RS.", t);
    }
  }

  /**
   * Bring up connection to zk ensemble and then wait until a master for this
   * cluster and then after that, wait until cluster 'up' flag has been set.
   * This is the order in which master does things.
   * Finally put up a catalog tracker.
   * @throws IOException
   * @throws InterruptedException
   */
  private void initializeZooKeeper() throws IOException, InterruptedException {
    // Open connection to zookeeper and set primary watcher
    this.zooKeeper = new ZooKeeperWatcher(conf, REGIONSERVER + ":" +
      this.isa.getPort(), this);

    // Create the master address tracker, register with zk, and start it.  Then
    // block until a master is available.  No point in starting up if no master
    // running.
    this.masterAddressTracker = new MasterAddressTracker(this.zooKeeper, this);
    this.masterAddressTracker.start();
    blockAndCheckIfStopped(this.masterAddressTracker);

    // Wait on cluster being up.  Master will set this flag up in zookeeper
    // when ready.
    this.clusterStatusTracker = new ClusterStatusTracker(this.zooKeeper, this);
    this.clusterStatusTracker.start();
    blockAndCheckIfStopped(this.clusterStatusTracker);

    // Create the catalog tracker and start it;
    this.catalogTracker = new CatalogTracker(this.zooKeeper, this.conf, this);
    catalogTracker.start();

    // Retrieve clusterId
    // Since cluster status is now up
    // ID should have already been set by HMaster
    try {
      clusterId = ZKClusterId.readClusterIdZNode(this.zooKeeper);
      if (clusterId == null) {
        this.abort("Cluster ID has not been set");
      }
      LOG.info("ClusterId : "+clusterId);
    } catch (KeeperException e) {
      this.abort("Failed to retrieve Cluster ID",e);
    }

    // watch for snapshots and other procedures
    try {
      rspmHost = new RegionServerProcedureManagerHost();
      rspmHost.loadProcedures(conf);
      rspmHost.initialize(this);
    } catch (KeeperException e) {
      this.abort("Failed to reach zk cluster when creating procedure handler.", e);
    }
    this.tableLockManager = TableLockManager.createTableLockManager(conf, zooKeeper,
        ServerName.valueOf(isa.getHostName(), isa.getPort(), startcode));

    // register watcher for recovering regions
    this.recoveringRegionWatcher = new RecoveringRegionWatcher(this.zooKeeper, this);
    
    // register throttle state tracker
    this.throttleStateTracker = new ThrottleStateTracker(this.zooKeeper, this, this);
    this.throttleStateTracker.start();

    this.splitOrMergeTracker = new SplitOrMergeTracker(zooKeeper, conf, this);
    this.splitOrMergeTracker.start();
  }

  /**
   * Utilty method to wait indefinitely on a znode availability while checking
   * if the region server is shut down
   * @param tracker znode tracker to use
   * @throws IOException any IO exception, plus if the RS is stopped
   * @throws InterruptedException
   */
  private void blockAndCheckIfStopped(ZooKeeperNodeTracker tracker)
      throws IOException, InterruptedException {
    while (tracker.blockUntilAvailable(this.msgInterval, false) == null) {
      if (this.stopped) {
        throw new IOException("Received the shutdown message while waiting.");
      }
    }
  }

  /**
   * @return False if cluster shutdown in progress
   */
  private boolean isClusterUp() {
    return this.clusterStatusTracker.isClusterUp();
  }

  private void initializeThreads() throws IOException {
    // Cache flushing thread.
    this.cacheFlusher = new MemStoreFlusher(conf, this);

    // Compaction thread
    this.compactSplitThread = new CompactSplitThread(this);

    // Background thread to check for compactions; needed if region has not gotten updates
    // in a while. It will take care of not checking too frequently on store-by-store basis.
    this.compactionChecker = new CompactionChecker(this, this.threadWakeFrequency, this);
    this.periodicFlusher = new PeriodicMemstoreFlusher(this.threadWakeFrequency, this);

    if (conf.getBoolean("hbase.regionserver.queuefull.detector.enable", false)) {
      this.queueFullDetector = new QueueFullDetector(this, conf);
    }

    // Health checker thread.
    int sleepTime = this.conf.getInt(HConstants.HEALTH_CHECKER_PERIOD,
      HConstants.DEFAULT_HEALTH_CHECKER_PERIOD);
    if (isHealthCheckerConfigured()) {
      healthCheckChore = new HealthCheckChore(sleepTime, this, getConfiguration());
    }

    int compactorPeriod = conf.getInt(RegionCompactor.REGION_ATUO_COMPACT_PERIOD,
      RegionCompactor.DEFAULT_REGION_ATUO_COMPACT_PERIOD);
    this.regionCompactor = new RegionCompactor(this, compactorPeriod);

    int flushCounterPeriod = conf.getInt(AccessCounter.FLUSH_ACCESS_COUNTER_PERIOD,
      AccessCounter.DEFAULT_FLUSH_ACCESS_COUNTER_PERIOD_MS);
    this.accessCounter = new AccessCounter(this, flushCounterPeriod);

    this.leases = new Leases(this.threadWakeFrequency);

    // Create the thread to clean the moved regions list
    movedRegionsCleaner = MovedRegionsCleaner.createAndStart(this);

    if (this.nonceManager != null) {
      // Create the chore that cleans up nonces.
      nonceManagerChore = this.nonceManager.createCleanupChore(this);
    }

    // Setup the Quota Manager
    rsQuotaManager = new RegionServerQuotaManager(this);

    // Setup RPC client for master communication
    rpcClient = RpcClientFactory.createClient(conf, clusterId, new InetSocketAddress(
        this.isa.getAddress(), 0));
    jvmThreadMonitor = new JvmThreadMonitor(conf);
    jvmThreadMonitor.start();
  }

  public String getClusterName() {
    return clusterName;
  }

  /**
   * The HRegionServer sticks in this loop until closed.
   */
  @Override
  public void run() {
    try {
      // Do pre-registration initializations; zookeeper, lease threads, etc.
      preRegistrationInitialization();
    } catch (Throwable e) {
      abort("Fatal exception during initialization", e);
    }

    try {
      // Try and register with the Master; tell it we are here.  Break if
      // server is stopped or the clusterup flag is down or hdfs went wacky.
      while (keepLooping()) {
        RegionServerStartupResponse w = reportForDuty();
        if (w == null) {
          LOG.warn("reportForDuty failed; sleeping and then retrying.");
          this.sleeper.sleep();
        } else {
          handleReportForDutyResponse(w);
          break;
        }
      }

      if (!this.stopped && isHealthy()){
        // start the snapshot handler and other procedure handlers,
        // since the server is ready to run
        rspmHost.start();

        // Start the Quota Manager
        if (rsQuotaManager != null) {
          rsQuotaManager.start(getRpcServer().getScheduler());
          this.switchThrottle();
        }
      }

      // We registered with the Master.  Go into run mode.
      long lastMsg = 0;
      long oldRequestCount = -1;
      // The main run loop.
      while (!this.stopped && isHealthy()) {
        if (!isClusterUp()) {
          if (isOnlineRegionsEmpty()) {
            stop("Exiting; cluster shutdown set and not carrying any regions");
          } else if (!this.stopping) {
            this.stopping = true;
            LOG.info("Closing user regions");
            closeUserRegions(this.abortRequested);
          } else if (this.stopping) {
            boolean allUserRegionsOffline = areAllUserRegionsOffline();
            if (allUserRegionsOffline) {
              // Set stopped if no more write requests tp meta tables
              // since last time we went around the loop.  Any open
              // meta regions will be closed on our way out.
              if (oldRequestCount == getWriteRequestCount()) {
                stop("Stopped; only catalog regions remaining online");
                break;
              }
              oldRequestCount = getWriteRequestCount();
            } else {
              // Make sure all regions have been closed -- some regions may
              // have not got it because we were splitting at the time of
              // the call to closeUserRegions.
              closeUserRegions(this.abortRequested);
            }
            LOG.debug("Waiting on " + getOnlineRegionsAsPrintableString());
          }
        }
        long now = System.currentTimeMillis();
        if ((now - lastMsg) >= msgInterval) {
          tryRegionServerReport(lastMsg, now);
          lastMsg = System.currentTimeMillis();
        }
        if (!this.stopped) this.sleeper.sleep();
      } // for
    } catch (Throwable t) {
      if (!checkOOME(t)) {
        String prefix = t instanceof YouAreDeadException? "": "Unhandled: ";
        abort(prefix + t.getMessage(), t);
      }
    }

    // Add a timer to monitor the procedure in case something hang
    Timer exitMonitor = new Timer(true);
    exitMonitor.schedule(new TimerTask() {
      public void run() {
        LOG.warn("Aborting region server timed out, terminate forcibly...");
        Runtime.getRuntime().halt(1);
      }
    }, conf.getLong("hbase.exit.timeout.ms", 30000));

    // Run shutdown.
    if (mxBean != null) {
      MBeanUtil.unregisterMBean(mxBean);
      mxBean = null;
    }
    if (this.leases != null) this.leases.closeAfterLeasesExpire();
    if (this.pauseMonitor != null) pauseMonitor.stop();
    this.rpcServer.stop();
    if (this.splitLogWorker != null) {
      splitLogWorker.stop();
    }
    if (this.infoServer != null) {
      LOG.info("Stopping infoServer");
      try {
        this.infoServer.stop();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    // Send cache a shutdown.
    if (cacheConfig.isBlockCacheEnabled()) {
      cacheConfig.getBlockCache().shutdown();
    }

    if (movedRegionsCleaner != null) {
      movedRegionsCleaner.stop("Region Server stopping");
    }

    // Send interrupts to wake up threads if sleeping so they notice shutdown.
    // TODO: Should we check they are alive? If OOME could have exited already
    if (this.hMemManager != null) this.hMemManager.stop();
    if (this.cacheFlusher != null) {
      if (queueFullDetected) {
        this.cacheFlusher.tryInterruptIfNecessary();
      } else {
        this.cacheFlusher.interruptIfNecessary();
      }
    }
    if (this.compactSplitThread != null) this.compactSplitThread.interruptIfNecessary();
    if (this.hlogRoller != null) {
      if (queueFullDetected) {
        this.hlogRoller.tryInterruptIfNecessary();
      } else {
        this.hlogRoller.interruptIfNecessary();
      }
    }
    if (this.metaHLogRoller != null) {
      if (queueFullDetected) {
        this.metaHLogRoller.tryInterruptIfNecessary();
      } else {
        this.metaHLogRoller.interruptIfNecessary();
      }
    }
    if (this.compactionChecker != null)
      this.compactionChecker.interrupt();
    if (this.healthCheckChore != null) {
      this.healthCheckChore.interrupt();
    }
    if (this.nonceManagerChore != null) {
      this.nonceManagerChore.interrupt();
    }
    if (this.queueFullDetector != null) {
      this.queueFullDetector.interrupt();
    }
    if (this.regionCompactor != null) {
      this.regionCompactor.interrupt();
    }
    if (this.accessCounter != null) {
      this.accessCounter.interrupt();
    }

    // Stop the quota manager
    if (rsQuotaManager != null) {
      rsQuotaManager.stop();
    }

    // Stop the snapshot and other procedure handlers, forcefully killing all running tasks
    if (rspmHost != null) {
      rspmHost.stop(this.abortRequested || this.killed);
    }

    if (this.killed) {
      // Just skip out w/o closing regions.  Used when testing.
    } else if (abortRequested) {
      if (this.fsOk) {
        closeUserRegions(abortRequested); // Don't leave any open file handles
      }
      LOG.info("aborting server " + this.serverNameFromMasterPOV);
    } else {
      closeUserRegions(abortRequested);
      closeAllScanners();
      LOG.info("stopping server " + this.serverNameFromMasterPOV);
    }
    // Interrupt catalog tracker here in case any regions being opened out in
    // handlers are stuck waiting on meta.
    if (this.catalogTracker != null) this.catalogTracker.stop();

    // Closing the compactSplit thread before closing meta regions
    if (!this.killed && containsMetaTableRegions()) {
      if (!abortRequested || this.fsOk) {
        if (this.compactSplitThread != null) {
          this.compactSplitThread.join();
          this.compactSplitThread = null;
        }
        closeMetaTableRegions(abortRequested);
      }
    }

    if (!this.killed && this.fsOk) {
      waitOnAllRegionsToClose(abortRequested);
      LOG.info("stopping server " + this.serverNameFromMasterPOV +
        "; all regions closed.");
    }

    //fsOk flag may be changed when closing regions throws exception.
    if (this.fsOk) {
      closeWAL(!abortRequested);
    }

    // Make sure the proxy is down.
    if (this.rssStub != null) {
      this.rssStub = null;
    }
    if (this.rpcClient != null) {
      this.rpcClient.close();
    }
    if (this.leases != null) {
      this.leases.close();
    }
    if (this.pauseMonitor != null) {
      this.pauseMonitor.stop();
    }
    if (this.jvmThreadMonitor != null) {
      this.jvmThreadMonitor.stop();
    }

    if (!killed) {
      join();
    }

    try {
      deleteMyEphemeralNode();
    } catch (KeeperException e) {
      LOG.warn("Failed deleting my ephemeral node", e);
    }
    // We may have failed to delete the znode at the previous step, but
    //  we delete the file anyway: a second attempt to delete the znode is likely to fail again.
    ZNodeClearer.deleteMyEphemeralNodeOnDisk();
    if (this.zooKeeper != null) {
      this.zooKeeper.close();
    }
    LOG.info("stopping server " + this.serverNameFromMasterPOV +
      "; zookeeper connection closed.");

    LOG.info(Thread.currentThread().getName() + " exiting");
  }

  public void moveOutRegions(String reason) {
    List<HRegionInfo> regionInfoList =
        getCopyOfOnlineRegionsSortedBySize().values().stream().map(HRegion::getRegionInfo)
            .collect(Collectors.toList());
    try (HBaseAdmin admin = new HBaseAdmin(HConnectionManager.getConnection(getConfiguration()))) {
      ServerName serverName = getServerName();

      for (HRegionInfo regionInfo : regionInfoList) {
        try {
          admin.move(regionInfo.getEncodedNameAsBytes(), null);
          LOG.info(
              "Success to move region " + regionInfo.getRegionNameAsString() + " from " + serverName
                  + " before exit for " + reason);

        } catch (IOException e) {
          LOG.warn(
              "Failed to move region " + regionInfo.getRegionNameAsString() + " from " + serverName
                  + " before exit for " + reason);
        }
      }
    } catch (IOException e) {
      LOG.warn("Failed to new HBaseAdmin when move regions before exit for " + reason, e);
    }
  }

  private void moveRegion(HRegionInfo region, ServerName source, ServerName dest, HBaseAdmin admin,
      String reason) {
	  try {
      byte[] destBytes = (dest == null ? null : Bytes.toBytes(dest.getServerName()));
		  admin.move(region.getEncodedNameAsBytes(), destBytes);
		  LOG.info(
				  "Success to move region " + region.getRegionNameAsString() + " from " + source
              + " before exit for " + reason);

	  } catch (IOException e) {
		  LOG.warn(
				  "Failed to move region " + region.getRegionNameAsString() + " from " + source
              + " before exit for " + reason);
	  }
  }

  private boolean containsMetaTableRegions() {
    return onlineRegions.containsKey(HRegionInfo.FIRST_META_REGIONINFO.getEncodedName());
  }

  private boolean areAllUserRegionsOffline() {
    if (getNumberOfOnlineRegions() > 2) return false;
    boolean allUserRegionsOffline = true;
    for (Map.Entry<String, HRegion> e: this.onlineRegions.entrySet()) {
      if (!e.getValue().getRegionInfo().isMetaTable()) {
        allUserRegionsOffline = false;
        break;
      }
    }
    return allUserRegionsOffline;
  }

  /**
   * @return Current write count for all online regions.
   */
  private long getWriteRequestCount() {
    int writeCount = 0;
    for (Map.Entry<String, HRegion> e: this.onlineRegions.entrySet()) {
      writeCount += e.getValue().getWriteRequestsCount();
    }
    return writeCount;
  }

  @VisibleForTesting
  public void tryRegionServerReport(long reportStartTime, long reportEndTime)
  throws IOException {
    RegionServerStatusService.BlockingInterface rss = rssStub;
    if (rss == null) {
      // the current server could be stopping.
      return;
    }
    ClusterStatusProtos.ServerLoad serverLoadPB = buildServerLoad(reportStartTime, reportEndTime);
    this.serverLoad = new ServerLoad(serverLoadPB);
    try {
      RegionServerReportRequest.Builder request = RegionServerReportRequest.newBuilder();
      ServerName sn = ServerName.parseVersionedServerName(
        this.serverNameFromMasterPOV.getVersionedBytes());
      request.setServer(ProtobufUtil.toServerName(sn));
      request.setLoad(serverLoadPB);
      reportResponse = rss.regionServerReport(null, request.build());
    } catch (ServiceException se) {
      LOG.warn("Try to report to hmaster failed", se);
      IOException ioe = ProtobufUtil.getRemoteException(se);
      if (ioe instanceof YouAreDeadException) {
        // This will be caught and handled as a fatal error in run()
        throw ioe;
      }
      if (rssStub == rss) {
        rssStub = null;
      }
      // Couldn't connect to the master, get location from zk and reconnect
      // Method blocks until new master is found or we are stopped
      createRegionServerStatusStub();
    }
  }

  ClusterStatusProtos.ServerLoad buildServerLoad(long reportStartTime, long reportEndTime) {
    // We're getting the MetricsRegionServerWrapper here because the wrapper computes requests
    // per second, and other metrics  As long as metrics are part of ServerLoad it's best to use
    // the wrapper to compute those numbers in one place.
    // In the long term most of these should be moved off of ServerLoad and the heart beat.
    // Instead they should be stored in an HBase table so that external visibility into HBase is
    // improved; Additionally the load balancer will be able to take advantage of a more complete
    // history.
    MetricsRegionServerWrapper regionServerWrapper = this.metricsRegionServer.getRegionServerWrapper();
    Collection<HRegion> regions = getOnlineRegionsLocalContext();
    MemoryUsage memory =
      ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();

    ClusterStatusProtos.ServerLoad.Builder serverLoad =
      ClusterStatusProtos.ServerLoad.newBuilder();
    serverLoad.setNumberOfRequests((int) regionServerWrapper.getRequestsPerSecond());
    serverLoad.setTotalNumberOfRequests((int) regionServerWrapper.getTotalRequestCount());
    serverLoad.setUsedHeapMB((int)(memory.getUsed() / 1024 / 1024));
    serverLoad.setMaxHeapMB((int) (memory.getMax() / 1024 / 1024));
    Set<String> coprocessors = this.hlog.getCoprocessorHost().getCoprocessors();
    for (String coprocessor : coprocessors) {
      serverLoad.addCoprocessors(
        Coprocessor.newBuilder().setName(coprocessor).build());
    }
    RegionLoad.Builder regionLoadBldr = RegionLoad.newBuilder();
    RegionSpecifier.Builder regionSpecifier = RegionSpecifier.newBuilder();
    long readRequestsPerSecond = 0;
    long writeRequestsPerSecond = 0;
    long readCellCountPerSecond = 0;
    long readRawCellCountPerSecond = 0;
    long scanCountPerSecond = 0;
    long scanRowsPerSecond = 0;
    for (HRegion region : regions) {
      RegionLoad load = createRegionLoad(region, regionLoadBldr, regionSpecifier);
      serverLoad.addRegionLoads(load);
      readRequestsPerSecond += load.getReadRequestsPerSecond();
      writeRequestsPerSecond += load.getWriteRequestsPerSecond();
      readCellCountPerSecond += load.getReadCellCountPerSecond();
      readRawCellCountPerSecond += load.getReadRawCellCountPerSecond();
      scanCountPerSecond += load.getScanCountPerSecond();
      scanRowsPerSecond += load.getScanRowsPerSecond();
    }
    serverLoad.setReadRequestsPerSecond(readRequestsPerSecond);
    serverLoad.setWriteRequestsPerSecond(writeRequestsPerSecond);
    serverLoad.setReadCellCountPerSecond(readCellCountPerSecond);
    serverLoad.setReadRawCellCountPerSecond(readRawCellCountPerSecond);
    serverLoad.setScanCountPerSecond(scanCountPerSecond);
    serverLoad.setScanRowsPerSecond(scanRowsPerSecond);
    serverLoad.setReportStartTime(reportStartTime);
    serverLoad.setReportEndTime(reportEndTime);
    if (this.infoServer != null) {
      serverLoad.setInfoServerPort(this.infoServer.getPort());
    } else {
      serverLoad.setInfoServerPort(-1);
    }

    // for the replicationLoad purpose. Only need to get from one service
    // either source or sink will get the same info
    ReplicationSourceService rsources = getReplicationSourceService();

    if (rsources != null) {
      // always refresh first to get the latest value
      ReplicationLoad rLoad = rsources.refreshAndGetReplicationLoad();
      if (rLoad != null) {
        serverLoad.setReplLoadSink(rLoad.getReplicationLoadSink());
        for (ClusterStatusProtos.ReplicationLoadSource rLS : rLoad.getReplicationLoadSourceList()) {
          serverLoad.addReplLoadSource(rLS);
        }
      }
    }
    serverLoad.addAllRegionServerTableLatency(buildRegionServerTableLatency());
    return serverLoad.build();
  }

  public List<MetricsSource> getReplicationSourceMetrics(){
    return replicationSourceHandler == null? null : replicationSourceHandler.getSourceMetrics();
  }
  
  private List<ClusterStatusProtos.RegionServerTableLatency> buildRegionServerTableLatency() {
    List<ClusterStatusProtos.RegionServerTableLatency> regionServerTableLatencies =
        new ArrayList<>();
    try {
      if (metricsRegionServer.getTableMetrics() != null
              && metricsRegionServer.getTableMetrics().getMetricsTableLatency() != null) {
        MetricsTableLatencies metricsTableLatencies =
                metricsRegionServer.getTableMetrics().getMetricsTableLatency();
        if (metricsTableLatencies instanceof MetricsTableLatenciesImpl) {
          MetricsTableLatenciesImpl tableLatenciesImpl =
                  (MetricsTableLatenciesImpl) metricsTableLatencies;
          Iterator<Entry<TableName, MetricsTableLatenciesImpl.TableHistograms>> iterator =
                  tableLatenciesImpl.getHistogramsByTable().entrySet().iterator();
          while (iterator.hasNext()) {
            Entry<TableName, MetricsTableLatenciesImpl.TableHistograms> entry = iterator.next();
            ClusterStatusProtos.RegionServerTableLatency.Builder builder =
                    ClusterStatusProtos.RegionServerTableLatency.newBuilder();
            builder.setTableName(entry.getKey().getNameAsString());
            MetricsTableLatenciesImpl.TableHistograms histograms = entry.getValue();
            long[] value = histograms.getTimeHisto.getOperationCountAndMeanAnd99PercentileTime();
            builder.setGetOperationCount(value[0]);
            builder.setGetTimeMean(value[1]);
            builder.setGetTime99Percentile(value[2]);
            value = histograms.putTimeHisto.getOperationCountAndMeanAnd99PercentileTime();
            builder.setPutOperationCount(value[0]);
            builder.setPutTimeMean(value[1]);
            builder.setPutTime99Percentile(value[2]);
            value = histograms.scanTimeHisto.getOperationCountAndMeanAnd99PercentileTime();
            builder.setScanOperationCount(value[0]);
            builder.setScanTimeMean(value[1]);
            builder.setScanTime99Percentile(value[2]);
            value = histograms.batchTimeHisto.getOperationCountAndMeanAnd99PercentileTime();
            builder.setBatchOperationCount(value[0]);
            builder.setBatchTimeMean(value[1]);
            builder.setBatchTime99Percentile(value[2]);
            value = histograms.deleteTimeHisto.getOperationCountAndMeanAnd99PercentileTime();
            builder.setDeleteOperationCount(value[0]);
            builder.setDeleteTimeMean(value[1]);
            value = histograms.appendTimeHisto.getOperationCountAndMeanAnd99PercentileTime();
            builder.setAppendOperationCount(value[0]);
            builder.setAppendTimeMean(value[1]);
            value = histograms.incrementTimeHisto.getOperationCountAndMeanAnd99PercentileTime();
            builder.setIncrementOperationCount(value[0]);
            builder.setIncrementTimeMean(value[1]);
            regionServerTableLatencies.add(builder.build());
          }
        }
      }
    } catch (Exception  e) {
      LOG.error("buildRegionServerTableLatency exception", e);
    }
    return regionServerTableLatencies;
  }

  String getOnlineRegionsAsPrintableString() {
    StringBuilder sb = new StringBuilder();
    for (HRegion r: this.onlineRegions.values()) {
      if (sb.length() > 0) sb.append(", ");
      sb.append(r.getRegionInfo().getEncodedName());
    }
    return sb.toString();
  }

  /**
   * Wait on regions close.
   */
  private void waitOnAllRegionsToClose(final boolean abort) {
    // Wait till all regions are closed before going out.
    int lastCount = -1;
    long previousLogTime = 0;
    Set<String> closedRegions = new HashSet<String>();
    while (!isOnlineRegionsEmpty()) {
      int count = getNumberOfOnlineRegions();
      // Only print a message if the count of regions has changed.
      if (count != lastCount) {
        // Log every second at most
        if (System.currentTimeMillis() > (previousLogTime + 1000)) {
          previousLogTime = System.currentTimeMillis();
          lastCount = count;
          LOG.info("Waiting on " + count + " regions to close");
          // Only print out regions still closing if a small number else will
          // swamp the log.
          if (count < 10 && LOG.isDebugEnabled()) {
            LOG.debug(this.onlineRegions);
          }
        }
      }
      // Ensure all user regions have been sent a close. Use this to
      // protect against the case where an open comes in after we start the
      // iterator of onlineRegions to close all user regions.
      for (Map.Entry<String, HRegion> e : this.onlineRegions.entrySet()) {
        HRegionInfo hri = e.getValue().getRegionInfo();
        if (!this.regionsInTransitionInRS.containsKey(hri.getEncodedNameAsBytes())
            && !closedRegions.contains(hri.getEncodedName())) {
          closedRegions.add(hri.getEncodedName());
          // Don't update zk with this close transition; pass false.
          closeRegionIgnoreErrors(hri, abort);
        }
      }
      // No regions in RIT, we could stop waiting now.
      if (this.regionsInTransitionInRS.isEmpty()) {
        if (!isOnlineRegionsEmpty()) {
          LOG.info("We were exiting though online regions are not empty," +
              " because some regions failed closing");
        }
        break;
      }
      Threads.sleep(200);
    }
  }

  private void closeWAL(final boolean delete) {
    if (this.hlogForMeta != null) {
      // All hlogs (meta and non-meta) are in the same directory. Don't call
      // closeAndDelete here since that would delete all hlogs not just the
      // meta ones. We will just 'close' the hlog for meta here, and leave
      // the directory cleanup to the follow-on closeAndDelete call.
      try {
        this.hlogForMeta.close();
      } catch (Throwable e) {
        LOG.error("Metalog close and delete failed", RemoteExceptionHandler.checkThrowable(e));
      }
    }
    if (this.hlog != null) {
      try {
        if (delete) {
          hlog.closeAndDelete();
        } else {
          hlog.close();
        }
      } catch (Throwable e) {
        LOG.error("Close and delete failed", RemoteExceptionHandler.checkThrowable(e));
      }
    }
  }

  private void closeAllScanners() {
    // Close any outstanding scanners. Means they'll get an UnknownScanner
    // exception next time they come in.
    for (Map.Entry<String, RegionScannerHolder> e : this.scanners.entrySet()) {
      try {
        e.getValue().s.close(false);
      } catch (IOException ioe) {
        LOG.warn("Closing scanner " + e.getKey(), ioe);
      }
    }
  }

  /*
   * Run init. Sets up hlog and starts up all server threads.
   *
   * @param c Extra configuration.
   */
  protected void handleReportForDutyResponse(final RegionServerStartupResponse c)
  throws IOException {
    try {
      for (NameStringPair e : c.getMapEntriesList()) {
        String key = e.getName();
        // The hostname the master sees us as.
        if (key.equals(HConstants.KEY_FOR_HOSTNAME_SEEN_BY_MASTER)) {
          String hostnameFromMasterPOV = e.getValue();
          this.serverNameFromMasterPOV = ServerName.valueOf(hostnameFromMasterPOV,
              this.isa.getPort(), this.startcode);
          if (!hostnameFromMasterPOV.equals(this.isa.getHostName())) {
            LOG.info("Master passed us a different hostname to use; was=" +
              this.isa.getHostName() + ", but now=" + hostnameFromMasterPOV);
          }
          continue;
        }
        String value = e.getValue();
        if (LOG.isDebugEnabled()) {
          LOG.debug("Config from master: " + key + "=" + value);
        }
        this.conf.set(key, value);
      }

      // hack! Maps DFSClient => RegionServer for logs.  HDFS made this
      // config param for task trackers, but we can piggyback off of it.
      if (this.conf.get("mapred.task.id") == null) {
        this.conf.set("mapred.task.id", "hb_rs_" +
          this.serverNameFromMasterPOV.toString());
      }
      // Set our ephemeral znode up in zookeeper now we have a name.
      createMyEphemeralNode();

      // Initialize the RegionServerCoprocessorHost now that our ephemeral
      // node was created, in case any coprocessors want to use ZooKeeper
      this.rsHost = new RegionServerCoprocessorHost(this, this.conf);

      // Save it in a file, this will allow to see if we crash
      ZNodeClearer.writeMyEphemeralNodeOnDisk(getMyEphemeralNodePath());

      // Master sent us hbase.rootdir to use. Should be fully qualified
      // path with file system specification included. Set 'fs.defaultFS'
      // to match the filesystem on hbase.rootdir else underlying hadoop hdfs
      // accessors will be going against wrong filesystem (unless all is set
      // to defaults).
      FSUtils.setFsDefault(this.conf, FSUtils.getRootDir(this.conf));
      // Get fs instance used by this RS.  Do we use checksum verification in the hbase? If hbase
      // checksum verification enabled, then automatically switch off hdfs checksum verification.
      boolean useHBaseChecksum = conf.getBoolean(HConstants.HBASE_CHECKSUM_VERIFICATION, true);
      this.fs = new HFileSystem(this.conf, useHBaseChecksum);
      this.rootDir = FSUtils.getRootDir(this.conf);
      this.tableDescriptors = new FSTableDescriptors(this.conf, this.fs, this.rootDir, true, false);
      this.hlog = setupWALAndReplication();
      // Init in here rather than in constructor after thread name has been set
      this.metricsRegionServer = new MetricsRegionServer(new MetricsRegionServerWrapperImpl(this), conf);
      // Metrics are up, now we can init the pause monitor
      this.pauseMonitor = new JvmPauseMonitor(conf, metricsRegionServer.getMetricsSource());
      pauseMonitor.start();

      spanReceiverHost = SpanReceiverHost.getInstance(getConfiguration());

      startServiceThreads();
      startHeapMemoryManager();

      if (conf.get(HConstants.CRYPTO_KEYCENTER_KEY) != null) {
        KeyCenterKeyProvider.loadCacheFromKeyCenter(conf);
      }

      LOG.info("Serving as " + this.serverNameFromMasterPOV +
        ", RpcServer on " + this.isa +
        ", sessionid=0x" +
        Long.toHexString(this.zooKeeper.getRecoverableZooKeeper().getSessionId()));
      isOnline = true;
    } catch (Throwable e) {
      this.isOnline = false;
      stop("Failed initialization");
      throw convertThrowableToIOE(cleanup(e, "Failed init"),
          "Region server startup failed");
    } finally {
      sleeper.skipSleepCycle();
    }
  }

  private void startHeapMemoryManager() {
    this.hMemManager = HeapMemoryManager.create(this);
    if (this.hMemManager != null) {
      this.hMemManager.start();
    }
  }

  private void createMyEphemeralNode() throws KeeperException, IOException {
    byte[] data = ProtobufUtil.prependPBMagic(rsInfo.build().toByteArray());
    ZKUtil.createEphemeralNodeAndWatch(this.zooKeeper,
      getMyEphemeralNodePath(), data);
  }

  private void deleteMyEphemeralNode() throws KeeperException {
    ZKUtil.deleteNode(this.zooKeeper, getMyEphemeralNodePath());
  }

  @Override
  public RegionServerAccounting getRegionServerAccounting() {
    return regionServerAccounting;
  }

  @Override
  public TableLockManager getTableLockManager() {
    return tableLockManager;
  }

  /*
   * @param r Region to get RegionLoad for.
   * @param regionLoadBldr the RegionLoad.Builder, can be null
   * @param regionSpecifier the RegionSpecifier.Builder, can be null
   * @return RegionLoad instance.
   *
   * @throws IOException
   */
  private RegionLoad createRegionLoad(final HRegion r, RegionLoad.Builder regionLoadBldr,
      RegionSpecifier.Builder regionSpecifier) {
    byte[] name = r.getRegionName();
    int stores = 0;
    int storefiles = 0;
    int storeUncompressedSizeMB = 0;
    int storefileSizeMB = 0;
    int memstoreSizeMB = (int) (r.memstoreSize.get() / 1024 / 1024);
    int storefileIndexSizeMB = 0;
    int rootIndexSizeKB = 0;
    int totalStaticIndexSizeKB = 0;
    int totalStaticBloomSizeKB = 0;
    long totalCompactingKVs = 0;
    long currentCompactedKVs = 0;
    List<ClusterStatusProtos.FamilyInfo> familyInfos = new ArrayList<>();
    synchronized (r.stores) {
      stores += r.stores.size();
      for (Store store : r.stores.values()) {
        storefiles += store.getStorefilesCount();
        storeUncompressedSizeMB += (int) (store.getStoreSizeUncompressed()
            / 1024 / 1024);
        storefileSizeMB += (int) (store.getStorefilesSize() / 1024 / 1024);
        storefileIndexSizeMB += (int) (store.getStorefilesIndexSize() / 1024 / 1024);
        CompactionProgress progress = store.getCompactionProgress();
        if (progress != null) {
          totalCompactingKVs += progress.totalCompactingKVs;
          currentCompactedKVs += progress.currentCompactedKVs;
        }

        rootIndexSizeKB +=
            (int) (store.getStorefilesIndexSize() / 1024);

        totalStaticIndexSizeKB +=
          (int) (store.getTotalStaticIndexSize() / 1024);

        totalStaticBloomSizeKB +=
          (int) (store.getTotalStaticBloomSize() / 1024);

        long rowCnt = 0;
        long kvCnt = 0;
        long delFamilyCnt = 0;
        long delKvCnt = 0;
        for (StoreFile storeFile : store.getStorefiles()) {
          rowCnt += storeFile.getReader().getRowCnt();
          kvCnt += storeFile.getReader().getKvCnt();
          delFamilyCnt += storeFile.getReader().getDeleteFamilyCnt();
          delKvCnt += storeFile.getReader().getDeleteKvCnt();
        }

        ClusterStatusProtos.FamilyInfo familyInfo = ClusterStatusProtos.FamilyInfo.newBuilder()
                .setFamilyname(store.getColumnFamilyName())
                .setRowCount(rowCnt)
                .setKvCount(kvCnt)
                .setDelFamilyCount(delFamilyCnt)
                .setDelKvCount(delKvCnt)
                .build();
        familyInfos.add(familyInfo);
      }
    }
    float dataLocality =
        r.getHDFSBlocksDistribution().getBlockLocalityIndex(serverName.getHostname());
    if (regionLoadBldr == null) {
      regionLoadBldr = RegionLoad.newBuilder();
    }
    if (regionSpecifier == null) {
      regionSpecifier = RegionSpecifier.newBuilder();
    }
    regionSpecifier.setType(RegionSpecifierType.REGION_NAME);
    regionSpecifier.setValue(ByteStringer.wrap(name));
    regionLoadBldr.setRegionSpecifier(regionSpecifier.build())
      .setStores(stores)
      .setStorefiles(storefiles)
      .setStoreUncompressedSizeMB(storeUncompressedSizeMB)
      .setStorefileSizeMB(storefileSizeMB)
      .setMemstoreSizeMB(memstoreSizeMB)
      .setStorefileIndexSizeMB(storefileIndexSizeMB)
      .setRootIndexSizeKB(rootIndexSizeKB)
      .setTotalStaticIndexSizeKB(totalStaticIndexSizeKB)
      .setTotalStaticBloomSizeKB(totalStaticBloomSizeKB)
      .setReadRequestsCount(r.readRequestsCount.get())
      .setWriteRequestsCount(r.writeRequestsCount.get())
      .setReadRequestsPerSecond(r.getReadRequestsPerSecond())
      .setWriteRequestsPerSecond(r.getWriteRequestsPerSecond())
      .setReadCellCountPerSecond(r.getReadCellCountPerSecond())
      .setReadRawCellCountPerSecond(r.getReadRawCellCountPerSecond())
      .setTotalCompactingKVs(totalCompactingKVs)
      .setCurrentCompactedKVs(currentCompactedKVs)
      .setCompleteSequenceId(r.completeSequenceId)
      .setDataLocality(dataLocality)
      .setGetRequestsCount(r.getRequestsCount.get())
      .setReadRequestsByCapacityUnitPerSecond(r.getReadRequestsByCapacityUnitPerSecond())
      .setWriteRequestsByCapacityUnitPerSecond(r.getWriteRequestsByCapacityUnitPerSecond())
      .setThrottledReadRequestsCount(r.getThrottleadReadCount())
      .setThrottledWriteRequestsCount(r.getThrottledWriteCount())
      .setScanCountPerSecond(r.getScanCountPerSecond())
      .setScanRowsPerSecond(r.getScanRowsPerSecond())
      .setUserReadRequestsPerSecond(r.getUserReadRequestsPerSecond())
      .setUserWriteRequestsPerSecond(r.getUserWriteRequestsPerSecond())
      .setUserReadRequestsByCapacityUnitPerSecond(r.getUserReadRequestsByCapacityUnitPerSecond())
      .setUserWriteRequestsByCapacityUnitPerSecond(r.getUserWriteRequestsByCapacityUnitPerSecond());
    regionLoadBldr.clearFamilyInfo();
    regionLoadBldr.addAllFamilyInfo(familyInfos);

    return regionLoadBldr.build();
  }

  /**
   * @param encodedRegionName
   * @return An instance of RegionLoad.
   */
  public RegionLoad createRegionLoad(final String encodedRegionName) {
    HRegion r = null;
    r = this.onlineRegions.get(encodedRegionName);
    return r != null ? createRegionLoad(r, null, null) : null;
  }

  /*
   * Inner class that runs on a long period checking if regions need compaction.
   */
  private static class CompactionChecker extends Chore {
    private final HRegionServer instance;
    private final int majorCompactPriority;
    private final static int DEFAULT_PRIORITY = Integer.MAX_VALUE;
    private long iteration = 0;

    CompactionChecker(final HRegionServer h, final int sleepTime,
        final Stoppable stopper) {
      super("CompactionChecker", sleepTime, h);
      this.instance = h;
      LOG.info(this.getName() + " runs every " + StringUtils.formatTime(sleepTime));

      /* MajorCompactPriority is configurable.
       * If not set, the compaction will use default priority.
       */
      this.majorCompactPriority = this.instance.conf.
        getInt("hbase.regionserver.compactionChecker.majorCompactPriority",
        DEFAULT_PRIORITY);
    }

    @Override
    protected void chore() {
      for (HRegion r : this.instance.onlineRegions.values()) {
        if (r == null)
          continue;
        for (Store s : r.getStores().values()) {
          try {
            long multiplier = s.getCompactionCheckMultiplier();
            assert multiplier > 0;
            if (iteration % multiplier != 0) continue;
            if (s.needsCompaction()) {
              // Queue a compaction. Will recognize if major is needed.
              this.instance.compactSplitThread.requestSystemCompaction(r, s, getName()
                  + " requests compaction");
            } else if (s.isMajorCompaction()) {
              if (majorCompactPriority == DEFAULT_PRIORITY
                  || majorCompactPriority > r.getCompactPriority()) {
                this.instance.compactSplitThread.requestCompaction(r, s, getName()
                    + " requests major compaction; use default priority", null);
              } else {
                this.instance.compactSplitThread.requestCompaction(r, s, getName()
                    + " requests major compaction; use configured priority",
                  this.majorCompactPriority, null);
              }
            }
          } catch (IOException e) {
            LOG.warn("Failed major compaction check on " + r, e);
          }
        }
      }
      iteration = (iteration == Long.MAX_VALUE) ? 0 : (iteration + 1);
    }
  }

  class PeriodicMemstoreFlusher extends Chore {
    final HRegionServer server;
    final static int RANGE_OF_DELAY = 20000; //millisec
    final static int MIN_DELAY_TIME = 3000; //millisec
    public PeriodicMemstoreFlusher(int cacheFlushInterval, final HRegionServer server) {
      super(server.getServerName() + "-MemstoreFlusherChore", cacheFlushInterval, server);
      this.server = server;
    }

    @Override
    protected void chore() {
      for (HRegion r : this.server.onlineRegions.values()) {
        if (r == null)
          continue;
        if (r.shouldFlush()) {
          FlushRequester requester = server.getFlushRequester();
          if (requester != null) {
            long randomDelay = rand.nextInt(RANGE_OF_DELAY) + MIN_DELAY_TIME;
            LOG.info(getName() + " requesting flush for region " + r.getRegionNameAsString() +
                " after a delay of " + randomDelay);
            //Throttle the flushes by putting a delay. If we don't throttle, and there
            //is a balanced write-load on the regions in a table, we might end up
            //overwhelming the filesystem with too many flushes at once.
            requester.requestDelayedFlush(r, randomDelay);
          }
        }
      }
    }
  }

  class QueueFullDetector extends Chore {
    final HRegionServer server;
    private int densePeriod;
    private int denseCheckNum;
    private int sampleThreshold;
    private int rejectThreshold;
    private List<QueueCounter> queueCounters;
    private int continuousQueueFullCount;
    private int continuousQueueFullCountThreshold;

    public QueueFullDetector(HRegionServer server, Configuration conf) {
      super("QueueFullDetector", conf.getInt("hbase.regionserver.queuefull.detector.sparseperiod",
        5000), server);
      this.server = server;
      this.densePeriod = conf.getInt("hbase.regionserver.queuefull.detector.denseperiod", 1000);
      this.denseCheckNum = conf.getInt("hbase.regionserver.queuefull.detector.densechecknum", 100);
      this.sampleThreshold = conf.getInt("hbase.regionserver.queuefull.detector.samplethreshold",
        95);
      if (sampleThreshold < 10 || sampleThreshold >= 100) {
        LOG.warn("Sample threshold of queue full detector should be in range (10, 100), but the configured value is "
            + sampleThreshold + ", will use 95 instead");
        sampleThreshold = 95;
      }
      this.rejectThreshold = conf.getInt("hbase.regionserver.queuefull.detector.rejectthreshold",
        95);
      if (rejectThreshold < 10 || rejectThreshold >= 100) {
        LOG.warn("Reject threshold of queue full detector should be in range (10, 100), but the configured value is "
            + rejectThreshold + ", will use 95 instead");
        rejectThreshold = 95;
      }
      this.continuousQueueFullCountThreshold =
          conf.getInt("hbase.regionserver.queuefull.detector.continuous.threshold", 100);


      this.queueCounters = server.getRpcServer().getScheduler().getQueueCounters();

      LOG.info("QueueFullDetector is started, denseCheckNum=" + denseCheckNum + ", densePeriod="
          + densePeriod + ", sampleThreshold=" + sampleThreshold + ", rejectThreshold="
          + rejectThreshold);
    }

    private void checkQueueFull(QueueCounter queueCounter) {
      boolean queueFull = queueCounter.getQueueFull();
      if (!queueFull) {
        continuousQueueFullCount = 0;
        return;
      }
      ++continuousQueueFullCount;
      // Found "queue full" events, start dense detecting
      LOG.warn("Detected queue full events for " + queueCounter
          + ",  start dense checking, continuousQueueFullCount=" + continuousQueueFullCount);
      int queueFullNum = 0;
      int maxNotHit = Math.max((int) (denseCheckNum * (100 - sampleThreshold) / 100.0), 1);
      long beforeCheckRequestCount = queueCounter.getIncomeRequestCount();
      long beforeCheckRejectedRequestCount = queueCounter.getRejectedRequestCount();
      // To detect queue full events in a higher frequency
      for (int i = 0; i < denseCheckNum; i++) {
        try {
          Thread.sleep(densePeriod);
        } catch (InterruptedException e) {
          // Check if we should stop after the try-catch block
        }
        if (isStopping() || isStopped()) {
          LOG.info("Exit queue full dense checking for " + queueCounter
              + ", because region server is stopping or stopped!");
          return;
        }
        if (queueCounter.getQueueFull()) {
          queueFullNum++;
        }
        int notHit = i + 1 - queueFullNum;
        if (notHit > maxNotHit) {
          LOG.info("Exit queue full dense checking for " + queueCounter + ", queueFullNum= "
              + queueFullNum + ", notHit=" + notHit + ", maxNotHit=" + maxNotHit);
          return;
        }
      }

      long afterCheckRequestCount = queueCounter.getIncomeRequestCount();
      long afterCheckRejectedRequestCount = queueCounter.getRejectedRequestCount();
      long newRequestCount = afterCheckRequestCount - beforeCheckRequestCount;
      long newRejectedRequestCount =
          afterCheckRejectedRequestCount - beforeCheckRejectedRequestCount;
      StringBuilder sb = new StringBuilder();
      sb.append("After dense checking for ").append(queueCounter).append(", queueFullNum is ")
          .append(queueFullNum);
      sb.append(", new incoming requests count is ").append(newRequestCount)
          .append(", rejected requests count is ").append(newRejectedRequestCount);
      // If the following conditions are detected, we should exit gracefully:
      // a. the percentage of queueFullNum has reached the configured threshold
      // b. the region server is not idle (i.e. there are new incoming rpc)
      // c. the percentage of rejected new incoming request in this period (due to queue full) has
      // reached the configured threshold
      // d. the dense check is executed frequently but not exit, e.g queue full, drop timeout,
      // queue full and drop timeout again and again.
      boolean shouldExit = isUnrecoverable(queueFullNum, newRequestCount, newRejectedRequestCount);
      sb.append(", shouldExit is ").append(shouldExit);
      LOG.info(sb.toString());
      if (shouldExit) {
        ThreadInfoUtils.logThreadInfo("Thread dump from QueueFullDetector", true);
        queueFullDetected = true;
        moveOutRegions(" queue full");
        sendEmailWhenAbort();
        abort("Detected queue full and canot come back to normal state in a long duration");
      }

    }

    private boolean isUnrecoverable(int queueFullNum, long newRequestCount,
        long newRejectedRequestCount) {
      return
          (queueFullDetectedFrequently(queueFullNum) && tooManyNewRequestsRejected(newRequestCount,
              newRejectedRequestCount)) || denseCheckFrequently();
    }

    private boolean denseCheckFrequently() {
      return continuousQueueFullCount > continuousQueueFullCountThreshold;
    }

    private boolean queueFullDetectedFrequently(int queueFullNum) {
      return queueFullNum * 100 > sampleThreshold * denseCheckNum;
    }

    private boolean tooManyNewRequestsRejected(long newRequestCount, long newRejectedRequestCount) {
      return newRequestCount > 0 && (newRejectedRequestCount * 100
          > rejectThreshold * newRequestCount);
    }


    private void sendEmailWhenAbort() {
      if ((clusterName != null) && !"".equals(clusterName)) {
        String mailBody = "Cluster " + clusterName + " abort regionserver " + serverName + " "
            + "because of full queue";
        MailUtils.sendMail(HConstants.MAIL_TO,
            "QueueFullDetector for cluster " + clusterName + " abort regionserver " + serverName,
            mailBody);
      }
    }

    @Override
    protected void chore() {
      for (QueueCounter queueCounter : queueCounters) {
        checkQueueFull(queueCounter);
      }
    }
  }

  /**
   * Report the status of the server. A server is online once all the startup is
   * completed (setting up filesystem, starting service threads, etc.). This
   * method is designed mostly to be useful in tests.
   *
   * @return true if online, false if not.
   */
  public boolean isOnline() {
    return isOnline;
  }

  /**
   * Setup WAL log and replication if enabled.
   * Replication setup is done in here because it wants to be hooked up to WAL.
   * @return A WAL instance.
   * @throws IOException
   */
  private HLog setupWALAndReplication() throws IOException {
    final Path oldLogDir = new Path(rootDir, HConstants.HREGION_OLDLOGDIR_NAME);
    final String logName
      = HLogUtil.getHLogDirectoryName(this.serverNameFromMasterPOV.toString());

    Path logdir = new Path(rootDir, logName);
    if (LOG.isDebugEnabled()) LOG.debug("logdir=" + logdir);
    if (this.fs.exists(logdir)) {
      throw new RegionServerRunningException("Region server has already " +
        "created directory at " + this.serverNameFromMasterPOV.toString());
    }

    // Instantiate replication manager if replication enabled.  Pass it the
    // log directories.
    createNewReplicationInstance(conf, this, this.fs, logdir, oldLogDir);

    return instantiateHLog(rootDir, logName);
  }

  private HLog getMetaWAL() throws IOException {
    if (this.hlogForMeta != null) return this.hlogForMeta;
    final String logName = HLogUtil.getHLogDirectoryName(this.serverNameFromMasterPOV.toString());
    Path logdir = new Path(rootDir, logName);
    if (LOG.isDebugEnabled()) LOG.debug("logdir=" + logdir);
    this.hlogForMeta = HLogFactory.createMetaHLog(this.fs.getBackingFs(), rootDir, logName,
      this.conf, getMetaWALActionListeners(), this.serverNameFromMasterPOV.toString());
    return this.hlogForMeta;
  }

  /**
   * Called by {@link #setupWALAndReplication()} creating WAL instance.
   * @param rootdir
   * @param logName
   * @return WAL instance.
   * @throws IOException
   */
  protected HLog instantiateHLog(Path rootdir, String logName) throws IOException {
    return HLogFactory.createHLog(this.fs.getBackingFs(), rootdir, logName, this.conf,
      getWALActionListeners(), this.serverNameFromMasterPOV.toString());
  }

  /**
   * Called by {@link #instantiateHLog(Path, String)} setting up WAL instance.
   * Add any {@link WALActionsListener}s you want inserted before WAL startup.
   * @return List of WALActionsListener that will be passed in to
   * {@link org.apache.hadoop.hbase.regionserver.wal.FSHLog} on construction.
   */
  protected List<WALActionsListener> getWALActionListeners() {
    List<WALActionsListener> listeners = new ArrayList<WALActionsListener>();
    // Log roller.
    this.hlogRoller = new LogRoller(this, this);
    listeners.add(this.hlogRoller);
    if (this.replicationSourceHandler != null &&
        this.replicationSourceHandler.getWALActionsListener() != null) {
      // Replication handler is an implementation of WALActionsListener.
      listeners.add(this.replicationSourceHandler.getWALActionsListener());
    }
    return listeners;
  }

  protected List<WALActionsListener> getMetaWALActionListeners() {
    List<WALActionsListener> listeners = new ArrayList<WALActionsListener>();
    // Using a tmp log roller to ensure metaLogRoller is alive once it is not
    // null
    MetaLogRoller tmpLogRoller = new MetaLogRoller(this, this);
    String n = Thread.currentThread().getName();
    Threads.setDaemonThreadRunning(tmpLogRoller.getThread(),
        n + "-MetaLogRoller", uncaughtExceptionHandler);
    this.metaHLogRoller = tmpLogRoller;
    tmpLogRoller = null;
    listeners.add(this.metaHLogRoller);
    return listeners;
  }

  protected LogRoller getLogRoller() {
    return hlogRoller;
  }

  public MetricsRegionServer getMetrics() {
    return this.metricsRegionServer;
  }

  /**
   * @return Master address tracker instance.
   */
  public MasterAddressTracker getMasterAddressTracker() {
    return this.masterAddressTracker;
  }

  /*
   * Start maintenance Threads, Server, Worker and lease checker threads.
   * Install an UncaughtExceptionHandler that calls abort of RegionServer if we
   * get an unhandled exception. We cannot set the handler on all threads.
   * Server's internal Listener thread is off limits. For Server, if an OOME, it
   * waits a while then retries. Meantime, a flush or a compaction that tries to
   * run should trigger same critical condition and the shutdown will run. On
   * its way out, this server will shut down Server. Leases are sort of
   * inbetween. It has an internal thread that while it inherits from Chore, it
   * keeps its own internal stop mechanism so needs to be stopped by this
   * hosting server. Worker logs the exception and exits.
   */
  private void startServiceThreads() throws IOException {
    String n = Thread.currentThread().getName();
    // Start executor services
    this.service = new ExecutorService(getServerName().toShortString());
    this.service.startExecutorService(ExecutorType.RS_OPEN_REGION,
      conf.getInt("hbase.regionserver.executor.openregion.threads", 3));
    this.service.startExecutorService(ExecutorType.RS_OPEN_META,
      conf.getInt("hbase.regionserver.executor.openmeta.threads", 1));
    this.service.startExecutorService(ExecutorType.RS_CLOSE_REGION,
      conf.getInt("hbase.regionserver.executor.closeregion.threads", 3));
    this.service.startExecutorService(ExecutorType.RS_CLOSE_META,
      conf.getInt("hbase.regionserver.executor.closemeta.threads", 1));
    if (conf.getBoolean(StoreScanner.STORESCANNER_PARALLEL_SEEK_ENABLE, false)) {
      this.service.startExecutorService(ExecutorType.RS_PARALLEL_SEEK,
        conf.getInt("hbase.storescanner.parallel.seek.threads", 10));
    }
    this.service.startExecutorService(ExecutorType.RS_LOG_REPLAY_OPS,
      conf.getInt("hbase.regionserver.wal.max.splitters", SplitLogWorker.DEFAULT_MAX_SPLITTERS));

    Threads.setDaemonThreadRunning(this.hlogRoller.getThread(), n + ".logRoller",
        uncaughtExceptionHandler);
    this.cacheFlusher.start(uncaughtExceptionHandler);
    Threads.setDaemonThreadRunning(this.compactionChecker.getThread(), n +
      ".compactionChecker", uncaughtExceptionHandler);
    Threads.setDaemonThreadRunning(this.periodicFlusher.getThread(), n +
        ".periodicFlusher", uncaughtExceptionHandler);
    if (this.queueFullDetector != null) {
      Threads.setDaemonThreadRunning(this.queueFullDetector.getThread(), n + ".queueFullDetector",
        uncaughtExceptionHandler);
    }
    if (this.healthCheckChore != null) {
      Threads.setDaemonThreadRunning(this.healthCheckChore.getThread(), n + ".healthChecker",
            uncaughtExceptionHandler);
    }
    if (this.nonceManagerChore != null) {
      Threads.setDaemonThreadRunning(this.nonceManagerChore.getThread(), n + ".nonceCleaner",
            uncaughtExceptionHandler);
    }
    if (this.regionCompactor != null) {
      Threads.setDaemonThreadRunning(this.regionCompactor.getThread(), n +
        ".regionCompactor", uncaughtExceptionHandler);
    }
    if (this.accessCounter != null) {
      Threads.setDaemonThreadRunning(this.accessCounter.getThread(), n + ".accessCounter",
        uncaughtExceptionHandler);
    }

    Threads.setDaemonThreadRunning(leases.getThread(), n + ".leaseChecker",
      uncaughtExceptionHandler);

    if (this.replicationSourceHandler == this.replicationSinkHandler &&
        this.replicationSourceHandler != null) {
      this.replicationSourceHandler.startReplicationService();
    } else {
      if (this.replicationSourceHandler != null) {
        this.replicationSourceHandler.startReplicationService();
      }
      if (this.replicationSinkHandler != null) {
        this.replicationSinkHandler.startReplicationService();
      }
    }

    // Start Server.  This service is like leases in that it internally runs
    // a thread.
    this.rpcServer.start();

    // Create the log splitting worker and start it
    // set a smaller retries to fast fail otherwise splitlogworker could be blocked for
    // quite a while inside HConnection layer. The worker won't be available for other
    // tasks even after current task is preempted after a split task times out.
    Configuration sinkConf = HBaseConfiguration.create(conf);
    sinkConf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER,
      conf.getInt("hbase.log.replay.retries.number", 8)); // 8 retries take about 23 seconds
    sinkConf.setInt(HConstants.HBASE_RPC_TIMEOUT_KEY,
      conf.getInt("hbase.log.replay.rpc.timeout", 30000)); // default 30 seconds
    sinkConf.setInt(HConstants.HBASE_CLIENT_SERVERSIDE_RETRIES_MULTIPLIER, 1);
    this.splitLogWorker = new SplitLogWorker(this.zooKeeper, sinkConf, this, this);
    splitLogWorker.start();
  }

  /**
   * Puts up the webui.
   * @return Returns final port -- maybe different from what we started with.
   * @throws IOException
   */
  private int putUpWebUI() throws IOException {
    int port = this.conf.getInt(HConstants.REGIONSERVER_INFO_PORT, 60030);
    // -1 is for disabling info server
    if (port < 0) return port;
    String addr = this.conf.get("hbase.regionserver.info.bindAddress", "0.0.0.0");
    // check if auto port bind enabled
    boolean auto = this.conf.getBoolean(HConstants.REGIONSERVER_INFO_PORT_AUTO,
        false);
    while (true) {
      try {
        this.infoServer = new InfoServer("regionserver", addr, port, false, this.conf);
        this.infoServer.addServlet("status", "/rs-status", RSStatusServlet.class);
        this.infoServer.addServlet("dump", "/dump", RSDumpServlet.class);
        this.infoServer.setAttribute(REGIONSERVER, this);
        this.infoServer.setAttribute(REGIONSERVER_CONF, conf);
        this.infoServer.start();
        break;
      } catch (BindException e) {
        if (!auto) {
          // auto bind disabled throw BindException
          LOG.error("Failed binding http info server to port: " + port);
          throw e;
        }
        // auto bind enabled, try to use another port
        LOG.info("Failed binding http info server to port: " + port);
        port++;
      }
    }
    return this.infoServer.getPort();
  }

  /*
   * Verify that server is healthy
   */
  private boolean isHealthy() {
    if (!fsOk) {
      // File system problem
      return false;
    }
    // Verify that all threads are alive
    if (!(leases.isAlive()
        && cacheFlusher.isAlive() && hlogRoller.isAlive()
        && this.compactionChecker.isAlive()
        && this.periodicFlusher.isAlive()
        && this.regionCompactor.isAlive())) {
      stop("One or more threads are no longer alive -- stop");
      return false;
    }
    if (metaHLogRoller != null && !metaHLogRoller.isAlive()) {
      stop("Meta HLog roller thread is no longer alive -- stop");
      return false;
    }
    return true;
  }

  public HLog getWAL() {
    try {
      return getWAL(null);
    } catch (IOException e) {
      LOG.warn("getWAL threw exception " + e);
      return null;
    }
  }

  @Override
  public HLog getWAL(HRegionInfo regionInfo) throws IOException {
    //TODO: at some point this should delegate to the HLogFactory
    //currently, we don't care about the region as much as we care about the
    //table.. (hence checking the tablename below)
    //_ROOT_ and hbase:meta regions have separate WAL.
    if (regionInfo != null && regionInfo.isMetaTable()) {
      return getMetaWAL();
    }
    return this.hlog;
  }

  @Override
  public CatalogTracker getCatalogTracker() {
    return this.catalogTracker;
  }

  @Override
  public void stop(final String msg) {
    if (!this.stopped) {
      try {
        if (this.rsHost != null) {
          this.rsHost.preStop(msg);
        }
        this.stopped = true;
        LOG.info("STOPPED: " + msg);
        // Wakes run() if it is sleeping
        sleeper.skipSleepCycle();
      } catch (IOException exp) {
        LOG.warn("The region server did not stop", exp);
      }
    }
  }

  public void waitForServerOnline(){
    while (!isOnline() && !isStopped()){
       sleeper.sleep();
    }
  }

  @Override
  public void postOpenDeployTasks(final HRegion r, final CatalogTracker ct)
  throws KeeperException, IOException {
    checkOpen();
    LOG.info("Post open deploy tasks for region=" + r.getRegionNameAsString());
    // Do checks to see if we need to compact (references or too many files)
    for (Store s : r.getStores().values()) {
      if (s.hasReferences() || s.needsCompaction()) {
       this.compactSplitThread.requestSystemCompaction(r, s, "Opening Region");
      }
    }
    long openSeqNum = r.getOpenSeqNum();
    if (openSeqNum == HConstants.NO_SEQNUM) {
      // If we opened a region, we should have read some sequence number from it.
      LOG.error("No sequence number found when opening " + r.getRegionNameAsString());
      openSeqNum = 0;
    }

    // Update flushed sequence id of a recovering region in ZK
    updateRecoveringRegionLastFlushedSequenceId(r);

    if (useZKForAssignment) {
      if (r.getRegionInfo().isMetaRegion()) {
        LOG.info("Updating zk with meta location");
        // The state field is for zk less assignment 
        // For zk assignment, always set it to OPEN
        MetaRegionTracker.setMetaLocation(getZooKeeper(), this.serverNameFromMasterPOV, State.OPEN);
      } else {
        MetaEditor.updateRegionLocation(ct, r.getRegionInfo(), this.serverNameFromMasterPOV,
          openSeqNum);
      }
    }
     if (!useZKForAssignment
        && !reportRegionStateTransition(TransitionCode.OPENED, openSeqNum, r.getRegionInfo())) {
      throw new IOException("Failed to report opened region to master: "
          + r.getRegionNameAsString());
    }

    LOG.info("Finished post open deploy task for " + r.getRegionNameAsString());

  }

  @Override
  public RpcServerInterface getRpcServer() {
    return rpcServer;
  }

  /**
   * Cause the server to exit without closing the regions it is serving, the log
   * it is using and without notifying the master. Used unit testing and on
   * catastrophic events such as HDFS is yanked out from under hbase or we OOME.
   *
   * @param reason
   *          the reason we are aborting
   * @param cause
   *          the exception that caused the abort, or null
   */
  @Override
  public void abort(String reason, Throwable cause) {
    String msg = "ABORTING region server " + this + ": " + reason;
    if (cause != null) {
      LOG.fatal(msg, cause);
    } else {
      LOG.fatal(msg);
    }
    this.abortRequested = true;
    // HBASE-4014: show list of coprocessors that were loaded to help debug
    // regionserver crashes.Note that we're implicitly using
    // java.util.HashSet's toString() method to print the coprocessor names.
    LOG.fatal("RegionServer abort: loaded coprocessors are: " +
        CoprocessorHost.getLoadedCoprocessors());
    // Do our best to report our abort to the master, but this may not work
    try {
      if (cause != null) {
        msg += "\nCause:\n" + StringUtils.stringifyException(cause);
      }
      // Report to the master but only if we have already registered with the master.
      if (rssStub != null && this.serverNameFromMasterPOV != null) {
        ReportRSFatalErrorRequest.Builder builder =
          ReportRSFatalErrorRequest.newBuilder();
        ServerName sn =
          ServerName.parseVersionedServerName(this.serverNameFromMasterPOV.getVersionedBytes());
        builder.setServer(ProtobufUtil.toServerName(sn));
        builder.setErrorMessage(msg);
        rssStub.reportRSFatalError(null, builder.build());
      }
    } catch (Throwable t) {
      LOG.warn("Unable to report fatal error to master", t);
    }
    stop(reason);
  }

  /**
   * @see HRegionServer#abort(String, Throwable)
   */
  public void abort(String reason) {
    abort(reason, null);
  }

  public void abortIfFileSystemAvailable(String why, Throwable e) {
    if (FSUtils.isFileSystemAvailable(this.fs)) {
      abort(why, e);
    } else {
      // Just log here but not abort regionserver.
      LOG.error(why + ", but filesystem is not available so not abort regionserver!!!", e);
    }
  }

  @Override
  public boolean isAborted() {
    return this.abortRequested;
  }

  /*
   * Simulate a kill -9 of this server. Exits w/o closing regions or cleaninup
   * logs but it does close socket in case want to bring up server on old
   * hostname+port immediately.
   */
  protected void kill() {
    this.killed = true;
    abort("Simulated kill");
  }

  /**
   * Wait on all threads to finish. Presumption is that all closes and stops
   * have already been called.
   */
  protected void join() {
    if (this.nonceManagerChore != null) {
      Threads.shutdown(this.nonceManagerChore.getThread());
    }
    if (this.compactionChecker != null) {
      Threads.shutdown(this.compactionChecker.getThread());
    }
    if (regionCompactor != null) {
      Threads.shutdown(this.regionCompactor.getThread());
    }
    if (accessCounter != null) {
      Threads.shutdown(this.accessCounter.getThread());
    }
    if (this.periodicFlusher != null) {
      Threads.shutdown(this.periodicFlusher.getThread());
    }
    if (queueFullDetector != null) {
      Threads.shutdown(queueFullDetector.getThread());
    }
    if (this.cacheFlusher != null) {
      this.cacheFlusher.join();
    }
    if (this.healthCheckChore != null) {
      Threads.shutdown(this.healthCheckChore.getThread());
    }
    if (this.spanReceiverHost != null) {
      this.spanReceiverHost.closeReceivers();
    }
    if (this.hlogRoller != null) {
      Threads.shutdown(this.hlogRoller.getThread());
    }
    if (this.metaHLogRoller != null) {
      Threads.shutdown(this.metaHLogRoller.getThread());
    }
    if (this.compactSplitThread != null) {
      this.compactSplitThread.join();
    }
    if (this.service != null) this.service.shutdown();
    if (this.replicationSourceHandler != null &&
        this.replicationSourceHandler == this.replicationSinkHandler) {
      this.replicationSourceHandler.stopReplicationService();
    } else {
      if (this.replicationSourceHandler != null) {
        this.replicationSourceHandler.stopReplicationService();
      }
      if (this.replicationSinkHandler != null) {
        this.replicationSinkHandler.stopReplicationService();
      }
    }
  }

  @Override
  public boolean reportRegionStateTransition(TransitionCode code, HRegionInfo... hris) {
    return reportRegionStateTransition(code, HConstants.NO_SEQNUM, hris);
  }

  @Override
  public boolean reportRegionStateTransition(TransitionCode code, long openSeqNum, HRegionInfo... hris) {
    ReportRegionStateTransitionRequest.Builder builder = ReportRegionStateTransitionRequest.newBuilder();
    builder.setServer(ProtobufUtil.toServerName(serverName));
    RegionStateTransition.Builder transition = builder.addTransitionBuilder();
    transition.setTransitionCode(code);
    if (code == TransitionCode.OPENED && openSeqNum >= 0) {
      transition.setOpenSeqNum(openSeqNum);
    }
    for (HRegionInfo hri : hris) {
      transition.addRegionInfo(HRegionInfo.convert(hri));
    }
    ReportRegionStateTransitionRequest request = builder.build();
    while (keepLooping()) {
      RegionServerStatusService.BlockingInterface rss = rssStub;
      try {
        if (rss == null) {
          createRegionServerStatusStub();
          continue;
        }
        ReportRegionStateTransitionResponse response = rss.reportRegionStateTransition(null, request);
        if (response.hasErrorMessage()) {
          LOG.info("Failed to transition " + hris[0] + " to " + code + ": "
              + response.getErrorMessage());
          return false;
        }
        return true;
      } catch (ServiceException se) {
        IOException ioe = ProtobufUtil.getRemoteException(se);
        LOG.info("Failed to report region transition, will retry", ioe);
        if (rssStub == rss) {
          rssStub = null;
        }
      }
    }
    return false;
  }

  /**
   * Get the current master from ZooKeeper and open the RPC connection to it.
   * To get a fresh connection, the current rssStub must be null.
   * Method will block until a master is available. You can break from this
   * block by requesting the server stop.
   *
   * @return master + port, or null if server has been stopped
   */
  @VisibleForTesting
  protected synchronized ServerName createRegionServerStatusStub() {
    if (rssStub != null) {
      return masterAddressTracker.getMasterAddress();
    }
    ServerName sn = null;
    long previousLogTime = 0;
    RegionServerStatusService.BlockingInterface master = null;
    boolean refresh = false; // for the first time, use cached data
    RegionServerStatusService.BlockingInterface intf = null;
    while (keepLooping() && master == null) {
      sn = this.masterAddressTracker.getMasterAddress(refresh);
      if (sn == null) {
        if (!keepLooping()) {
          // give up with no connection.
          LOG.debug("No master found and cluster is stopped; bailing out");
          return null;
        }
        LOG.debug("No master found; retry");
        previousLogTime = System.currentTimeMillis();
        refresh = true; // let's try pull it from ZK directly
        sleeper.sleep();
        continue;
      }

      new InetSocketAddress(sn.getHostname(), sn.getPort());
      try {
        BlockingRpcChannel channel =
            this.rpcClient.createBlockingRpcChannel(sn, userProvider.getCurrent(), this.rpcTimeout);
        intf = RegionServerStatusService.newBlockingStub(channel);
        break;
      } catch (IOException e) {
        e = e instanceof RemoteException ?
            ((RemoteException)e).unwrapRemoteException() : e;
        if (e instanceof ServerNotRunningYetException) {
          if (System.currentTimeMillis() > (previousLogTime+1000)){
            LOG.info("Master isn't available yet, retrying");
            previousLogTime = System.currentTimeMillis();
          }
        } else {
          if (System.currentTimeMillis() > (previousLogTime + 1000)) {
            LOG.warn("Unable to connect to master. Retrying. Error was:", e);
            previousLogTime = System.currentTimeMillis();
          }
        }
        try {
          Thread.sleep(200);
        } catch (InterruptedException ignored) {
        }
      }
    }
    rssStub = intf;
    return sn;
  }

  /**
   * @return True if we should break loop because cluster is going down or
   * this server has been stopped or hdfs has gone bad.
   */
  private boolean keepLooping() {
    return !this.stopped && isClusterUp();
  }

  /*
   * Let the master know we're here Run initialization using parameters passed
   * us by the master.
   * @return A Map of key/value configurations we got from the Master else
   * null if we failed to register.
   * @throws IOException
   */
  private RegionServerStartupResponse reportForDuty() throws IOException {
    ServerName masterServerName = createRegionServerStatusStub();
    if (masterServerName == null) return null;
    RegionServerStartupResponse result = null;
    try {
      this.requestCount.set(0);
      LOG.info("reportForDuty to master=" + masterServerName + " with port=" + this.isa.getPort() +
        ", startcode=" + this.startcode);
      long now = EnvironmentEdgeManager.currentTimeMillis();
      int port = this.isa.getPort();
      RegionServerStartupRequest.Builder request = RegionServerStartupRequest.newBuilder();
      request.setPort(port);
      request.setServerStartCode(this.startcode);
      request.setServerCurrentTime(now);
      result = this.rssStub.regionServerStartup(null, request.build());
    } catch (ServiceException se) {
      IOException ioe = ProtobufUtil.getRemoteException(se);
      if (ioe instanceof ClockOutOfSyncException) {
        LOG.fatal("Master rejected startup because clock is out of sync", ioe);
        // Re-throw IOE will cause RS to abort
        throw ioe;
      } else if (ioe instanceof ServerNotRunningYetException) {
        LOG.debug("Master is not running yet");
      } else {
        LOG.warn("error telling master we are up", se);
        rssStub = null;
      }
    }
    return result;
  }

  @Override
  public long getLastSequenceId(byte[] encodedRegionName) {
    long lastFlushedSequenceId = -1L;
    try {
      GetLastFlushedSequenceIdRequest req = RequestConverter
          .buildGetLastFlushedSequenceIdRequest(encodedRegionName);
      lastFlushedSequenceId = rssStub.getLastFlushedSequenceId(null, req)
          .getLastFlushedSequenceId();
    } catch (ServiceException e) {
      lastFlushedSequenceId = -1L;
      LOG.warn("Unable to connect to the master to check " + "the last flushed sequence id", e);
    }
    return lastFlushedSequenceId;
  }

  /**
   * Closes all regions.  Called on our way out.
   * Assumes that its not possible for new regions to be added to onlineRegions
   * while this method runs.
   */
  protected void closeAllRegions(final boolean abort) {
    closeUserRegions(abort);
    closeMetaTableRegions(abort);
  }

  /**
   * Close meta region if we carry it
   * @param abort Whether we're running an abort.
   */
  void closeMetaTableRegions(final boolean abort) {
    HRegion meta = null;
    this.lock.writeLock().lock();
    try {
      for (Map.Entry<String, HRegion> e: onlineRegions.entrySet()) {
        HRegionInfo hri = e.getValue().getRegionInfo();
        if (hri.isMetaRegion()) {
          meta = e.getValue();
        }
        if (meta != null) break;
      }
    } finally {
      this.lock.writeLock().unlock();
    }
    if (meta != null) closeRegionIgnoreErrors(meta.getRegionInfo(), abort);
  }

  /**
   * Schedule closes on all user regions.
   * Should be safe calling multiple times because it wont' close regions
   * that are already closed or that are closing.
   * @param abort Whether we're running an abort.
   */
  void closeUserRegions(final boolean abort) {
    this.lock.writeLock().lock();
    try {
      for (Map.Entry<String, HRegion> e: this.onlineRegions.entrySet()) {
        HRegion r = e.getValue();
        if (!r.getRegionInfo().isMetaTable() && r.isAvailable()) {
          // Don't update zk with this close transition; pass false.
          closeRegionIgnoreErrors(r.getRegionInfo(), abort);
        }
      }
    } finally {
      this.lock.writeLock().unlock();
    }
  }

  /** @return the info server */
  public InfoServer getInfoServer() {
    return infoServer;
  }

  /**
   * @return true if a stop has been requested.
   */
  @Override
  public boolean isStopped() {
    return this.stopped;
  }

  @Override
  public boolean isStopping() {
    return this.stopping;
  }

  @Override
  public Map<String, HRegion> getRecoveringRegions() {
    return this.recoveringRegions;
  }

  /**
   *
   * @return the configuration
   */
  @Override
  public Configuration getConfiguration() {
    return conf;
  }

  /** @return the write lock for the server */
  ReentrantReadWriteLock.WriteLock getWriteLock() {
    return lock.writeLock();
  }

  public int getNumberOfOnlineRegions() {
    return this.onlineRegions.size();
  }

  boolean isOnlineRegionsEmpty() {
    return this.onlineRegions.isEmpty();
  }

  /**
   * For tests, web ui and metrics.
   * This method will only work if HRegionServer is in the same JVM as client;
   * HRegion cannot be serialized to cross an rpc.
   */
  public Collection<HRegion> getOnlineRegionsLocalContext() {
    Collection<HRegion> regions = this.onlineRegions.values();
    return Collections.unmodifiableCollection(regions);
  }

  @Override
  public void addToOnlineRegions(HRegion region) {
    this.onlineRegions.put(region.getRegionInfo().getEncodedName(), region);
    LOG.info("onlineRegions add region : " + region.getRegionInfo().getEncodedName() + "for table "
        + region.getRegionInfo().getTable());
  }

  /**
   * @return A new Map of online regions sorted by region size with the first entry being the
   * biggest.  If two regions are the same size, then the last one found wins; i.e. this method
   * may NOT return all regions.
   */
  SortedMap<Long, HRegion> getCopyOfOnlineRegionsSortedBySize() {
    // we'll sort the regions in reverse
    SortedMap<Long, HRegion> sortedRegions = new TreeMap<Long, HRegion>(
        new Comparator<Long>() {
          @Override
          public int compare(Long a, Long b) {
            return -1 * a.compareTo(b);
          }
        });
    // Copy over all regions. Regions are sorted by size with biggest first.
    for (HRegion region : this.onlineRegions.values()) {
      sortedRegions.put(region.memstoreSize.get(), region);
    }
    return sortedRegions;
  }

  /**
   * @return time stamp in millis of when this region server was started
   */
  public long getStartcode() {
    return this.startcode;
  }

  /** @return reference to FlushRequester */
  @Override
  public FlushRequester getFlushRequester() {
    return this.cacheFlusher;
  }

  /**
   * Get the top N most loaded regions this server is serving so we can tell the
   * master which regions it can reallocate if we're overloaded. TODO: actually
   * calculate which regions are most loaded. (Right now, we're just grabbing
   * the first N regions being served regardless of load.)
   */
  protected HRegionInfo[] getMostLoadedRegions() {
    ArrayList<HRegionInfo> regions = new ArrayList<HRegionInfo>();
    for (HRegion r : onlineRegions.values()) {
      if (!r.isAvailable()) {
        continue;
      }
      if (regions.size() < numRegionsToReport) {
        regions.add(r.getRegionInfo());
      } else {
        break;
      }
    }
    return regions.toArray(new HRegionInfo[regions.size()]);
  }

  @Override
  public Leases getLeases() {
    return leases;
  }

  /**
   * @return Return the rootDir.
   */
  protected Path getRootDir() {
    return rootDir;
  }

  /**
   * @return Return the fs.
   */
  @Override
  public FileSystem getFileSystem() {
    return fs;
  }

  @Override
  public String toString() {
    return getServerName().toString();
  }

  /**
   * Interval at which threads should run
   *
   * @return the interval
   */
  public int getThreadWakeFrequency() {
    return threadWakeFrequency;
  }

  @Override
  public ZooKeeperWatcher getZooKeeper() {
    return zooKeeper;
  }

  @Override
  public ServerName getServerName() {
    // Our servername could change after we talk to the master.
    return this.serverNameFromMasterPOV == null?
        ServerName.valueOf(this.isa.getHostName(), this.isa.getPort(), this.startcode) :
        this.serverNameFromMasterPOV;
  }

  @Override
  public CompactionRequestor getCompactionRequester() {
    return this.compactSplitThread;
  }

  public ZooKeeperWatcher getZooKeeperWatcher() {
    return this.zooKeeper;
  }

  public RegionServerCoprocessorHost getCoprocessorHost(){
    return this.rsHost;
  }

  @Override
  public ConcurrentMap<byte[], Boolean> getRegionsInTransitionInRS() {
    return this.regionsInTransitionInRS;
  }

  @Override
  public ExecutorService getExecutorService() {
    return service;
  }

  @Override
  public RegionServerQuotaManager getRegionServerQuotaManager() {
    return rsQuotaManager;
  }

  //
  // Main program and support routines
  //

  /**
   * Load the replication service objects, if any
   */
  static private void createNewReplicationInstance(Configuration conf,
    HRegionServer server, FileSystem fs, Path logDir, Path oldLogDir) throws IOException{

    // If replication is not enabled, then return immediately.
    if (!conf.getBoolean(HConstants.REPLICATION_ENABLE_KEY,
        HConstants.REPLICATION_ENABLE_DEFAULT)) {
      return;
    }

    // read in the name of the source replication class from the config file.
    String sourceClassname = conf.get(HConstants.REPLICATION_SOURCE_SERVICE_CLASSNAME,
                               HConstants.REPLICATION_SERVICE_CLASSNAME_DEFAULT);

    // read in the name of the sink replication class from the config file.
    String sinkClassname = conf.get(HConstants.REPLICATION_SINK_SERVICE_CLASSNAME,
                             HConstants.REPLICATION_SERVICE_CLASSNAME_DEFAULT);

    // If both the sink and the source class names are the same, then instantiate
    // only one object.
    if (sourceClassname.equals(sinkClassname)) {
      server.replicationSourceHandler = (ReplicationSourceService)
                                         newReplicationInstance(sourceClassname,
                                         conf, server, fs, logDir, oldLogDir);
      server.replicationSinkHandler = (ReplicationSinkService)
                                         server.replicationSourceHandler;
    } else {
      server.replicationSourceHandler = (ReplicationSourceService)
                                         newReplicationInstance(sourceClassname,
                                         conf, server, fs, logDir, oldLogDir);
      server.replicationSinkHandler = (ReplicationSinkService)
                                         newReplicationInstance(sinkClassname,
                                         conf, server, fs, logDir, oldLogDir);
    }
  }

  static private ReplicationService newReplicationInstance(String classname,
    Configuration conf, HRegionServer server, FileSystem fs, Path logDir,
    Path oldLogDir) throws IOException{

    Class<?> clazz = null;
    try {
      ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
      clazz = Class.forName(classname, true, classLoader);
    } catch (java.lang.ClassNotFoundException nfe) {
      throw new IOException("Could not find class for " + classname);
    }

    // create an instance of the replication object.
    ReplicationService service = (ReplicationService)
                              ReflectionUtils.newInstance(clazz, conf);
    service.initialize(server, fs, logDir, oldLogDir);
    return service;
  }

  /**
   * @param hrs
   * @return Thread the RegionServer is running in correctly named.
   * @throws IOException
   */
  public static Thread startRegionServer(final HRegionServer hrs)
      throws IOException {
    return startRegionServer(hrs, "regionserver" + hrs.isa.getPort());
  }

  /**
   * @param hrs
   * @param name
   * @return Thread the RegionServer is running in correctly named.
   * @throws IOException
   */
  public static Thread startRegionServer(final HRegionServer hrs,
      final String name) throws IOException {
    Thread t = new Thread(hrs);
    t.setName(name);
    t.start();
    // Install shutdown hook that will catch signals and run an orderly shutdown
    // of the hrs.
    ShutdownHook.install(hrs.getConfiguration(), FileSystem.get(hrs
        .getConfiguration()), hrs, t);
    return t;
  }

  /**
   * Utility for constructing an instance of the passed HRegionServer class.
   *
   * @param regionServerClass
   * @param conf2
   * @return HRegionServer instance.
   */
  public static HRegionServer constructRegionServer(
      Class<? extends HRegionServer> regionServerClass,
      final Configuration conf2) {
    try {
      Constructor<? extends HRegionServer> c = regionServerClass
          .getConstructor(Configuration.class);
      return c.newInstance(conf2);
    } catch (Exception e) {
      throw new RuntimeException("Failed construction of " + "Regionserver: "
          + regionServerClass.toString(), e);
    }
  }

  /**
   * @see org.apache.hadoop.hbase.regionserver.HRegionServerCommandLine
   */
  public static void main(String[] args) throws Exception {
	VersionInfo.logVersion();
    Configuration conf = HBaseConfiguration.create();
    @SuppressWarnings("unchecked")
    Class<? extends HRegionServer> regionServerClass = (Class<? extends HRegionServer>) conf
        .getClass(HConstants.REGION_SERVER_IMPL, HRegionServer.class);

    new HRegionServerCommandLine(regionServerClass).doMain(args);
  }

  /**
   * Gets the online regions of the specified table.
   * This method looks at the in-memory onlineRegions.  It does not go to <code>hbase:meta</code>.
   * Only returns <em>online</em> regions.  If a region on this table has been
   * closed during a disable, etc., it will not be included in the returned list.
   * So, the returned list may not necessarily be ALL regions in this table, its
   * all the ONLINE regions in the table.
   * @param tableName
   * @return Online regions from <code>tableName</code>
   */
  @Override
  public List<HRegion> getOnlineRegions(TableName tableName) {
     List<HRegion> tableRegions = new ArrayList<HRegion>();
     synchronized (this.onlineRegions) {
       for (HRegion region: this.onlineRegions.values()) {
         HRegionInfo regionInfo = region.getRegionInfo();
         if(regionInfo.getTable().equals(tableName)) {
           tableRegions.add(region);
         }
       }
     }
     return tableRegions;
   }

  /**
   * Gets the online tables in this RS.
   * This method looks at the in-memory onlineRegions.
   * @return all the online tables in this RS
   */
  @Override
  public Set<TableName> getOnlineTables() {
    Set<TableName> tables = new HashSet<TableName>();
    synchronized (this.onlineRegions) {
      for (HRegion region: this.onlineRegions.values()) {
        tables.add(region.getTableDesc().getTableName());
      }
    }
    return tables;
  }

  // used by org/apache/hbase/tmpl/regionserver/RSStatusTmpl.jamon (HBASE-4070).
  public String[] getCoprocessors() {
    TreeSet<String> coprocessors = new TreeSet<String>(
        this.hlog.getCoprocessorHost().getCoprocessors());
    Collection<HRegion> regions = getOnlineRegionsLocalContext();
    for (HRegion region: regions) {
      coprocessors.addAll(region.getCoprocessorHost().getCoprocessors());
    }
    return coprocessors.toArray(new String[coprocessors.size()]);
  }

  /**
   * Instantiated as a scanner lease. If the lease times out, the scanner is
   * closed
   */
  private class ScannerListener implements LeaseListener {
    private final String scannerName;

    ScannerListener(final String n) {
      this.scannerName = n;
    }

    @Override
    public void leaseExpired() {
      RegionScannerHolder rsh = scanners.remove(this.scannerName);
      if (rsh != null) {
        RegionScanner s = rsh.s;
        LOG.info("Scanner " + this.scannerName + " lease expired on region "
            + s.getRegionInfo().getRegionNameAsString());
        HRegion region = null;
        try {
          region = getRegion(s.getRegionInfo().getRegionName());
          if (region != null && region.getCoprocessorHost() != null) {
            region.getCoprocessorHost().preScannerClose(s);
          }
        } catch (IOException e) {
          LOG.error("Closing scanner for " + s.getRegionInfo().getRegionNameAsString(), e);
        } finally {
          try {
            s.close(false);
            if (region != null && region.getCoprocessorHost() != null) {
              region.getCoprocessorHost().postScannerClose(s);
            }
          } catch (IOException e) {
            LOG.error("Closing scanner for " + s.getRegionInfo().getRegionNameAsString(), e);
          }
        }
      } else {
        LOG.info("Scanner " + this.scannerName + " lease expired");
      }
    }
  }

  /**
   * Called to verify that this server is up and running.
   *
   * @throws IOException
   */
  protected void checkOpen() throws IOException {
    if (this.stopped || this.abortRequested) {
      throw new RegionServerStoppedException("Server " + getServerName() +
        " not running" + (this.abortRequested ? ", aborting" : ""));
    }
    if (!fsOk) {
      throw new RegionServerStoppedException("File system not available");
    }
  }


  /**
   * Try to close the region, logs a warning on failure but continues.
   * @param region Region to close
   */
  private void closeRegionIgnoreErrors(HRegionInfo region, final boolean abort) {
    try {
      if (!closeRegion(region.getEncodedName(), abort, false, -1, null)) {
        LOG.warn("Failed to close " + region.getRegionNameAsString() +
            " - ignoring and continuing");
      }
    } catch (IOException e) {
      LOG.warn("Failed to close " + region.getRegionNameAsString() +
          " - ignoring and continuing", e);
    }
  }

  /**
   * Close asynchronously a region, can be called from the master or internally by the regionserver
   * when stopping. If called from the master, the region will update the znode status.
   *
   * <p>
   * If an opening was in progress, this method will cancel it, but will not start a new close. The
   * coprocessors are not called in this case. A NotServingRegionException exception is thrown.
   * </p>

   * <p>
   *   If a close was in progress, this new request will be ignored, and an exception thrown.
   * </p>
   *
   * @param encodedName Region to close
   * @param abort True if we are aborting
   * @param zk True if we are to update zk about the region close; if the close
   * was orchestrated by master, then update zk.  If the close is being run by
   * the regionserver because its going down, don't update zk.
   * @param versionOfClosingNode the version of znode to compare when RS transitions the znode from
   *   CLOSING state.
   * @return True if closed a region.
   * @throws NotServingRegionException if the region is not online
   * @throws RegionAlreadyInTransitionException if the region is already closing
   */
  protected boolean closeRegion(String encodedName, final boolean abort,
      final boolean zk, final int versionOfClosingNode, final ServerName sn)
      throws NotServingRegionException, RegionAlreadyInTransitionException {
    //Check for permissions to close.
    HRegion actualRegion = this.getFromOnlineRegions(encodedName);
    if ((actualRegion != null) && (actualRegion.getCoprocessorHost() != null)) {
      try {
        actualRegion.getCoprocessorHost().preClose(false);
      } catch (IOException exp) {
        LOG.warn("Unable to close region: the coprocessor launched an error ", exp);
        return false;
      }
    }

    final Boolean previous = this.regionsInTransitionInRS.putIfAbsent(encodedName.getBytes(),
        Boolean.FALSE);

    if (Boolean.TRUE.equals(previous)) {
      LOG.info("Received CLOSE for the region:" + encodedName + " , which we are already " +
          "trying to OPEN. Cancelling OPENING.");
      if (!regionsInTransitionInRS.replace(encodedName.getBytes(), previous, Boolean.FALSE)){
        // The replace failed. That should be an exceptional case, but theoretically it can happen.
        // We're going to try to do a standard close then.
        LOG.warn("The opening for region " + encodedName + " was done before we could cancel it." +
            " Doing a standard close now");
        return closeRegion(encodedName, abort, zk, versionOfClosingNode, sn);
      }
      // Let's get the region from the online region list again
      actualRegion = this.getFromOnlineRegions(encodedName);
      if (actualRegion == null) { // If already online, we still need to close it.
        LOG.info("The opening previously in progress has been cancelled by a CLOSE request.");
        // The master deletes the znode when it receives this exception.
        throw new RegionAlreadyInTransitionException("The region " + encodedName +
          " was opening but not yet served. Opening is cancelled.");
      }
    } else if (Boolean.FALSE.equals(previous)) {
      LOG.info("Received CLOSE for the region: " + encodedName +
        " ,which we are already trying to CLOSE, but not completed yet");
      // The master will retry till the region is closed. We need to do this since
      // the region could fail to close somehow. If we mark the region closed in master
      // while it is not, there could be data loss.
      // If the region stuck in closing for a while, and master runs out of retries,
      // master will move the region to failed_to_close. Later on, if the region
      // is indeed closed, master can properly re-assign it.
      throw new RegionAlreadyInTransitionException("The region " + encodedName +
        " was already closing. New CLOSE request is ignored.");
    }

    if (actualRegion == null) {
      LOG.error("Received CLOSE for a region which is not online, and we're not opening.");
      this.regionsInTransitionInRS.remove(encodedName.getBytes());
      // The master deletes the znode when it receives this exception.
      throw new NotServingRegionException("The region " + encodedName +
          " is not online, and is not opening.");
    }

    CloseRegionHandler crh;
    final HRegionInfo hri = actualRegion.getRegionInfo();
    if (hri.isMetaRegion()) {
      crh = new CloseMetaHandler(this, this, hri, abort, zk, versionOfClosingNode);
    } else {
      crh = new CloseRegionHandler(this, this, hri, abort, zk, versionOfClosingNode, sn);
    }
    this.service.submit(crh);
    return true;
  }

   /**
   * @param regionName
   * @return HRegion for the passed binary <code>regionName</code> or null if
   *         named region is not member of the online regions.
   */
  public HRegion getOnlineRegion(final byte[] regionName) {
    String encodedRegionName = HRegionInfo.encodeRegionName(regionName);
    return this.onlineRegions.get(encodedRegionName);
  }

  public List<HRegion> getOnlineRegions() {
    return new ArrayList<>(onlineRegions.values());
  }

  public InetSocketAddress[] getRegionBlockLocations(final String encodedRegionName) {
    return this.regionFavoredNodesMap.get(encodedRegionName);
  }

  @Override
  public HRegion getFromOnlineRegions(final String encodedRegionName) {
    return this.onlineRegions.get(encodedRegionName);
  }


  @Override
  public boolean removeFromOnlineRegions(final HRegion r, ServerName destination) {
    HRegion toReturn = this.onlineRegions.remove(r.getRegionInfo().getEncodedName());
    LOG.info("onlineRegions remove region : " + r.getRegionInfo().getEncodedName() + "for table "
        + r.getRegionInfo().getTable());
    if (destination != null) {
      HLog wal = getWAL();
      long closeSeqNum = wal.getEarliestMemstoreSeqNum(r.getRegionInfo().getEncodedNameAsBytes());
      if (closeSeqNum == HConstants.NO_SEQNUM) {
        // No edits in WAL for this region; get the sequence number when the region was opened.
        closeSeqNum = r.getOpenSeqNum();
        if (closeSeqNum == HConstants.NO_SEQNUM) {
          closeSeqNum = 0;
        }
      }
      addToMovedRegions(r.getRegionInfo().getEncodedName(), destination, closeSeqNum);
    }
    this.regionFavoredNodesMap.remove(r.getRegionInfo().getEncodedName());
    return toReturn != null;
  }

  /**
   * Protected utility method for safely obtaining an HRegion handle.
   *
   * @param regionName
   *          Name of online {@link HRegion} to return
   * @return {@link HRegion} for <code>regionName</code>
   * @throws NotServingRegionException
   */
  protected HRegion getRegion(final byte[] regionName)
      throws NotServingRegionException {
    String encodedRegionName = HRegionInfo.encodeRegionName(regionName);
    return getRegionByEncodedName(regionName, encodedRegionName);
  }

  protected HRegion getRegionByEncodedName(String encodedRegionName)
      throws NotServingRegionException {
    return getRegionByEncodedName(null, encodedRegionName);
  }

  protected HRegion getRegionByEncodedName(byte[] regionName, String encodedRegionName)
    throws NotServingRegionException {
    HRegion region = this.onlineRegions.get(encodedRegionName);
    if (region == null) {
      MovedRegionInfo moveInfo = getMovedRegion(encodedRegionName);
      if (moveInfo != null) {
        throw new RegionMovedException(moveInfo.getServerName(), moveInfo.getSeqNum());
      }
      Boolean isOpening = this.regionsInTransitionInRS.get(Bytes.toBytes(encodedRegionName));
      String regionNameStr = regionName == null?
        encodedRegionName: Bytes.toStringBinary(regionName);
      if (isOpening != null && isOpening.booleanValue()) {
        throw new RegionOpeningException("Region " + regionNameStr +
          " is opening on " + this.serverNameFromMasterPOV);
      }
      throw new NotServingRegionException("Region " + regionNameStr +
        " is not online on " + this.serverNameFromMasterPOV);
    }
    return region;
  }

  /*
   * Cleanup after Throwable caught invoking method. Converts <code>t</code> to
   * IOE if it isn't already.
   *
   * @param t Throwable
   *
   * @return Throwable converted to an IOE; methods can only let out IOEs.
   */
  protected Throwable cleanup(final Throwable t) {
    return cleanup(t, null);
  }

  /*
   * Cleanup after Throwable caught invoking method. Converts <code>t</code> to
   * IOE if it isn't already.
   *
   * @param t Throwable
   *
   * @param msg Message to log in error. Can be null.
   *
   * @return Throwable converted to an IOE; methods can only let out IOEs.
   */
  protected Throwable cleanup(final Throwable t, final String msg) {
    // Don't log as error if NSRE; NSRE is 'normal' operation.
    if (t instanceof NotServingRegionException) {
      LOG.debug("NotServingRegionException; " + t.getMessage());
      return t;
    }
    if (msg == null) {
      LOG.error("", RemoteExceptionHandler.checkThrowable(t));
    } else {
      LOG.error(msg, RemoteExceptionHandler.checkThrowable(t));
    }
    if (!checkOOME(t)) {
      checkFileSystem();
    }
    return t;
  }

  /*
   * @param t
   *
   * @param msg Message to put in new IOE if passed <code>t</code> is not an IOE
   *
   * @return Make <code>t</code> an IOE if it isn't already.
   */
  protected IOException convertThrowableToIOE(final Throwable t, final String msg) {
    return (t instanceof IOException ? (IOException) t : msg == null
        || msg.length() == 0 ? new IOException(t) : new IOException(msg, t));
  }

  /*
   * Check if an OOME and, if so, abort immediately to avoid creating more objects.
   *
   * @param e
   *
   * @return True if we OOME'd and are aborting.
   */
  public static boolean exitIfOOME(final Throwable e) {
    boolean stop = false;
    try {
      if (e instanceof OutOfMemoryError
          || (e.getCause() != null && e.getCause() instanceof OutOfMemoryError)
          || (e.getMessage() != null && e.getMessage().contains(
              "java.lang.OutOfMemoryError"))) {
        stop = true;
        LOG.fatal(
          "Run out of memory; HRegionServer will abort itself immediately", e);
        ThreadInfoUtils.logThreadInfo("thread dump due to OOME", true);
      }
    } finally {
      if (stop) {
        Runtime.getRuntime().halt(1);
      }
    }
    return stop;
  }

  @Override
  public boolean checkOOME(final Throwable e) {
    return exitIfOOME(e);
  }

  /**
   * Checks to see if the file system is still accessible. If not, sets
   * abortRequested and stopRequested
   *
   * @return false if file system is not available
   */
  public boolean checkFileSystem() {
    if (this.fsOk && this.fs != null) {
      try {
        FSUtils.checkFileSystemAvailable(this.fs);
      } catch (IOException e) {
        abort("File System not available", e);
        this.fsOk = false;
      }
    }
    return this.fsOk;
  }

  /**
   * Method to account for the size of retained cells and retained data blocks.
   * @return an object that represents the last referenced block from this response.
   */
  Object addSize(RpcCallContext context, Result r, Object lastBlock) {
    if (context != null && !r.isEmpty()) {
      for (Cell c : r.rawCells()) {
        context.incrementResponseCellSize(CellUtil.estimatedSizeOf(c));
        // We're using the last block being the same as the current block as
        // a proxy for pointing to a new block. This won't be exact.
        // If there are multiple gets that bounce back and forth
        // Then it's possible that this will over count the size of
        // referenced blocks. However it's better to over count and
        // use two rpcs than to OOME the regionserver.
        byte[] valueArray = c.getValueArray();
        if (valueArray != lastBlock) {
          context.incrementResponseBlockSize(valueArray.length);
          lastBlock = valueArray;
        }
      }
    }
    return lastBlock;
  }

  private RegionScannerHolder addScanner(String scannerName, RegionScanner s, HRegion r,
      boolean needCursor) throws LeaseStillHeldException {
    RegionScannerHolder rsh = new RegionScannerHolder(scannerName, s, r, needCursor);
    RegionScannerHolder existing = scanners.putIfAbsent(scannerName, rsh);
    assert existing == null : "scannerId must be unique within regionserver's whole lifecycle!";

    this.leases.createLease(scannerName, this.scannerLeaseTimeoutPeriod,
      new ScannerListener(scannerName));

    return rsh;
  }

  // Start Client methods

  /**
   * Get data from a table.
   *
   * @param controller the RPC controller
   * @param request the get request
   * @throws ServiceException
   */
  @Override
  public GetResponse get(final RpcController controller,
      final GetRequest request) throws ServiceException {
    long before = EnvironmentEdgeManager.currentTimeMillis();
    TableName tableName = null;
    try {
      checkOpen();
      requestCount.increment();
      HRegion region = getRegion(request.getRegion());
      tableName = region.getTableDesc().getTableName();
      if (isQuotaEnabled()) {
        rsQuotaManager.checkQuota(region, OperationQuota.OperationType.GET);
      }

      GetResponse.Builder builder = GetResponse.newBuilder();
      ClientProtos.Get get = request.getGet();
      Boolean existence = null;
      Result r = null;

      if (Trace.isTracing() && Trace.currentSpan() != null) {
        Trace.currentSpan().addTimelineAnnotation("start processing a  get request");
        Trace.currentSpan().addKVAnnotation(Bytes.toBytes("region"), Bytes.toBytes(region.toString()));
        Trace.currentSpan().addKVAnnotation(Bytes.toBytes("key"), get.getRow().toByteArray());
      }

      if (get.hasClosestRowBefore() && get.getClosestRowBefore()) {
        if (get.getColumnCount() != 1) {
          throw new DoNotRetryIOException(
            "get ClosestRowBefore supports one and only one family now, not "
              + get.getColumnCount() + " families");
        }
        byte[] row = get.getRow().toByteArray();
        byte[] family = get.getColumn(0).getFamily().toByteArray();
        r = region.getClosestRowBefore(row, family);
      } else {
        Get clientGet = ProtobufUtil.toGet(get);
        if (get.getExistenceOnly() && region.getCoprocessorHost() != null) {
          existence = region.getCoprocessorHost().preExists(clientGet);
        }
        if (existence == null) {
          r = region.get(clientGet);
          if (get.getExistenceOnly()) {
            boolean exists = r.getExists();
            if (region.getCoprocessorHost() != null) {
              exists = region.getCoprocessorHost().postExists(clientGet, exists);
            }
            existence = exists;
          }
        }
      }
      if (existence != null){
        ClientProtos.Result pbr = ProtobufUtil.toResult(existence);
        builder.setResult(pbr);
      } else if (r != null) {
        ClientProtos.Result pbr = ProtobufUtil.toResult(r);
        builder.setResult(pbr);
      }
      if (r != null && isQuotaEnabled()) {
        rsQuotaManager.grabQuota(region, r);
      }
      return builder.build();
    } catch (IOException ie) {
      throw new ServiceException(ie);
    } finally {
      metricsRegionServer.updateGet(tableName, EnvironmentEdgeManager.currentTimeMillis() - before);
    }
  }


  /**
   * Mutate data in a table.
   *
   * @param rpcc the RPC controller
   * @param request the mutate request
   * @throws ServiceException
   */
  @Override
  public MutateResponse mutate(final RpcController rpcc,
      final MutateRequest request) throws ServiceException {
    // rpc controller is how we bring in data via the back door;  it is unprotobuf'ed data.
    // It is also the conduit via which we pass back data.
    HBaseRpcController controller = (HBaseRpcController)rpcc;
    CellScanner cellScanner = controller != null? controller.cellScanner(): null;
    // Clear scanner so we are not holding on to reference across call.
    if (controller != null) controller.setCellScanner(null);
    try {
      checkOpen();
      requestCount.increment();
      HRegion region = getRegion(request.getRegion());
      MutateResponse.Builder builder = MutateResponse.newBuilder();
      MutationProto mutation = request.getMutation();
      if (!region.getRegionInfo().isMetaTable()) {
        cacheFlusher.reclaimMemStoreMemory();
      }
      long nonceGroup = request.hasNonceGroup()
          ? request.getNonceGroup() : HConstants.NO_NONCE;
      Result r = null;
      Boolean processed = null;
      MutationType type = mutation.getMutateType();
      long mutationSize = 0;
      long before = EnvironmentEdgeManager.currentTimeMillis();

      switch (type) {
      case APPEND:
        // TODO: this doesn't actually check anything.
        r = append(region, mutation, cellScanner, nonceGroup);
        break;
      case INCREMENT:
        // TODO: this doesn't actually check anything.
        r = increment(region, mutation, cellScanner, nonceGroup);
        break;
      case PUT:
        Put put = ProtobufUtil.toPut(mutation, cellScanner);
        if (isQuotaEnabled()) {
          rsQuotaManager.checkQuota(region, put);
        }
        if (request.hasCondition()) {
          Condition condition = request.getCondition();
          byte[] row = condition.getRow().toByteArray();
          byte[] family = condition.getFamily().toByteArray();
          byte[] qualifier = condition.getQualifier().toByteArray();
          CompareOp compareOp = CompareOp.valueOf(condition.getCompareType().name());
          ByteArrayComparable comparator =
            ProtobufUtil.toComparator(condition.getComparator());
          if (region.getCoprocessorHost() != null) {
            processed = region.getCoprocessorHost().preCheckAndPut(
              row, family, qualifier, compareOp, comparator, put);
          }
          if (processed == null) {
            boolean result = region.checkAndMutate(row, family,
              qualifier, compareOp, comparator, put, true);
            if (region.getCoprocessorHost() != null) {
              result = region.getCoprocessorHost().postCheckAndPut(row, family,
                qualifier, compareOp, comparator, put, result);
            }
            processed = result;
          }
        } else {
          region.put(put);
          processed = Boolean.TRUE;
        }
        metricsRegionServer.updatePut(region.getTableDesc().getTableName(), EnvironmentEdgeManager.currentTimeMillis() - before);
        break;
      case DELETE:
        Delete delete = ProtobufUtil.toDelete(mutation, cellScanner);
        if (isQuotaEnabled()) {
          rsQuotaManager.checkQuota(region, delete);
        }
        if (request.hasCondition()) {
          Condition condition = request.getCondition();
          byte[] row = condition.getRow().toByteArray();
          byte[] family = condition.getFamily().toByteArray();
          byte[] qualifier = condition.getQualifier().toByteArray();
          CompareOp compareOp = CompareOp.valueOf(condition.getCompareType().name());
          ByteArrayComparable comparator =
            ProtobufUtil.toComparator(condition.getComparator());
          if (region.getCoprocessorHost() != null) {
            processed = region.getCoprocessorHost().preCheckAndDelete(
              row, family, qualifier, compareOp, comparator, delete);
          }
          if (processed == null) {
            boolean result = region.checkAndMutate(row, family,
              qualifier, compareOp, comparator, delete, true);
            if (region.getCoprocessorHost() != null) {
              result = region.getCoprocessorHost().postCheckAndDelete(row, family,
                qualifier, compareOp, comparator, delete, result);
            }
            processed = result;
          }
        } else {
          region.delete(delete);
          processed = Boolean.TRUE;
        }
        metricsRegionServer.updateDelete(region.getTableDesc().getTableName(),
          EnvironmentEdgeManager.currentTimeMillis() - before);
        break;
        default:
          throw new DoNotRetryIOException(
            "Unsupported mutate type: " + type.name());
      }
      if (processed != null) {
        builder.setProcessed(processed.booleanValue());
      }
      boolean clientCellBlockSupported = isClientCellBlockSupport(RpcServer.getCurrentCall());
      addResult(builder, r, controller, clientCellBlockSupported);
      return builder.build();
    } catch (IOException ie) {
      checkFileSystem();
      throw new ServiceException(ie);
    }
  }


  private boolean isClientCellBlockSupport(RpcCallContext context) {
    return context != null && context.isClientCellBlockSupport();
  }

  private void addResult(final MutateResponse.Builder builder, final Result result,
      final HBaseRpcController rpcc, boolean clientCellBlockSupported) {
    if (result == null) return;
    if (clientCellBlockSupported) {
      builder.setResult(ProtobufUtil.toResultNoData(result));
      rpcc.setCellScanner(result.cellScanner());
    } else {
      ClientProtos.Result pbr = ProtobufUtil.toResult(result);
      builder.setResult(pbr);
    }
  }

  //
  // remote scanner interface
  //

  // This is used to keep compatible with the old client implementation. Consider remove it if we
  // decide to drop the support of the client that still sends close request to a region scanner
  // which has already been exhausted.
  @Deprecated
  private static final IOException SCANNER_ALREADY_CLOSED = new IOException() {

    private static final long serialVersionUID = -4305297078988180130L;

    @Override
    public Throwable fillInStackTrace() {
      return this;
    }
  };

  private RegionScannerHolder getRegionScanner(ScanRequest request) throws IOException {
    String scannerName = Long.toString(request.getScannerId());
    RegionScannerHolder rsh = scanners.get(scannerName);
    if (rsh == null) {
      // just ignore the next or close request if scanner does not exists.
      if (closedScanners.getIfPresent(scannerName) != null) {
        throw SCANNER_ALREADY_CLOSED;
      } else {
        LOG.warn("Client tried to access missing scanner " + scannerName);
        throw new UnknownScannerException(
            "Unknown scanner '" + scannerName + "'. This can happen due to any of the following " +
                "reasons: a) Scanner id given is wrong, b) Scanner lease expired because of " +
                "long wait between consecutive client checkins, c) Server may be closing down, " +
                "d) RegionServer restart during upgrade.\nIf the issue is due to reason (b), a " +
                "possible fix would be increasing the value of" +
                "'hbase.client.scanner.timeout.period' configuration.");
      }
    }
    HRegionInfo hri = rsh.s.getRegionInfo();
    // Yes, should be the same instance
    if (getOnlineRegion(hri.getRegionName()) != rsh.r) {
      String msg = "Region was re-opened after the scanner" + scannerName + " was created: "
          + hri.getRegionNameAsString();
      LOG.warn(msg + ", closing...");
      scanners.remove(scannerName);
      try {
        rsh.s.close(false);
      } catch (IOException e) {
        LOG.warn("Getting exception closing " + scannerName, e);
      } finally {
        try {
          leases.cancelLease(scannerName);
        } catch (LeaseException e) {
          LOG.warn("Getting exception closing " + scannerName, e);
        }
      }
      throw new NotServingRegionException(msg);
    }
    return rsh;
  }

  private RegionScannerHolder newRegionScanner(ScanRequest request, ScanResponse.Builder builder)
      throws IOException {
    // Check the count of opened region scanners when it has set the limit.
    // It's a soft limit, and to be more efficiency, we avoid using locks here.
    int openedRegionScanners = getScannersCount();
    if (maxOpenedRegionScannerLimit > 0 &&
        openedRegionScanners >= maxOpenedRegionScannerLimit) {
      LOG.warn("Currently opened " + openedRegionScanners +
          " region scanners, up to limit: " + maxOpenedRegionScannerLimit);
      throw new TooManyRegionScannersException("Too many region scanners");
    }

    HRegion region = getRegion(request.getRegion());
    ClientProtos.Scan protoScan = request.getScan();
    boolean isLoadingCfsOnDemandSet = protoScan.hasLoadColumnFamiliesOnDemand();
    Scan scan = ProtobufUtil.toScan(protoScan);
    // if the request doesn't set this, get the default region setting.
    if (!isLoadingCfsOnDemandSet) {
      scan.setLoadColumnFamiliesOnDemand(region.isLoadingCfsOnDemandDefault());
    }

    if (!scan.hasFamilies()) {
      // Adding all families to scanner
      for (byte[] family : region.getTableDesc().getFamiliesKeys()) {
        scan.addFamily(family);
      }
    }
    RegionScanner scanner = null;
    if (region.getCoprocessorHost() != null) {
      scanner = region.getCoprocessorHost().preScannerOpen(scan);
    }
    if (scanner == null) {
      scanner = region.getScanner(scan);
    }
    if (region.getCoprocessorHost() != null) {
      scanner = region.getCoprocessorHost().postScannerOpen(scan, scanner);
    }
    long scannerId = scannerIdGenerator.generateNewScannerId();
    builder.setScannerId(scannerId);
    builder.setMvccReadPoint(scanner.getMvccReadPoint());
    builder.setTtl(scannerLeaseTimeoutPeriod);
    String scannerName = String.valueOf(scannerId);
    return addScanner(scannerName, scanner, region, scan.isNeedCursorResult());
  }

  private void checkScanNextCallSeq(ScanRequest request, RegionScannerHolder rsh)
      throws OutOfOrderScannerNextException {
    // if nextCallSeq does not match throw Exception straight away. This needs to be
    // performed even before checking of Lease.
    // See HBASE-5974
    if (request.hasNextCallSeq()) {
      long callSeq = request.getNextCallSeq();
      if (!rsh.incNextCallSeq(callSeq)) {
        throw new OutOfOrderScannerNextException("Expected nextCallSeq: " + rsh.getNextCallSeq()
            + " But the nextCallSeq got from client: " + callSeq + "; request="
            + TextFormat.shortDebugString(request));
      }
    }
  }

  private void addScannerLeaseBack(Leases.Lease lease) {
    try {
      leases.addLease(lease);
    } catch (LeaseStillHeldException e) {
      // should not happen as the scanner id is unique.
      throw new AssertionError(e);
    }
  }

  private long getTimeLimit(HBaseRpcController controller, boolean allowHeartbeatMessages) {
    // Set the time limit to be half of the more restrictive timeout value (one of the
    // timeout values must be positive). In the event that both values are positive, the
    // more restrictive of the two is used to calculate the limit.
    if (allowHeartbeatMessages && (scannerLeaseTimeoutPeriod > 0 || rpcTimeout > 0)) {
      long timeLimitDelta;
      if (scannerLeaseTimeoutPeriod > 0 && rpcTimeout > 0) {
        timeLimitDelta = Math.min(scannerLeaseTimeoutPeriod, rpcTimeout);
      } else {
        timeLimitDelta = scannerLeaseTimeoutPeriod > 0 ? scannerLeaseTimeoutPeriod : rpcTimeout;
      }
      if (controller != null && controller.getCallTimeout() > 0) {
        timeLimitDelta = Math.min(timeLimitDelta, controller.getCallTimeout());
      }
      // Use half of whichever timeout value was more restrictive... But don't allow
      // the time limit to be less than the allowable minimum (could cause an
      // immediatate timeout before scanning any data).
      timeLimitDelta = Math.max(timeLimitDelta / 2, minimumScanTimeLimitDelta);
      // Do NOT allow the delta to be greater than maximumScanTimeLimitDelta. We want to prevent a
      // scan to hold a rpc handler too long.
      if (maximumScanTimeLimitDelta > 0) {
        timeLimitDelta = Math.min(timeLimitDelta, maximumScanTimeLimitDelta);
      }
      // XXX: Can not use EnvironmentEdge here because TestIncrementTimeRange use a
      // ManualEnvironmentEdge. Consider using System.nanoTime instead.
      return System.currentTimeMillis() + timeLimitDelta;
    }
    // Default value of timeLimit is negative to indicate no timeLimit should be
    // enforced.
    return -1L;
  }

  private void checkLimitOfRows(int numOfCompleteRows, int limitOfRows, boolean moreRows,
      ScannerContext scannerContext, ScanResponse.Builder builder) {
    if (numOfCompleteRows >= limitOfRows) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Done scanning, limit of rows reached, moreRows: " + moreRows +
            " scannerContext: " + scannerContext);
      }
      builder.setMoreResults(false);
    }
  }

  // return whether we have more results in region.
  private void scan(HBaseRpcController controller, ScanRequest request, RegionScannerHolder rsh,
      long maxQuotaResultSize, int maxResults, int limitOfRows, List<Result> results,
      ScanResponse.Builder builder, RpcCallContext context)
      throws IOException {
    long before = EnvironmentEdgeManager.currentTimeMillis();
    HRegion region = rsh.r;
    RegionScanner scanner = rsh.s;
    long maxResultSize;
    if (scanner.getMaxResultSize() > 0) {
      maxResultSize = Math.min(scanner.getMaxResultSize(), maxQuotaResultSize);
    } else {
      maxResultSize = maxQuotaResultSize;
    }
    // This is cells inside a row. Default size is 10 so if many versions or many cfs,
    // then we'll resize. Resizings show in profiler. Set it higher than 10. For now
    // arbitrary 32. TODO: keep record of general size of results being returned.
    List<Cell> values = new ArrayList<Cell>(32);
    region.startRegionOperation(Operation.SCAN);
    try {
      int numOfResults = 0;
      int numOfCompleteRows = 0;
      synchronized (scanner) {
        boolean clientHandlesPartials =
            request.hasClientHandlesPartials() && request.getClientHandlesPartials();
        boolean clientHandlesHeartbeats =
            request.hasClientHandlesHeartbeats() && request.getClientHandlesHeartbeats();

        // On the server side we must ensure that the correct ordering of partial results is
        // returned to the client to allow them to properly reconstruct the partial results.
        // If the coprocessor host is adding to the result list, we cannot guarantee the
        // correct ordering of partial results and so we prevent partial results from being
        // formed.
        boolean serverGuaranteesOrderOfPartials = results.isEmpty();
        boolean allowPartialResults = clientHandlesPartials && serverGuaranteesOrderOfPartials;
        boolean moreRows = false;

        // Heartbeat messages occur when the processing of the ScanRequest is exceeds a
        // certain time threshold on the server. When the time threshold is exceeded, the
        // server stops the scan and sends back whatever Results it has accumulated within
        // that time period (may be empty). Since heartbeat messages have the potential to
        // create partial Results (in the event that the timeout occurs in the middle of a
        // row), we must only generate heartbeat messages when the client can handle both
        // heartbeats AND partials
        boolean allowHeartbeatMessages = clientHandlesHeartbeats && allowPartialResults;

        long timeLimit = getTimeLimit(controller, allowHeartbeatMessages);

        final LimitScope sizeScope =
            allowPartialResults ? LimitScope.BETWEEN_CELLS : LimitScope.BETWEEN_ROWS;
        final LimitScope timeScope =
            allowHeartbeatMessages ? LimitScope.BETWEEN_CELLS : LimitScope.BETWEEN_ROWS;

        // Configure with limits for this RPC. Set keep progress true since size progress
        // towards size limit should be kept between calls to nextRaw
        ScannerContext.Builder contextBuilder = ScannerContext.newBuilder(true);
        contextBuilder.setSizeLimit(sizeScope, maxResultSize);
        contextBuilder.setBatchLimit(scanner.getBatch());
        contextBuilder.setTimeLimit(timeScope, timeLimit);
        ScannerContext scannerContext = contextBuilder.build();
        boolean limitReached = false;
        while (numOfResults  < maxResults) {
          // Reset the batch progress to 0 before every call to RegionScanner#nextRaw. The
          // batch limit is a limit on the number of cells per Result. Thus, if progress is
          // being tracked (i.e. scannerContext.keepProgress() is true) then we need to
          // reset the batch progress between nextRaw invocations since we don't want the
          // batch progress from previous calls to affect future calls
          scannerContext.setBatchProgress(0);

          // Collect values to be returned here
          moreRows = scanner.nextRaw(values, scannerContext);

          if (!values.isEmpty()) {
            if (limitOfRows > 0) {
              // First we need to check if the last result is partial and we have a row change. If
              // so then we need to increase the numOfCompleteRows.
              if (results.isEmpty()) {
                if (rsh.rowOfLastPartialResult != null &&
                    !CellUtil.matchingRow(values.get(0), rsh.rowOfLastPartialResult)) {
                  numOfCompleteRows++;
                  checkLimitOfRows(numOfCompleteRows, limitOfRows, moreRows, scannerContext,
                    builder);
                }
              } else {
                Result lastResult = results.get(results.size() - 1);
                if (lastResult.mayHaveMoreCellsInRow() &&
                    !CellUtil.matchingRow(values.get(0), lastResult.getRow())) {
                  numOfCompleteRows++;
                  checkLimitOfRows(numOfCompleteRows, limitOfRows, moreRows, scannerContext,
                    builder);
                }
              }
              if (builder.hasMoreResults() && !builder.getMoreResults()) {
                break;
              }
            }

            boolean mayHaveMoreCellsInRow = scannerContext.mayHaveMoreCellsInRow();
            Result r = Result.create(values, null, false, mayHaveMoreCellsInRow);
            results.add(r);
            numOfResults++;
            if (!mayHaveMoreCellsInRow && limitOfRows > 0) {
              numOfCompleteRows++;
              checkLimitOfRows(numOfCompleteRows, limitOfRows, moreRows, scannerContext, builder);
              if (builder.hasMoreResults() && !builder.getMoreResults()) {
                break;
              }
            }
          } else if (!moreRows && results.size() > 0) {
            // No more cells for the scan here, we need to ensure that the mayHaveMoreCellsInRow of
            // last result is false. Otherwise it's possible that: the first nextRaw returned
            // because BATCH_LIMIT_REACHED (BTW it happen to exhaust all cells of the scan),so the
            // last result's mayHaveMoreCellsInRow will be true. while the following nextRaw will
            // return with moreRows=false, which means moreResultsInRegion would be false, it will
            // be a contradictory state (HBASE-21206).
            int lastIdx = results.size() - 1;
            Result r = results.get(lastIdx);
            if (r.mayHaveMoreCellsInRow()) {
              results.set(lastIdx, Result.create(r.rawCells(), r.getExists(), r.isStale(), false));
            }
          }

          boolean sizeLimitReached = scannerContext.checkSizeLimit(LimitScope.BETWEEN_ROWS);
          boolean timeLimitReached = scannerContext.checkTimeLimit(LimitScope.BETWEEN_ROWS);
          boolean resultsLimitReached = numOfResults  >= maxResults;
          limitReached = sizeLimitReached || timeLimitReached || resultsLimitReached;

          if (limitReached || !moreRows) {
            if (LOG.isTraceEnabled()) {
              LOG.trace("Done scanning. limitReached: " + limitReached + " moreRows: " + moreRows
                  + " scannerContext: " + scannerContext);
            }
            // We only want to mark a ScanResponse as a heartbeat message in the event that
            // there are more values to be read server side. If there aren't more values,
            // marking it as a heartbeat is wasteful because the client will need to issue
            // another ScanRequest only to realize that they already have all the values
            if (moreRows) {
              // Heartbeat messages occur when the time limit has been reached.
              builder.setHeartbeatMessage(timeLimitReached);
              if (timeLimitReached && rsh.needCursor) {
                Cell cursorCell = scannerContext.getLastPeekedCell();
                if (cursorCell != null) {
                  builder.setCursor(ProtobufUtil.toCursor(cursorCell));
                }
              }
            }
            break;
          }
          values.clear();
        }
        if (limitReached || moreRows) {
          // We stopped prematurely
          builder.setMoreResultsInRegion(true);
        } else {
          // We didn't get a single batch
          builder.setMoreResultsInRegion(false);
        }
        region.updateReadCapacityUnitMetrics(scannerContext.getSizeProgress());
      }
      region.updateScanCountPerSecond(1);
    } finally {
      region.closeRegionOperation();
    }
    // coprocessor postNext hook
    if (region.getCoprocessorHost() != null) {
      region.getCoprocessorHost().postScannerNext(scanner, results, maxResults, true);
    }
    metricsRegionServer.updateScan(region.getTableDesc().getTableName(), EnvironmentEdgeManager.currentTimeMillis() - before);
  }

  private void closeScanner(HRegion region, RegionScanner scanner, String scannerName,
      RpcCallContext context) throws IOException {
    if (region.getCoprocessorHost() != null) {
      if (region.getCoprocessorHost().preScannerClose(scanner)) {
        // bypass the actual close.
        return;
      }
    }
    RegionScannerHolder rsh = scanners.remove(scannerName);
    if (rsh != null) {
      rsh.s.close(false);
      if (region.getCoprocessorHost() != null) {
        region.getCoprocessorHost().postScannerClose(scanner);
      }
      closedScanners.put(scannerName, scannerName);
    }
  }

  /**
   * Scan data in a table.
   *
   * @param controller the RPC controller
   * @param request the scan request
   * @throws ServiceException
   */
  @Override
  public ScanResponse scan(final RpcController controller, final ScanRequest request)
      throws ServiceException {
    if (controller != null && !(controller instanceof HBaseRpcController)) {
      throw new UnsupportedOperationException(
          "We only do HBaseRpcController! FIX IF A PROBLEM: " + controller);
    }
    if (!request.hasScannerId() && !request.hasScan()) {
      throw new ServiceException(
          new DoNotRetryIOException("Missing required input: scannerId or scan"));
    }
    if (Trace.isTracing() && Trace.currentSpan() != null) {
      Trace.currentSpan().addKVAnnotation(Bytes.toBytes("start_row"), request.getScan().getStartRow().toByteArray());
      Trace.currentSpan().addKVAnnotation(Bytes.toBytes("stop_row"), request.getScan().getStopRow().toByteArray());
    }
    try {
      checkOpen();
    } catch (IOException e) {
      if (request.hasScannerId()) {
        String scannerName = Long.toString(request.getScannerId());
        if (LOG.isDebugEnabled()) {
          LOG.debug(
            "Server shutting down and client tried to access missing scanner " + scannerName);
        }
        if (leases != null) {
          try {
            leases.cancelLease(scannerName);
          } catch (LeaseException le) {
            // No problem, ignore
            if (LOG.isTraceEnabled()) {
              LOG.trace("Un-able to cancel lease of scanner. It could already be closed.");
            }
          }
        }
      }
      throw new ServiceException(e);
    }
    requestCount.increment();
    RegionScannerHolder rsh;
    ScanResponse.Builder builder = ScanResponse.newBuilder();
    try {
      if (request.hasScannerId()) {
        // The downstream projects such as AsyncHBase in OpenTSDB need this value. See HBASE-18000
        // for more details.
        builder.setScannerId(request.getScannerId());
        rsh = getRegionScanner(request);
      } else {
        rsh = newRegionScanner(request, builder);
      }
    } catch (IOException e) {
      if (e == SCANNER_ALREADY_CLOSED) {
        // Now we will close scanner automatically if there are no more results for this region but
        // the old client will still send a close request to us. Just ignore it and return.
        return builder.build();
      }
      throw new ServiceException(e);
    }
    HRegion region = rsh.r;
    String scannerName = rsh.scannerName;
    Leases.Lease lease;
    try {
      // Remove lease while its being processed in server; protects against case
      // where processing of request takes > lease expiration time.
      lease = leases.removeLease(scannerName);
    } catch (LeaseException e) {
      throw new ServiceException(e);
    }
    if (request.hasRenew() && request.getRenew()) {
      // add back and return
      addScannerLeaseBack(lease);
      try {
        checkScanNextCallSeq(request, rsh);
      } catch (OutOfOrderScannerNextException e) {
        throw new ServiceException(e);
      }
      return builder.build();
    }
    long maxQuotaResultSize;
    if (isQuotaEnabled()) {
      try {
        OperationQuota quota = rsQuotaManager.checkQuota(region, OperationQuota.OperationType.SCAN);
        maxQuotaResultSize =
            Math.min(maxScannerResultSize, rsQuotaManager.getQuotaReadAvailable(quota));
      } catch (IOException e) {
        addScannerLeaseBack(lease);
        throw new ServiceException(e);
      }
    } else {
      maxQuotaResultSize = maxScannerResultSize;
    }
    try {
      checkScanNextCallSeq(request, rsh);
    } catch (OutOfOrderScannerNextException e) {
      addScannerLeaseBack(lease);
      throw new ServiceException(e);
    }
    // Now we have increased the next call sequence. If we give client an error, the retry will
    // never success. So we'd better close the scanner and return a DoNotRetryIOException to client
    // and then client will try to open a new scanner.
    boolean closeScanner = request.hasCloseScanner() ? request.getCloseScanner() : false;
    int rows; // this is scan.getCaching
    if (request.hasNumberOfRows()) {
      rows = request.getNumberOfRows();
    } else {
      rows = closeScanner ? 0 : 1;
    }
    RpcCallContext context = RpcServer.getCurrentCall();
    // now let's do the real scan.
    RegionScanner scanner = rsh.s;

    //trace the scan operation
    if (Trace.isTracing() && Trace.currentSpan() != null) {
      Trace.currentSpan().addKVAnnotation(Bytes.toBytes("region"), Bytes.toBytes(region.toString()));
    }
    // this is the limit of rows for this scan, if we the number of rows reach this value, we will
    // close the scanner.
    int limitOfRows;
    if (request.hasLimitOfRows()) {
      limitOfRows = request.getLimitOfRows();
    } else {
      limitOfRows = -1;
    }
    boolean scannerClosed = false;
    try {
      List<Result> results = new ArrayList<>();
      long totalKvSize = 0L;
      long resultCells = 0;
      if (rows > 0) {
        boolean done = false;
        // Call coprocessor. Get region info from scanner.
        if (region.getCoprocessorHost() != null) {
          Boolean bypass = region.getCoprocessorHost().preScannerNext(scanner, results, rows);
          if (!results.isEmpty()) {
            for (Result r : results) {
              for (Cell cell : r.rawCells()) {
                totalKvSize += KeyValueUtil.ensureKeyValue(cell).getLength();
              }
              resultCells += r.rawCells().length;
            }
          }
          if (bypass != null && bypass.booleanValue()) {
            done = true;
          }
        }
        if (!done) {
          // Update the scan metrics from coprocessor
          if (totalKvSize != 0) {
            region.getMetrics().updateScanNext(totalKvSize);
            region.updateReadCapacityUnitMetrics(totalKvSize);
          }
          region.updateReadCellMetrics(resultCells);
          scan((HBaseRpcController) controller, request, rsh, maxQuotaResultSize, rows, limitOfRows,
            results, builder, context);
        } else {
          builder.setMoreResultsInRegion(!results.isEmpty());
        }
      } else {
        builder.setMoreResultsInRegion(true);
      }

      if (isQuotaEnabled()) {
        rsQuotaManager.grabQuota(region, results);
      }
      addResults(builder, results, (HBaseRpcController) controller,
        isClientCellBlockSupport(context));
      if (scanner.isFilterDone() && results.isEmpty()) {
        // If the scanner's filter - if any - is done with the scan
        // only set moreResults to false if the results is empty. This is used to keep compatible
        // with the old scan implementation where we just ignore the returned results if moreResults
        // is false. Can remove the isEmpty check after we get rid of the old implementation.
        builder.setMoreResults(false);
      }
      // we only set moreResults to false in the above code, so set it to true if we haven't set it
      // yet.
      if (!builder.hasMoreResults()) {
        builder.setMoreResults(true);
      }
      assert builder.hasMoreResultsInRegion();
      if (builder.getMoreResults() && builder.getMoreResultsInRegion() && !results.isEmpty()) {
        // Record the last cell of the last result if it is a partial result
        // We need this to calculate the complete rows we have returned to client as the
        // mayHaveMoreCellsInRow is true does not mean that there will be extra cells for the
        // current row. We may filter out all the remaining cells for the current row and just
        // return the cells of the nextRow when calling RegionScanner.nextRaw. So here we need to
        // check for row change.
        Result lastResult = results.get(results.size() - 1);
        if (lastResult.mayHaveMoreCellsInRow()) {
          rsh.rowOfLastPartialResult = lastResult.getRow();
        } else {
          rsh.rowOfLastPartialResult = null;
        }
      }
      if (!builder.getMoreResults() || !builder.getMoreResultsInRegion() || closeScanner) {
        scannerClosed = true;
        closeScanner(region, scanner, scannerName, context);
      }
      return builder.build();
    } catch (Exception e) {
      try {
        // scanner is closed here
        scannerClosed = true;
        // The scanner state might be left in a dirty state, so we will tell the Client to
        // fail this RPC and close the scanner while opening up another one from the start of
        // row that the client has last seen.
        closeScanner(region, scanner, scannerName, context);

        // rethrow DoNotRetryIOException. This can avoid the retry in ClientScanner.
        if (e instanceof DoNotRetryIOException) {
          throw e;
        }

        // We closed the scanner already. Instead of throwing the IOException, and client
        // retrying with the same scannerId only to get USE on the next RPC, we directly throw
        // a special exception to save an RPC.
        throw new UnknownScannerException("Scanner is closed on the server-side", e);
      } catch (IOException ioe) {
        throw new ServiceException(ioe);
      }
    } finally {
      if (!scannerClosed) {
        // Adding resets expiration time on lease.
        addScannerLeaseBack(lease);
      }
    }
  }

  private void addResults(ScanResponse.Builder builder, List<Result> results,
      HBaseRpcController controller, boolean clientCellBlockSupported) {
    if (results.isEmpty()) return;
    if (clientCellBlockSupported) {
      for (Result res : results) {
        builder.addCellsPerResult(res.size());
        builder.addPartialFlagPerResult(res.mayHaveMoreCellsInRow());
      }
      controller.setCellScanner(CellUtil.createCellScanner(results));
    } else {
      for (Result res : results) {
        ClientProtos.Result pbr = ProtobufUtil.toResult(res);
        builder.addResults(pbr);
      }
    }
  }

  /**
   * Atomically bulk load several HFiles into an open region
   * @return true if successful, false is failed but recoverably (no action)
   * @throws IOException if failed unrecoverably
   */
  @Override
  public BulkLoadHFileResponse bulkLoadHFile(final RpcController controller,
      final BulkLoadHFileRequest request) throws ServiceException {
    try {
      checkOpen();
      requestCount.increment();
      HRegion region = getRegion(request.getRegion());
      List<Pair<byte[], String>> familyPaths = new ArrayList<Pair<byte[], String>>();
      for (FamilyPath familyPath: request.getFamilyPathList()) {
        familyPaths.add(new Pair<byte[], String>(familyPath.getFamily().toByteArray(),
          familyPath.getPath()));
      }
      boolean bypass = false;
      if (region.getCoprocessorHost() != null) {
        bypass = region.getCoprocessorHost().preBulkLoadHFile(familyPaths);
      }
      boolean loaded = false;
      if (!bypass) {
        loaded = region.bulkLoadHFiles(familyPaths, request.getAssignSeqNum());
      }
      if (region.getCoprocessorHost() != null) {
        loaded = region.getCoprocessorHost().postBulkLoadHFile(familyPaths, loaded);
      }
      BulkLoadHFileResponse.Builder builder = BulkLoadHFileResponse.newBuilder();
      builder.setLoaded(loaded);
      return builder.build();
    } catch (IOException ie) {
      throw new ServiceException(ie);
    }
  }

  @Override
  public CoprocessorServiceResponse execService(final RpcController controller,
      final CoprocessorServiceRequest request) throws ServiceException {
    try {
      checkOpen();
      requestCount.increment();
      HRegion region = getRegion(request.getRegion());
      Message result = execServiceOnRegion(region, request.getCall());
      CoprocessorServiceResponse.Builder builder =
          CoprocessorServiceResponse.newBuilder();
      builder.setRegion(RequestConverter.buildRegionSpecifier(
          RegionSpecifierType.REGION_NAME, region.getRegionName()));
      builder.setValue(
          builder.getValueBuilder().setName(result.getClass().getName())
              .setValue(result.toByteString()));
      return builder.build();
    } catch (IOException ie) {
      throw new ServiceException(ie);
    }
  }

  private Message execServiceOnRegion(HRegion region,
      final ClientProtos.CoprocessorServiceCall serviceCall) throws IOException {
    // ignore the passed in controller (from the serialized call)
    ServerRpcController execController = new ServerRpcController();
    Message result = region.execService(execController, serviceCall);
    if (execController.getFailedOn() != null) {
      throw execController.getFailedOn();
    }
    return result;
  }

  @Override
  public CoprocessorServiceResponse execRegionServerService(final RpcController controller,
      final CoprocessorServiceRequest serviceRequest) throws ServiceException {
    try {
      ServerRpcController execController = new ServerRpcController();
      CoprocessorServiceCall call = serviceRequest.getCall();
      String serviceName = call.getServiceName();
      String methodName = call.getMethodName();
      if (!coprocessorServiceHandlers.containsKey(serviceName)) {
        throw new UnknownProtocolException(null,
            "No registered coprocessor service found for name " + serviceName);
      }
      Service service = coprocessorServiceHandlers.get(serviceName);
      Descriptors.ServiceDescriptor serviceDesc = service.getDescriptorForType();
      Descriptors.MethodDescriptor methodDesc = serviceDesc.findMethodByName(methodName);
      if (methodDesc == null) {
        throw new UnknownProtocolException(service.getClass(), "Unknown method " + methodName
            + " called on service " + serviceName);
      }
      Message request =
          service.getRequestPrototype(methodDesc).newBuilderForType().mergeFrom(call.getRequest())
              .build();
      final Message.Builder responseBuilder =
          service.getResponsePrototype(methodDesc).newBuilderForType();
      service.callMethod(methodDesc, controller, request, new RpcCallback<Message>() {
        @Override
        public void run(Message message) {
          if (message != null) {
            responseBuilder.mergeFrom(message);
          }
        }
      });
      Message execResult = responseBuilder.build();
      if (execController.getFailedOn() != null) {
        throw execController.getFailedOn();
      }
      ClientProtos.CoprocessorServiceResponse.Builder builder =
          ClientProtos.CoprocessorServiceResponse.newBuilder();
      builder.setRegion(RequestConverter.buildRegionSpecifier(RegionSpecifierType.REGION_NAME,
        HConstants.EMPTY_BYTE_ARRAY));
      builder.setValue(builder.getValueBuilder().setName(execResult.getClass().getName())
          .setValue(execResult.toByteString()));
      return builder.build();
    } catch (IOException ie) {
      throw new ServiceException(ie);
    }
  }

  /**
   * @return Return the object that implements the replication
   * source service.
   */
  public ReplicationSourceService getReplicationSourceService() {
    return replicationSourceHandler;
  }

  /**
   * @return Return the object that implements the replication
   * sink service.
   */
  public ReplicationSinkService getReplicationSinkService() {
    return replicationSinkHandler;
  }

  /**
   * Execute multiple actions on a table: get, mutate, and/or execCoprocessor
   *
   * @param rpcc the RPC controller
   * @param request the multi request
   * @throws ServiceException
   */
  @Override
  public MultiResponse multi(final RpcController rpcc, final MultiRequest request)
  throws ServiceException {
    try {
      checkOpen();
    } catch (IOException ie) {
      throw new ServiceException(ie);
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Received request " + request);
    }
    // rpc controller is how we bring in data via the back door;  it is unprotobuf'ed data.
    // It is also the conduit via which we pass back data.
    HBaseRpcController controller = (HBaseRpcController)rpcc;
    CellScanner cellScanner = controller != null ? controller.cellScanner(): null;
    if (controller != null) controller.setCellScanner(null);

    long nonceGroup = request.hasNonceGroup() ? request.getNonceGroup() : HConstants.NO_NONCE;

    // this will contain all the cells that we need to return. It's created later, if needed.
    List<CellScannable> cellsToReturn = null;
    MultiResponse.Builder responseBuilder = MultiResponse.newBuilder();
    RegionActionResult.Builder regionActionResultBuilder = RegionActionResult.newBuilder();
    Boolean processed = null;
    RpcCallContext context = RpcServer.getCurrentCall();

    for (RegionAction regionAction : request.getRegionActionList()) {
      this.requestCount.add(regionAction.getActionCount());
      HRegion region;
      regionActionResultBuilder.clear();
      try {
        region = getRegion(regionAction.getRegion());
      } catch (IOException e) {
        regionActionResultBuilder.setException(ResponseConverter.buildException(e));
        responseBuilder.addRegionActionResult(regionActionResultBuilder.build());
        if(cellScanner != null){
          skipCellsForMutations(regionAction.getActionList(), cellScanner);
        }
        continue;  // For this region it's a failure.
      }
      if (Trace.isTracing() && Trace.currentSpan() != null) {
        Trace.currentSpan().addTimelineAnnotation("do multi to a specific region " + Bytes.toString(region.getRegionName()));
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Get region " + region + " for " + regionAction);
      }
      if (regionAction.hasAtomic() && regionAction.getAtomic()) {
        // How does this call happen?  It may need some work to play well w/ the surroundings.
        // Need to return an item per Action along w/ Action index.  TODO.
        try {
          if (request.hasCondition()) {
            Condition condition = request.getCondition();
            byte[] row = condition.getRow().toByteArray();
            byte[] family = condition.getFamily().toByteArray();
            byte[] qualifier = condition.getQualifier().toByteArray();
            CompareOp compareOp = CompareOp.valueOf(condition.getCompareType().name());
            ByteArrayComparable comparator =
              ProtobufUtil.toComparator(condition.getComparator());
            processed = checkAndRowMutate(region, regionAction.getActionList(),
              cellScanner, row, family, qualifier, compareOp, comparator);
          } else {
            ClientProtos.RegionLoadStats stats = mutateRows(region, regionAction.getActionList(),
              cellScanner);
            // add the stats to the request
            if (stats != null) {
              responseBuilder.addRegionActionResult(RegionActionResult.newBuilder()
                .addResultOrException(ResultOrException.newBuilder().setLoadStats(stats)));
            }
            processed = Boolean.TRUE;
          }
        } catch (IOException e) {
          if ((e instanceof CallerDisconnectedException)
              || (e.getCause() instanceof CallerDisconnectedException)) {
            throw new ServiceException(e);
          }
          // As it's atomic, we may expect it's a global failure.
          regionActionResultBuilder.setException(ResponseConverter.buildException(e));
        }
      } else {
        // doNonAtomicRegionMutation manages the exception internally
        cellsToReturn = doNonAtomicRegionMutation(region, regionAction, cellScanner,
            regionActionResultBuilder, cellsToReturn, nonceGroup, context);
      }
      responseBuilder.addRegionActionResult(regionActionResultBuilder.build());
    }
    // Load the controller with the Cells to return.
    if (cellsToReturn != null && !cellsToReturn.isEmpty() && controller != null) {
      controller.setCellScanner(CellUtil.createCellScanner(cellsToReturn));
    }
    if (processed != null) responseBuilder.setProcessed(processed);
    return responseBuilder.build();
  }

  private void skipCellsForMutations(List<ClientProtos.Action> actions, CellScanner cellScanner) {
    for (ClientProtos.Action action : actions) {
      skipCellsForMutation(action, cellScanner);
    }
  }

  private void skipCellsForMutation(ClientProtos.Action action, CellScanner cellScanner) {
    try {
      if (action.hasMutation()) {
        MutationProto m = action.getMutation();
        if (m.hasAssociatedCellCount()) {
          for (int i = 0; i < m.getAssociatedCellCount(); i++) {
            cellScanner.advance();
          }
        }
      }
    } catch (IOException e) {
      // No need to handle these Individual Muatation level issue. Any way this entire RegionAction
      // marked as failed as we could not see the Region here. At client side the top level
      // RegionAction exception will be considered first.
      LOG.error("Error while skipping Cells in CellScanner for invalid Region Mutations", e);
    }
  }

  /**
   * Run through the regionMutation <code>rm</code> and per Mutation, do the work, and then when
   * done, add an instance of a {@link ResultOrException} that corresponds to each Mutation.
   * @param region
   * @param actions
   * @param cellScanner
   * @param builder
   * @param cellsToReturn  Could be null. May be allocated in this method.  This is what this
   * method returns as a 'result'.
   * @return Return the <code>cellScanner</code> passed
   */
  private List<CellScannable> doNonAtomicRegionMutation(final HRegion region,
      final RegionAction actions, final CellScanner cellScanner,
      final RegionActionResult.Builder builder, List<CellScannable> cellsToReturn, long nonceGroup,
      RpcCallContext context)
      throws ServiceException {
    long before = EnvironmentEdgeManager.currentTimeMillis();
    // Gather up CONTIGUOUS Puts and Deletes in this mutations List.  Idea is that rather than do
    // one at a time, we instead pass them in batch.  Be aware that the corresponding
    // ResultOrException instance that matches each Put or Delete is then added down in the
    // doBatchOp call.  We should be staying aligned though the Put and Delete are deferred/batched
    List<ClientProtos.Action> mutations = null;
    IOException sizeIOE = null;
    Object lastBlock = null;
    for (ClientProtos.Action action: actions.getActionList()) {
      ClientProtos.ResultOrException.Builder resultOrExceptionBuilder = null;
      try {
        Result r = null;
        if (context != null
            && (context.getResponseCellSize() > maxScannerResultSize || context
                .getResponseBlockSize() > maxScannerResultSize)) {
          // We're storing the exception since the exception and reason string won't
          // change after the response size limit is reached.
          if (sizeIOE == null) {
            // We don't need the stack un-winding do don't throw the exception.
            // Throwing will kill the JVM's JIT.
            //
            // Instead just create the exception and then store it.
            sizeIOE = new MultiActionResultTooLarge("Max response size exceeded: " + " CellSize: "
                + context.getResponseCellSize() + " BlockSize: " + context.getResponseBlockSize());

            // Only report the exception once since there's only one request that
            // caused the exception. Otherwise this number will dominate the exceptions count.
            rpcServer.getMetrics().exception(sizeIOE);
          }

          // Now that there's an exception is know to be created
          // use it for the response.
          //
          // This will create a copy in the builder.
          resultOrExceptionBuilder = ResultOrException.newBuilder().setException(
            ResponseConverter.buildException(sizeIOE));
          resultOrExceptionBuilder.setIndex(action.getIndex());
          builder.addResultOrException(resultOrExceptionBuilder.build());
          continue;
        }
        if (action.hasGet()) {
          if (isQuotaEnabled()) {
            this.rsQuotaManager.checkQuota(region, OperationType.GET);
          }
          Get get = ProtobufUtil.toGet(action.getGet());
          r = region.get(get);
          if (isQuotaEnabled() && r != null) {
            this.rsQuotaManager.grabQuota(region, r);
          }
        } else if (action.hasServiceCall()) {
          resultOrExceptionBuilder = ResultOrException.newBuilder();
          try {
            Message result = execServiceOnRegion(region, action.getServiceCall());
            ClientProtos.CoprocessorServiceResult.Builder serviceResultBuilder =
                ClientProtos.CoprocessorServiceResult.newBuilder();
            resultOrExceptionBuilder.setServiceResult(
                serviceResultBuilder.setValue(
                  serviceResultBuilder.getValueBuilder()
                    .setName(result.getClass().getName())
                    .setValue(result.toByteString())));
          } catch (IOException ioe) {
            resultOrExceptionBuilder.setException(ResponseConverter.buildException(ioe));
          }
        } else if (action.hasMutation()) {
          MutationType type = action.getMutation().getMutateType();
          if (type != MutationType.PUT && type != MutationType.DELETE && mutations != null &&
              !mutations.isEmpty()) {
            // Flush out any Puts or Deletes already collected.
            doBatchOp(builder, region, mutations, cellScanner);
            mutations.clear();
          }
          switch (type) {
          case APPEND:
            r = append(region, action.getMutation(), cellScanner, nonceGroup);
            break;
          case INCREMENT:
            r = increment(region, action.getMutation(), cellScanner,  nonceGroup);
            break;
          case PUT:
          case DELETE:
            // Collect the individual mutations and apply in a batch
            if (mutations == null) {
              mutations = new ArrayList<ClientProtos.Action>(actions.getActionCount());
            }
            mutations.add(action);
            break;
          default:
            throw new DoNotRetryIOException("Unsupported mutate type: " + type.name());
          }
        } else {
          throw new HBaseIOException("Unexpected Action type");
        }
        if (r != null) {
          ClientProtos.Result pbResult = null;
          if (isClientCellBlockSupport(RpcServer.getCurrentCall())) {
            pbResult = ProtobufUtil.toResultNoData(r);
            //  Hard to guess the size here.  Just make a rough guess.
            if (cellsToReturn == null) cellsToReturn = new ArrayList<CellScannable>();
            cellsToReturn.add(r);
          } else {
            pbResult = ProtobufUtil.toResult(r);
          }
          lastBlock = addSize(context, r, lastBlock);
          resultOrExceptionBuilder =
            ClientProtos.ResultOrException.newBuilder().setResult(pbResult);
        }
        // Could get to here and there was no result and no exception.  Presumes we added
        // a Put or Delete to the collecting Mutations List for adding later.  In this
        // case the corresponding ResultOrException instance for the Put or Delete will be added
        // down in the doBatchOp method call rather than up here.
      } catch (IOException ie) {
        if ((ie instanceof CallerDisconnectedException)
            || (ie.getCause() instanceof CallerDisconnectedException)) {
          throw new ServiceException(ie);
        }
        resultOrExceptionBuilder = ResultOrException.newBuilder().
          setException(ResponseConverter.buildException(ie));
      }
      if (resultOrExceptionBuilder != null) {
        // Propagate index.
        resultOrExceptionBuilder.setIndex(action.getIndex());
        builder.addResultOrException(resultOrExceptionBuilder.build());
      }
    }
    // Finish up any outstanding mutations
    if (mutations != null && !mutations.isEmpty()) {
      doBatchOp(builder, region, mutations, cellScanner);
    }
    metricsRegionServer.updateBatch(region.getTableDesc().getTableName(),
      EnvironmentEdgeManager.currentTimeMillis() - before);
    return cellsToReturn;
  }

// End Client methods
// Start Admin methods

  @Override
  @QosPriority(priority=HConstants.HIGH_QOS)
  public GetRegionInfoResponse getRegionInfo(final RpcController controller,
      final GetRegionInfoRequest request) throws ServiceException {
    try {
      checkOpen();
      requestCount.increment();
      HRegion region = getRegion(request.getRegion());
      HRegionInfo info = region.getRegionInfo();
      GetRegionInfoResponse.Builder builder = GetRegionInfoResponse.newBuilder();
      builder.setRegionInfo(HRegionInfo.convert(info));
      if (request.hasCompactionState() && request.getCompactionState()) {
        builder.setCompactionState(region.getCompactionState());
      }
      builder.setIsRecovering(region.isRecovering());
      return builder.build();
    } catch (IOException ie) {
      throw new ServiceException(ie);
    }
  }

  @Override
  public GetStoreFileResponse getStoreFile(final RpcController controller,
      final GetStoreFileRequest request) throws ServiceException {
    try {
      checkOpen();
      HRegion region = getRegion(request.getRegion());
      requestCount.increment();
      Set<byte[]> columnFamilies;
      if (request.getFamilyCount() == 0) {
        columnFamilies = region.getStores().keySet();
      } else {
        columnFamilies = new TreeSet<byte[]>(Bytes.BYTES_RAWCOMPARATOR);
        for (ByteString cf: request.getFamilyList()) {
          columnFamilies.add(cf.toByteArray());
        }
      }
      int nCF = columnFamilies.size();
      List<String>  fileList = region.getStoreFileList(
        columnFamilies.toArray(new byte[nCF][]));
      GetStoreFileResponse.Builder builder = GetStoreFileResponse.newBuilder();
      builder.addAllStoreFile(fileList);
      return builder.build();
    } catch (IOException ie) {
      throw new ServiceException(ie);
    }
  }

  @Override
  @QosPriority(priority=HConstants.HIGH_QOS)
  public GetOnlineRegionResponse getOnlineRegion(final RpcController controller,
      final GetOnlineRegionRequest request) throws ServiceException {
    try {
      checkOpen();
      requestCount.increment();
      List<HRegionInfo> list = new ArrayList<HRegionInfo>(onlineRegions.size());
      for (HRegion region: this.onlineRegions.values()) {
        list.add(region.getRegionInfo());
      }
      Collections.sort(list);
      return ResponseConverter.buildGetOnlineRegionResponse(list);
    } catch (IOException ie) {
      throw new ServiceException(ie);
    }
  }

  // Region open/close direct RPCs

  /**
   * Open asynchronously a region or a set of regions on the region server.
   *
   * The opening is coordinated by ZooKeeper, and this method requires the znode to be created
   *  before being called. As a consequence, this method should be called only from the master.
   * <p>
   * Different manages states for the region are:<ul>
   *  <li>region not opened: the region opening will start asynchronously.</li>
   *  <li>a close is already in progress: this is considered as an error.</li>
   *  <li>an open is already in progress: this new open request will be ignored. This is important
   *  because the Master can do multiple requests if it crashes.</li>
   *  <li>the region is already opened:  this new open request will be ignored./li>
   *  </ul>
   * </p>
   * <p>
   * Bulk assign: If there are more than 1 region to open, it will be considered as a bulk assign.
   * For a single region opening, errors are sent through a ServiceException. For bulk assign,
   * errors are put in the response as FAILED_OPENING.
   * </p>
   * @param controller the RPC controller
   * @param request the request
   * @throws ServiceException
   */
  @Override
  @QosPriority(priority=HConstants.HIGH_QOS)
  public OpenRegionResponse openRegion(final RpcController controller,
      final OpenRegionRequest request) throws ServiceException {
    try {
      checkOpen();
    } catch (IOException ie) {
      throw new ServiceException(ie);
    }
    requestCount.increment();
    if (request.hasServerStartCode() && this.serverNameFromMasterPOV != null) {
      // check that we are the same server that this RPC is intended for.
      long serverStartCode = request.getServerStartCode();
      if (this.serverNameFromMasterPOV.getStartcode() !=  serverStartCode) {
        throw new ServiceException(new DoNotRetryIOException("This RPC was intended for a " +
            "different server with startCode: " + serverStartCode + ", this server is: "
            + this.serverNameFromMasterPOV));
      }
    }
    OpenRegionResponse.Builder builder = OpenRegionResponse.newBuilder();
    final int regionCount = request.getOpenInfoCount();
    final Map<TableName, HTableDescriptor> htds =
        new HashMap<TableName, HTableDescriptor>(regionCount);
    final boolean isBulkAssign = regionCount > 1;
    for (RegionOpenInfo regionOpenInfo : request.getOpenInfoList()) {
      final HRegionInfo region = HRegionInfo.convert(regionOpenInfo.getRegion());

      int versionOfOfflineNode = -1;
      if (regionOpenInfo.hasVersionOfOfflineNode()) {
        versionOfOfflineNode = regionOpenInfo.getVersionOfOfflineNode();
      }
      HTableDescriptor htd;
      try {
        final HRegion onlineRegion = getFromOnlineRegions(region.getEncodedName());
        if (onlineRegion != null) {
          //Check if the region can actually be opened.
          if (onlineRegion.getCoprocessorHost() != null) {
            onlineRegion.getCoprocessorHost().preOpen();
          }
          // See HBASE-5094. Cross check with hbase:meta if still this RS is owning
          // the region.
          Pair<HRegionInfo, ServerName> p =
            MetaReader.getRegion(this.catalogTracker.getConnection(), region.getRegionName());
          if (this.getServerName().equals(p.getSecond())) {
            Boolean closing = regionsInTransitionInRS.get(region.getEncodedNameAsBytes());
            // Map regionsInTransitionInRSOnly has an entry for a region only if the region
            // is in transition on this RS, so here closing can be null. If not null, it can
            // be true or false. True means the region is opening on this RS; while false
            // means the region is closing. Only return ALREADY_OPENED if not closing (i.e.
            // not in transition any more, or still transition to open.
            if (!Boolean.FALSE.equals(closing)
                && getFromOnlineRegions(region.getEncodedName()) != null) {
              LOG.warn("Attempted open of " + region.getEncodedName()
                + " but already online on this server");
              builder.addOpeningState(RegionOpeningState.ALREADY_OPENED);
              continue;
            }
          } else {
            LOG.warn("The region " + region.getEncodedName() + " is online on this server" +
                " but hbase:meta does not have this server - continue opening.");
            removeFromOnlineRegions(onlineRegion, null);
          }
        }
        LOG.info("Open " + region.getRegionNameAsString());
        htd = htds.get(region.getTable());
        if (htd == null) {
          htd = this.tableDescriptors.get(region.getTable());
          htds.put(region.getTable(), htd);
        }

        final Boolean previous = this.regionsInTransitionInRS.putIfAbsent(
            region.getEncodedNameAsBytes(), Boolean.TRUE);

        if (Boolean.FALSE.equals(previous)) {
          // There is a close in progress. We need to mark this open as failed in ZK.
          OpenRegionHandler.
              tryTransitionFromOfflineToFailedOpen(this, region, versionOfOfflineNode);

          throw new RegionAlreadyInTransitionException("Received OPEN for the region:" +
              region.getRegionNameAsString() + " , which we are already trying to CLOSE ");
        }

        if (Boolean.TRUE.equals(previous)) {
          // An open is in progress. This is supported, but let's log this.
          LOG.info("Receiving OPEN for the region:" +
              region.getRegionNameAsString() + " , which we are already trying to OPEN" +
              " - ignoring this new request for this region.");
        }

        // We are opening this region. If it moves back and forth for whatever reason, we don't
        // want to keep returning the stale moved record while we are opening/if we close again.
        removeFromMovedRegions(region.getEncodedName());

        if (previous == null) {
          // check if the region to be opened is marked in recovering state in ZK
          if (SplitLogManager.isRegionMarkedRecoveringInZK(this.zooKeeper,
                region.getEncodedName())) {
            // check if current region open is for distributedLogReplay. This check is to support
            // rolling restart/upgrade where we want to Master/RS see same configuration
            if (!regionOpenInfo.hasOpenForDistributedLogReplay()
                  || regionOpenInfo.getOpenForDistributedLogReplay()) {
              this.recoveringRegions.put(region.getEncodedName(), null);
            } else {
              // remove stale recovery region from ZK when we open region not for recovering which
              // could happen when turn distributedLogReplay off from on.
              List<String> tmpRegions = new ArrayList<String>();
              tmpRegions.add(region.getEncodedName());
              SplitLogManager.deleteRecoveringRegionZNodes(this.zooKeeper, tmpRegions);
            }
          }
          // If there is no action in progress, we can submit a specific handler.
          // Need to pass the expected version in the constructor.
          if (region.isMetaRegion()) {
            this.service.submit(new OpenMetaHandler(this, this, region, htd,
                versionOfOfflineNode));
          } else {
            updateRegionFavoredNodesMapping(region.getEncodedName(),
                regionOpenInfo.getFavoredNodesList());
            this.service.submit(new OpenRegionHandler(this, this, region, htd,
                versionOfOfflineNode));
          }
        }

        builder.addOpeningState(RegionOpeningState.OPENED);

      } catch (KeeperException zooKeeperEx) {
        LOG.error("Can't retrieve recovering state from zookeeper", zooKeeperEx);
        throw new ServiceException(zooKeeperEx);
      } catch (IOException ie) {
        LOG.warn("Failed opening region " + region.getRegionNameAsString(), ie);
        if (isBulkAssign) {
          builder.addOpeningState(RegionOpeningState.FAILED_OPENING);
        } else {
          throw new ServiceException(ie);
        }
      }
    }

    return builder.build();
  }

  @Override
  public void updateRegionFavoredNodesMapping(String encodedRegionName,
      List<org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.ServerName> favoredNodes) {
    InetSocketAddress[] addr = new InetSocketAddress[favoredNodes.size()];
    // Refer to the comment on the declaration of regionFavoredNodesMap on why
    // it is a map of region name to InetSocketAddress[]
    for (int i = 0; i < favoredNodes.size(); i++) {
      addr[i] = InetSocketAddress.createUnresolved(favoredNodes.get(i).getHostName(),
          favoredNodes.get(i).getPort());
    }
    regionFavoredNodesMap.put(encodedRegionName, addr);
  }

  /**
   * Return the favored nodes for a region given its encoded name. Look at the
   * comment around {@link #regionFavoredNodesMap} on why it is InetSocketAddress[]
   * @param encodedRegionName
   * @return array of favored locations
   */
  @Override
  public InetSocketAddress[] getFavoredNodesForRegion(String encodedRegionName) {
    return regionFavoredNodesMap.get(encodedRegionName);
  }

  /**
   * Close a region on the region server.
   *
   * @param controller the RPC controller
   * @param request the request
   * @throws ServiceException
   */
  @Override
  @QosPriority(priority=HConstants.HIGH_QOS)
  public CloseRegionResponse closeRegion(final RpcController controller,
      final CloseRegionRequest request) throws ServiceException {
    int versionOfClosingNode = -1;
    if (request.hasVersionOfClosingNode()) {
      versionOfClosingNode = request.getVersionOfClosingNode();
    }
    boolean zk = request.getTransitionInZK();
    final ServerName sn = (request.hasDestinationServer() ?
      ProtobufUtil.toServerName(request.getDestinationServer()) : null);

    try {
      checkOpen();
      if (request.hasServerStartCode() && this.serverNameFromMasterPOV != null) {
        // check that we are the same server that this RPC is intended for.
        long serverStartCode = request.getServerStartCode();
        if (this.serverNameFromMasterPOV.getStartcode() !=  serverStartCode) {
          throw new ServiceException(new DoNotRetryIOException("This RPC was intended for a " +
              "different server with startCode: " + serverStartCode + ", this server is: "
              + this.serverNameFromMasterPOV));
        }
      }
      final String encodedRegionName = ProtobufUtil.getRegionEncodedName(request.getRegion());

      // Can be null if we're calling close on a region that's not online
      final HRegion region = this.getFromOnlineRegions(encodedRegionName);
      if ((region  != null) && (region .getCoprocessorHost() != null)) {
        region.getCoprocessorHost().preClose(false);
      }

      requestCount.increment();
      LOG.info("Close " + encodedRegionName + ", via zk=" + (zk ? "yes" : "no") +
        ", znode version=" + versionOfClosingNode + ", on " + sn);

      boolean closed = closeRegion(encodedRegionName, false, zk, versionOfClosingNode, sn);
      CloseRegionResponse.Builder builder = CloseRegionResponse.newBuilder().setClosed(closed);
      return builder.build();
    } catch (IOException ie) {
      throw new ServiceException(ie);
    }
  }

  /**
   * Flush a region on the region server.
   *
   * @param controller the RPC controller
   * @param request the request
   * @throws ServiceException
   */
  @Override
  @QosPriority(priority=HConstants.HIGH_QOS)
  public FlushRegionResponse flushRegion(final RpcController controller,
      final FlushRegionRequest request) throws ServiceException {
    try {
      checkOpen();
      requestCount.increment();
      HRegion region = getRegion(request.getRegion());
      LOG.info("Flushing " + region.getRegionNameAsString());
      boolean shouldFlush = true;
      if (request.hasIfOlderThanTs()) {
        shouldFlush = region.getLastFlushTime() < request.getIfOlderThanTs();
      }
      FlushRegionResponse.Builder builder = FlushRegionResponse.newBuilder();
      if (shouldFlush) {
        long startTime = EnvironmentEdgeManager.currentTimeMillis();
        HRegion.FlushResult flushResult = region.flushcache();
        if (flushResult.isFlushSucceeded()) {
          long endTime = EnvironmentEdgeManager.currentTimeMillis();
          metricsRegionServer.updateFlushTime(endTime - startTime);
        }
        boolean result = flushResult.isCompactionNeeded();
        if (result) {
          this.compactSplitThread.requestSystemCompaction(region,
              "Compaction through user triggered flush");
        }
        builder.setFlushed(result);
      }
      builder.setLastFlushTime(region.getLastFlushTime());
      return builder.build();
    } catch (DroppedSnapshotException ex) {
      // Cache flush can fail in a few places. If it fails in a critical
      // section, we get a DroppedSnapshotException and a replay of hlog
      // is required. Currently the only way to do this is a restart of
      // the server.
      abortIfFileSystemAvailable("Replay of HLog required. Forcing server shutdown", ex);
      throw new ServiceException(ex);
    } catch (IOException ie) {
      throw new ServiceException(ie);
    }
  }

  /**
   * Split a region on the region server.
   *
   * @param controller the RPC controller
   * @param request the request
   * @throws ServiceException
   */
  @Override
  @QosPriority(priority=HConstants.HIGH_QOS)
  public SplitRegionResponse splitRegion(final RpcController controller,
      final SplitRegionRequest request) throws ServiceException {
    try {
      checkOpen();
      requestCount.increment();
      HRegion region = getRegion(request.getRegion());
      region.startRegionOperation(Operation.SPLIT_REGION);
      LOG.info("Splitting " + region.getRegionNameAsString());
      long startTime = EnvironmentEdgeManager.currentTimeMillis();
      HRegion.FlushResult flushResult = region.flushcache();
      if (flushResult.isFlushSucceeded()) {
        long endTime = EnvironmentEdgeManager.currentTimeMillis();
        metricsRegionServer.updateFlushTime(endTime - startTime);
      }
      byte[] splitPoint = null;
      if (request.hasSplitPoint()) {
        splitPoint = request.getSplitPoint().toByteArray();
      }
      region.forceSplit(splitPoint);
      compactSplitThread.requestSplit(region, region.checkSplit());
      return SplitRegionResponse.newBuilder().build();
    } catch (IOException ie) {
      throw new ServiceException(ie);
    }
  }

  /**
   * Merge regions on the region server.
   *
   * @param controller the RPC controller
   * @param request the request
   * @return merge regions response
   * @throws ServiceException
   */
  @Override
  @QosPriority(priority = HConstants.HIGH_QOS)
  public MergeRegionsResponse mergeRegions(final RpcController controller,
      final MergeRegionsRequest request) throws ServiceException {
    try {
      if (splitOrMergeTracker != null
          && !splitOrMergeTracker.isSplitOrMergeEnabled(MasterSwitchType.MERGE)) {
        LOG.info("Skipping merge because merge switch is off");
        return MergeRegionsResponse.newBuilder().build();
      }
      checkOpen();
      requestCount.increment();
      HRegion regionA = getRegion(request.getRegionA());
      HRegion regionB = getRegion(request.getRegionB());
      boolean forcible = request.getForcible();
      regionA.startRegionOperation(Operation.MERGE_REGION);
      regionB.startRegionOperation(Operation.MERGE_REGION);
      LOG.info("Receiving merging request for  " + regionA + ", " + regionB
          + ",forcible=" + forcible);
      long startTime = EnvironmentEdgeManager.currentTimeMillis();
      HRegion.FlushResult flushResult = regionA.flushcache();
      if (flushResult.isFlushSucceeded()) {
        long endTime = EnvironmentEdgeManager.currentTimeMillis();
        metricsRegionServer.updateFlushTime(endTime - startTime);
      }
      startTime = EnvironmentEdgeManager.currentTimeMillis();
      flushResult = regionB.flushcache();
      if (flushResult.isFlushSucceeded()) {
        long endTime = EnvironmentEdgeManager.currentTimeMillis();
        metricsRegionServer.updateFlushTime(endTime - startTime);
      }
      compactSplitThread.requestRegionsMerge(regionA, regionB, forcible);
      return MergeRegionsResponse.newBuilder().build();
    } catch (IOException ie) {
      throw new ServiceException(ie);
    }
  }

  /**
   * Compact a region on the region server.
   *
   * @param controller the RPC controller
   * @param request the request
   * @throws ServiceException
   */
  @Override
  @QosPriority(priority=HConstants.HIGH_QOS)
  public CompactRegionResponse compactRegion(final RpcController controller,
      final CompactRegionRequest request) throws ServiceException {
    try {
      checkOpen();
      requestCount.increment();
      HRegion region = getRegion(request.getRegion());
      region.startRegionOperation(Operation.COMPACT_REGION);
      LOG.info("Compacting " + region.getRegionNameAsString());
      boolean major = false;
      byte [] family = null;
      Store store = null;
      if (request.hasFamily()) {
        family = request.getFamily().toByteArray();
        store = region.getStore(family);
        if (store == null) {
          throw new ServiceException(new IOException("column family " + Bytes.toString(family) +
            " does not exist in region " + region.getRegionNameAsString()));
        }
      }
      if (request.hasMajor()) {
        major = request.getMajor();
      }
      if (major) {
        if (family != null) {
          store.triggerMajorCompaction();
        } else {
          region.triggerMajorCompaction();
        }
      }

      String familyLogMsg = (family != null)?" for column family: " + Bytes.toString(family):"";
      LOG.trace("User-triggered compaction requested for region " +
        region.getRegionNameAsString() + familyLogMsg);
      String log = "User-triggered " + (major ? "major " : "") + "compaction" + familyLogMsg;
      if(family != null) {
        compactSplitThread.requestCompaction(region, store, log,
          Store.PRIORITY_USER, null);
      } else {
        compactSplitThread.requestCompaction(region, log,
          Store.PRIORITY_USER, null);
      }
      return CompactRegionResponse.newBuilder().build();
    } catch (IOException ie) {
      throw new ServiceException(ie);
    }
  }

  /**
   * Replicate WAL entries on the region server.
   *
   * @param controller the RPC controller
   * @param request the request
   * @throws ServiceException
   */
  @Override
  @QosPriority(priority=HConstants.REPLICATION_QOS)
  public ReplicateWALEntryResponse replicateWALEntry(final RpcController controller,
      final ReplicateWALEntryRequest request)
  throws ServiceException {
    try {
      if (replicationSinkHandler != null) {
        checkOpen();
        requestCount.increment();
        List<WALEntry> entries = request.getEntryList();
        CellScanner cellScanner = ((HBaseRpcController)controller).cellScanner();
        rsHost.preReplicateLogEntries(entries, cellScanner);
        replicationSinkHandler.replicateLogEntries(entries, cellScanner);
        rsHost.postReplicateLogEntries(entries, cellScanner);
      }
      return ReplicateWALEntryResponse.newBuilder().build();
    } catch (IOException ie) {
      throw new ServiceException(ie);
    }
  }

  /**
   * Replay the given changes when distributedLogReplay WAL edits from a failed RS. The guarantee is
   * that the given mutations will be durable on the receiving RS if this method returns without any
   * exception.
   * @param controller the RPC controller
   * @param request the request
   * @throws ServiceException
   */
  @Override
  @QosPriority(priority = HConstants.REPLAY_QOS)
  public ReplicateWALEntryResponse replay(final RpcController controller,
      final ReplicateWALEntryRequest request) throws ServiceException {
    long before = EnvironmentEdgeManager.currentTimeMillis();
    CellScanner cells = ((HBaseRpcController) controller).cellScanner();
    try {
      checkOpen();
      List<WALEntry> entries = request.getEntryList();
      if (entries == null || entries.isEmpty()) {
        // empty input
        return ReplicateWALEntryResponse.newBuilder().build();
      }
      HRegion region = this.getRegionByEncodedName(
        entries.get(0).getKey().getEncodedRegionName().toStringUtf8());
      RegionCoprocessorHost coprocessorHost = region.getCoprocessorHost();
      List<Pair<HLogKey, WALEdit>> walEntries = new ArrayList<Pair<HLogKey, WALEdit>>();
      List<HLogSplitter.MutationReplay> mutations = new ArrayList<HLogSplitter.MutationReplay>();
      // when tag is enabled, we need tag replay edits with log sequence number
      boolean needAddReplayTag = (HFile.getFormatVersion(this.conf) >= 3);
      for (WALEntry entry : entries) {
        if (nonceManager != null) {
          long nonceGroup = entry.getKey().hasNonceGroup()
              ? entry.getKey().getNonceGroup() : HConstants.NO_NONCE;
          long nonce = entry.getKey().hasNonce() ? entry.getKey().getNonce() : HConstants.NO_NONCE;
          nonceManager.reportOperationFromWal(nonceGroup, nonce, entry.getKey().getWriteTime());
        }
        Pair<HLogKey, WALEdit> walEntry = (coprocessorHost == null) ? null :
          new Pair<HLogKey, WALEdit>();
        List<HLogSplitter.MutationReplay> edits = HLogSplitter.getMutationsFromWALEntry(entry,
          cells, walEntry, needAddReplayTag);
        if (coprocessorHost != null) {
          // Start coprocessor replay here. The coprocessor is for each WALEdit instead of a
          // KeyValue.
          if (coprocessorHost.preWALRestore(region.getRegionInfo(), walEntry.getFirst(),
            walEntry.getSecond())) {
            // if bypass this log entry, ignore it ...
            continue;
          }
          walEntries.add(walEntry);
        }
        mutations.addAll(edits);
      }

      if (!mutations.isEmpty()) {
        OperationStatus[] result = doReplayBatchOp(region, mutations);
        // check if it's a partial success
        for (int i = 0; result != null && i < result.length; i++) {
          if (result[i] != OperationStatus.SUCCESS) {
            throw new IOException(result[i].getExceptionMsg());
          }
        }
      }
      if (coprocessorHost != null) {
        for (Pair<HLogKey, WALEdit> wal : walEntries) {
          coprocessorHost.postWALRestore(region.getRegionInfo(), wal.getFirst(),
            wal.getSecond());
        }
      }
      return ReplicateWALEntryResponse.newBuilder().build();
    } catch (IOException ie) {
      throw new ServiceException(ie);
    } finally {
      metricsRegionServer.updateReplay(EnvironmentEdgeManager.currentTimeMillis() - before);
    }
  }

  /**
   * Roll the WAL writer of the region server.
   * @param controller the RPC controller
   * @param request the request
   * @throws ServiceException
   */
  @Override
  public RollWALWriterResponse rollWALWriter(final RpcController controller,
      final RollWALWriterRequest request) throws ServiceException {
    try {
      checkOpen();
      requestCount.increment();
      if (this.rsHost != null) {
        this.rsHost.preRollWALWriterRequest();
      }
      HLog wal = this.getWAL();
      byte[][] regionsToFlush = wal.rollWriter(true);
      RollWALWriterResponse.Builder builder = RollWALWriterResponse.newBuilder();
      if (regionsToFlush != null) {
        for (byte[] region: regionsToFlush) {
          builder.addRegionToFlush(ByteStringer.wrap(region));
        }
      }
      return builder.build();
    } catch (IOException ie) {
      throw new ServiceException(ie);
    }
  }

  /**
   * Stop the region server.
   *
   * @param controller the RPC controller
   * @param request the request
   * @throws ServiceException
   */
  @Override
  public StopServerResponse stopServer(final RpcController controller,
      final StopServerRequest request) throws ServiceException {
    requestCount.increment();
    String reason = request.getReason();
    stop(reason);
    return StopServerResponse.newBuilder().build();
  }

  /**
   * Get some information of the region server.
   *
   * @param controller the RPC controller
   * @param request the request
   * @throws ServiceException
   */
  @Override
  public GetServerInfoResponse getServerInfo(final RpcController controller,
      final GetServerInfoRequest request) throws ServiceException {
    try {
      checkOpen();
    } catch (IOException ie) {
      throw new ServiceException(ie);
    }
    ServerName serverName = getServerName();
    requestCount.increment();
    return ResponseConverter.buildGetServerInfoResponse(serverName, rsInfo.getInfoPort());
  }

// End Admin methods

  /**
   * Find the HRegion based on a region specifier
   *
   * @param regionSpecifier the region specifier
   * @return the corresponding region
   * @throws IOException if the specifier is not null,
   *    but failed to find the region
   */
  protected HRegion getRegion(
      final RegionSpecifier regionSpecifier) throws IOException {
    return getRegionByEncodedName(regionSpecifier.getValue().toByteArray(),
        ProtobufUtil.getRegionEncodedName(regionSpecifier));
  }

  /**
   * Execute an append mutation.
   *
   * @param region
   * @param mutation
   * @param cellScanner
   * @return result to return to client if default operation should be
   * bypassed as indicated by RegionObserver, null otherwise
   * @throws IOException
   */
  protected Result append(final HRegion region,
      final MutationProto mutation, final CellScanner cellScanner, long nonceGroup) throws IOException {
    long before = EnvironmentEdgeManager.currentTimeMillis();
    Append append = ProtobufUtil.toAppend(mutation, cellScanner);
    if (isQuotaEnabled()) {
      rsQuotaManager.checkQuota(region, append);
    }
    if (Trace.isTracing() && Trace.currentSpan() != null) {
      Trace.currentSpan().addTimelineAnnotation("start processing an append request");
      Trace.currentSpan().addKVAnnotation(Bytes.toBytes("region"), Bytes.toBytes(region.toString()));
      Trace.currentSpan().addKVAnnotation(Bytes.toBytes("key"), mutation.getRow().toByteArray());
    }

    Result r = null;
    if (region.getCoprocessorHost() != null) {
      r = region.getCoprocessorHost().preAppend(append);
    }
    if (r == null) {
      boolean canProceed = startNonceOperation(mutation, nonceGroup);
      boolean success = false;
      try {
        long nonce = mutation.hasNonce() ? mutation.getNonce() : HConstants.NO_NONCE;
        if (canProceed) {
          r = region.append(append, nonceGroup, nonce);
        } else {
          // convert duplicate append to get
          List<Cell> results = region.get(ProtobufUtil.toGet(mutation, cellScanner), false,
            nonceGroup, nonce);
          r = Result.create(results);
        }
        success = true;
      } finally {
        if (canProceed) {
          endNonceOperation(mutation, nonceGroup, success);
        }
      }
      if (region.getCoprocessorHost() != null) {
        region.getCoprocessorHost().postAppend(append, r);
      }
    }
    metricsRegionServer.updateAppend(region.getTableDesc().getTableName(), EnvironmentEdgeManager.currentTimeMillis() - before);
    return r;
  }

  /**
   * Execute an increment mutation.
   *
   * @param region
   * @param mutation
   * @return the Result
   * @throws IOException
   */
  protected Result increment(final HRegion region, final MutationProto mutation,
      final CellScanner cellScanner, long nonceGroup) throws IOException {
    long before = EnvironmentEdgeManager.currentTimeMillis();
    Increment increment = ProtobufUtil.toIncrement(mutation, cellScanner);
    if (isQuotaEnabled()) {
      rsQuotaManager.checkQuota(region, OperationType.GET);
      rsQuotaManager.checkQuota(region, increment);
    }
    if (Trace.isTracing() && Trace.currentSpan() != null) {
      Trace.currentSpan().addTimelineAnnotation("start processing an increment request");
      Trace.currentSpan().addKVAnnotation(Bytes.toBytes("region"), Bytes.toBytes(region.toString()));
      Trace.currentSpan().addKVAnnotation(Bytes.toBytes("key"), mutation.getRow().toByteArray());
    }
    Result r = null;
    if (region.getCoprocessorHost() != null) {
      r = region.getCoprocessorHost().preIncrement(increment);
    }
    if (r == null) {
      boolean canProceed = startNonceOperation(mutation, nonceGroup);
      boolean success = false;
      try {
        long nonce = mutation.hasNonce() ? mutation.getNonce() : HConstants.NO_NONCE;
        if (canProceed) {
          r = region.increment(increment, nonceGroup, nonce);
        } else {
          // convert duplicate increment to get
          List<Cell> results = region.get(ProtobufUtil.toGet(mutation, cellScanner), false,
            nonceGroup, nonce);
          r = Result.create(results);
        }
        success = true;
      } finally {
        if (canProceed) {
          endNonceOperation(mutation, nonceGroup, success);
        }
      }
      if (region.getCoprocessorHost() != null) {
        r = region.getCoprocessorHost().postIncrement(increment, r);
      }
    }
    metricsRegionServer.updateIncrement(region.getTableDesc().getTableName(), EnvironmentEdgeManager.currentTimeMillis() - before);
    return r;
  }

  /**
   * Starts the nonce operation for a mutation, if needed.
   * @param mutation Mutation.
   * @param nonceGroup Nonce group from the request.
   * @returns whether to proceed this mutation.
   */
  private boolean startNonceOperation(final MutationProto mutation, long nonceGroup)
      throws IOException {
    if (nonceManager == null || !mutation.hasNonce()) return true;
    boolean canProceed = false;
    try {
      canProceed = nonceManager.startOperation(nonceGroup, mutation.getNonce(), this);
    } catch (InterruptedException ex) {
      throw new InterruptedIOException("Nonce start operation interrupted");
    }
    return canProceed;
  }

  /**
   * Ends nonce operation for a mutation, if needed.
   * @param mutation Mutation.
   * @param nonceGroup Nonce group from the request. Always 0 in initial implementation.
   * @param success Whether the operation for this nonce has succeeded.
   */
  private void endNonceOperation(final MutationProto mutation, long nonceGroup,
      boolean success) {
    if (nonceManager == null || !mutation.hasNonce()) return;
    nonceManager.endOperation(nonceGroup, mutation.getNonce(), success);
  }

  @Override
  public ServerNonceManager getNonceManager() {
    return this.nonceManager;
  }

  /**
   * Execute a list of Put/Delete mutations.
   *
   * @param builder
   * @param region
   * @param mutations
   */
  protected void doBatchOp(final RegionActionResult.Builder builder, final HRegion region,
      final List<ClientProtos.Action> mutations, final CellScanner cells) throws ServiceException {
    Mutation[] mArray = new Mutation[mutations.size()];
    try {
      int i = 0;
      for (ClientProtos.Action action: mutations) {
        MutationProto m = action.getMutation();
        Mutation mutation;
        if (m.getMutateType() == MutationType.PUT) {
          mutation = ProtobufUtil.toPut(m, cells);
        } else {
          mutation = ProtobufUtil.toDelete(m, cells);
        }
        mArray[i++] = mutation;
      }

      if (isQuotaEnabled()) {
        this.rsQuotaManager.checkQuota(region, Arrays.asList(mArray));
      }

      if (!region.getRegionInfo().isMetaTable()) {
        cacheFlusher.reclaimMemStoreMemory();
      }

      OperationStatus codes[] = region.batchMutate(mArray);
      for (i = 0; i < codes.length; i++) {
        int index = mutations.get(i).getIndex();
        Exception e = null;
        switch (codes[i].getOperationStatusCode()) {
          case BAD_FAMILY:
            e = new NoSuchColumnFamilyException(codes[i].getExceptionMsg());
            builder.addResultOrException(getResultOrException(e, index));
            break;

          case SANITY_CHECK_FAILURE:
            e = new FailedSanityCheckException(codes[i].getExceptionMsg());
            builder.addResultOrException(getResultOrException(e, index));
            break;

          default:
            e = new DoNotRetryIOException(codes[i].getExceptionMsg());
            builder.addResultOrException(getResultOrException(e, index));
            break;

          case SUCCESS:
            builder.addResultOrException(getResultOrException(
              ClientProtos.Result.getDefaultInstance(), index, region.getRegionStats()));
            break;
        }
      }
    } catch (IOException ie) {
      if (ie instanceof ThrottlingException) {
        throw new ServiceException(
            "Throttle multi operation as mutations size " + mutations.size() + " is too large", ie);
      }
      for (int i = 0; i < mutations.size(); i++) {
        builder.addResultOrException(getResultOrException(ie, mutations.get(i).getIndex()));
      }
    }
  }

  private static ResultOrException getResultOrException(final ClientProtos.Result r,
      final int index, final ClientProtos.RegionLoadStats stats) {
    return getResultOrException(ResponseConverter.buildActionResult(r, stats), index);
  }

  private static ResultOrException getResultOrException(final Exception e, final int index) {
    return getResultOrException(ResponseConverter.buildActionResult(e), index);
  }

  private static ResultOrException getResultOrException(final ResultOrException.Builder builder,
      final int index) {
    return builder.setIndex(index).build();
  }

  /**
   * Execute a list of Put/Delete mutations. The function returns OperationStatus instead of
   * constructing MultiResponse to save a possible loop if caller doesn't need MultiResponse.
   * @param region
   * @param mutations
   * @return an array of OperationStatus which internally contains the OperationStatusCode and the
   *         exceptionMessage if any
   * @throws IOException
   */
  protected OperationStatus [] doReplayBatchOp(final HRegion region,
      final List<HLogSplitter.MutationReplay> mutations) throws IOException {

    long before = EnvironmentEdgeManager.currentTimeMillis();
    try {
      for (Iterator<HLogSplitter.MutationReplay> it = mutations.iterator(); it.hasNext();) {
        HLogSplitter.MutationReplay m = it.next();
        NavigableMap<byte[], List<Cell>> map = m.mutation.getFamilyCellMap();
        List<Cell> metaCells = map.get(WALEdit.METAFAMILY);
        if (metaCells != null && !metaCells.isEmpty()) {
          for (Cell metaCell : metaCells) {
            CompactionDescriptor compactionDesc = WALEdit.getCompaction(metaCell);
            if (compactionDesc != null) {
              region.completeCompactionMarker(compactionDesc);
            }
          }
          it.remove();
        }
      }
      requestCount.add(mutations.size());
      if (!region.getRegionInfo().isMetaTable()) {
        cacheFlusher.reclaimMemStoreMemory();
      }
      return region.batchReplay(mutations.toArray(
        new HLogSplitter.MutationReplay[mutations.size()]));
    } finally {
      long after = EnvironmentEdgeManager.currentTimeMillis();
      metricsRegionServer.updateBatch(region.getTableDesc().getTableName(), after - before);
    }
  }

  /**
   * Mutate a list of rows atomically.
   *
   * @param region
   * @param actions
   * @param cellScanner if non-null, the mutation data -- the Cell content.
   * @throws IOException
   */
  protected ClientProtos.RegionLoadStats mutateRows(final HRegion region,
      final List<ClientProtos.Action> actions, final CellScanner cellScanner)
      throws IOException {
    long before = EnvironmentEdgeManager.currentTimeMillis();
    if (!region.getRegionInfo().isMetaTable()) {
      cacheFlusher.reclaimMemStoreMemory();
    }
    RowMutations rm = null;
    for (ClientProtos.Action action: actions) {
      if (action.hasGet()) {
        throw new DoNotRetryIOException("Atomic put and/or delete only, not a Get=" +
          action.getGet());
      }
      MutationType type = action.getMutation().getMutateType();
      if (rm == null) {
        rm = new RowMutations(action.getMutation().getRow().toByteArray());
      }
      switch (type) {
      case PUT:
        rm.add(ProtobufUtil.toPut(action.getMutation(), cellScanner));
        break;
      case DELETE:
        rm.add(ProtobufUtil.toDelete(action.getMutation(), cellScanner));
        break;
      default:
          throw new DoNotRetryIOException("Atomic put and/or delete only, not " + type.name());
      }
    }
    if (rm != null && isQuotaEnabled()) {
      rsQuotaManager.checkQuota(region, rm);
    }
    region.mutateRow(rm);
    ClientProtos.RegionLoadStats regionLoadStats = region.getRegionStats();
    metricsRegionServer.updateBatch(region.getTableDesc().getTableName(),
      EnvironmentEdgeManager.currentTimeMillis() - before);
    return regionLoadStats;
  }

  /**
   * Mutate a list of rows atomically.
   *
   * @param region
   * @param actions
   * @param cellScanner if non-null, the mutation data -- the Cell content.
   * @param row
   * @param family
   * @param qualifier
   * @param compareOp
   * @param comparator
   * @throws IOException
   */
  private boolean checkAndRowMutate(final HRegion region, final List<ClientProtos.Action> actions,
      final CellScanner cellScanner, byte[] row, byte[] family, byte[] qualifier,
      CompareOp compareOp, ByteArrayComparable comparator) throws IOException {
    long before = EnvironmentEdgeManager.currentTimeMillis();
    if (!region.getRegionInfo().isMetaTable()) {
      cacheFlusher.reclaimMemStoreMemory();
    }
    RowMutations rm = null;
    for (ClientProtos.Action action: actions) {
      if (action.hasGet()) {
        throw new DoNotRetryIOException("Atomic put and/or delete only, not a Get=" +
            action.getGet());
      }
      MutationType type = action.getMutation().getMutateType();
      if (rm == null) {
        rm = new RowMutations(action.getMutation().getRow().toByteArray());
      }
      switch (type) {
      case PUT:
        rm.add(ProtobufUtil.toPut(action.getMutation(), cellScanner));
        break;
      case DELETE:
        rm.add(ProtobufUtil.toDelete(action.getMutation(), cellScanner));
        break;
      default:
        throw new DoNotRetryIOException("Atomic put and/or delete only, not " + type.name());
      }
    }
    boolean result = region.checkAndRowMutate(row, family, qualifier, compareOp, comparator, rm, Boolean.TRUE);
    metricsRegionServer.updateBatch(region.getTableDesc().getTableName(),
      EnvironmentEdgeManager.currentTimeMillis() - before);
    return result;
  }

  private static class MovedRegionInfo {
    private final ServerName serverName;
    private final long seqNum;
    private final long ts;

    public MovedRegionInfo(ServerName serverName, long closeSeqNum) {
      this.serverName = serverName;
      this.seqNum = closeSeqNum;
      ts = EnvironmentEdgeManager.currentTimeMillis();
     }

    public ServerName getServerName() {
      return serverName;
    }

    public long getSeqNum() {
      return seqNum;
    }

    public long getMoveTime() {
      return ts;
    }
  }

  // This map will contains all the regions that we closed for a move.
  //  We add the time it was moved as we don't want to keep too old information
  protected Map<String, MovedRegionInfo> movedRegions =
      new ConcurrentHashMap<String, MovedRegionInfo>(3000);

  // We need a timeout. If not there is a risk of giving a wrong information: this would double
  //  the number of network calls instead of reducing them.
  private static final int TIMEOUT_REGION_MOVED = (2 * 60 * 1000);

  protected void addToMovedRegions(String encodedName, ServerName destination, long closeSeqNum) {
    if (ServerName.isSameHostnameAndPort(destination, this.getServerName())) {
      LOG.warn("Not adding moved region record: " + encodedName + " to self.");
      return;
    }
    LOG.info("Adding moved region record: " + encodedName + " to "
        + destination.getServerName() + ":" + destination.getPort()
        + " as of " + closeSeqNum);
    movedRegions.put(encodedName, new MovedRegionInfo(destination, closeSeqNum));
  }

  private void removeFromMovedRegions(String encodedName) {
    movedRegions.remove(encodedName);
  }

  private MovedRegionInfo getMovedRegion(final String encodedRegionName) {
    MovedRegionInfo dest = movedRegions.get(encodedRegionName);

    long now = EnvironmentEdgeManager.currentTimeMillis();
    if (dest != null) {
      if (dest.getMoveTime() > (now - TIMEOUT_REGION_MOVED)) {
        return dest;
      } else {
        movedRegions.remove(encodedRegionName);
      }
    }

    return null;
  }

  /**
   * Remove the expired entries from the moved regions list.
   */
  protected void cleanMovedRegions() {
    final long cutOff = System.currentTimeMillis() - TIMEOUT_REGION_MOVED;
    Iterator<Entry<String, MovedRegionInfo>> it = movedRegions.entrySet().iterator();

    while (it.hasNext()){
      Map.Entry<String, MovedRegionInfo> e = it.next();
      if (e.getValue().getMoveTime() < cutOff) {
        it.remove();
      }
    }
  }

  /**
   * Creates a Chore thread to clean the moved region cache.
   */
  protected static class MovedRegionsCleaner extends Chore implements Stoppable {
    private HRegionServer regionServer;
    Stoppable stoppable;

    private MovedRegionsCleaner(
      HRegionServer regionServer, Stoppable stoppable){
      super("MovedRegionsCleaner for region "+regionServer, TIMEOUT_REGION_MOVED, stoppable);
      this.regionServer = regionServer;
      this.stoppable = stoppable;
    }

    static MovedRegionsCleaner createAndStart(HRegionServer rs){
      Stoppable stoppable = new Stoppable() {
        private volatile boolean isStopped = false;
        @Override public void stop(String why) { isStopped = true;}
        @Override public boolean isStopped() {return isStopped;}
      };

      return new MovedRegionsCleaner(rs, stoppable);
    }

    @Override
    protected void chore() {
      regionServer.cleanMovedRegions();
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

  private String getMyEphemeralNodePath() {
    return ZKUtil.joinZNode(this.zooKeeper.znodePaths.rsZNode, getServerName().toString());
  }

  /**
   * Holder class which holds the RegionScanner and nextCallSeq together.
   */
  private static class RegionScannerHolder {
    private final String scannerName;
    private final RegionScanner s;
    private final AtomicLong nextCallSeq = new AtomicLong(0L);
    private final HRegion r;
    private byte[] rowOfLastPartialResult;
    private boolean needCursor;

    public RegionScannerHolder(String scannerName, RegionScanner s, HRegion r, boolean needCursor) {
      this.scannerName = scannerName;
      this.s = s;
      this.r = r;
      this.needCursor = needCursor;
    }

    public long getNextCallSeq() {
      return nextCallSeq.get();
    }

    public boolean incNextCallSeq(long currentSeq) {
      // Use CAS to prevent multiple scan request running on the same scanner.
      return nextCallSeq.compareAndSet(currentSeq, currentSeq + 1);
    }
  }

  private boolean isHealthCheckerConfigured() {
    return conf.getBoolean(HConstants.HEALTH_CHECKER_ENABLE, HConstants.HEALTH_CHECKER_OFF);
  }

  /**
   * @return the underlying {@link CompactSplitThread} for the servers
   */
  public CompactSplitThread getCompactSplitThread() {
    return this.compactSplitThread;
  }

  /**
   * A helper function to store the last flushed sequence Id with the previous failed RS for a
   * recovering region. The Id is used to skip wal edits which are flushed. Since the flushed
   * sequence id is only valid for each RS, we associate the Id with corresponding failed RS.
   * @throws KeeperException
   * @throws IOException
   */
  private void updateRecoveringRegionLastFlushedSequenceId(HRegion r) throws KeeperException,
      IOException {
    if (!r.isRecovering()) {
      // return immdiately for non-recovering regions
      return;
    }

    HRegionInfo region = r.getRegionInfo();
    ZooKeeperWatcher zkw = getZooKeeper();
    String previousRSName = this.getLastFailedRSFromZK(region.getEncodedName());
    Map<byte[], Long> maxSeqIdInStores = r.getMaxStoreSeqIdForLogReplay();
    long minSeqIdForLogReplay = -1;
    for (Long storeSeqIdForReplay : maxSeqIdInStores.values()) {
      if (minSeqIdForLogReplay == -1 || storeSeqIdForReplay < minSeqIdForLogReplay) {
        minSeqIdForLogReplay = storeSeqIdForReplay;
      }
    }

    try {
      long lastRecordedFlushedSequenceId = -1;
      String nodePath = ZKUtil.joinZNode(this.zooKeeper.znodePaths.recoveringRegionsZNode,
        region.getEncodedName());
      // recovering-region level
      byte[] data = ZKUtil.getData(zkw, nodePath);
      if (data != null) {
        lastRecordedFlushedSequenceId = SplitLogManager.parseLastFlushedSequenceIdFrom(data);
      }
      if (data == null || lastRecordedFlushedSequenceId < minSeqIdForLogReplay) {
        ZKUtil.setData(zkw, nodePath, ZKUtil.positionToByteArray(minSeqIdForLogReplay));
      }
      if (previousRSName != null) {
        // one level deeper for the failed RS
        nodePath = ZKUtil.joinZNode(nodePath, previousRSName);
        ZKUtil.setData(zkw, nodePath,
          ZKUtil.regionSequenceIdsToByteArray(minSeqIdForLogReplay, maxSeqIdInStores));
        LOG.debug("Update last flushed sequence id of region " + region.getEncodedName() + " for "
            + previousRSName);
      } else {
        LOG.warn("Can't find failed region server for recovering region " + region.getEncodedName());
      }
    } catch (NoNodeException ignore) {
      LOG.debug("Region " + region.getEncodedName() +
        " must have completed recovery because its recovery znode has been removed", ignore);
    }
  }

  /**
   * Return the last failed RS name under /hbase/recovering-regions/encodedRegionName
   * @param encodedRegionName
   * @throws KeeperException
   */
  private String getLastFailedRSFromZK(String encodedRegionName) throws KeeperException {
    String result = null;
    long maxZxid = 0;
    ZooKeeperWatcher zkw = this.getZooKeeper();
    String nodePath = ZKUtil.joinZNode(zkw.znodePaths.recoveringRegionsZNode, encodedRegionName);
    List<String> failedServers = ZKUtil.listChildrenNoWatch(zkw, nodePath);
    if (failedServers == null || failedServers.isEmpty()) {
      return result;
    }
    for (String failedServer : failedServers) {
      String rsPath = ZKUtil.joinZNode(nodePath, failedServer);
      Stat stat = new Stat();
      ZKUtil.getDataNoWatch(zkw, rsPath, stat);
      if (maxZxid < stat.getCzxid()) {
        maxZxid = stat.getCzxid();
        result = failedServer;
      }
    }
    return result;
  }

  @Override
  public UpdateFavoredNodesResponse updateFavoredNodes(RpcController controller,
      UpdateFavoredNodesRequest request) throws ServiceException {
    List<UpdateFavoredNodesRequest.RegionUpdateInfo> openInfoList = request.getUpdateInfoList();
    UpdateFavoredNodesResponse.Builder respBuilder = UpdateFavoredNodesResponse.newBuilder();
    for (UpdateFavoredNodesRequest.RegionUpdateInfo regionUpdateInfo : openInfoList) {
      HRegionInfo hri = HRegionInfo.convert(regionUpdateInfo.getRegion());
      updateRegionFavoredNodesMapping(hri.getEncodedName(),
          regionUpdateInfo.getFavoredNodesList());
    }
    respBuilder.setResponse(openInfoList.size());
    return respBuilder.build();
  }
  
  @Override
  public void switchThrottle() {
    if (this.rsQuotaManager != null && this.rsQuotaManager.isQuotaEnabled()) {
      ThrottleState state = this.throttleStateTracker.getThrottleState();
      LOG.info("set throttleSwitch to " + state);
      switch(state) {
        case ON:
          startQuotaManager();
          rsQuotaManager.setThrottleSimulated(false);
          break;
        case SIMULATION:
          startQuotaManager();
          rsQuotaManager.setThrottleSimulated(true);
          break;
        case OFF:
          rsQuotaManager.stop();
          rsQuotaManager.setThrottleSimulated(false);
          break;
      }
    }
  }
  
  @Override
  public CompactionEnableResponse switchCompaction(RpcController controller,
      CompactionEnableRequest request) throws ServiceException {
    CompactionEnableResponse.Builder builder = CompactionEnableResponse.newBuilder();
    builder.setEnable(this.enableCompact);
    this.enableCompact = request.getEnable();
    return builder.build();
  }

  @Override
  public GetRegionLoadResponse getRegionLoad(RpcController controller,
      GetRegionLoadRequest request) throws ServiceException {
    Collection<HRegion> regions;
    if (request.hasTableName()) {
      TableName tableName = ProtobufUtil.toTableName(request.getTableName());
      regions = getOnlineRegions(tableName);
    } else {
      regions = getOnlineRegionsLocalContext();
    }
    List<RegionLoad> rLoads = new ArrayList<>(regions.size());
    RegionLoad.Builder regionLoadBuilder = ClusterStatusProtos.RegionLoad.newBuilder();
    RegionSpecifier.Builder regionSpecifier = RegionSpecifier.newBuilder();
    for (HRegion region : regions) {
      rLoads.add(createRegionLoad(region, regionLoadBuilder, regionSpecifier));
    }
    GetRegionLoadResponse.Builder builder = GetRegionLoadResponse.newBuilder();
    builder.addAllRegionLoads(rLoads);
    return builder.build();
  }

  public boolean isEnableCompact() {
    return this.enableCompact;
  }

  private void startQuotaManager() {
    if (rsQuotaManager != null && rsQuotaManager.isStopped()) {
      try {
        rsQuotaManager.start(getRpcServer().getScheduler());
      } catch (Throwable t) {
        LOG.error("Fail to statr regionserver quota manager", t);
      }
    }
  }

  /**
   * @return The cache config instance used by the regionserver.
   */
  public CacheConfig getCacheConfig() {
    return this.cacheConfig;
  }

  @Override
  public HeapMemoryManager getHeapMemoryManager() {
    return hMemManager;
  }

  @Override
  public double getCompactionPressure() {
    double max = 0;
    for (HRegion region : onlineRegions.values()) {
      for (Store store : region.getStores().values()) {
        double normCount = store.getCompactionPressure();
        if (normCount > max) {
          max = normCount;
        }
      }
    }
    return max;
  }

  @Override
  public RegionServerReportResponse getRegionServerReportResponse() {
    return reportResponse;
  }

  @Override
  public AccessCounter getAccessCounter() {
    return this.accessCounter;
  }

  private boolean isQuotaEnabled() {
    return (this.rsQuotaManager != null) && (this.rsQuotaManager.isQuotaEnabled())
        && (!this.rsQuotaManager.isStopped());
  }

  /**
   * Register bean with platform management server
   */
  void registerMBean() {
    MXBeanImpl mxBeanInfo = MXBeanImpl.init(this);
    mxBean = CompatibilitySingletonFactory.getInstance(MBeanSource.class).register("hbase",
      "RegionServer,sub=ServerLoad", mxBeanInfo);
    LOG.info("Registered HRegionServer MXBean for ServerLoad");
  }

  ServerLoad getServerLoad() {
    return this.serverLoad;
  }

  public SplitOrMergeTracker getSplitOrMergeTracker() {
    return splitOrMergeTracker;
  }

  /**
   * Only call this when {@link SplitTransaction} or {@link RegionMergeTransaction} got in the step
   * point-of-no-return. It means that if report failed, will abort the regionserver. So need to
   * take care of this and report region transition with retry.
   */
  public static boolean reportRegionStateTransitionWithRetry(RegionServerServices services,
      String failedMessage, TransitionCode code, HRegionInfo... hris) {
    int retries = services.getConfiguration()
        .getInt(HConstants.REPORT_REGION_TRANSITION_RETRIES_NUMBER,
            HConstants.DEFAULT_REPORT_REGION_TRANSITION_RETRIES_NUMBER);
    int pauseTime = services.getConfiguration()
        .getInt(HConstants.HBASE_SERVER_PAUSE, HConstants.DEFAULT_HBASE_SERVER_PAUSE);
    for (int i = 0; i < retries; i++) {
      if (services.reportRegionStateTransition(code, hris)) {
        return true;
      }
      LOG.warn(failedMessage + ", will sleep " + pauseTime + " ms and retry, tries = " + i);
      try {
        Thread.sleep(pauseTime);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    return false;
  }
}
