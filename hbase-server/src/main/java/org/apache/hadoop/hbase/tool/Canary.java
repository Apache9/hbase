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

package org.apache.hadoop.hbase.tool;

import static org.apache.hadoop.hbase.HConstants.EMPTY_BYTE_ARRAY;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.BindException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.canary.CanaryStatusServlet;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.AsyncTable;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.MetaScanner;
import org.apache.hadoop.hbase.client.MetaScanner.MetaScannerVisitor;
import org.apache.hadoop.hbase.client.MetaScanner.MetaScannerVisitorBase;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.replication.ReplicationLoadSource;
import org.apache.hadoop.hbase.replication.ReplicationPeerDescription;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.InfoServer;
import org.apache.hadoop.hbase.util.RegionSplitter;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * HBase Canary Tool, that that can be used to do "canary monitoring" of a running HBase cluster.
 * For each region tries to get one row per column family and outputs some information about failure
 * or latency.
 */
public final class Canary implements Tool {
  // Sink interface used by the canary to outputs information
  public interface Sink {
    public void publishOldWalsFilesCount(long count);

    public void publishReadFailure(HRegionInfo region, Throwable e);

    public void publishReadFailure(HRegionInfo region, HColumnDescriptor column, Throwable e);

    public void publishReadTiming(HRegionInfo region, HColumnDescriptor column, long msTime);

    public void publishWriteFailure(HRegionInfo region, Throwable e);

    public void publishWriteFailure(HRegionInfo region, HColumnDescriptor column, Throwable e);

    public void publishWriteTiming(HRegionInfo region, HColumnDescriptor column, long msTime);

    public void publishReplicationLag(String PeerId, long replicationLagInMilliseconds);

    public void publishMasterAvilability(double availability);

    public void publishTableMinAvilability(Pair<TableName, Double> tableNameDoublePair);

    public void publishRegionServerMinAvilability(Pair<ServerName, Double> serverNameDoublePair);


    public void reportSummary();

    default public double getReadAvailability() {
      return 100.0;
    }

    default public double getWriteAvailability() {
      return 100.0;
    }

    default public double getAvailability() {
      return 100.0;
    }
  }

  // Simple implementation of canary sink that allows to plot on
  // file or standard output timings or failures.
  public static class StdOutSink implements Sink {
    @Override
    public void publishOldWalsFilesCount(long count) {
      LOG.error("OldWals files count current not support in StdOutSink");
    }

    @Override
    public void publishReadFailure(HRegionInfo region, Throwable e) {
      LOG.error(String.format("read from region %s failed", region.getRegionNameAsString()), e);
    }

    @Override
    public void publishReadFailure(HRegionInfo region, HColumnDescriptor column, Throwable e) {
      LOG.error(String.format("read from region %s column family %s failed",
        region.getRegionNameAsString(), column.getNameAsString()), e);
    }

    @Override
    public void publishReadTiming(HRegionInfo region, HColumnDescriptor column, long msTime) {
      LOG.info(String.format("read from region %s column family %s in %dms",
        region.getRegionNameAsString(), column.getNameAsString(), msTime));
    }

    @Override
    public void publishWriteFailure(HRegionInfo region, Throwable e) {
      LOG.error(String.format("write to region %s failed", region.getRegionNameAsString()), e);
    }

    @Override
    public void publishWriteFailure(HRegionInfo region, HColumnDescriptor column, Throwable e) {
      LOG.error(String.format("write to region %s column family %s failed",
        region.getRegionNameAsString(), column.getNameAsString()), e);
    }

    @Override
    public void publishWriteTiming(HRegionInfo region, HColumnDescriptor column, long msTime) {
      LOG.info(String.format("write to region %s column family %s in %dms",
        region.getRegionNameAsString(), column.getNameAsString(), msTime));
    }

    @Override
    public void publishReplicationLag(String clusterKey, long replicationLagInMilliseconds) {
      LOG.info(
          "The replication lag of cluster:" + clusterKey + " is " + replicationLagInMilliseconds
              + "ms");
    }
    public void publishMasterAvilability(double availability) {
      LOG.info("The availability of Master is " + availability + "%");
    }

    @Override
    public void publishTableMinAvilability(Pair<TableName, Double> tableNameDoublePair) {
      LOG.info("The minAvailability of table " + tableNameDoublePair.getFirst() + " was set to "
          + tableNameDoublePair.getSecond() + "%");
    }

    @Override
    public void publishRegionServerMinAvilability(Pair<ServerName, Double> serverNameDoublePair) {
      LOG.info("The minAvailability of regionServer " + serverNameDoublePair.getFirst() + " was set to "
              + serverNameDoublePair.getSecond() + "%");
    }

    @Override
    public void reportSummary() {
    }
  }

  private static byte[] randomKey(byte[] start, byte[] end) {
    try {
      return Bytes.randomKey(start, end);
    } catch (IOException e) {
      // should not happen
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Contact a region server and get all information from it
   */
  static class RegionTask {
    private final AsyncTable<?> table;
    private final HTableDescriptor tableDesc;
    private final HRegionInfo region;
    private final ServerName server;
    private final Sink sink;
    private final Canary canary;

    RegionTask(Configuration conf, AsyncTable<?> table, HTableDescriptor tableDesc,
        HRegionInfo region, ServerName server, Sink sink, Canary canary) {
      this.table = table;
      this.tableDesc = tableDesc;
      this.region = region;
      this.server = server;
      this.sink = sink;
      this.canary = canary;
    }

    /**
     * check read for normal user tables
     * @return
     */
    private CompletableFuture<Void> read() {
      List<CompletableFuture<?>> futures = new ArrayList<>();
      tableDesc.getFamilies().forEach(column -> {
        StopWatch watch = new StopWatch();
        watch.start();
        futures.add(table
            .scanAll(new Scan().withStartRow(randomKey(region.getStartKey(), region.getEndKey()))
                .withStopRow(region.getEndKey()).addFamily(column.getName()).setRaw(true)
                .setFilter(new FirstKeyOnlyFilter()).setCacheBlocks(false).setOneRowLimit())
            .whenComplete((r, e) -> {
              canary.caclTotalRequest(server, tableDesc.getTableName());
              if (e != null) {
                handleReadException(e, column);
              } else {
                watch.stop();
                sink.publishReadTiming(region, column, watch.getTime());
              }
            }));
      });
      return CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[0]));
    }

    private void handleReadException(Throwable e, HColumnDescriptor column) {
      // check whether the table is enabled by the exception message. The exception
      // is RetriesExhaustedException, we need to judge from the its message
      // table deleted or disabled. galaxy(sds/emq) will run bvt all the time, and will
      // create/delete table in bvt, this will make the region unavailable.
      if (e instanceof TableNotFoundException || e instanceof TableNotEnabledException ||
          e.getMessage().contains("TableNotFoundException") ||
          e.getMessage().contains("is disabled")) {
        LOG.warn("read failure from disabled or deleted table, region=" + region.getEncodedName() +
            ", table=" + tableDesc.getNameAsString());
        return;
      }
      clearCacheAndPublishReadFailure(column, e);
    }

    private void clearCacheAndPublishReadFailure(HColumnDescriptor column, Throwable e) {
      canary.clearCachedTasks(tableDesc.getNameAsString());
      canary.recordFailure(server, tableDesc.getTableName());
      if (column == null) {
        sink.publishReadFailure(region, e);
      } else {
        sink.publishReadFailure(region, column, e);
      }
    }

    /**
     * Check writes for the canary table
     * @return
     */
    private CompletableFuture<Void> write() {
      List<CompletableFuture<?>> futures = new ArrayList<>();
      tableDesc.getFamilies().forEach(column -> {
        StopWatch watch = new StopWatch();
        watch.start();
        futures
            .add(table
                .put(new Put(randomKey(region.getStartKey(), region.getEndKey()))
                    .add(column.getName(), EMPTY_BYTE_ARRAY, EMPTY_BYTE_ARRAY))
                .whenComplete((r, e) -> {
                  canary.caclTotalRequest(server, tableDesc.getTableName());
                  if (e != null) {
                    sink.publishWriteFailure(region, column, e);
                    canary.recordFailure(server, tableDesc.getTableName());
                  } else {
                    watch.stop();
                    sink.publishWriteTiming(region, column, watch.getTime());
                  }
                }));
      });
      return CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[0]));
    }

    public CompletableFuture<Void> call() {
      if (tableDesc.getNameAsString().equals(HConstants.CANARY_TABLE_NAME)) {
        return write();
      } else {
        return read();
      }
    }
  }

  private static final String EXCLUDE_NAMESPACE = "hbase.canary.exclude.namespace";
  private String excludeNamespace;

  public static final String CANARY = "canary";
  public static final String CANARY_CONF = "canary_conf";
  private static int DEFAULT_REGIONS_PER_SERVER = 2;

  private static final int DEFAULT_MAX_CONCURRENCY = 200;

  private static final long DEFAULT_INTERVAL = 6000;

  private static final long DEFAULT_TIMEOUT = 600000; // 10 mins

  private static final Log LOG = LogFactory.getLog(Canary.class);

  private final Sink sink;
  private Configuration conf;
  private HConnection conn;
  private HBaseAdmin admin;
  private AsyncConnection asyncConn;
  private long interval;
  private long timeout;
  private int maxConcurrency;
  private List<RegionTask> tasks;
  private final ConcurrentMap<String, List<RegionTask>> cachedTasks = new ConcurrentHashMap<>();
  private FileSystem fs;
  private Path rootdir;
  private ConcurrentMap<ServerName, Integer> failuresByServer;
  private ConcurrentMap<TableName, Integer> failuresByTable;
  private ConcurrentMap<ServerName, Integer> totalRequestByServer;
  private ConcurrentMap<TableName, Integer> totalRequestByTable;
  private InfoServer infoServer;

  public Canary(Sink sink) {
    this.sink = sink;
    this.failuresByServer = new ConcurrentHashMap<>();
    this.failuresByTable = new ConcurrentHashMap<>();
    this.totalRequestByServer = new ConcurrentHashMap<>();
    this.totalRequestByTable = new ConcurrentHashMap<>();

  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  private void putUpWebUI() throws IOException {
    int port = this.conf.getInt("hbase.canary.info.port", 60050);
    // -1 is for disabling info server
    if (port < 0) return;
    String addr = this.conf.get("hbase.canary.info.bindAddress", "0.0.0.0");
    try {
      this.infoServer = new InfoServer("canary", addr, port, false, this.conf);
      this.infoServer.addServlet("status", "/canary-status", CanaryStatusServlet.class);
      this.infoServer.setAttribute(CANARY, this);
      this.infoServer.setAttribute(CANARY_CONF, conf);
      this.infoServer.start();
      LOG.info("Bind http info server to port: " + port);
    } catch (BindException e) {
      LOG.info("Failed binding http info server to port: " + port);
    }
  }

  private long lastCheckTime;

  private void runOnce(List<String> tables) throws InterruptedException {
    long startTime = EnvironmentEdgeManager.currentTimeMillis();

    try {
      tasks = getSniffTasks(tables);
    } catch (Exception e) {
      LOG.error("Update sniff tasks failed. Using previous sniffing tasks", e);
    }

    // clear cached tasks and check canary distribution for 10 minutes
    if (EnvironmentEdgeManager.currentTimeMillis() - lastCheckTime > 10 * 60 * 1000) {
      clearCachedTasks();
      try {
        checkCanaryDistribution();
      } catch (Exception e) {
        LOG.error("Check canary distribution failed.", e);
      }
      lastCheckTime = EnvironmentEdgeManager.currentTimeMillis();
    }

    // clear the previous failures
    this.failuresByServer.clear();
    this.failuresByTable.clear();

    //clear the previous total count
    this.totalRequestByServer.clear();
    this.totalRequestByTable.clear();

    List<RegionTask> tasksNotRun = new ArrayList<>();
    Semaphore concurrencyControl = new Semaphore(maxConcurrency);
    CountDownLatch unfinishedTasks = new CountDownLatch(tasks.size());
    tasks.forEach(task -> {
      // check whether we have already timed out
      long remainingTime = timeout - (EnvironmentEdgeManager.currentTimeMillis() - startTime);
      if (remainingTime <= 0) {
        tasksNotRun.add(task);
        unfinishedTasks.countDown();
        return;
      }
      try {
        if (!concurrencyControl.tryAcquire(remainingTime, TimeUnit.MILLISECONDS)) {
          tasksNotRun.add(task);
          unfinishedTasks.countDown();
          return;
        }
      } catch (InterruptedException e) {
        tasksNotRun.add(task);
        unfinishedTasks.countDown();
        return;
      }
      task.call().whenComplete((r, e) -> {
        concurrencyControl.release();
        if (e != null) {
          LOG.error("Sniff region failed", e);
        }
        unfinishedTasks.countDown();
      });
    });
    checkOldWalsFilesCount();
    unfinishedTasks.await();
    getReplicationLag();
    getMasterAvailability();
    getTableMinAvailability();
    getRegionServerMinAvailability();
    sink.reportSummary();
    long finishTime = EnvironmentEdgeManager.currentTimeMillis();
    LOG.info("Finish one turn sniff, consume(ms)=" + (finishTime - startTime) + ", interval(ms)=" +
        interval + ", timeout(ms)=" + timeout + ", taskCount=" + tasks.size() +
        ", taskNotRunCount=" + tasksNotRun.size());
    logFailedServerAndTable();
    if (finishTime < startTime + interval) {
      Thread.sleep(startTime + interval - finishTime);
    }
  }

  @Override
  public int run(String[] args) throws Exception {
    int tablesIndex = -1;

    // Process command line args
    for (int i = 0; i < args.length; i++) {
      String cmd = args[i];

      if (cmd.startsWith("-")) {
        if (tablesIndex >= 0) {
          // command line args must be in the form: [opts] [table 1 [table 2 ...]]
          System.err.println("Invalid command line options");
          printUsageAndExit();
        }

        if (cmd.equals("-help")) {
          // user asked for help, print the help and quit.
          printUsageAndExit();
        } else if (cmd.equals("-daemon") && interval == 0) {
          // user asked for daemon mode, set a default interval between checks
          interval = DEFAULT_INTERVAL;
        } else if (cmd.equals("-interval")) {
          // user has specified an interval for canary breaths (-interval N)
          i++;

          if (i == args.length) {
            System.err.println("-interval needs a numeric value argument.");
            printUsageAndExit();
          }

          try {
            interval = Long.parseLong(args[i]) * 1000;
          } catch (NumberFormatException e) {
            System.err.println("-interval needs a numeric value argument.");
            printUsageAndExit();
          }
        } else {
          // no options match
          System.err.println(cmd + " options is invalid.");
          printUsageAndExit();
        }
      } else if (tablesIndex < 0) {
        // keep track of first table name specified by the user
        tablesIndex = i;
      }
    }
    List<String> tables = new ArrayList<>();
    if (tablesIndex >= 0) {
      for (int i = tablesIndex; i < args.length; i++) {
        tables.add(args[i]);
      }
    }

    // initialize HBase conf and admin
    maxConcurrency = conf.getInt("hbase.canary.concurrency.max", DEFAULT_MAX_CONCURRENCY);
    timeout = conf.getLong("hbase.canary.executor.timeout", DEFAULT_TIMEOUT);
    conn = HConnectionManager.createConnection(conf);
    admin = new HBaseAdmin(conn);
    asyncConn = HConnectionManager.createAsyncConnection(conf).get();
    lastCheckTime = EnvironmentEdgeManager.currentTimeMillis();
    rootdir = FSUtils.getRootDir(conf);
    fs = rootdir.getFileSystem(conf);
    excludeNamespace = conf.get(EXCLUDE_NAMESPACE, "");
    // initialize server principal (if using secure Hadoop)
    // User.login(conf, "hbase.canary.keytab.file", "hbase.canary.kerberos.principal", hostname);
    // lets the canary monitor the cluster
    try {
      putUpWebUI();
      runOnce(tables);
      if (interval > 0) {
        for (;;) {
          runOnce(tables);
        }
      }
    } finally {
      IOUtils.closeQuietly(asyncConn);
      IOUtils.closeQuietly(admin);
      IOUtils.closeQuietly(conn);
      IOUtils.closeQuietly(fs);
    }
    return 0;
  }

  public List<Entry<ServerName, Integer>> getFailuresByServer() {
    return failuresByServer.entrySet().stream()
        .sorted((o1, o2) -> o2.getValue().compareTo(o1.getValue())).collect(Collectors.toList());
  }

  public List<Entry<TableName, Integer>> getFailuresByTable() {
    return failuresByTable.entrySet().stream()
        .sorted((o1, o2) -> o2.getValue().compareTo(o1.getValue())).collect(Collectors.toList());
  }

  public double getReadAvailability() {
    return sink.getReadAvailability();
  }

  public double getWriteAvailability() {
    return sink.getWriteAvailability();
  }

  public double getAvailability() {
    return sink.getAvailability();
  }

  private void logFailedServerAndTable() {
    failuresByServer.entrySet().stream().sorted((o1, o2) -> o2.getValue().compareTo(o1.getValue()))
        .forEach(entry -> {
          LOG.warn("Failed server and count: " + entry.getKey() + " , " + entry.getValue());
        });
    failuresByTable.entrySet().stream().sorted((o1, o2) -> o2.getValue().compareTo(o1.getValue()))
        .forEach(entry -> {
          LOG.warn("Failed table and count: " + entry.getKey() + " , " + entry.getValue());
        });
  }

  private void checkOldWalsFilesCount() {
    try {
      Path oldWalPath = new Path(rootdir, HConstants.HREGION_OLDLOGDIR_NAME);
      ContentSummary contentSummary = fs.getContentSummary(oldWalPath);
      sink.publishOldWalsFilesCount(contentSummary.getFileCount());
    } catch (IOException e) {
      LOG.info("check oldWal directory failed, " ,e);
    }
  }

  /**
   * Update sniff tasks
   * @param tables
   */
  private List<RegionTask> getSniffTasks(List<String> tables) throws Exception {
    List<RegionTask> tmpTasks = new ArrayList<>();
    if (tables.size() > 0) {
      for (String table : tables) {
        tmpTasks.addAll(sniff(table));
      }
    } else {
      tmpTasks = sniff();
    }
    Collections.shuffle(tmpTasks);
    return tmpTasks;
  }

  private void printUsageAndExit() {
    System.err.printf("Usage: bin/hbase %s [opts] [table 1 [table 2...]]\n", getClass().getName());
    System.err.println(" where [opts] are:");
    System.err.println("   -help          Show this help and exit.");
    System.err.println("   -daemon        Continuous check at defined intervals.");
    System.err.println("   -interval <N>  Interval between checks, default is 6 (sec)");
    System.exit(1);
  }

  protected void recordFailure(ServerName server, TableName table) {
    failuresByServer.compute(server, (k, v) -> v == null ? 1 : v + 1);
    failuresByTable.compute(table, (k, v) -> v == null ? 1 : v + 1);
  }

  protected void caclTotalRequest(ServerName server, TableName table) {
    totalRequestByServer.compute(server, (k, v) -> v == null ? 1 : v + 1);
    totalRequestByTable.compute(table, (k, v) -> v == null ? 1 : v + 1);
  }

  /*
   * canary entry point to monitor all the tables.
   */
  private List<RegionTask> sniff() throws Exception {
    List<RegionTask> tasks = new LinkedList<RegionTask>();
    // admin.listTables invoke connection.listTables directly, won't create zkw
    for (HTableDescriptor table : admin.listTables()) {
      if (isTableEnabled(table.getName())) {
        tasks.addAll(sniff(table));
      }
    }
    return tasks;
  }

  /*
   * canary entry point to monitor specified table.
   */
  private List<RegionTask> sniff(String tableName) throws Exception {
    List<RegionTask> tasks = new LinkedList<RegionTask>();
    // admin.isTableAvailable invoke connection.isTableAvailable directly, won't create zkw
    if (admin.isTableAvailable(tableName)) {
      // admin.getTableDescriptor invoke connection.getTableDescriptor dirctly, won't create zkw
      tasks.addAll(sniff(admin.getTableDescriptor(tableName.getBytes())));
    } else {
      LOG.warn(String.format("Table %s is not available", tableName));
    }
    return tasks;
  }

  protected void clearCachedTasks(String tableName) {
    cachedTasks.remove(tableName);
  }

  protected void clearCachedTasks() {
    cachedTasks.clear();
  }

  /*
   * Loops over regions that owns this table, and output some information abouts the state.
   */
  private List<RegionTask> sniff(HTableDescriptor tableDesc) throws Exception {
    if (tableDesc.getTableName().getNamespaceAsString().equals(excludeNamespace)) {
      return new ArrayList<>();
    }
    List<RegionTask> tasks = cachedTasks.get(tableDesc.getNameAsString());
    if (tasks != null) {
      return tasks;
    }
    AsyncTable<?> table = asyncConn.getTable(tableDesc.getTableName());
    tasks = MetaScanner.findAndCacheAllTableRegions(conf, conn, tableDesc.getTableName(), asyncConn)
        .entrySet().stream().map(
            entry -> new RegionTask(conf, table, tableDesc, entry.getKey(), entry.getValue(), sink,
                this)).collect(Collectors.toList());
    cachedTasks.put(tableDesc.getNameAsString(), tasks);
    LOG.info("get task from meta table, table=" + tableDesc.getNameAsString() + ", taskCount=" +
        tasks.size());
    return tasks;
  }

  // return true if table region exist for in meta table; the logic is the same as
  // HBaseAdmin.tableExists, but won't create new zkw
  private boolean isTableExists(byte[] tableName) throws IOException {
    AtomicBoolean exist = new AtomicBoolean(false);
    MetaScannerVisitor visitor = new MetaScannerVisitorBase() {
      @Override
      public boolean processRow(Result row) throws IOException {
        try {
          byte[] tableNameInMeta = HRegionInfo.parseRegionName(row.getRow())[0];
          if (Bytes.compareTo(tableName, tableNameInMeta) == 0) {
            exist.set(true);
            // break the meta scan once region hit
            return false;
          }
        } catch (Exception e) {
          // Parse table name failed. ignore.
        }
        return true;
      }
    };
    MetaScanner.metaScan(conf, conn, visitor, TableName.valueOf(tableName), null, 1,
      TableName.META_TABLE_NAME);
    return exist.get();
  }

  // won't create new zkw
  private boolean isTableEnabled(byte[] table) throws IOException {
    return isTableExists(table) && conn.isTableEnabled(TableName.valueOf(table));
  }

  private void checkCanaryDistribution() throws IOException {
    if (!isTableExists(Bytes.toBytes(HConstants.CANARY_TABLE_NAME))) {
      int numberOfServers =
          admin.getClusterStatus(EnumSet.of(ClusterStatus.Option.SERVERS_NAME)).getServers().size();
      if (numberOfServers == 0) {
        throw new IllegalStateException("No live regionservers");
      }
      createCanaryTable(numberOfServers);
    }

    if (!isTableEnabled(Bytes.toBytes(HConstants.CANARY_TABLE_NAME))) {
      admin.enableTable(HConstants.CANARY_TABLE_NAME);
    }

    int numberOfServers =
        admin.getClusterStatus(EnumSet.of(ClusterStatus.Option.SERVERS_NAME)).getServers().size();
    HTable table = new HTable(getConf(), HConstants.CANARY_TABLE_NAME);
    Collection<ServerName> regionsevers = table.getRegionLocations().values();
    int numberOfRegions = regionsevers.size();
    double rate = 1.0 * numberOfRegions / numberOfServers;
    if ((rate < DEFAULT_REGIONS_PER_SERVER * 0.7) || (rate > DEFAULT_REGIONS_PER_SERVER * 1.5)) {
      LOG.info("Current canary region num: " + numberOfRegions + " server num: " + numberOfServers);
      admin.disableTable(HConstants.CANARY_TABLE_NAME);
      admin.deleteTable(HConstants.CANARY_TABLE_NAME);
      createCanaryTable(numberOfServers);
    }
    int numberOfCoveredServers = new HashSet<ServerName>(regionsevers).size();
    if (numberOfCoveredServers < numberOfServers) {
      // TODO(cuijianwei): support balance by table
      // admin.balancer(Bytes.toBytes(CANARY_TABLE_NAME));
    }
    table.close();
  }

  private void createCanaryTable(int numberOfServers) throws IOException {
    int totalNumberOfRegions = numberOfServers * DEFAULT_REGIONS_PER_SERVER;
    LOG.info("Number of live regionservers: " + numberOfServers + ", " +
        "pre-splitting the canary table into " + totalNumberOfRegions + " regions " +
        "(default regions per server: " + DEFAULT_REGIONS_PER_SERVER + ")");

    HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(HConstants.CANARY_TABLE_NAME));
    HColumnDescriptor family = new HColumnDescriptor(HConstants.CANARY_TABLE_FAMILY_NAME);
    family.setMaxVersions(1);
    // 1day
    family.setTimeToLive(24 * 60 * 60 * 1000);

    desc.addFamily(family);
    byte[][] splits = new RegionSplitter.HexStringSplit().split(totalNumberOfRegions);
    admin.createTable(desc, splits);
  }

  private void getReplicationLag() {
    List<ReplicationPeerDescription> replicationPeerDescriptionList;
    try {
      replicationPeerDescriptionList = this.admin.listReplicationPeers();
    } catch (IOException e) {
      LOG.warn("get replication peers list error!!!replication availability will force to 100%", e);
      return;
    }
    for (ReplicationPeerDescription peerDescription : replicationPeerDescriptionList) {
      ReplicationLoadSource replicationLoadSource;
      try {
        replicationLoadSource = this.admin.getPeerMaxReplicationLoad(peerDescription.getPeerId());
        long replicationLag = replicationLoadSource.getReplicationLag();
        sink.publishReplicationLag(peerDescription.getPeerId(), replicationLag);
      } catch (IOException e) {
        LOG.warn(
            "get replication load of Peer " + peerDescription.getPeerId()
                + " error.ignoring availability calculate", e);
      }
    }
  }

  private void getMasterAvailability() {
    int retrys = 3;
    while (retrys > 0) {
      try {
        this.admin.getTableDescriptor(TableName.valueOf(HConstants.CANARY_TABLE_NAME));
        sink.publishMasterAvilability(100.0);
        return;
      } catch (IOException e) {
        LOG.warn("a remote or network exception occurs when getTableDescriptor from master", e);
        retrys--;
      }
    }
    LOG.warn("HMaster is unavailable after 3 retrys");
    sink.publishMasterAvilability(0.0);
  }

  private void getTableMinAvailability() {
    Pair<TableName, Double> tableMinAvailabilityPair = new Pair<>();
    totalRequestByTable.forEach((table, totalCount) -> {
      if (totalCount != 0) {
        double tableFailures = failuresByTable.get(table) != null ? failuresByTable.get(table) : 0;
        double tableAvailability = (totalCount - tableFailures) * 100.0 / totalCount;
        if (tableMinAvailabilityPair.getFirst() == null
            || tableAvailability < tableMinAvailabilityPair.getSecond()) {
          tableMinAvailabilityPair.setFirst(table);
          tableMinAvailabilityPair.setSecond(tableAvailability);
        }
      }
    });
    if (tableMinAvailabilityPair.getFirst() != null) {
      sink.publishTableMinAvilability(tableMinAvailabilityPair);
    }
  }

  private void getRegionServerMinAvailability(){
    Pair<ServerName, Double> regionServerMinAvailabilityPair = new Pair<>();
    totalRequestByServer.forEach((server, totalCount) -> {
      if (totalCount != 0) {
        double serverFailures =
            failuresByServer.get(server) != null ? failuresByServer.get(server) : 0;
        double serverAvailability = (totalCount - serverFailures) * 100.0 / totalCount;
        if (regionServerMinAvailabilityPair.getFirst() == null
            || serverAvailability < regionServerMinAvailabilityPair.getSecond()) {
          regionServerMinAvailabilityPair.setFirst(server);
          regionServerMinAvailabilityPair.setSecond(serverAvailability);
        }
      }
    });
    if (regionServerMinAvailabilityPair.getFirst() != null) {
      sink.publishRegionServerMinAvilability(regionServerMinAvailabilityPair);
    }
  }

  public static void main(String[] args) {
    // TODO : In test, there are security-related errors if these properties are set.
    // As a temporary solution, set the properties before starting. Need to
    // find out the root cause in future.
    Configuration conf = HBaseConfiguration.create();
    System.setProperty("hadoop.property.hadoop.security.authentication", "kerberos");
    System.setProperty("hadoop.property.hadoop.client.keytab.file",
      conf.get("hbase.canary.keytab.file"));
    System.setProperty("hadoop.property.hadoop.client.kerberos.principal",
      conf.get("hbase.canary.kerberos.principal"));
    conf = HBaseConfiguration.create();

    conf.setInt("hbase.rpc.timeout",
        conf.getInt(HConstants.CANARY_RPC_TIMEOUT, HConstants.DEFAULT_CANARY_RPC_TIMEOUT));
    conf.setInt("hbase.client.pause", conf.getInt("hbase.canary.client.pause", 100));
    conf.setInt("hbase.client.operation.timeout",
      conf.getInt("hbase.canary.client.operation.timeout", 500));
    conf.setInt("hbase.client.retries.number",
      conf.getInt("hbase.canary.client.retries.number", 2));
    conf.setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD,
      conf.getInt("hbase.canary.client.scanner.timeout.period", 2000));

    int exitCode = 0;
    Class<? extends Sink> sinkClass =
        conf.getClass("hbase.canary.sink.class", StdOutSink.class, Sink.class);
    Sink sink = ReflectionUtils.newInstance(sinkClass, conf);
    try {
      exitCode = ToolRunner.run(conf, new Canary(sink), args);
    } catch (Exception e) {
      LOG.error("Canry tool exited with exception. ", e);
    }
    System.exit(exitCode);
  }
}
