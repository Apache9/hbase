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

import com.xiaomi.infra.hbase.CanaryStatusServlet;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.BindException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.AsyncConnection;
import org.apache.hadoop.hbase.client.AsyncTable;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.http.InfoServer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.RegionSplitter;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * HBase Canary Tool, that that can be used to do "canary monitoring" of a running HBase cluster.
 * For each region tries to get one row per column family and outputs some information about failure
 * or latency.
 */
@InterfaceAudience.Private
public final class Canary implements Tool {
  // Sink interface used by the canary to outputs information
  public interface Sink {
    void publishOldWalsFilesCount(long count);

    void publishReadFailure(RegionInfo region, Throwable e);

    void publishReadFailure(RegionInfo region, ColumnFamilyDescriptor column, Throwable e);

    void publishReadTiming(RegionInfo region, ColumnFamilyDescriptor column, long msTime);

    void publishWriteFailure(RegionInfo region, Throwable e);

    void publishWriteFailure(RegionInfo region, ColumnFamilyDescriptor column, Throwable e);

    void publishWriteTiming(RegionInfo region, ColumnFamilyDescriptor column, long msTime);

    void reportSummary();

    default double getReadAvailability() {
      return 100.0;
    }

    default double getWriteAvailability() {
      return 100.0;
    }

    default double getAvailability() {
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
    public void publishReadFailure(RegionInfo region, Throwable e) {
      LOG.error(String.format("read from region %s failed", region.getRegionNameAsString()), e);
    }

    @Override
    public void publishReadFailure(RegionInfo region, ColumnFamilyDescriptor column, Throwable e) {
      LOG.error(String.format("read from region %s column family %s failed",
        region.getRegionNameAsString(), column.getNameAsString()), e);
    }

    @Override
    public void publishReadTiming(RegionInfo region, ColumnFamilyDescriptor column, long msTime) {
      LOG.info(String.format("read from region %s column family %s in %dms",
        region.getRegionNameAsString(), column.getNameAsString(), msTime));
    }

    @Override
    public void publishWriteFailure(RegionInfo region, Throwable e) {
      LOG.error(String.format("write to region %s failed", region.getRegionNameAsString()), e);
    }

    @Override
    public void publishWriteFailure(RegionInfo region, ColumnFamilyDescriptor column, Throwable e) {
      LOG.error(String.format("write to region %s column family %s failed",
        region.getRegionNameAsString(), column.getNameAsString()), e);
    }

    @Override
    public void publishWriteTiming(RegionInfo region, ColumnFamilyDescriptor column, long msTime) {
      LOG.info(String.format("write to region %s column family %s in %dms",
        region.getRegionNameAsString(), column.getNameAsString(), msTime));
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
    private final TableDescriptor tableDesc;
    private final RegionInfo region;
    private final ServerName server;
    private final Sink sink;
    private final Canary canary;

    RegionTask(AsyncTable<?> table, TableDescriptor tableDesc, RegionInfo region, ServerName server,
        Sink sink, Canary canary) {
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
      Arrays.stream(tableDesc.getColumnFamilies()).forEach(column -> {
        StopWatch watch = new StopWatch();
        watch.start();
        futures.add(table
            .scanAll(new Scan().withStartRow(randomKey(region.getStartKey(), region.getEndKey()))
                .withStopRow(region.getEndKey()).addFamily(column.getName()).setRaw(true)
                .setFilter(new FirstKeyOnlyFilter()).setCacheBlocks(false).setOneRowLimit())
            .whenComplete((r, e) -> {
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

    private void handleReadException(Throwable e, ColumnFamilyDescriptor column) {
      // check whether the table is enabled by the exception message. The exception
      // is RetriesExhaustedException, we need to judge from the its message
      // table deleted or disabled. galaxy(sds/emq) will run bvt all the time, and will
      // create/delete table in bvt, this will make the region unavailable.
      if (e instanceof TableNotFoundException || e instanceof TableNotEnabledException
          || e.getMessage().contains("TableNotFoundException")
          || e.getMessage().contains("is disabled")) {
        LOG.warn("read failure from disabled or deleted table, region=" + region.getEncodedName()
            + ", table=" + tableDesc.getTableName().getNameAsString());
        return;
      }
      clearCacheAndPublishReadFailure(column, e);
    }

    private void clearCacheAndPublishReadFailure(ColumnFamilyDescriptor column, Throwable e) {
      canary.clearCachedTasks(tableDesc.getTableName().getNameAsString());
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
      Arrays.stream(tableDesc.getColumnFamilies()).forEach(column -> {
        StopWatch watch = new StopWatch();
        watch.start();
        futures.add(table
            .put(new Put(randomKey(region.getStartKey(), region.getEndKey()))
                .addColumn(column.getName(), EMPTY_BYTE_ARRAY, EMPTY_BYTE_ARRAY))
            .whenComplete((r, e) -> {
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
      if (DEFAULT_WRITE_TABLE_NAME.equals(tableDesc.getTableName())) {
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
  public static final TableName DEFAULT_WRITE_TABLE_NAME = TableName.valueOf("hbase:canary");
  private static final byte[] CANARY_TABLE_FAMILY_NAME = Bytes.toBytes("Test");
  private static int DEFAULT_REGIONS_PER_SERVER = 2;

  private static final int DEFAULT_MAX_CONCURRENCY = 200;

  private static final long DEFAULT_INTERVAL = 6000;

  private static final long DEFAULT_TIMEOUT = 600000; // 10 mins

  private static final Logger LOG = LoggerFactory.getLogger(Canary.class);

  private final Sink sink;
  private Configuration conf;
  private Connection conn;
  private Admin admin;
  private AsyncConnection asyncConn;
  private long interval;
  private long timeout;
  private int maxConcurrency;
  private List<RegionTask> tasks;
  private final ConcurrentMap<String, List<RegionTask>> cachedTasks =
      new ConcurrentHashMap<String, List<RegionTask>>();
  private FileSystem fs;
  private Path rootdir;
  private Map<ServerName, Integer> failuresByServer;
  private Map<TableName, Integer> failuresByTable;
  private InfoServer infoServer;

  public Canary(Sink sink) {
    this.sink = sink;
    this.failuresByServer = new ConcurrentHashMap<>();
    this.failuresByTable = new ConcurrentHashMap<>();
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
    long startTime = System.currentTimeMillis();

    try {
      tasks = getSniffTasks(tables);
    } catch (Exception e) {
      LOG.error("Update sniff tasks failed. Using previous sniffing tasks", e);
    }

    // clear cached tasks and check canary distribution for 10 minutes
    if (System.currentTimeMillis() - lastCheckTime > 10 * 60 * 1000) {
      clearCachedTasks();
      try {
        checkCanaryDistribution();
      } catch (Exception e) {
        LOG.error("Check canary distribution failed.", e);
      }
      lastCheckTime = System.currentTimeMillis();
    }

    // clear the previous failures
    this.failuresByServer.clear();
    this.failuresByTable.clear();

    List<RegionTask> tasksNotRun = new ArrayList<>();
    Semaphore concurrencyControl = new Semaphore(maxConcurrency);
    CountDownLatch unfinishedTasks = new CountDownLatch(tasks.size());
    tasks.forEach(task -> {
      // check whether we have already timed out
      long remainingTime = timeout - (System.currentTimeMillis() - startTime);
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
    sink.reportSummary();
    long finishTime = System.currentTimeMillis();
    LOG.info("Finish one turn sniff, consume(ms)=" + (finishTime - startTime) + ", interval(ms)="
        + interval + ", timeout(ms)=" + timeout + ", taskCount=" + tasks.size()
        + ", taskNotRunCount=" + tasksNotRun.size());
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
    conn = ConnectionFactory.createConnection(conf);
    admin = conn.getAdmin();
    asyncConn = ConnectionFactory.createAsyncConnection(conf).get();
    lastCheckTime = System.currentTimeMillis();
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
      LOG.info("check oldWal directory failed, ", e);
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
    System.err
        .println("Usage: bin/hbase " + getClass().getName() + " [opts] [table 1 [table 2...]]");
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

  /*
   * canary entry point to monitor all the tables.
   */
  private List<RegionTask> sniff() throws Exception {
    List<RegionTask> tasks = new LinkedList<>();
    // admin.listTables invoke connection.listTables directly, won't create zkw
    for (TableDescriptor table : admin.listTableDescriptors()) {
      if (admin.isTableEnabled(table.getTableName())) {
        tasks.addAll(sniff(table));
      }
    }
    return tasks;
  }

  /*
   * canary entry point to monitor specified table.
   */
  private List<RegionTask> sniff(String tableName) throws Exception {
    List<RegionTask> tasks = new LinkedList<>();
    // admin.isTableAvailable invoke connection.isTableAvailable directly, won't create zkw
    if (admin.isTableAvailable(TableName.valueOf(tableName))) {
      // admin.getTableDescriptor invoke connection.getTableDescriptor dirctly, won't create zkw
      tasks.addAll(sniff(admin.getDescriptor(TableName.valueOf(tableName))));
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
  private List<RegionTask> sniff(TableDescriptor tableDesc) throws Exception {
    if (tableDesc.getTableName().getNamespaceAsString().equals(excludeNamespace)) {
      return new ArrayList<>();
    }
    List<RegionTask> tasks = cachedTasks.get(tableDesc.getTableName().getNameAsString());
    if (tasks != null) {
      return tasks;
    }
    AsyncTable<?> table = asyncConn.getTable(tableDesc.getTableName());
    tasks = MetaTableAccessor.getTableRegionsAndLocations(conn, tableDesc.getTableName()).stream()
        .map(r -> new RegionTask(table, tableDesc, r.getFirst(), r.getSecond(), sink, this))
        .collect(Collectors.toList());
    cachedTasks.put(tableDesc.getTableName().getNameAsString(), tasks);
    LOG.info("get task from meta table, table=" + tableDesc.getTableName().getNameAsString()
        + ", taskCount=" + tasks.size());
    return tasks;
  }

  private void checkCanaryDistribution() throws IOException {
    if (!admin.tableExists(DEFAULT_WRITE_TABLE_NAME)) {
      int numberOfServers = admin.getRegionServers().size();
      if (numberOfServers == 0) {
        throw new IllegalStateException("No live regionservers");
      }
      createCanaryTable(numberOfServers);
    }

    if (!admin.isTableEnabled(DEFAULT_WRITE_TABLE_NAME)) {
      admin.enableTable(DEFAULT_WRITE_TABLE_NAME);
    }

    Collection<HRegionLocation> regionLocations;
    try (Connection conn = ConnectionFactory.createConnection(conf)) {
      try (Table table = conn.getTable(DEFAULT_WRITE_TABLE_NAME)) {
        regionLocations = ((HTable) table).getRegionLocator().getAllRegionLocations();
      }
    }
    int numberOfServers = admin.getRegionServers().size();
    int numberOfRegions = regionLocations.size();
    double rate = 1.0 * numberOfRegions / numberOfServers;
    if ((rate < DEFAULT_REGIONS_PER_SERVER * 0.7) || (rate > DEFAULT_REGIONS_PER_SERVER * 1.5)) {
      LOG.info("Current canary region num: " + numberOfRegions + " server num: " + numberOfServers);
      admin.disableTable(DEFAULT_WRITE_TABLE_NAME);
      admin.deleteTable(DEFAULT_WRITE_TABLE_NAME);
      createCanaryTable(numberOfServers);
    }
  }

  private void createCanaryTable(int numberOfServers) throws IOException {
    int totalNumberOfRegions = numberOfServers * DEFAULT_REGIONS_PER_SERVER;
    LOG.info("Number of live regionservers: " + numberOfServers + ", "
        + "pre-splitting the canary table into " + totalNumberOfRegions + " regions "
        + "(default regions per server: " + DEFAULT_REGIONS_PER_SERVER + ")");

    TableDescriptor desc = TableDescriptorBuilder.newBuilder(DEFAULT_WRITE_TABLE_NAME)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(CANARY_TABLE_FAMILY_NAME)
            .setMaxVersions(1).setTimeToLive(24 * 60 * 60 * 1000).build())
        .build();
    byte[][] splits = new RegionSplitter.HexStringSplit().split(totalNumberOfRegions);
    admin.createTable(desc, splits);
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

    conf.setInt("hbase.rpc.timeout", conf.getInt("hbase.canary.rpc.timeout", 200));
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
