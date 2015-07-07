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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.RegionSplitter;
import org.apache.hadoop.hbase.util.Strings;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * HBase Canary Tool, that that can be used to do
 * "canary monitoring" of a running HBase cluster.
 *
 * For each region tries to get one row per column family
 * and outputs some information about failure or latency.
 */
public final class Canary implements Tool {
  // Sink interface used by the canary to outputs information
  public interface Sink {
    public void publishReadFailure(HRegionInfo region, Exception e);
    public void publishReadFailure(HRegionInfo region, HColumnDescriptor column, Exception e);
    public void publishReadTiming(HRegionInfo region, HColumnDescriptor column, long msTime);

    public void publishWriteFailure(HRegionInfo region, Exception e);
    public void publishWriteFailure(HRegionInfo region, HColumnDescriptor column, Exception e);
    public void publishWriteTiming(HRegionInfo region, HColumnDescriptor column, long msTime);
  }

  // Simple implementation of canary sink that allows to plot on
  // file or standard output timings or failures.
  public static class StdOutSink implements Sink {
    @Override
    public void publishReadFailure(HRegionInfo region, Exception e) {
      LOG.error(String.format("read from region %s failed", region.getRegionNameAsString()), e);
    }

    @Override
    public void publishReadFailure(HRegionInfo region, HColumnDescriptor column, Exception e) {
      LOG.error(String.format("read from region %s column family %s failed",
                region.getRegionNameAsString(), column.getNameAsString()), e);
    }

    @Override
    public void publishReadTiming(HRegionInfo region, HColumnDescriptor column, long msTime) {
      LOG.info(String.format("read from region %s column family %s in %dms",
               region.getRegionNameAsString(), column.getNameAsString(), msTime));
    }

    @Override
    public void publishWriteFailure(HRegionInfo region, Exception e) {
      LOG.error(String.format("write to region %s failed", region.getRegionNameAsString()), e);
    }
    @Override
    public void publishWriteFailure(HRegionInfo region, HColumnDescriptor column, Exception e) {
      LOG.error(String.format("write to region %s column family %s failed",
        region.getRegionNameAsString(), column.getNameAsString()), e);
    }

    @Override
    public void publishWriteTiming(HRegionInfo region, HColumnDescriptor column, long msTime) {
      LOG.info(String.format("write to region %s column family %s in %dms",
        region.getRegionNameAsString(), column.getNameAsString(), msTime));
    }
  }

  /**
   * Contact a region server and get all information from it
   */
  static class RegionTask implements Callable<Void> {
    private HConnection connection;
    private HTableDescriptor tableDesc;
    private HRegionInfo region;
    private Sink sink;

    RegionTask(Configuration conf, HConnection connection, HTableDescriptor tableDesc,
        HRegionInfo region, Sink sink) {
      this.connection = connection;
      this.tableDesc = tableDesc;
      this.region = region;
      this.sink = sink;
    }

    /**
     * check read for normal user tables
     * @return
     */
    private Void read() {
      /*
       * For each column family of the region tries to get one row
       * and outputs the latency, or the failure.
       */
      HTableInterface table;
      try {
        table = this.connection.getTable(tableDesc.getName());
        byte[] rowToCheck = Bytes.randomKey(region.getStartKey(), region.getEndKey());
        for (HColumnDescriptor column : tableDesc.getColumnFamilies()) {
          Scan scan = new Scan(rowToCheck, rowToCheck);
          scan.setRaw(true);
          scan.setCaching(1);
          scan.setFilter(new FirstKeyOnlyFilter());
          scan.addFamily(column.getName());
          scan.setCacheBlocks(false);
          scan.setIgnoreTtl(true);
          scan.setSmall(true);
          ResultScanner scanner = table.getScanner(scan);
          try {
            long startTime = System.currentTimeMillis();
            Result r = scanner.next();
            long time = System.currentTimeMillis() - startTime;
            sink.publishReadTiming(region, column, time);
          } catch (Exception e) {
            sink.publishReadFailure(region, column, e);
          } finally {
            scanner.close();
          }
        }
        table.close();
      } catch (IOException e) {
        sink.publishReadFailure(region, e);
      }
      return null;
    }

    /**
     * Check writes for the canary table
     * @return
     */
    private Void write() {
      HTableInterface table;
      try {
        table = this.connection.getTable(tableDesc.getName());
        byte[] rowToCheck = region.getStartKey();
        for (HColumnDescriptor column : tableDesc.getColumnFamilies()) {
          Put put = new Put(rowToCheck);
          put.add(column.getName(), HConstants.EMPTY_BYTE_ARRAY,
            HConstants.EMPTY_BYTE_ARRAY);
          try {
            long startTime = System.currentTimeMillis();
            table.put(put);
            long time = System.currentTimeMillis() - startTime;
            sink.publishWriteTiming(region, column, time);
          } catch (Exception e) {
            sink.publishWriteFailure(region, column, e);
          }
        }
        table.close();
      } catch (IOException e) {
        sink.publishWriteFailure(region, e);
      }
      return null;
    }

    @Override
    public Void call() {
     if (tableDesc.getNameAsString().equals(CANARY_TABLE_NAME)) {
       return write();
     } else {
       return read();
     }
    }
  }

  private static final String CANARY_TABLE_NAME = "_canary_";
  private static final String CANARY_TABLE_FAMILY_NAME = "Test";
  private static int DEFAULT_REGIONS_PER_SERVER = 2;

  private static final int MAX_THREADS_NUM = 16; // #threads to contact regions

  private static final long DEFAULT_INTERVAL = 6000;

  private static final Log LOG = LogFactory.getLog(Canary.class);

  private Configuration conf = null;
  private HBaseAdmin admin = null;
  private long interval = 0;
  private ExecutorService executor; // threads to retrieve data from regionservers
  private Sink sink = null;
  private HConnection connection = null;
  private List<RegionTask> tasks;

  public Canary(ExecutorService executor, Sink sink) {
    this.executor = executor;
    this.sink = sink;
    this.tasks = new LinkedList<RegionTask>();
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public int run(String[] args) throws Exception {
    int tables_index = -1;

    // Process command line args
    for (int i = 0; i < args.length; i++) {
      String cmd = args[i];

      if (cmd.startsWith("-")) {
        if (tables_index >= 0) {
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
      } else if (tables_index < 0) {
        // keep track of first table name specified by the user
        tables_index = i;
      }
    }
    List<String> tables = new LinkedList<String>();
    if (tables_index >= 0) {
      for (int i = tables_index; i < args.length; i++) {
        tables.add(args[i]);
      }
    }

    // initialize HBase conf and admin
    if (conf == null) conf = HBaseConfiguration.create();
    connection = HConnectionManager.createConnection(this.conf);
    String hostname =
        conf.get(
          "hbase.canary.ipc.address",
          Strings.domainNamePointerToHostName(DNS.getDefaultHost(
            conf.get("hbase.regionserver.dns.interface", "default"),
            conf.get("hbase.regionserver.dns.nameserver", "default"))));

    // initialize server principal (if using secure Hadoop)
    User.login(conf, "hbase.canary.keytab.file", "hbase.canary.kerberos.principal", hostname);

    admin = new HBaseAdmin(connection);
    // lets the canary monitor the cluster
    long lastCheckTime = -1;

    do {
      long startTime = System.currentTimeMillis();

      try {
        tasks = getSniffTasks(tables);
      } catch (Exception e) {
        LOG.error("Update sniff tasks failed. Using previous sniffing tasks", e);
      }
      // check canary distribution for 10 minutes
      if (EnvironmentEdgeManager.currentTimeMillis() -lastCheckTime > 10 * 60 * 1000) {
        try {
          checkCanaryDistribution();
        } catch (Exception e) {
          LOG.error("Check canary distribution failed.", e);
        }
        lastCheckTime = EnvironmentEdgeManager.currentTimeMillis();
      }
      List<Future<Void>> taskFutures = this.executor.invokeAll(tasks);
      for (Future<Void> future : taskFutures) {
        try {
          future.get();
        } catch (ExecutionException e) {
          LOG.error("Sniff region failed!", e);
        }
      }
      long  finishTime = System.currentTimeMillis();
      if (finishTime < startTime + interval) {
        Thread.sleep(startTime + interval - finishTime);
      }
    } while (interval > 0);

    admin.close();
    connection.close();
    return(0);
  }

  /**
   * Update sniff tasks
   * @param tables
   */
  private List<RegionTask> getSniffTasks(List<String> tables) throws Exception {
    List<RegionTask> tmpTasks = new LinkedList<RegionTask>();
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
    System.err.println("   -interval <N>  Interval between checks (sec)");
    System.exit(1);
  }

  /*
   * canary entry point to monitor all the tables.
   */
  private List<RegionTask> sniff() throws Exception {
    List<RegionTask> tasks = new LinkedList<RegionTask>();
    for (HTableDescriptor table : admin.listTables()) {
      if (admin.isTableEnabled(table.getName())) {
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
    if (admin.isTableAvailable(tableName)) {
      tasks.addAll(sniff(admin.getTableDescriptor(tableName.getBytes())));
    } else {
      LOG.warn(String.format("Table %s is not available", tableName));
    }
    return tasks;
  }

  /*
   * Loops over regions that owns this table,
   * and output some information abouts the state.
   */
  private List<RegionTask> sniff(HTableDescriptor tableDesc) throws Exception {
    HTableInterface table = null;
    try {
      table = this.connection.getTable(tableDesc.getName());
    } catch (TableNotFoundException e) {
      return new ArrayList<RegionTask>();
    }
    List<RegionTask> tasks = new ArrayList<RegionTask>();
    for (HRegionInfo region : admin.getTableRegions(tableDesc.getName())) {
       tasks.add(new RegionTask(conf, connection, tableDesc, region, sink));
    }
    table.close();
    return tasks;
  }


  private void checkCanaryDistribution() throws IOException {
    if (!admin.tableExists(CANARY_TABLE_NAME)) {
      int numberOfServers = admin.getClusterStatus().getServers().size();
      if (numberOfServers == 0) {
        throw new IllegalStateException("No live regionservers");
      }
      createCanaryTable(numberOfServers);
    }

    if (!admin.isTableEnabled(CANARY_TABLE_NAME)) {
      admin.enableTable(CANARY_TABLE_NAME);
    }

    int numberOfServers = admin.getClusterStatus().getServers().size();
    HTable table = new HTable(getConf(), CANARY_TABLE_NAME);
    Collection<ServerName> regionsevers = table.getRegionLocations().values();
    int numberOfRegions = regionsevers.size();
    double rate = 1.0 * numberOfRegions / numberOfServers;
    if ((rate < DEFAULT_REGIONS_PER_SERVER * 0.7) || (rate > DEFAULT_REGIONS_PER_SERVER * 1.5)) {
      LOG.info("Current canary region num: " + numberOfRegions + " server num: " + numberOfServers);
      admin.disableTable(CANARY_TABLE_NAME);
      admin.deleteTable(CANARY_TABLE_NAME);
      createCanaryTable(numberOfServers);
    }
    int numberOfCoveredServers = new HashSet<ServerName>(regionsevers).size();
    if (numberOfCoveredServers < numberOfServers) {
      admin.balancer(Bytes.toBytes(CANARY_TABLE_NAME));
    }
    table.close();
  }

  private void createCanaryTable(int numberOfServers) throws IOException {
    int totalNumberOfRegions = numberOfServers * DEFAULT_REGIONS_PER_SERVER;
    LOG.info("Number of live regionservers: " + numberOfServers + ", "
        + "pre-splitting the canary table into " + totalNumberOfRegions
        + " regions " + "(default regions per server: "
        + DEFAULT_REGIONS_PER_SERVER + ")");

    HTableDescriptor desc = new HTableDescriptor(CANARY_TABLE_NAME);
    HColumnDescriptor family = new HColumnDescriptor(CANARY_TABLE_FAMILY_NAME);
    family.setMaxVersions(1);
    // 1day
    family.setTimeToLive(24 * 60 * 60 *1000);

    desc.addFamily(family);
    byte[][] splits =
        new RegionSplitter.HexStringSplit().split(totalNumberOfRegions);
    admin.createTable(desc, splits);
  }

  public static void main(String[] args) {
    Configuration conf = HBaseConfiguration.create();

    conf.setInt("hbase.rpc.timeout", conf.getInt("hbase.canary.rpc.timeout", 200));
    conf.setInt("hbase.client.pause", conf.getInt("hbase.canary.client.pause", 100));
    conf.setInt("hbase.client.operation.timeout",
      conf.getInt("hbase.canary.client.operation.timeout", 500));
    conf.setInt("hbase.client.retries.number",
      conf.getInt("hbase.canary.client.retries.number", 2));

    int numThreads = conf.getInt("hbase.canary.threads.num", MAX_THREADS_NUM);
    ExecutorService executor = new ScheduledThreadPoolExecutor(numThreads);

    Class<? extends Sink> sinkClass =
        (Class<? extends Sink>) conf.getClass("hbase.canary.sink.class", StdOutSink.class);
    Sink sink = ReflectionUtils.newInstance(sinkClass, conf);

    int exitCode = 0;
    try {
      exitCode = ToolRunner.run(conf, new Canary(executor, sink), args);
    } catch (Exception e) {
      LOG.error("Canry tool exited with exception. ", e);
    }
    executor.shutdown();
    System.exit(exitCode);
  }
}
