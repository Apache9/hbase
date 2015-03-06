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
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.security.User;
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
    public void publishReadFailure(HRegionInfo region);
    public void publishReadFailure(HRegionInfo region, HColumnDescriptor column);
    public void publishReadTiming(HRegionInfo region, HColumnDescriptor column, long msTime);
  }

  // Simple implementation of canary sink that allows to plot on
  // file or standard output timings or failures.
  public static class StdOutSink implements Sink {
    @Override
    public void publishReadFailure(HRegionInfo region) {
      LOG.error(String.format("read from region %s failed", region.getRegionNameAsString()));
    }

    @Override
    public void publishReadFailure(HRegionInfo region, HColumnDescriptor column) {
      LOG.error(String.format("read from region %s column family %s failed",
                region.getRegionNameAsString(), column.getNameAsString()));
    }

    @Override
    public void publishReadTiming(HRegionInfo region, HColumnDescriptor column, long msTime) {
      LOG.info(String.format("read from region %s column family %s in %dms",
               region.getRegionNameAsString(), column.getNameAsString(), msTime));
    }
  }

  /**
   * Contact a region server and get all information from it
   */
  static class RegionTask implements Callable<Void> {
    private Configuration conf;
    private HConnection connection;
    private HTableDescriptor tableDesc;
    private HRegionInfo region;
    private Sink sink;

    RegionTask(Configuration conf, HConnection connection, HTableDescriptor tableDesc,
        HRegionInfo region, Sink sink) {
      this.conf = conf;
      this.connection = connection;
      this.tableDesc = tableDesc;
      this.region = region;
      this.sink = sink;
    }

    @Override
    public Void call() {
      /*
       * For each column family of the region tries to get one row
       * and outputs the latency, or the failure.
       */
      HTableInterface table;
      try {
        table = this.connection.getTable(tableDesc.getName());
        for (HColumnDescriptor column : tableDesc.getColumnFamilies()) {
          Get get = new Get(region.getStartKey());
          get.setFilter(new FirstKeyOnlyFilter());
          get.addFamily(column.getName());
          get.setCacheBlocks(false);
          try {
            long startTime = System.currentTimeMillis();
            table.exists(get);
            long time = System.currentTimeMillis() - startTime;

            sink.publishReadTiming(region, column, time);
          } catch (Exception e) {
            sink.publishReadFailure(region, column);
          }
        }
        table.close();
      } catch (IOException e) {
        sink.publishReadFailure(region);
      }
      return null;
    }
  }

  private static final int MAX_THREADS_NUM = 16; // #threads to contact regions

  private static final long DEFAULT_INTERVAL = 6000;

  private static final Log LOG = LogFactory.getLog(Canary.class);

  private Configuration conf = null;
  private HBaseAdmin admin = null;
  private long interval = 0;
  private ExecutorService executor; // threads to retrieve data from regionservers
  private Sink sink = null;
  private HConnection connection = null;

  public Canary(ExecutorService executor, Sink sink) {
    this.executor = executor;
    this.sink = sink;
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
    do {
      long startTime = System.currentTimeMillis();
      if (admin.isAborted()) {
        LOG.error("HBaseAdmin aborted");
        return(1);
      }
      try {
        if (tables_index >= 0) {
          for (int i = tables_index; i < args.length; i++) {
            sniff(args[i]);
          }
        } else {
          sniff();
        }
      } catch (Exception e) {
        LOG.error("Sniff tables failed.", e);
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
  private void sniff() throws Exception {
    List<Future<Void>> taskFutures = new LinkedList<Future<Void>>();
    for (HTableDescriptor table : admin.listTables()) {
      if (admin.isTableEnabled(table.getName())) {
        taskFutures.addAll(sniff(table));
      }
    }
    for (Future<Void> future : taskFutures) {
      try {
        future.get();
      } catch (ExecutionException e) {
        LOG.error("Sniff region failed!", e);
      }
    }
  }

  /*
   * canary entry point to monitor specified table.
   */
  private void sniff(String tableName) throws Exception {
    List<Future<Void>> taskFutures = new LinkedList<Future<Void>>();
    if (admin.isTableAvailable(tableName)) {
      taskFutures.addAll(sniff(admin.getTableDescriptor(tableName.getBytes())));
    } else {
      LOG.warn(String.format("Table %s is not available", tableName));
    }
    for (Future<Void> future : taskFutures) {
      try {
        future.get();
      } catch (ExecutionException e) {
        LOG.error("Sniff region failed!", e);
      }
    }
  }

  /*
   * Loops over regions that owns this table,
   * and output some information abouts the state.
   */
  private List<Future<Void>> sniff(HTableDescriptor tableDesc) throws Exception {
    HTableInterface table = null;
    try {
      table = this.connection.getTable(tableDesc.getName());
    } catch (TableNotFoundException e) {
      return new ArrayList<Future<Void>>();
    }
    List<RegionTask> tasks = new ArrayList<RegionTask>();
    for (HRegionInfo region : admin.getTableRegions(tableDesc.getName())) {
       tasks.add(new RegionTask(conf, connection, tableDesc, region, sink));
    }
    table.close();
    return this.executor.invokeAll(tasks);
  }

  public static void main(String[] args) {
    Configuration conf = HBaseConfiguration.create();

    conf.setInt("hbase.rpc.timeout", 200);
    conf.setInt("hbase.client.pause", 100);
    conf.setInt("hbase.client.operation.timeout", 500);
    conf.setInt("hbase.client.retries.number", 2);

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
