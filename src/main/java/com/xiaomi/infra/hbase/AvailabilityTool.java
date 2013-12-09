package com.xiaomi.infra.hbase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;

/**
 * Check the important tables periodically<br>
 * Scan the Meta table to get all regions for special tables
 * @author liushaohui
 */
public class AvailabilityTool {
  
  private static final int MAX_NUM_THREADS = 50; // #threads to contact regions
  private static final int DEFAULT_MAX_REGIONS_PER_TABLE = 100;

  private static final Log LOG = LogFactory.getLog(AvailabilityTool.class);
  private Configuration conf;
  private HBaseAdmin admin;
  private ExecutorService executor; // threads to retrieve data from regionservers

  // The maximum number regions to check for a table
  // If the region number of table is larger than this number,
  // just randomly select this number regions from the table
  private int maxRegionsPerTable = 100;

  private List<String> tables = new ArrayList<String>();
  private Reportor reportor = new Reportor();

  private Random rand = new Random();

  /**
   * Contact a region server and get all information from it
   */
  static class WorkItemRegion implements Callable<Void> {
    private HRegionInfo region;
    private Configuration conf;
    private Reportor reportor;

    WorkItemRegion(Configuration conf, HRegionInfo region, Reportor reportor) {
      this.conf = conf;
      this.region = region;
      this.reportor = reportor;
    }

    @Override
    public Void call() {
      HTable table = null;
      try {
        table = new HTable(conf, region.getTableName());
      } catch (IOException e) {
        LOG.error("Create table: " + region.getTableName() + " error", e);
        this.reportor.reportAvailable(region, false);
        return null;
      }
      Get get = new Get(region.getStartKey());
      try {
        table.exists(get);
        LOG.info("This region: " + region.getRegionNameAsString()
            + " is available.");
        this.reportor.reportAvailable(region, true);
      } catch (IOException e) {
        LOG.error("Get from region: " + region + " error", e);
        this.reportor.reportAvailable(region, false);
      }

      try {
        table.close();
      } catch (IOException e) {
        LOG.error("Close Table:" + region.getTableName() + " error", e);
      }
      return null;
    }
  }

  /**
   * Constructor
   * @param conf Configuration object
   */
  public AvailabilityTool(Configuration conf) {
    this.conf = conf;
    int numThreads = conf.getInt("hbase.availabilitytool.numthreads",
      MAX_NUM_THREADS);
    executor = new ScheduledThreadPoolExecutor(numThreads);
    this.maxRegionsPerTable = conf.getInt(
      "hbase.availabilitytool.maxregionnum", DEFAULT_MAX_REGIONS_PER_TABLE);
  }

  public AvailabilityTool(Configuration conf, ExecutorService exec) {
    this.conf = conf;
    this.executor = exec;
  }

  public Reportor getReportor() {
    return this.reportor;
  }

  private void connect() throws IOException {
    admin = new HBaseAdmin(conf);
  }

  public void exec(String[] args) throws IOException, InterruptedException {
    connect();
    parseArgs(args);
    
    if (tables.size() < 1) {
      LOG.error("No valid table to check!");
      return;
    }

    int sleeptime = conf.getInt("table-avilibility-period-ms", 10000);
    String group = "infra-hbase-"
        + conf.get("hbase.cluster.name", "ggsrv-miliao");
    String owl = conf.get("hbase.owl.collecot.url",
      "http://10.0.3.216:8000/monitor/addCounter/");
    Agent agent = new Agent(group, owl);
    
    while (true) {
      // check the availability of selected tables;
      List<Future<Void>> taskFutures = new LinkedList<Future<Void>>();
      for (String tablename : tables) {
        taskFutures.addAll(checkTableAvailable(tablename));
        LOG.info("Check the table avalibility. Table name: " + tablename);
      }
      // join
      for (Future<Void> future : taskFutures) {
        try {
          future.get();
        } catch (ExecutionException e) {
          LOG.error("Check region failed!", e);
        }
      }
      this.reportor.summary(agent);
      agent.sendCounters();
      // reset reportor
      this.reportor.reset();
      Thread.sleep(sleeptime);
    }
  }

  private List<Future<Void>> checkTableAvailable(String table) {
    try {
      List<HRegionInfo> regions = admin.getTableRegions(Bytes.toBytes(table));
      return checkRegionsAvailable(table, regions);
    } catch (IOException e) {
      this.reportor.failedTable(table);
      LOG.error("Get table:" + table + " region failed!", e);
    }
    return new LinkedList<Future<Void>>();
  }

  private List<Future<Void>> checkRegionsAvailable(String tablename,
      List<HRegionInfo> regions) {
    List<HRegionInfo> selectedRegions = selectRegions(regions);
    List<WorkItemRegion> tasks = new ArrayList<WorkItemRegion>(
        selectedRegions.size());
    if (selectedRegions.size() == 0) {
      LOG.error("NO regions selected!");
    }
    for (HRegionInfo region : selectedRegions) {
      tasks.add(new WorkItemRegion(this.conf, region, this.reportor));
    }
    try {
      return this.executor.invokeAll(tasks);
    } catch (InterruptedException e) {
      this.reportor.failedTable(tablename);
      LOG.error("Try invoke all tasks failed!", e);
    }
    return new LinkedList<Future<Void>>();
  }

  private List<HRegionInfo> selectRegions(List<HRegionInfo> regions) {
    if (regions.size() <= this.maxRegionsPerTable) {
      return regions;
    } else {
      List<HRegionInfo> selectedRegions = new ArrayList<HRegionInfo>();
      int total = regions.size();
      for (HRegionInfo region : regions) {
        if (rand.nextInt(total) < this.maxRegionsPerTable) {
          selectedRegions.add(region);
        }
      }
      return selectedRegions;
    }
  }

  private void parseArgs(String[] args) throws IOException {
    // Process command-line args.
    for (int i = 0; i < args.length; i++) {
      String cmd = args[i];
      if (cmd.equals("-help") || cmd.equals("-h")) {
        printUsageAndExit();
        return;
      } else {
        String tableName = cmd;
        if (admin.isTableAvailable(tableName)) {
          tables.add(tableName);
          LOG.info("Allow checking for table: " + tableName);
        } else {
          LOG.error("Table: " + tableName
              + " does not exist in this hbase cluster");
        }
      }
    }
  }

  private void printUsageAndExit() {
    System.out
        .println("Usage: bin/hbase com.xiaomi.infra.hbase.AvailabilityTool table_name1, table_name2, ... ");
  }

  /**
   * @param args
   * @throws IOException
   */
  public static void main(String[] args) {
    Configuration conf = HBaseConfiguration.create();
    int numThreads = conf.getInt("hbase.availabilitytool.numthreads",
      MAX_NUM_THREADS);
    ExecutorService exec = new ScheduledThreadPoolExecutor(numThreads);
    AvailabilityTool tool = new AvailabilityTool(conf, exec);
    try {
      tool.exec(args);
    } catch (Throwable e) {
      LOG.error("Running availability tool error", e);
    }
    exec.shutdown();
    Runtime.getRuntime().exit(0);
  }
}
