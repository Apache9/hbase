/**
 * Copyright 2010 The Apache Software Foundation
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
package org.apache.hadoop.hbase.mapreduce.replication;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.replication.thrift.ThriftClient;
import org.apache.hadoop.hbase.thrift2.ThriftServer;
import org.apache.hadoop.hbase.thrift2.generated.TColumn;
import org.apache.hadoop.hbase.thrift2.generated.TColumnValue;
import org.apache.hadoop.hbase.thrift2.generated.TGet;
import org.apache.hadoop.hbase.thrift2.generated.THBaseService;
import org.apache.hadoop.hbase.thrift2.generated.TResult;
import org.apache.hadoop.hbase.thrift2.generated.TScan;
import org.apache.hadoop.hbase.thrift2.generated.TTimeRange;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;

/**
 * This map-only job compares the data from a local table with a remote one.
 * Every cell is compared and must have exactly the same keys (even timestamp)
 * as well as same value. It is possible to restrict the job by time range and
 * families. The peer id that's provided must match the one given when the
 * replication stream was setup.
 * <p>
 * Two counters are provided, Verifier.Counters.GOODROWS and BADROWS. The reason
 * for a why a row is different is shown in the map's log.
 */
public class VerifyReplication98 extends Configured implements Tool {

  private static final Log LOG =
      LogFactory.getLog(VerifyReplication98.class);

  public final static String NAME = "verifyrep";
  
  private long startTime = 0;
  private long endTime = 0;
  private String tableName = null;
  private String families = null;
  private String thriftServer = null;
  private String startRow = null;
  private String stopRow = null;
  private int scanRateLimit = -1;
  private int versions = 1;
  private long verifyRows = Long.MAX_VALUE;
  private int maxErrorLog = 1000;
  private String logTable = null;
  private int sleepToReCompare = 0;
      
  public VerifyReplication98(Configuration conf) {
    super(conf);
  }
  /**
   * Map-only comparator for 2 tables
   */
  public static class Verifier
      extends TableMapper<ImmutableBytesWritable, Put> {

    public static enum Counters {
      GOODROWS, BADROWS, ONLY_IN_SOURCE_TABLE_ROWS, ONLY_IN_PEER_TABLE_ROWS, CONTENT_DIFFERENT_ROWS}

    private int replicatedScannerId = -1;
    private Result currentCompareRowInPeerTable;

    private String thriftServer;
    private long st = 0;
    private int scanRateLimit = -1;
    private long rowdone = 0;
    private String tableName;
    private HTable sourceTable;
    private TSocket socket;
    private THBaseService.Client thriftClient;
    private List<TResult> cachedResults = new ArrayList<TResult>();
    private int cachedResultIndex = 0;
    private int caching;
    private ByteBuffer peerTableNameBuffer;
    private HTable logTable;
    private int maxErrorLog;
    private int errors = 0;
    private int sleepToReCompare;

    @Override
    public void setup(Context context) {
      Configuration conf = context.getConfiguration();
      st = EnvironmentEdgeManager.currentTimeMillis();
      scanRateLimit = conf.getInt(TableMapper.SCAN_RATE_LIMIT, -1);
      sleepToReCompare = conf.getInt(NAME +".sleepToReCompare", 0);
      LOG.info("The scan rate limit for verify is " + scanRateLimit
          + " rows per second, sleepToReCompare=" + sleepToReCompare);
    }
    
    protected static Pair<String, Integer> parseThriftServerAddress(String address) {
      String[] items = address.split(":");
      String[] serverNames = items[0].split(",");
      Random rd = new Random(System.currentTimeMillis());
      String serverName = serverNames[rd.nextInt(serverNames.length)];
      String port = ThriftServer.DEFAULT_LISTEN_PORT;
      if (items.length > 1) {
        port = items[1];
      }
      LOG.info("totally " + serverNames.length + " thrift servers, selected " + serverName + ":"
          + port);
      return new Pair<String, Integer>(serverName, Integer.parseInt(port));
    }
    
    private Result nextPeerTableResult() throws IOException {
      try {
        if (cachedResults == null) {
          return null;
        }
        if (cachedResultIndex == cachedResults.size()) {
          cachedResults = thriftClient.getScannerRows(replicatedScannerId, caching);
          if (cachedResults.size() == 0) {
            cachedResults = null;
            return null;
          } else {
            cachedResultIndex = 0;
          }
        }
        TResult result = cachedResults.get(cachedResultIndex++);
        return fromTResult(result);
      } catch (Exception e) {
        throw new IOException("read peer table fail", e);
      }
    }
    
    public static Result fromTResult(TResult result) {
      Iterator<TColumnValue> iter = result.getColumnValuesIterator();
      List<KeyValue> kvs = new ArrayList<KeyValue>();
      while (iter.hasNext()) {
        TColumnValue colV = iter.next();
        KeyValue kv = new KeyValue(result.getRow(), colV.getFamily(), colV.getQualifier(),
            colV.getTimestamp(), colV.getValue());
        kvs.add(kv);
      }
      return new Result(kvs);
    }
    
    /**
     * Map method that compares every scanned row with the equivalent from
     * a distant cluster.
     * @param row  The current table row key.
     * @param value  The columns.
     * @param context  The current context.
     * @throws IOException When something is broken with the data.
     */
    @Override
    public void map(ImmutableBytesWritable row, final Result value,
                    Context context)
        throws IOException {
      if (replicatedScannerId < 0) {
        Configuration conf = context.getConfiguration();
        final TScan scan = new TScan();
        caching = conf.getInt(TableInputFormat.SCAN_CACHEDROWS, 1);
        scan.setCaching(caching);
        long startTime = conf.getLong(NAME + ".startTime", 0);
        long endTime = conf.getLong(NAME + ".endTime", 0);
        String families = conf.get(NAME + ".families", null);
        long verifyRows = conf.getLong(NAME + ".verifyrows", Long.MAX_VALUE);
        if(families != null) {
          String[] fams = families.split(",");
          for(String fam : fams) {
            TColumn column = new TColumn();
            column.setFamily(Bytes.toBytes(fam));
            scan.addToColumns(column);
          }
        }
        if (startTime != 0) {
          TTimeRange range = new TTimeRange(startTime,
              endTime == 0 ? HConstants.LATEST_TIMESTAMP : endTime);
          scan.setTimeRange(range);
        }
        if (verifyRows != Long.MAX_VALUE) {
          scan.setFilterString(Bytes.toBytes("PageFilter("+ verifyRows +")"));
        }
        int versions = conf.getInt(NAME + ".versions", 1);
        if (versions != 1) {
          scan.setMaxVersions(versions);
        }

        thriftServer = conf.get(NAME + ".thriftServer");
        tableName = conf.get(NAME + ".tableName");
        sourceTable = new HTable(conf, tableName);
        maxErrorLog = conf.getInt(NAME + ".maxErrorLog", 0);
        String logTableName = conf.get(NAME + ".logTable");
        if (logTableName != null) {
          logTable = new HTable(conf, logTableName);
        }
        
        LOG.info("peer thrift server address: " + thriftServer + ", caching: " + caching);
        
        final TableSplit tableSplit = (TableSplit)(context.getInputSplit());
        Pair<String, Integer> hostAndPort = parseThriftServerAddress(thriftServer);
        socket = new TSocket(hostAndPort.getFirst(), hostAndPort.getSecond());
        try {
          socket.open();
        } catch (TException e) {
          throw new IOException("open socket to thrift server:" + thriftServer, e);
        }
        thriftClient = new THBaseService.Client(new TBinaryProtocol(socket));
        scan.setStartRow(value.getRow());
        scan.setStopRow(tableSplit.getEndRow());
        String peerTableName = conf.get(NAME+".tableName");
        
        // table name transfer
        String mappingStr = conf.get(ThriftClient.HBASE_REPLICATION_THRIFT_TABLE_NAME_MAP);
        if (mappingStr != null) {
          HashMap<String, String> mappingMap = new HashMap<String, String>();
          ThriftClient.loadTableName(mappingStr, mappingMap);
          if (mappingMap.containsKey(peerTableName)) {
            peerTableName = mappingMap.get(peerTableName);
          }
        }
        LOG.info("Will verify to peerTable: " + peerTableName);
        
        peerTableNameBuffer = ByteBuffer.wrap(Bytes.toBytes(peerTableName));
        
        try {
          replicatedScannerId = thriftClient.openScanner(peerTableNameBuffer, scan);
        } catch (Exception e) {
          throw new IOException("create peer scanner fail, peerTableName=" + peerTableName
              + ", thriftServer=" + hostAndPort.getFirst() + ":" + hostAndPort.getSecond(), e);
        }
        currentCompareRowInPeerTable = nextPeerTableResult();
      }
      
      while (true) {
        if (currentCompareRowInPeerTable == null) {
          // reach the region end of peer table, row only in source table
          logFailRowAndIncreaseCounter(context, Counters.ONLY_IN_SOURCE_TABLE_ROWS, value);
          break;
        }
        int rowCmpRet = Bytes.compareTo(value.getRow(), currentCompareRowInPeerTable.getRow());
        if (rowCmpRet == 0) {
          // rowkey is same, need to compare the content of the row
          try {
            Result.compareResults(value, currentCompareRowInPeerTable);
            context.getCounter(Counters.GOODROWS).increment(1);
          } catch (Exception e) {
            logFailRowAndIncreaseCounter(context, Counters.CONTENT_DIFFERENT_ROWS, value);
          }
          currentCompareRowInPeerTable = nextPeerTableResult();
          break;
        } else if (rowCmpRet < 0) {
          // row only exists in source table
          logFailRowAndIncreaseCounter(context, Counters.ONLY_IN_SOURCE_TABLE_ROWS, value);
          break;
        } else {
          // row only exists in peer table
          logFailRowAndIncreaseCounter(context, Counters.ONLY_IN_PEER_TABLE_ROWS,
            currentCompareRowInPeerTable);
          currentCompareRowInPeerTable = nextPeerTableResult();
        }
      }

      rowdone ++;
      TableMapReduceUtil.limitScanRate(scanRateLimit, rowdone,
        EnvironmentEdgeManager.currentTimeMillis() - st);
    }

    private void logFailRowAndIncreaseCounter(Context context, Counters counter, Result row)
        throws IOException {
      if (sleepToReCompare > 0) {
        Threads.sleep(sleepToReCompare);
        Result sourceResult = sourceTable.get(new Get(row.getRow()));
        try {
          Result peerResult = fromTResult(thriftClient.get(peerTableNameBuffer,
            new TGet(ByteBuffer.wrap(row.getRow()))));
          // online replication is eventually consistency, need recompare
          Result.compareResults(sourceResult, peerResult);
          return;
        } catch (Exception e) {
          LOG.error("recompare fail!", e);
        }
      }
      context.getCounter(counter).increment(1);
      context.getCounter(Counters.BADROWS).increment(1);
      LOG.error(counter.toString() + ", rowkey=" + Bytes.toStringBinary(row.getRow()));
      recordError(row.getRow(), counter);
    }

    private void recordError(byte[] row, Counters type) throws IOException {
      if (logTable != null && errors < maxErrorLog) {
        // TODO : record into log table
        ++errors;
      }
    }

    protected void cleanup(Context context) {
      if (replicatedScannerId >= 0) {
        try {
          while (currentCompareRowInPeerTable != null) {
            logFailRowAndIncreaseCounter(context, Counters.ONLY_IN_PEER_TABLE_ROWS,
              currentCompareRowInPeerTable);
            currentCompareRowInPeerTable = nextPeerTableResult();
          }
        } catch (Exception e) {
          LOG.error("fail to scan peer table in cleanup", e);
        } finally {
          try {
            thriftClient.closeScanner(replicatedScannerId);
          } catch (Exception e) {
            LOG.error("close peer scanner fail", e);
          }
          replicatedScannerId = -1;
        }
      }
      
      if (socket != null) {
        try {
          socket.close();
        } catch (Exception e) {
          LOG.error("close peer HTable fail", e);
        }
      }

      if (sourceTable != null) {
        try {
          sourceTable.close();
        } catch (IOException e) {
          LOG.error("close source HTable fail", e);
        }
      }
    }
  }

  /**
   * Sets up the actual job.
   *
   * @param conf  The current configuration.
   * @param args  The command line parameters.
   * @return The newly created job.
   * @throws java.io.IOException When setting up the job fails.
   */
  public Job createSubmittableJob(String[] args)
  throws IOException {
    if (!doCommandLine(args)) {
      return null;
    }
    Configuration conf = this.getConf();
    if (!conf.getBoolean(HConstants.REPLICATION_ENABLE_KEY, false)) {
      throw new IOException("Replication needs to be enabled to verify it.");
    }

    // TODO : test whether the thrift server could be connected
    
    conf.set(NAME+".thriftServer", thriftServer);
    conf.set(NAME+".tableName", tableName);
    conf.setLong(NAME+".startTime", startTime);
    conf.setLong(NAME+".endTime", endTime);
    conf.setLong(NAME+".verifyrows", verifyRows);
    conf.setInt(NAME +".sleepToReCompare", sleepToReCompare);
    conf.setInt(NAME + ".versions", versions);
    if (logTable != null) {
      conf.set(NAME + ".logTable", logTable);
    }
    conf.setInt(NAME+".maxErrorLog", maxErrorLog);
    
    if (families != null) {
      conf.set(NAME+".families", families);
    }
    Job job = new Job(conf, NAME + "_" + tableName);
    job.setJarByClass(VerifyReplication98.class);

    Scan scan = new Scan();
    if (startTime != 0) {
      scan.setTimeRange(startTime,
          endTime == 0 ? HConstants.LATEST_TIMESTAMP : endTime);
    }
    if(families != null) {
      String[] fams = families.split(",");
      for(String fam : fams) {
        scan.addFamily(Bytes.toBytes(fam));
      }
    }

    if (startRow != null) {
      scan.setStartRow(Bytes.toBytes(startRow));
    }
    
    if (stopRow != null) {
      scan.setStopRow(Bytes.toBytes(stopRow));
    }

    if (verifyRows != Long.MAX_VALUE) {
      scan.setFilter(new PageFilter(verifyRows));
    }

    if (scanRateLimit > 0) {
      job.getConfiguration().setInt(TableMapper.SCAN_RATE_LIMIT, scanRateLimit);
    }
        
    TableMapReduceUtil.initTableMapperJob(tableName, scan,
        Verifier.class, null, null, job);

    // TODO : connect thrift server of the replication cluster directly,
    //        check whether need security?

    job.setOutputFormatClass(NullOutputFormat.class);
    job.setNumReduceTasks(0);
    return job;
  }

  private boolean doCommandLine(final String[] args) {
    if (args.length < 2) {
      printUsage(null);
      return false;
    }
    try {
      for (int i = 0; i < args.length; i++) {
        String cmd = args[i];
        if (cmd.equals("-h") || cmd.startsWith("--h")) {
          printUsage(null);
          return false;
        }

        final String startTimeArgKey = "--starttime=";
        if (cmd.startsWith(startTimeArgKey)) {
          startTime = Long.parseLong(cmd.substring(startTimeArgKey.length()));
          continue;
        }

        final String endTimeArgKey = "--endtime=";
        if (cmd.startsWith(endTimeArgKey)) {
          endTime = Long.parseLong(cmd.substring(endTimeArgKey.length()));
          continue;
        }

        final String startRowArgKey = "--startrow=";
        if (cmd.startsWith(startRowArgKey)) {
          startRow = cmd.substring(startRowArgKey.length());
          continue;
        }
        
        final String stopRowArgKey = "--stoprow=";
        if (cmd.startsWith(stopRowArgKey)) {
          stopRow = cmd.substring(stopRowArgKey.length());
          continue;
        }
        
        final String familiesArgKey = "--families=";
        if (cmd.startsWith(familiesArgKey)) {
          families = cmd.substring(familiesArgKey.length());
          continue;
        }
        
        final String versionsArgKey = "--versions=";
        if (cmd.startsWith(versionsArgKey)) {
          versions = Integer.parseInt(cmd.substring(versionsArgKey.length()));
          continue;
        }

        final String scanRateArgKey = "--scanrate=";
        if (cmd.startsWith(scanRateArgKey)) {
          scanRateLimit = Integer.parseInt(cmd.substring(scanRateArgKey.length()));
          continue;
        }
        
        final String verifyRowKey = "--verifyrows=";
        if (cmd.startsWith(verifyRowKey)) {
          verifyRows = Long.parseLong(cmd.substring(verifyRowKey.length()));
          continue;
        }
        
        final String sleepToReCompareKey = "--recomparesleep=";
        if (cmd.startsWith(sleepToReCompareKey)) {
          sleepToReCompare = Integer.parseInt(cmd.substring(sleepToReCompareKey.length()));
          continue;
        }

        final String logTableKey = "--logtable=";
        if (cmd.startsWith(logTableKey)) {
          logTable = cmd.substring(logTableKey.length());
          continue;
        }

        final String maxErrorLogKey = "--maxerrorlog=";
        if (cmd.startsWith(maxErrorLogKey)) {
          maxErrorLog = Integer.parseInt(cmd.substring(maxErrorLogKey.length()));
          continue;
        }

        if (i == args.length-2) {
          thriftServer = cmd;
        }

        if (i == args.length-1) {
          tableName = cmd;
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      printUsage("Can't start because " + e.getMessage());
      return false;
    }
    return true;
  }

  /*
   * @param errorMsg Error message.  Can be null.
   */
  private static void printUsage(final String errorMsg) {
    if (errorMsg != null && errorMsg.length() > 0) {
      System.err.println("ERROR: " + errorMsg);
    }
    System.err.println("Usage: verifyrep [--starttime=X]" +
        " [--stoptime=Y] [--families=A] <peerid> <tablename>");
    System.err.println();
    System.err.println("Options:");
    System.err.println(" starttime    beginning of the time range");
    System.err.println("              without endtime means from starttime to forever");
    System.err.println(" stoptime     end of the time range");
    System.err.println(" startrow     beginning of row");
    System.err.println(" stoprow      end of the row");
    System.err.println(" families     comma-separated list of families to copy");
    System.err.println(" scanrate     the scan rate limit: rows per second for each region.");
    System.err.println(" versions     number of cell versions to verify.");
    System.err.println(" verifyrows   number of rows each region in source table to verify.");
    System.err.println(" recomparesleep   milliseconds to sleep before recompare row.");
    System.err.println(" logtable     table to log the errors/differences (with column family C).");
    System.err.println(" maxerrorlog  max number of errors to log for each region.");
    System.err.println();
    System.err.println("Args:");
    System.err.println(" thriftserver thrift server address to schedule a scanner");
    System.err.println(" tablename    name of the table to verify");
    System.err.println();
    System.err.println("Examples:");
    System.err.println(" To verify the data replicated from TestTable for a 1 hour window with peer #5 ");
    System.err.println(" $ bin/hbase " +
        "org.apache.hadoop.hbase.mapreduce.replication.VerifyReplication98" +
        " --starttime=1265875194289 --stoptime=1265878794289 127.0.0.1:15000 TestTable ");
  }

  /**
   * Main entry point.
   *
   * @param args  The command line parameters.
   * @throws Exception When running the job fails.
   */
  public static void main(String[] args) throws Exception {
    int ret = ToolRunner.run(new VerifyReplication98(HBaseConfiguration.create()), args);
    System.exit(ret);
  }

  @Override
  public int run(String[] args) throws Exception {
    String[] otherArgs = new GenericOptionsParser(getConf(), args).getRemainingArgs();
    Job job = createSubmittableJob(otherArgs);
    if (job == null) return 1;
    return job.waitForCompletion(true) ? 0 : 1;
  }
}
