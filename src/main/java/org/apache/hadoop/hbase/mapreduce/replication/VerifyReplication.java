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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HConnectionManager.HConnectable;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.replication.ReplicationPeer;
import org.apache.hadoop.hbase.replication.ReplicationZookeeper;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.zookeeper.KeeperException;

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
public class VerifyReplication  extends Configured implements Tool {

  private static final Log LOG =
      LogFactory.getLog(VerifyReplication.class);

  public final static String NAME = "verifyrep";
  
  private long startTime = 0;
  private long endTime = 0;
  private String tableName = null;
  private String families = null;
  private String peerId = null;
  private String startRow = null;
  private String stopRow = null;
  private int scanRateLimit = -1;
  private long verifyRows = Long.MAX_VALUE;
  private long maxErrorLog = Long.MAX_VALUE;
  private String logTable = null;
  private boolean skipWal = false;
  private boolean repair = false;
  private int sleepToReCompare = 0;
      
  public VerifyReplication(Configuration conf) {
    super(conf);
  }
  /**
   * Map-only comparator for 2 tables
   */
  public static class Verifier
      extends TableMapper<ImmutableBytesWritable, Put> {

    public static enum Counters {
      GOODROWS, BADROWS, ONLY_IN_SOURCE_TABLE_ROWS, ONLY_IN_PEER_TABLE_ROWS, CONTENT_DIFFERENT_ROWS}

    private ResultScanner replicatedScanner;
    private Result currentCompareRowInPeerTable;

    private long st = 0;
    private int scanRateLimit = -1;
    private long rowdone = 0;
    private String peerId;
    private String tableName;
    private HTable sourceTable;
    private HTable peerTable;
    private HTable logTable;
    private boolean skipWal;
    private boolean repair;
    private long maxErrorLog;
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
      if (replicatedScanner == null) {
        Configuration conf = context.getConfiguration();
        final Scan scan = new Scan();
        scan.setCaching(conf.getInt(TableInputFormat.SCAN_CACHEDROWS, 1));
        long startTime = conf.getLong(NAME + ".startTime", 0);
        long endTime = conf.getLong(NAME + ".endTime", 0);
        String families = conf.get(NAME + ".families", null);
        long verifyRows = conf.getLong(NAME + ".verifyrows", Long.MAX_VALUE);
        if(families != null) {
          String[] fams = families.split(",");
          for(String fam : fams) {
            scan.addFamily(Bytes.toBytes(fam));
          }
        }
        if (startTime != 0) {
          scan.setTimeRange(startTime,
              endTime == 0 ? HConstants.LATEST_TIMESTAMP : endTime);
        }
        if (verifyRows != Long.MAX_VALUE) {
          scan.setFilter(new PageFilter(verifyRows));
        }

        peerId = conf.get(NAME + ".peerId");
        tableName = conf.get(NAME + ".tableName");
        sourceTable = new HTable(conf, tableName);
        maxErrorLog = conf.getLong(NAME + ".maxErrorLog", Long.MAX_VALUE);
        String logTableName = conf.get(NAME + ".logTable");
        if (logTableName != null) {
          logTable = new HTable(conf, logTableName);
        }
        skipWal = conf.getBoolean(NAME + ".skipWal", false);
        repair = conf.getBoolean(NAME + ".repair", false);

        final TableSplit tableSplit = (TableSplit)(context.getInputSplit());
        peerTable = peerHTable(conf, peerId, tableName);
        scan.setStartRow(value.getRow());
        scan.setStopRow(tableSplit.getEndRow());
        replicatedScanner = peerTable.getScanner(scan);
        currentCompareRowInPeerTable = replicatedScanner.next();
      }
      
      while (true) {
        if (currentCompareRowInPeerTable == null) {
          // reach the region end of peer table, row only in source table
          logFailRowAndIncreaseCounter(context, Counters.ONLY_IN_SOURCE_TABLE_ROWS, value);
          if (repair) {
            // we must repair both side, since the peer side may contains a delete marker
            put(sourceTable, rawGet(peerTable, value.getRow()));
            put(peerTable, rawGet(sourceTable, value.getRow()));
          }
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
            if (repair) {
              put(sourceTable, rawGet(peerTable, value.getRow()));
              put(peerTable, rawGet(sourceTable, value.getRow()));
            }
          }
          currentCompareRowInPeerTable = replicatedScanner.next();
          break;
        } else if (rowCmpRet < 0) {
          // row only exists in source table
          logFailRowAndIncreaseCounter(context, Counters.ONLY_IN_SOURCE_TABLE_ROWS, value);
          if (repair) {
            put(sourceTable, rawGet(peerTable, value.getRow()));
            put(peerTable, rawGet(sourceTable, value.getRow()));
          }
          break;
        } else {
          // row only exists in peer table
          logFailRowAndIncreaseCounter(context, Counters.ONLY_IN_PEER_TABLE_ROWS,
            currentCompareRowInPeerTable);
          if (repair) {
            put(sourceTable, rawGet(peerTable, currentCompareRowInPeerTable.getRow()));
            put(peerTable, rawGet(sourceTable, currentCompareRowInPeerTable.getRow()));
          }
          currentCompareRowInPeerTable = replicatedScanner.next();
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
        Result peerResult = peerTable.get(new Get(row.getRow()));
        try {
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
        byte[] peerIdBytes = peerId.getBytes();
        byte[] tableNameBytes = tableName.getBytes();
        // rowkey format: [salt][peerId-length][peerId][table-name-length][table-name][row-len][row]
        int bufflen = 1 + 2 + peerIdBytes.length + 2 + tableNameBytes.length + 2 + row.length;
        ByteBuffer buff = ByteBuffer.allocate(bufflen);
        buff.put((byte) (Bytes.hashCode(row) % 256)); // append salt
        buff.putShort((short) peerIdBytes.length);
        buff.put(peerIdBytes);
        buff.putShort((short) tableNameBytes.length);
        buff.put(tableNameBytes);
        buff.putShort((short) row.length);
        buff.put(row);
        Put put = new Put(buff.array());
        put.add("A".getBytes(), "e".getBytes(), type.name().getBytes());
        if (skipWal) {
          put.setDurability(Durability.SKIP_WAL);
        }
        logTable.put(put);
        ++errors;
      }
    }

    protected void cleanup(Context context) {
      if (replicatedScanner != null) {
        try {
          long verifyRows = context.getConfiguration().getLong(NAME + ".verifyrows", Long.MAX_VALUE);
          // page filter may return more rows since the region border may not be aligned
          // with the source table
          while (currentCompareRowInPeerTable != null && rowdone++ < verifyRows) {
            logFailRowAndIncreaseCounter(context, Counters.ONLY_IN_PEER_TABLE_ROWS,
              currentCompareRowInPeerTable);
            if (repair) {
              put(sourceTable, rawGet(peerTable, currentCompareRowInPeerTable.getRow()));
              put(peerTable, rawGet(sourceTable, currentCompareRowInPeerTable.getRow()));
            }
            currentCompareRowInPeerTable = replicatedScanner.next();
          }
        } catch (Exception e) {
          LOG.error("fail to scan peer table in cleanup", e);
        } finally {
          replicatedScanner.close();
          replicatedScanner = null;
        }
      }
      
      if (peerTable != null) {
        try {
          peerTable.close();
        } catch (IOException e) {
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

  public static HTable peerHTable(final Configuration conf, final String peerId,
      final String tableName) throws IOException {
    final HTable htable[] = new HTable[1];
    HConnectionManager.execute(new HConnectionManager.HConnectable<Void>(conf) {
      @Override
      public Void connect(HConnection conn) throws IOException {
        try {
          ReplicationZookeeper zk = new ReplicationZookeeper(conn, conf,
              conn.getZooKeeperWatcher());
          ReplicationPeer peer = zk.getPeer(peerId);
          HTable peerTable = new HTable(peer.getConfiguration(), tableName);
          htable[0] = peerTable;
        } catch (KeeperException e) {
          throw new IOException("Got a ZK exception", e);
        }
        return null;
      }
    });
    return htable[0];
  }

  public static Result rawGet(HTable table, byte[] row) throws IOException {
    Scan scan = new Scan();
    scan.setRaw(true);
    scan.setStartRow(row);
    scan.setStopRow(row);
    scan.setCaching(1);
    scan.setCacheBlocks(false);
    scan.setMaxVersions(Integer.MAX_VALUE);
    ResultScanner scanner = table.getScanner(scan);
    try {
      return scanner.next();
    } finally {
      scanner.close();
    }
  }

  public static void put(HTable table, Result result) throws IOException {
    if (result != null && !result.isEmpty()) {
      Put put = new Put(result.getRow());
      for (KeyValue kv : result.raw()) {
        put.add(kv);
      }
      table.put(put);
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
    HConnectionManager.execute(new HConnectable<Void>(conf) {
      @Override
      public Void connect(HConnection conn) throws IOException {
        try {
          ReplicationZookeeper zk = new ReplicationZookeeper(conn, conf,
              conn.getZooKeeperWatcher());
          // Just verifying it we can connect
          ReplicationPeer peer = zk.getPeer(peerId);
          if (peer == null) {
            throw new IOException("Couldn't get access to the slave cluster," +
                "please see the log");
          }
        } catch (KeeperException ex) {
          throw new IOException("Couldn't get access to the slave cluster" +
              " because: ", ex);
        }
        return null;
      }
    });
    conf.set(NAME+".peerId", peerId);
    conf.set(NAME+".tableName", tableName);
    conf.setLong(NAME+".startTime", startTime);
    conf.setLong(NAME+".endTime", endTime);
    conf.setLong(NAME+".verifyrows", verifyRows);
    conf.setInt(NAME +".sleepToReCompare", sleepToReCompare);
    if (logTable != null) {
      conf.set(NAME + ".logTable", logTable);
    }
    conf.setBoolean(NAME + ".skipWal", skipWal);
    conf.setLong(NAME + ".maxErrorLog", maxErrorLog);
    conf.setBoolean(NAME + ".repair", repair);
    
    if (families != null) {
      conf.set(NAME+".families", families);
    }
    Job job = new Job(conf, NAME + "_" + tableName);
    job.setJarByClass(VerifyReplication.class);

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

    /**
     * Obtain the auth token from peer cluster for job before start map.
     */
    if (User.isHBaseSecurityEnabled(job.getConfiguration())) {
      try {
        HConnection connection = HConnectionManager.getConnection(conf);
        ReplicationZookeeper zk = new ReplicationZookeeper(connection, conf,
            connection.getZooKeeperWatcher());
        ReplicationPeer peer = zk.getPeer(conf.get(NAME + ".peerId"));
        User.getCurrent().obtainAuthTokenForJob(peer.getConfiguration(), job);
      } catch (InterruptedException e) {
        throw new IOException("Got an InterruptedException", e);
      } catch (KeeperException e) {
        throw new IOException("Got a ZK exception", e);
      }
    }

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

        final String skipWalKey = "--skipwal";
        if (cmd.equals(skipWalKey)) {
          skipWal = true;
          continue;
        }

        final String maxErrorLogKey = "--maxerrorlog=";
        if (cmd.startsWith(maxErrorLogKey)) {
          maxErrorLog = Long.parseLong(cmd.substring(maxErrorLogKey.length()));
          continue;
        }

        final String repairKey = "--repair";
        if (cmd.equals(repairKey)) {
          repair = true;
          continue;
        }

        if (i == args.length-2) {
          peerId = cmd;
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
    System.err.println(" verifyrows   number of rows each region in source table to verify.");
    System.err.println(" recomparesleep   milliseconds to sleep before recompare row.");
    System.err.println(" logtable     table to log the errors/differences (with column family C).");
    System.err.println(" skipwal      skip writing WAL of log table.");
    System.err.println(" maxerrorlog  max number of errors to log for each region.");
    System.err.println(" repair       repair the data by copy rows.");
    System.err.println();
    System.err.println("Args:");
    System.err.println(" peerid       Id of the peer used for verification, must match the one given for replication");
    System.err.println(" tablename    Name of the table to verify");
    System.err.println();
    System.err.println("Examples:");
    System.err.println(" To verify the data replicated from TestTable for a 1 hour window with peer #5 ");
    System.err.println(" $ bin/hbase " +
        "org.apache.hadoop.hbase.mapreduce.replication.VerifyReplication" +
        " --starttime=1265875194289 --stoptime=1265878794289 5 TestTable ");
  }

  /**
   * Main entry point.
   *
   * @param args  The command line parameters.
   * @throws Exception When running the job fails.
   */
  public static void main(String[] args) throws Exception {
    int ret = ToolRunner.run(new VerifyReplication(HBaseConfiguration.create()), args);
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
