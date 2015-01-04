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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HConnectionManager.HConnectable;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
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
    
    @Override
    public void setup(Context context) {
      Configuration conf = context.getConfiguration();
      st = EnvironmentEdgeManager.currentTimeMillis();
      scanRateLimit = conf.getInt(TableMapper.SCAN_RATE_LIMIT, -1);
      LOG.info("The scan rate limit for verify is " + scanRateLimit
          + " rows per second");
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

        final TableSplit tableSplit = (TableSplit)(context.getInputSplit());
        HConnectionManager.execute(new HConnectable<Void>(conf) {
          @Override
          public Void connect(HConnection conn) throws IOException {
            try {
              ReplicationZookeeper zk = new ReplicationZookeeper(conn, conf,
                  conn.getZooKeeperWatcher());
              ReplicationPeer peer = zk.getPeer(conf.get(NAME+".peerId"));
              HTable replicatedTable = new HTable(peer.getConfiguration(),
                  conf.get(NAME+".tableName"));
              scan.setStartRow(value.getRow());
              scan.setStopRow(tableSplit.getEndRow());
              replicatedScanner = replicatedTable.getScanner(scan);
            } catch (KeeperException e) {
              throw new IOException("Got a ZK exception", e);
            }
            return null;
          }
        });
        currentCompareRowInPeerTable = replicatedScanner.next();
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
          currentCompareRowInPeerTable = replicatedScanner.next();
          break;
        } else if (rowCmpRet < 0) {
          // row only exists in source table
          logFailRowAndIncreaseCounter(context, Counters.ONLY_IN_SOURCE_TABLE_ROWS, value);
          break;
        } else {
          // row only exists in peer table
          logFailRowAndIncreaseCounter(context, Counters.ONLY_IN_PEER_TABLE_ROWS,
            currentCompareRowInPeerTable);
          currentCompareRowInPeerTable = replicatedScanner.next();
        }
      }

      rowdone ++;
      TableMapReduceUtil.limitScanRate(scanRateLimit, rowdone,
        EnvironmentEdgeManager.currentTimeMillis() - st);
    }

    private void logFailRowAndIncreaseCounter(Context context, Counters counter, Result row) {
      context.getCounter(counter).increment(1);
      context.getCounter(Counters.BADROWS).increment(1);
      LOG.error(counter.toString() + ", rowkey=" + Bytes.toString(row.getRow()));
    }

    protected void cleanup(Context context) {
      if (replicatedScanner != null) {
        try {
          while (currentCompareRowInPeerTable != null) {
            logFailRowAndIncreaseCounter(context, Counters.ONLY_IN_PEER_TABLE_ROWS,
              currentCompareRowInPeerTable);
            currentCompareRowInPeerTable = replicatedScanner.next();
          }
        } catch (Exception e) {
          LOG.error("fail to scan peer table in cleanup", e);
        } finally {
          replicatedScanner.close();
          replicatedScanner = null;
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
