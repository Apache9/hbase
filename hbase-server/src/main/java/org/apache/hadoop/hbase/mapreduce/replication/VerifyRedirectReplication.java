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
package org.apache.hadoop.hbase.mapreduce.replication;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.HConnectable;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
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
import org.apache.hadoop.hbase.mapreduce.TableSnapshotInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationFactory;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.ReplicationPeers;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * This map-only job compares the data from a local table with a remote one which has different
 * table name. Every cell is compared and must have exactly the same keys (even timestamp)
 * as well as same value. It is possible to restrict the job by time range and
 * families. The peer id that's provided must match the one given when the
 * replication stream was setup.
 * <p>
 * Two counters are provided, Verifier.Counters.GOODROWS and BADROWS. The reason
 * for a why a row is different is shown in the map's log.
 */
public class VerifyRedirectReplication extends Configured implements Tool {

  private static final Log LOG =
      LogFactory.getLog(VerifyRedirectReplication.class);

  public final static String NAME = "verifyrep";
  long startTime = 0;
  long endTime = Long.MAX_VALUE;
  int versions = Integer.MAX_VALUE;
  String tableName = null;
  String peerTableName = null;
  String families = null;
  String peerId = null;
  String startRow = null;
  String stopRow = null;
  int scanRateLimit = -1;
  long verifyRows = Long.MAX_VALUE;
  String logTable = null;
  int sleepToReCompare = 0;
  boolean repairPeer = false;

  /**
   * Map-only comparator for 2 tables
   */
  public static class Verifier
      extends TableMapper<ImmutableBytesWritable, Put> {

    public static enum Counters {
      GOODROWS, BADROWS, ONLY_IN_SOURCE_TABLE_ROWS, ONLY_IN_PEER_TABLE_ROWS, CONTENT_DIFFERENT_ROWS, REPAIR_PEER_ROWS}

    private ResultScanner replicatedScanner;
    private Scan scan;
    private Result currentCompareRowInPeerTable;
    private long st = 0;
    private int scanRateLimit = -1;
    private long rowdone = 0;
    private HTable sourceTable;
    private HTable peerTable;
    private int sleepToReCompare;
    private boolean repairPeer = false;

    private HTable logTable;
    private String peerId;
    private String tableName;
    private long startTime = 0;
    private long endTime = Long.MAX_VALUE;

    @Override
    public void setup(Context context) {
      Configuration conf = context.getConfiguration();
      st = EnvironmentEdgeManager.currentTimeMillis();
      scanRateLimit = conf.getInt(TableMapper.SCAN_RATE_LIMIT, -1);
      sleepToReCompare = conf.getInt(NAME +".sleepToReCompare", 0);
      repairPeer = conf.getBoolean(NAME + ".repairPeer", false);
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
        scan = new Scan();
        scan.setCaching(conf.getInt(TableInputFormat.SCAN_CACHEDROWS, 1));
        startTime = conf.getLong(NAME + ".startTime", 0);
        endTime = conf.getLong(NAME + ".endTime", Long.MAX_VALUE);
        String families = conf.get(NAME + ".families", null);
        long verifyRows = conf.getLong(NAME + ".verifyrows", Long.MAX_VALUE);
        int versions = conf.getInt(NAME + ".versions", Integer.MAX_VALUE);
        if (families != null) {
          String[] fams = families.split(",");
          for (String fam : fams) {
            scan.addFamily(Bytes.toBytes(fam));
          }
        }
        scan.setMaxVersions(versions);
        if (verifyRows != Long.MAX_VALUE) {
          scan.setFilter(new PageFilter(verifyRows));
        }

        peerId = conf.get(NAME + ".peerId");
        tableName = conf.get(NAME + ".tableName");
        String logTableName = conf.get(NAME + ".logTable");
        if (logTableName != null) {
          logTable = new HTable(conf, logTableName);
        }
        sourceTable = new HTable(conf, tableName);

        final InputSplit tableSplit = context.getInputSplit();
        byte[] endRow;
        if (tableSplit instanceof TableSnapshotInputFormat.TableSnapshotRegionSplit) {
          HRegionInfo regionInfo =
              ((TableSnapshotInputFormat.TableSnapshotRegionSplit) tableSplit).getRegionInfo();
          endRow = regionInfo.getEndKey();
        } else {
          endRow = ((TableSplit) tableSplit).getEndRow();
        }
        scan.withStartRow(row.get()).withStopRow(endRow);

        Configuration peerConf = HBaseConfiguration.create(conf);
        String zkClusterKey = conf.get(NAME + ".peerQuorumAddress");
        ZKUtil.applyClusterKeyToConf(peerConf, zkClusterKey);

        scan.setTimeRange(startTime, endTime);
        HConnectionManager.execute(new HConnectable<Void>(conf) {
          @Override
          public Void connect(HConnection conn) throws IOException {
            peerTable = new HTable(peerConf, conf.get(NAME + ".peerTableName"));
            replicatedScanner = peerTable.getScanner(scan);
            return null;
          }
        });
        currentCompareRowInPeerTable = replicatedScanner.next();
      }
      while (true) {
        if (currentCompareRowInPeerTable == null) {
          // reach the region end of peer table, row only in source table
          handleBadRow(context, Counters.ONLY_IN_SOURCE_TABLE_ROWS, value);
          break;
        }
        int rowCmpRet = Bytes.compareTo(value.getRow(), currentCompareRowInPeerTable.getRow());
        if (rowCmpRet == 0) {
          try {
            Result.compareResults(value, currentCompareRowInPeerTable);
            context.getCounter(Counters.GOODROWS).increment(1);
          } catch (Exception e) {
            handleBadRow(context, Counters.CONTENT_DIFFERENT_ROWS, value);
          }
          currentCompareRowInPeerTable = replicatedScanner.next();
          break;
        } else if (rowCmpRet < 0) {
          // row only exists in source table
          handleBadRow(context, Counters.ONLY_IN_SOURCE_TABLE_ROWS, value);
          break;
        } else {
          // row only exists in peer table
          handleBadRow(context, Counters.ONLY_IN_PEER_TABLE_ROWS, currentCompareRowInPeerTable);
          currentCompareRowInPeerTable = replicatedScanner.next();
        }
      }
      rowdone ++;
      TableMapReduceUtil.limitScanRate(scanRateLimit, rowdone,
          EnvironmentEdgeManager.currentTimeMillis() - st);
    }

    boolean allCellTsLessThanEndTime(long endTime, List<Cell> cells) {
      return cells != null && cells.stream().allMatch(c -> c.getTimestamp() < endTime);
    }

    // Return true to indicate results are the same.
    private boolean verifyResultByGet(byte[] row, boolean isScanSnapshot) throws IOException {
      Scan verifyScan =
          new Scan(scan).setAllowPartialResults(true).withStartRow(row).withStopRow(row, true);
      if (isScanSnapshot) {
        verifyScan.setTimeRange(startTime, Long.MAX_VALUE);
      }
      try (ResultScanner srcScanner = sourceTable.getScanner(verifyScan)) {
        try (ResultScanner dstScanner = peerTable.getScanner(verifyScan)) {
          int idx = 0;
          Result r, d;
          List<Cell> srcCells = new ArrayList<>();
          List<Cell> dstCells = new ArrayList<>();
          for (r = srcScanner.next(), d = dstScanner.next(); r != null; r = srcScanner.next()) {
            srcCells = r.listCells();
            // fill the dstCells until its length reach the length of srcCells.
            do {
              if (d == null) return false;
              while (idx < d.rawCells().length && dstCells.size() < srcCells.size()) {
                dstCells.add(d.rawCells()[idx++]);
              }
              if (dstCells.size() == srcCells.size()) {
                break;
              }
              d = dstScanner.next();
              idx = 0;
            } while (d != null && d.mayHaveMoreCellsInRow());

            // The row has a cell whose ts >= endTime. we skip this row for repair.
            if (isScanSnapshot && !(allCellTsLessThanEndTime(endTime, srcCells)
                && allCellTsLessThanEndTime(endTime, dstCells)))
              return true;

            try {
              Result.compareResults(Result.create(srcCells), Result.create(dstCells));
            } catch (Exception e) {
              return false;
            }
            dstCells.clear();
          }
          if (d != null) {
            if (idx < d.rawCells().length) {
              return false;
            }
            d = dstScanner.next();
            if (d != null && !d.isEmpty()) {
              return false;
            }
          }
        }
      }
      return true;
    }

    private void handleBadRow(Context context, Counters counter, Result row)
        throws IOException {
      boolean isResultDiff = true;
      // verify replication by scanning the region server.
      if (sleepToReCompare > 0) {
        Threads.sleep(sleepToReCompare);
        isResultDiff = !verifyResultByGet(row.getRow(), false);
      }

      if (isResultDiff) {
        context.getCounter(counter).increment(1);
        context.getCounter(Counters.BADROWS).increment(1);
        LOG.error(counter.toString() + ", rowKey=" + Bytes.toStringBinary(row.getRow()));

        // repair peer table
        repair(context, row.getRow(), counter);

        // log the row into HBase Table.
        recordError(row.getRow(), counter);
      } else {
        context.getCounter(Counters.GOODROWS).increment(1);
      }
    }

    private void repair(Context context, byte[] row, Counters counter) throws IOException {
      if (repairPeer && !Counters.ONLY_IN_PEER_TABLE_ROWS.equals(counter)) {
        // use allow partial for avoiding OOM here.
        Scan repairScan = new Scan(scan).withStartRow(row).withStopRow(row, true).setRaw(true)
            .setAllowPartialResults(true);
        boolean isEmptyRow = true;
        try (ResultScanner rs = sourceTable.getScanner(repairScan)) {
          for (Result r = rs.next(); r != null; r = rs.next()) {
            if (!r.isEmpty()) {
              isEmptyRow = false;
              Put put = new Put(row);
              for (Cell kv : r.rawCells()) {
                put.add(kv);
              }
              peerTable.put(put);
            }
          }
        }
        if (!isEmptyRow) {
          context.getCounter(Counters.REPAIR_PEER_ROWS).increment(1);
        }
      }
    }

    private void recordError(byte[] row, Counters type) throws IOException {
      if (logTable != null) {
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
        logTable.put(put);
      }
    }

    @Override
    protected void cleanup(Context context) {
      if (replicatedScanner != null) {
        try {
          while (currentCompareRowInPeerTable != null) {
            handleBadRow(context, Counters.ONLY_IN_PEER_TABLE_ROWS,
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

      if (logTable != null) {
        try {
          logTable.close();
        } catch (IOException e) {
          LOG.error("close logTable failed.", e);
        }
      }
    }
  }

  private String getPeerQuorumAddress(final Configuration conf) throws IOException {
    ZooKeeperWatcher localZKW = null;
    try {
      localZKW = new ZooKeeperWatcher(conf, "VerifyReplication",
          new Abortable() {
            @Override public void abort(String why, Throwable e) {}
            @Override public boolean isAborted() {return false;}
          });

      ReplicationPeers rp = ReplicationFactory.getReplicationPeers(localZKW, conf, localZKW);
      rp.init();

      Pair<ReplicationPeerConfig, Configuration> pair = rp.getPeerConf(peerId);
      if (pair == null) {
        throw new IOException("Couldn't get peer conf!");
      }
      Configuration peerConf = rp.getPeerConf(peerId).getSecond();
      return ZKUtil.getZooKeeperClusterKey(peerConf);
    } catch (ReplicationException e) {
      throw new IOException(
          "An error occured while trying to connect to the remove peer cluster", e);
    } finally {
      if (localZKW != null) {
        localZKW.close();
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
  public Job createSubmittableJob(Configuration conf, String[] args)
      throws IOException {
    if (!doCommandLine(args)) {
      return null;
    }
    if (!conf.getBoolean(HConstants.REPLICATION_ENABLE_KEY,
        HConstants.REPLICATION_ENABLE_DEFAULT)) {
      throw new IOException("Replication needs to be enabled to verify it.");
    }
    conf.set(NAME + ".peerId", peerId);
    conf.set(NAME + ".tableName", tableName);
    conf.set(NAME + ".peerTableName", peerTableName);
    LOG.info("Source cluster table name: " + tableName + " and peer cluster table name " +
        peerTableName);
    conf.setLong(NAME + ".startTime", startTime);
    conf.setLong(NAME + ".endTime", endTime);
    conf.setLong(NAME + ".verifyrows", verifyRows);
    conf.setInt(NAME + ".sleepToReCompare", sleepToReCompare);
    conf.setInt(NAME + ".versions", versions);
    if (families != null) {
      conf.set(NAME+".families", families);
    }
    if (logTable != null) {
      conf.set(NAME + ".logTable", logTable);
    }
    conf.setBoolean(NAME + ".repairPeer", repairPeer);

    String peerQuorumAddress = getPeerQuorumAddress(conf);
    conf.set(NAME + ".peerQuorumAddress", peerQuorumAddress);
    LOG.info("Peer Quorum Address: " + peerQuorumAddress);

    Job job = new Job(conf, NAME + "_" + tableName + "_" + peerId);
    job.setJarByClass(VerifyReplication.class);

    Scan scan = new Scan();
    scan.setMaxVersions(versions);
    if (families != null) {
      String[] fams = families.split(",");
      for (String fam : fams) {
        scan.addFamily(Bytes.toBytes(fam));
      }
    }
    if (startRow != null) {
      scan.withStartRow(Bytes.toBytes(startRow));
    }

    if (stopRow != null) {
      scan.withStopRow(Bytes.toBytes(stopRow));
    }

    if (verifyRows != Long.MAX_VALUE) {
      scan.setFilter(new PageFilter(verifyRows));
    }

    if (scanRateLimit > 0) {
      job.getConfiguration().setInt(TableMapper.SCAN_RATE_LIMIT, scanRateLimit);
    }

    scan.setTimeRange(startTime, endTime);
    TableMapReduceUtil.initTableMapperJob(tableName, scan, Verifier.class, null, null, job);

    // Obtain the auth token from peer cluster
    TableMapReduceUtil.initCredentialsForCluster(job, peerQuorumAddress);

    job.setOutputFormatClass(NullOutputFormat.class);
    job.setNumReduceTasks(0);
    return job;
  }

  @VisibleForTesting
  public boolean doCommandLine(final String[] args) {
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

        final String versionsArgKey = "--versions=";
        if (cmd.startsWith(versionsArgKey)) {
          versions = Integer.parseInt(cmd.substring(versionsArgKey.length()));
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

        final String verifyRowKey = "--verifyrows";
        if (cmd.startsWith(verifyRowKey)) {
          verifyRows = Long.parseLong(cmd.substring(verifyRowKey.length()));
          continue;
        }

        final String logTableKey = "--logtable=";
        if (cmd.startsWith(logTableKey)) {
          logTable = cmd.substring(logTableKey.length());
          continue;
        }

        final String peerTableKey = "--peerTable=";
        if (cmd.startsWith(peerTableKey)) {
          peerTableName = cmd.substring(peerTableKey.length());
          continue;
        }

        final String sleepToReCompareKey = "--recomparesleep=";
        if (cmd.startsWith(sleepToReCompareKey)) {
          sleepToReCompare = Integer.parseInt(cmd.substring(sleepToReCompareKey.length()));
          continue;
        }

        final String repairPeerKey  = "--repairPeer";
        if (cmd.startsWith(repairPeerKey)) {
          repairPeer = true;
          continue;
        }

        if (cmd.startsWith("--")) {
          printUsage("Invalid argument '" + cmd + "'");
          return false;
        }

        if (i == args.length-2) {
          peerId = cmd;
        }

        if (i == args.length-1) {
          tableName = cmd;
        }
      }

      if (peerTableName == null) {
        printUsage("peerTable cannot be null");
        return false;
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
    System.err.println("Usage: verifyrep [--starttime=X] \n"
        + " [--endtime=Y] [--families=A] [--logtable=TB] [--repairPeer] \n"
        + " <peerid> <tablename>");
    System.err.println();
    System.err.println("Options:");
    System.err.println(" starttime              beginning of the time range");
    System.err.println("                        without endtime means from starttime to forever");
    System.err.println(" endtime                end of the time range");
    System.err.println(" startrow               beginning of row");
    System.err.println(" stoprow                end of the row");
    System.err.println(" versions               number of cell versions to verify");
    System.err.println(" families               comma-separated list of families to copy");
    System.err.println(" scanrate               the scan rate limit: rows per second for each region.");
    System.err.println(" verifyrows             number of rows each region in source table to verify.");
    System.err.println(" recomparesleep         milliseconds to sleep before recompare row.");
    System.err.println(" logtable               table to log the errors/differences (with column family A).");
    System.err.println(" repairPeer             repair the peer cluster's data by copy raw rows from source cluster.");
    System.err.println(" peerTable              the table name in peer cluster.");
    System.err.println();
    System.err.println("Args:");
    System.err.println(" peerid                 Id of the peer used for verification, must match the one given for replication");
    System.err.println(" tablename              Name of the table to verify in source cluster");
    System.err.println();
    System.err.println("Examples:");
    System.err.println(" To verify the data replicated from TestTable for a 1 hour window with peer #5 ");
    System.err.println(" $ bin/hbase " +
        "org.apache.hadoop.hbase.mapreduce.replication.VerifyRedirectReplication" +
        " --starttime=1265875194289 --endtime=1265878794289 5 TestTable ");
  }

  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = this.getConf();
    Job job = createSubmittableJob(conf, args);
    if (job != null) {
      return job.waitForCompletion(true) ? 0 : 1;
    }
    return 1;
  }

  /**
   * Main entry point.
   *
   * @param args  The command line parameters.
   * @throws Exception When running the job fails.
   */
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(HBaseConfiguration.create(), new VerifyReplication(), args);
    System.exit(res);
  }
}