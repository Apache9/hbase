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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.DeleteTracker;
import org.apache.hadoop.hbase.regionserver.ScanDeleteTracker;
import org.apache.hadoop.hbase.replication.ReplicationPeer;
import org.apache.hadoop.hbase.replication.ReplicationZookeeper;
import org.apache.hadoop.hbase.throughput.ThroughputLimiter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.zookeeper.KeeperException;

/**
 * HBase Replication verification tool, which monitoring the difference
 * of the replication with cooperation of VerifyReplication tool.
 */
public final class ReplicationVerifier implements Tool {
  private static final Log LOG = LogFactory.getLog(ReplicationVerifier.class);
  private static enum CompareResult {
    OK, SLAVE_SIDE_MISSING, MASTER_SIDE_MISSING, TWO_SIDE_MISSING
  }
  private Configuration conf;

  @Override public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override public Configuration getConf() {
    return conf;
  }

  @Override public int run(String[] args) throws Exception {
    LOG.info("Start replication verification");
    String logTableName = conf.get("hbase.replication.verification.logtable", "replication-errors");
    long alertTime =  conf.getInt("hbase.replication.verification.alerttime", 6 * 3600 * 1000);
    int workers =  conf.getInt("hbase.replication.verification.workers", 10);
    int rate =  conf.getInt("hbase.replication.verification.rate", 1000);
    int round = -1;
    final HTable logTable = new HTable(conf, logTableName);

    // Process command line args
    for (int i = 0; i < args.length; i++) {
      String cmd = args[i];

      if (cmd.startsWith("-")) {
        if (cmd.equals("-help")) {
          // user asked for help, print the help and quit.
          printUsageAndExit();
        } else if (cmd.equals("-round")) {
          i++;

          if (i == args.length) {
            System.err.println("-round needs a numeric value argument.");
            printUsageAndExit();
          }

          try {
            round = Integer.parseInt(args[i]);
          } catch (NumberFormatException e) {
            System.err.println("-round needs a numeric value argument.");
            printUsageAndExit();
          }
        } else if (cmd.equals("-alerttime")) {
          i++;

          if (i == args.length) {
            System.err.println("-alerttime needs a numeric value argument.");
            printUsageAndExit();
          }

          try {
            alertTime = Long.parseLong(args[i]);
          } catch (NumberFormatException e) {
            System.err.println("-alerttime needs a numeric value argument.");
            printUsageAndExit();
          }
        } else if (cmd.equals("-workers")) {
          i++;

          if (i == args.length) {
            System.err.println("-workers needs a numeric value argument.");
            printUsageAndExit();
          }

          try {
            workers = Integer.parseInt(args[i]);
          } catch (NumberFormatException e) {
            System.err.println("-workers needs a numeric value argument.");
            printUsageAndExit();
          }
        } else if (cmd.equals("-rate")) {
          i++;

          if (i == args.length) {
            System.err.println("-rate needs a numeric value argument.");
            printUsageAndExit();
          }

          try {
            rate = Integer.parseInt(args[i]);
          } catch (NumberFormatException e) {
            System.err.println("-rate needs a numeric value argument.");
            printUsageAndExit();
          }
        }
      }
    }

    ThroughputLimiter rateLimiter = new ThroughputLimiter(rate);
    BlockingQueue<Runnable> taskQueue = new ArrayBlockingQueue<Runnable>(1000);
    ExecutorService executorService =
        new ThreadPoolExecutor(1, workers, 30, TimeUnit.SECONDS, taskQueue);
    int i = 0;
    while (true) {
      long checked = 0;
      AtomicLong falseAlarms = new AtomicLong(0);
      AtomicLong alerts = new AtomicLong(0);

      Scan scan = new Scan();
      ResultScanner scanner = logTable.getScanner(scan);
      Result current = scanner.next();

      while (current != null) {
        ++checked;
        while (true) {
          try {
            executorService.submit(
                new ReplicationCheckingTask(current, alertTime, falseAlarms, alerts, logTable));
            break;
          } catch (RejectedExecutionException e) {
            Thread.sleep(100);
          }
        }

        rateLimiter.tryAcquire(1, 1, TimeUnit.HOURS);
        current = scanner.next();
      }

      while(taskQueue.size() > 0) {
        Thread.sleep(1000);
      }
      LOG.info(String.format("Verification finished, checked: %d, alerts: %d, false alarms: %d",
          checked, alerts.get(), falseAlarms.get()));
      if (round < 0 || ++i < round) {
        Thread.sleep(10000); // sleep for 10 seconds
      } else {
        break;
      }
    }
    executorService.shutdown();

    return 0;
  }
  
  private class ReplicationCheckingTask implements Runnable {
    private Result record;
    private long alertTime;
    private AtomicLong falseAlarms;
    private AtomicLong alerts;
    private HTable logTable;

    private ReplicationCheckingTask(Result record, long alertTime,
        AtomicLong falseAlarms, AtomicLong alerts, HTable logTable) {
      this.record = record;
      this.alertTime = alertTime;
      this.falseAlarms = falseAlarms;
      this.alerts = alerts;
      this.logTable = logTable;
    }

    @Override public void run() {
      try {
      ByteBuffer buff = ByteBuffer.wrap(record.getRow());
      buff.get(); // discard salt byte
      byte[] peerIdBytes = new byte[buff.getShort()];
      buff.get(peerIdBytes);
      String peerId = new String(peerIdBytes);
      byte[] tableNameBytes = new byte[buff.getShort()];
      buff.get(tableNameBytes);
      String tableName = new String(tableNameBytes);
      byte[] row = new byte[buff.getShort()];
      buff.get(row);
      Result masterRawResult = masterRawGet(tableName, row);
      Result slaveRawResult = slaveRawGet(peerId, tableName, row);

      Result masterResult = visibleResult(masterRawResult == null ? null : masterRawResult.list());
      Result slaveResult = visibleResult(slaveRawResult == null ? null : slaveRawResult.list());

        CompareResult compareResult;
        try {
          Result.compareResults(masterResult, slaveResult);
          compareResult = CompareResult.OK;
        } catch (Exception e0) {
          Result mergedResult = mergeResults(masterRawResult, slaveRawResult);
          try {
            Result.compareResults(masterResult, mergedResult);
            compareResult = CompareResult.SLAVE_SIDE_MISSING;
          } catch (Exception e1) {
            try {
              Result.compareResults(slaveResult, mergedResult);
              compareResult = CompareResult.MASTER_SIDE_MISSING;
            } catch (Exception e3) {
              compareResult = CompareResult.TWO_SIDE_MISSING;
            }
          }
        }
        // The time of the first check
        KeyValue kvt = record.getColumnLatest("A".getBytes(), "t".getBytes());
        // Count how many checks
        KeyValue kvc = record.getColumnLatest("A".getBytes(), "c".getBytes());
        int counter = kvc == null ? 0 : Integer.parseInt(new String(kvc.getValue()));
        // alert if still not converge in specified time
        boolean alert = kvt != null &&
            EnvironmentEdgeManager.currentTimeMillis() - kvt.getTimestamp() > alertTime;
        switch (compareResult) {
        case OK:
          falseAlarms.incrementAndGet();
          Delete delete = new Delete(record.getRow());
          delete.setDurability(Durability.SKIP_WAL);
          logTable.delete(delete);
          break;
        case MASTER_SIDE_MISSING:
        case TWO_SIDE_MISSING:
          alert = true;
        case SLAVE_SIDE_MISSING:
          Put put = new Put(record.getRow());
          put.setDurability(Durability.SKIP_WAL);
          // A:t is used to record the check counter
          put.add("A".getBytes(), "c".getBytes(), Integer.toString(++counter).getBytes());
          if (kvt == null) {
            // A:t is used record first check time
            put.add("A".getBytes(), "t".getBytes(), new Date().toString().getBytes());
          }
          if (alert) {
            // B:e is used to record triggered alerts
            alerts.incrementAndGet();
            put.add("B".getBytes(), "e".getBytes(), compareResult.name().getBytes());
          }
          logTable.put(put);
        }
      } catch (Exception e) {
        LOG.warn("Runtime error when verify record: " + record, e);
      }
    }
  }
  
  private ThreadLocal<Map<String, HTable>> sourceTables = new ThreadLocal<Map<String, HTable>>() {
    @Override protected Map<String, HTable> initialValue() {
      return new HashMap<String, HTable>();
    }
  };
  private ThreadLocal<Map<String, HTable>> peerTables = new ThreadLocal<Map<String, HTable>>() {
    @Override protected Map<String, HTable> initialValue() {
      return new HashMap<String, HTable>();
    }
  };

  private Result masterRawGet(String tableName, byte[] row) throws IOException {
    HTable sourceTable = sourceTables.get().get(tableName);
    if (sourceTable == null) {
      sourceTable = new HTable(conf, tableName);
      sourceTables.get().put(tableName, sourceTable);
    }
    return rawGet(sourceTable, row);
  }

  private Result slaveRawGet(final String peerId, final String tableName, final byte[] row)
      throws IOException {
    final String key = peerId + tableName;
    HTable peerTable = peerTables.get().get(key);
    if (peerTable == null) {
      HConnectionManager.execute(new HConnectionManager.HConnectable<Void>(conf) {
        @Override
        public Void connect(HConnection conn) throws IOException {
          try {
            ReplicationZookeeper zk = new ReplicationZookeeper(conn, conf,
                conn.getZooKeeperWatcher());
            ReplicationPeer peer = zk.getPeer(peerId);
            HTable peerTable = new HTable(peer.getConfiguration(), tableName);
            peerTables.get().put(key, peerTable);
          } catch (KeeperException e) {
            throw new IOException("Got a ZK exception", e);
          }
          return null;
        }
      });
      peerTable = peerTables.get().get(key);
    }
    return rawGet(peerTable, row);
  }
  
  private Result rawGet(HTable table, byte[] row) throws IOException {
    Scan scan = new Scan();
    scan.setRaw(true);
    scan.setStartRow(row);
    scan.setStopRow(row);
    scan.setCaching(1);
    scan.setCacheBlocks(false);
    ResultScanner scanner = table.getScanner(scan);
    try {
      return scanner.next();
    } finally {
      scanner.close();
    }
  }

  // merge key values from multiple results and get the final visible result
  private Result mergeResults(Result ... rawResults) {
    Set<KeyValue> kvSet = new TreeSet<KeyValue>(KeyValue.COMPARATOR);
    for (Result result : rawResults) {
      if (result != null) {
        kvSet.addAll(result.list());
      }
    }
    return visibleResult(kvSet);
  }

  // Get the visible result
  private Result visibleResult(Iterable<KeyValue> sortedKeyValues) {
    List<KeyValue> kvs = new ArrayList<KeyValue>();
    if (sortedKeyValues != null) {
      byte[] currentFamily = null;
      byte[] qualifier = null;
      ScanDeleteTracker familyDeleteTracker = null;
      for (KeyValue kv : sortedKeyValues) {
        if (currentFamily == null || !Bytes.equals(currentFamily, kv.getFamily())) {
          // switch to next family and clean previous delete tracker
          currentFamily = kv.getFamily();
          familyDeleteTracker = null;
        }
        switch (KeyValue.Type.codeToType(kv.getType())) {
        case DeleteFamily:
        case DeleteColumn:
        case DeleteFamilyVersion:
        case Delete:
          if (familyDeleteTracker == null) {
            familyDeleteTracker = new ScanDeleteTracker();
          }
          familyDeleteTracker.add(kv.getBuffer(), kv.getQualifierOffset(),
              kv.getQualifierLength(), kv.getTimestamp(), kv.getType());
          break;
        case Put:
          boolean visible = true;
          if (familyDeleteTracker != null) {
            DeleteTracker.DeleteResult dr = familyDeleteTracker.isDeleted(kv.getBuffer(),
                kv.getQualifierOffset(), kv.getQualifierLength(), kv.getTimestamp());
            visible = dr == DeleteTracker.DeleteResult.NOT_DELETED;
          }
          if (visible) {
            // keep the most recent version only
            if (qualifier == null || !Bytes.equals(qualifier, kv.getQualifier())) {
              qualifier = kv.getQualifier();
              kvs.add(kv);
            }
          }
        }
      }
    }
    return new Result(kvs);
  }

  private void printUsageAndExit() {
    System.err.printf("Usage: bin/hbase %s [opts]\n", getClass().getName());
    System.err.println(" where [opts] are:");
    System.err.println("   -help          Show this help and exit.");
    System.err.println("   -round <N>     Number of round to check.");
    System.err.println("   -workers <N>   Number of worker threads.");
    System.err.println("   -rate <N>      Max scan rate.");
    System.err.println("   -alerttime <N> Min time to trigger an alert.");
    System.exit(1);
  }

  public static void main(String[] args) {
    Configuration conf = HBaseConfiguration.create();

    conf.setInt("hbase.rpc.timeout",
        conf.getInt("hbase.replication_verification.rpc.timeout", 20000));
    conf.setInt("hbase.client.pause", conf.getInt("hbase.canary.client.pause", 2000));
    conf.setInt("hbase.client.operation.timeout",
        conf.getInt("hbase.replication_verification.client.operation.timeout", 120000));
    conf.setInt("hbase.client.retries.retries",
        conf.getInt("hbase.replication_verification.client.retries.retries", 5));

    int exitCode = 0;
    try {
      exitCode = ToolRunner.run(conf, new ReplicationVerifier(), args);
    } catch (Exception e) {
      LOG.error("Replication verification tool exited with exception. ", e);
    }
    System.exit(exitCode);
  }
}
