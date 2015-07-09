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
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.replication.VerifyReplication;
import org.apache.hadoop.hbase.throughput.ThroughputLimiter;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * HBase Replication verification tool, which monitoring the difference
 * of the replication with cooperation of VerifyReplication tool.
 */
public final class ReplicationVerifier implements Tool {
  private static final Log LOG = LogFactory.getLog(ReplicationVerifier.class);
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
    long alertTime = conf.getInt("hbase.replication.verification.alerttime", 6 * 3600 * 1000);
    boolean repair = conf.getBoolean("hbase.replication.verification.repair", false);
    int workers = conf.getInt("hbase.replication.verification.workers", 10);
    int rate = conf.getInt("hbase.replication.verification.rate", 1000);
    int round = -1;
    boolean clearFalseAlarm = false;

    // Process command line args
    for (int i = 0; i < args.length; i++) {
      String cmd = args[i];

      if (cmd.startsWith("-")) {
        if (cmd.equals("-help")) {
          // user asked for help, print the help and quit.
          printUsageAndExit();
        } else if (cmd.equals("-logtable")) {
          i++;

          if (i == args.length) {
            System.err.println("-logtable needs a sting value argument.");
            printUsageAndExit();
          }

          logTableName = args[i];
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
        } else if (cmd.equals("-repair")) {
          repair = true;
        } else if (cmd.equals("-clear")) {
          clearFalseAlarm = true;
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

    HTable logTable = new HTable(conf, logTableName);
    ThroughputLimiter rateLimiter = new ThroughputLimiter(rate);
    BlockingQueue<Runnable> taskQueue = new ArrayBlockingQueue<Runnable>(1000);
    ExecutorService executorService =
        new ThreadPoolExecutor(1, workers, 30, TimeUnit.SECONDS, taskQueue);
    BlockingQueue<Row> logTableMutations = new ArrayBlockingQueue<Row>(10000);
    ReplicationLogTableUpdateTask logTableUpdateTask =
        new ReplicationLogTableUpdateTask(logTable, logTableMutations);
    Thread logTableUpdator = new Thread(logTableUpdateTask, "logTableUpdator");
    logTableUpdator.start();
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
            executorService.submit(new ReplicationCheckingTask(current, alertTime, repair,
                clearFalseAlarm, falseAlarms, alerts, logTableMutations));
            break;
          } catch (RejectedExecutionException e) {
            Thread.sleep(100);
          }
        }

        rateLimiter.tryAcquire(1, 1, TimeUnit.HOURS);
        current = scanner.next();
      }

      while (taskQueue.size() > 0) {
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
    executorService.awaitTermination(1, TimeUnit.MINUTES);
    logTableUpdateTask.stop();

    return 0;
  }

  private class ReplicationLogTableUpdateTask implements Runnable {
    private BlockingQueue<Row> mutations;
    private HTable logTable;
    private volatile boolean stopping = false;

    private ReplicationLogTableUpdateTask(HTable logTable, BlockingQueue<Row> mutations) {
      this.logTable = logTable;
      this.mutations = mutations;
    }

    public void stop() {
      this.stopping = true;
    }

    @Override public void run() {
      while (true) {
        List<Row> m = new ArrayList<Row>();
        mutations.drainTo(m, 1000);
        if (!m.isEmpty()) {
          try {
            logTable.batch(m);
          } catch (Exception e) {
            // just discard it
            LOG.error("Failed to write to log table", e);
          }
        } else {
          if (stopping) {
            break;
          }
          try {
            Thread.sleep(1000);
          } catch (InterruptedException e) {
            // ignore
          }
        }
      }
    }
  }

  private class ReplicationCheckingTask implements Runnable {
    private Result record;
    private long alertTime;
    private boolean repair;
    private boolean clearFalseAlarms;
    private AtomicLong falseAlarms;
    private AtomicLong alerts;
    private BlockingQueue<Row> logTableMutations;

    private ReplicationCheckingTask(Result record, long alertTime, boolean repair,
        boolean clearFalseAlarm, AtomicLong falseAlarms, AtomicLong alerts,
        BlockingQueue<Row> logTableMutations) {
      this.record = record;
      this.alertTime = alertTime;
      this.repair = repair;
      this.clearFalseAlarms = clearFalseAlarm;
      this.falseAlarms = falseAlarms;
      this.alerts = alerts;
      this.logTableMutations = logTableMutations;
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

        final Get get = new Get(row);
        Result sourceResult = sourceHTable(tableName).get(get);
        Result peerResult = peerHTable(peerId, tableName).get(get);

        VerifyReplication.Verifier.Counters compareResult;
        if (sourceResult == null && peerResult != null) {
          compareResult = VerifyReplication.Verifier.Counters.ONLY_IN_PEER_TABLE_ROWS;
        } else if (sourceResult != null && peerResult == null) {
          compareResult = VerifyReplication.Verifier.Counters.ONLY_IN_SOURCE_TABLE_ROWS;
        } else if (sourceResult != null && peerResult != null) {
          try {
            Result.compareResults(sourceResult, peerResult);
            compareResult = VerifyReplication.Verifier.Counters.GOODROWS;
          } catch (Exception e) {
            compareResult = VerifyReplication.Verifier.Counters.CONTENT_DIFFERENT_ROWS;
          }
        } else {
          compareResult = VerifyReplication.Verifier.Counters.GOODROWS;
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
        case GOODROWS:
          falseAlarms.incrementAndGet();
          if (clearFalseAlarms) {
            Delete delete = new Delete(record.getRow());
            delete.setDurability(Durability.SKIP_WAL);
            logTableMutations.put(delete);
          }
          break;
        case ONLY_IN_PEER_TABLE_ROWS:
        case CONTENT_DIFFERENT_ROWS:
          alert = true;
        case ONLY_IN_SOURCE_TABLE_ROWS:
          if (repair) {
            Result sourceRawResult = VerifyReplication.rawGet(sourceHTable(tableName), row);
            Result peerRawResult = VerifyReplication.rawGet(peerHTable(peerId, tableName), row);
            VerifyReplication.put(sourceHTable(tableName), peerRawResult);
            VerifyReplication.put(peerHTable(peerId, tableName), sourceRawResult);
          }
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
          logTableMutations.put(put);
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

  private HTable sourceHTable(String tableName) throws IOException {
    HTable sourceTable = sourceTables.get().get(tableName);
    if (sourceTable == null) {
      sourceTable = new HTable(conf, tableName);
      sourceTables.get().put(tableName, sourceTable);
    }
    return sourceTable;
  }

  private HTable peerHTable(final String peerId, final String tableName)
      throws IOException {
    final String key = peerId + tableName;
    HTable peerTable = peerTables.get().get(key);
    if (peerTable == null) {
      peerTable = VerifyReplication.peerHTable(conf, peerId, tableName);
      peerTables.get().put(key, peerTable);
    }
    return peerTable;
  }

  private void printUsageAndExit() {
    System.err.printf("Usage: bin/hbase %s [opts]\n", getClass().getName());
    System.err.println(" where [opts] are:");
    System.err.println("   -help          Show this help and exit.");
    System.err.println("   -logtable <S>  Error log table to read from.");
    System.err.println("   -round <N>     Number of round to check.");
    System.err.println("   -workers <N>   Number of worker threads.");
    System.err.println("   -rate <N>      Max scan rate.");
    System.err.println("   -alerttime <N> Min time to trigger an alert.");
    System.err.println("   -repair        Repair mismatched rows.");
    System.err.println("   -clear         Clear false alarms.");
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
