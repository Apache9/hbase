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
package org.apache.hadoop.hbase.mapreduce;

import com.google.common.collect.Lists;
import com.xiaomi.infra.base.nameservice.NameService;
import com.xiaomi.infra.base.nameservice.NameServiceEntry;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.TableSnapshotScanner;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.snapshot.RestoreSnapshotHelper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VerifySnapshot {
  private static final Logger LOG = LoggerFactory.getLogger(VerifySnapshot.class);
  private final static String SNAPSHOT = "hbase.verify.snapshot.uri";
  private final static String RESTORE_DIR = "hbase.restore.dir";
  private final static String RESTORE_DIR_COMPARE = "hbase.compare.restore.dir";

  private final static String COMPARE_SNAPSHOT = "hbase.verify.compare.snapshot.uri";
  private final static String END_TIME = "hbase.scan.endTime";

  private final static String COMPARE_CLUSTER_HDFS = "hbase.compare.cluster.hdfs";
  private final static String COMPARE_CLUSTER_HBASE_ROOT = "hbase.compare.cluster.hbase.root";
  final static String UT_FLAG = "hbase.UT.flag";
  final static String SNAPSHOT_NAME = "hbase.verify.snapshot.name";
  final static String COMPARE_SNAPSHOT_NAME = "hbase.verify.compare.snapshot.name";

  static class SnapshotVerifier extends TableMapper<ImmutableBytesWritable, Put> {
    private ResultScanner comparisonSnapshotScanner;
    private Result currentCompareRow;
    private long endTime;

    public enum Counters {
      GOOD_ROWS, ONLY_IN_SOURCE, ONLY_IN_COMPARISON, CONTENT_DIFFERENT_ROWS
    }

    @Override
    public void setup(Context context) {
      this.endTime = context.getConfiguration().getLong(END_TIME, Long.MAX_VALUE);
    }

    private ResultScanner initialComparisonSnapshotScanner(Context context,
        ImmutableBytesWritable row) throws IOException {
      Configuration conf = context.getConfiguration();
      Scan scan = constructScan(context, row);
      Configuration comparisonConf = getComparisonClusterConf(conf);
      return new TableSnapshotScanner(comparisonConf, FSUtils.getRootDir(comparisonConf),
          new Path(conf.get(COMPARE_CLUSTER_HDFS), conf.get(RESTORE_DIR_COMPARE)),
          comparisonConf.get(COMPARE_SNAPSHOT_NAME), scan, true);
    }

    private Scan constructScan(Context context, ImmutableBytesWritable row) throws IOException {
      Configuration conf = context.getConfiguration();
      Scan scan = new Scan();
      scan.setCaching(conf.getInt(TableInputFormat.SCAN_CACHEDROWS, 1));
      scan.setTimeRange(0, endTime);
      final InputSplit tableSplit = context.getInputSplit();
      byte[] endRow;
      HRegionInfo regionInfo =
          ((TableSnapshotInputFormat.TableSnapshotRegionSplit) tableSplit).getRegionInfo();
      endRow = regionInfo.getEndKey();
      scan.withStartRow(row.get()).withStopRow(endRow);
      LOG.info("scan scope is from {} to {}", Bytes.toStringBinary(row.get()),
        Bytes.toStringBinary(endRow));
      return scan;
    }

    @Override
    public void map(ImmutableBytesWritable row, final Result value, Context context)
        throws IOException {
      if (comparisonSnapshotScanner == null) {
        comparisonSnapshotScanner = initialComparisonSnapshotScanner(context, row);
        currentCompareRow = comparisonSnapshotScanner.next();
      }
      compareRows(value, context);
    }

    @Override
    protected void cleanup(Context context) {
      if (comparisonSnapshotScanner != null) {
        try{
          Result[] results = comparisonSnapshotScanner.next(100);
          if(results.length > 0) {
            context.getCounter(Counters.ONLY_IN_COMPARISON).increment(results.length);
          }
        } catch (Exception e) {
          LOG.error("fail to scan peer table in cleanup", e);
        } finally {
          comparisonSnapshotScanner.close();
        }
      }
    }

    private void compareRows(Result value, Context context)
        throws IOException {
      while (true) {
        if (currentCompareRow == null) {
          // reach the region end of peer table, row only in source table
          context.getCounter(Counters.ONLY_IN_SOURCE).increment(1);
          break;
        }
        int rowCmpRet = Bytes.compareTo(value.getRow(), currentCompareRow.getRow());
        if (rowCmpRet == 0) {
          try {
            Result.compareResults(value, currentCompareRow);
            context.getCounter(Counters.GOOD_ROWS).increment(1);
          } catch (Exception e) {
            context.getCounter(Counters.CONTENT_DIFFERENT_ROWS).increment(1);
          }
          currentCompareRow = comparisonSnapshotScanner.next();
          break;
        } else if (rowCmpRet < 0) {
          // row only exists in source snapshot
          context.getCounter(Counters.ONLY_IN_SOURCE).increment(1);
          break;
        } else {
          // row only exists in comparison snapshot
          context.getCounter(Counters.ONLY_IN_COMPARISON).increment(1);
          currentCompareRow = comparisonSnapshotScanner.next();
        }
      }
    }
  }

  private static Configuration getComparisonClusterConf(Configuration conf) throws IOException {
    if(conf.getBoolean(UT_FLAG, false)) {
      return conf;
    }
    NameServiceEntry snapshotCompare = NameService.resolve(conf.get(COMPARE_SNAPSHOT));
    Configuration comparisonConf = snapshotCompare.createClusterConf(conf);
    FileSystem.setDefaultUri(comparisonConf, conf.get(COMPARE_CLUSTER_HDFS));
    FSUtils.setRootDir(comparisonConf,
        new Path(conf.get(COMPARE_CLUSTER_HDFS), conf.get(COMPARE_CLUSTER_HBASE_ROOT)));
    comparisonConf.setIfUnset(COMPARE_SNAPSHOT_NAME, snapshotCompare.getResource());
    return comparisonConf;
  }

  private static Configuration getClusterConf(Configuration conf) throws IOException {
    if(conf.getBoolean(UT_FLAG, false)) {
      return conf;
    }
    NameServiceEntry snapshot = NameService.resolve(conf.get(SNAPSHOT));
    Configuration clusterConf = snapshot.createClusterConf(conf);
    clusterConf.setIfUnset(SNAPSHOT_NAME, snapshot.getResource());
    return clusterConf;
  }

  public static Job createSubmittableJob(Configuration conf, String[] args) throws IOException {
    parseConfigs(conf, args);
    Configuration clusterConf = getClusterConf(conf);
    clusterConf.setStrings(MRJobConfig.JOB_NAMENODES, clusterConf.get(RESTORE_DIR),
        clusterConf.get(RESTORE_DIR_COMPARE));
    Job job = Job.getInstance(clusterConf,
      "VerifySnapshot between " + conf.get(SNAPSHOT) + " and " + conf.get(COMPARE_SNAPSHOT));
    Scan scan = new Scan();
    scan.setTimeRange(0, conf.getLong(END_TIME, Long.MAX_VALUE));
    TableMapReduceUtil.initTableSnapshotMapperJob(clusterConf.get(SNAPSHOT_NAME), scan,
      SnapshotVerifier.class, null, null, job, true, new Path(conf.get(RESTORE_DIR)));
    restoreCompareSnapshot(job, conf);
    job.setOutputFormatClass(NullOutputFormat.class);
    job.setNumReduceTasks(0);
    return job;
  }

  private static void restoreCompareSnapshot(Job job, Configuration conf) throws IOException {
    Configuration comparisonConf = getComparisonClusterConf(conf);
    FileSystem fs = FileSystem.get(comparisonConf);
    RestoreSnapshotHelper.copySnapshotForScanner(comparisonConf, fs,
      FSUtils.getRootDir(comparisonConf),
      new Path(conf.get(COMPARE_CLUSTER_HDFS), conf.get(RESTORE_DIR_COMPARE)),
      conf.get(COMPARE_SNAPSHOT_NAME));
    TableMapReduceUtil.initCredentialsForCluster(job,
      ZKUtil.getZooKeeperClusterKey(comparisonConf));
  }

  private static void checkConfig(Configuration conf) {
    List<String> requiredParameters = Lists.newArrayList(SNAPSHOT, COMPARE_SNAPSHOT, END_TIME,
      RESTORE_DIR, RESTORE_DIR_COMPARE, COMPARE_CLUSTER_HDFS, COMPARE_CLUSTER_HBASE_ROOT);
    requiredParameters.stream().forEach(para ->
    {
      if (conf.get(para) == null) {
        printUsageAndExit(para + " should not be null");
      }
    });
  }

  private static void parseConfigs(Configuration conf, String[] args) throws IOException {
    for (int i = 0; i < args.length; i++) {
      String cmd = args[i];
      if (cmd.equals("--snapshot-URI")) {
        conf.set(SNAPSHOT, args[++i]);
      } else if (cmd.equals("--compare-snapshot-URI")) {
        conf.set(COMPARE_SNAPSHOT, args[++i]);
      } else if (cmd.equals("--endTime")) {
        conf.setLong(END_TIME, Long.parseLong(args[++i]));
      } else if (cmd.equals("--restoreDir")) {
        conf.set(RESTORE_DIR, args[++i]);
      } else if (cmd.equals("--compare-restoreDir")) {
        conf.set(RESTORE_DIR_COMPARE, new Path(args[++i], UUID.randomUUID().toString()).toString());
      } else if (cmd.equals("--compare-cluster-hdfs")) {
        conf.set(COMPARE_CLUSTER_HDFS, args[++i]);
      } else if (cmd.equals("--compare-cluster-hbase-root")) {
        conf.set(COMPARE_CLUSTER_HBASE_ROOT, args[++i]);
      } else {
        printUsageAndExit("no such parameter" + cmd);
      }
    }
    checkConfig(conf);
  }

  private static void printUsageAndExit(String cause) {
    System.err.println(cause);
    System.err.println("Usage: ./hbase org.apache.hadoop.hbase.mapreduce.VerifySnapshot");
    System.err.println("--snapshot-URI [required] snapshotName");
    System.err.println("--compare-snapshot-URI [required] the other snapshotName");
    System.err.println("--endTime [required] set the end time of scan");
    System.err.println("--restoreDir [required] the directory to restore snapshot");
    System.err.println("--compare-restoreDir [required] the other directory to restore snapshot");
    System.err.println(
      "--compare-cluster-hdfs [required] the hdfs cluster that store the compare snapshot");
    System.err.println(
      "--compare-cluster-hbase-root [required] the hbase cluster that store the compare snapshot");
    System.err.println("Example: ./hbase org.apache.hadoop.hbase.mapreduce.VerifySnapshot ");
    System.err.println("  --snapshot-URI hbase://cluster/snapshot ");
    System.err.println("  --compare-snapshot-URI hbase://compare-cluster/compare-snapshot ");
    System.err.println("  --restoreDir hdfs://cluster/hbase/tmp-source ");
    System.err.println("  --compare-restoreDir hdfs://compare-cluster/hbase/tmp-sink ");
    System.err.println("  --compare-cluster-hdfs hdfs://compare-cluster ");
    System.err
        .println("  --compare-cluster-hbase-root  hdfs://compare-cluster/hbase/azormicloudsrv-hdd ");
    System.err.println("  --endTime 1547693501000");
    System.exit(1);
  }

  public static void main(String[] args)
      throws IOException, ClassNotFoundException, InterruptedException {
    Configuration conf = HBaseConfiguration.create();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    Job job = createSubmittableJob(conf, otherArgs);
    boolean isJobSuccessful = job.waitForCompletion(true);
    System.exit(isJobSuccessful ? 0 : 1);
  }
}
