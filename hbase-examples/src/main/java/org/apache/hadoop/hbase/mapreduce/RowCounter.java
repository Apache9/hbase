/**
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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.RowCounter.ScanMapper.COUNTER;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.yetus.audience.InterfaceAudience;

import java.io.IOException;

@InterfaceAudience.Public
public class RowCounter {

  private static final String TABLE_ARG = "--table=";
  private static final String SNAPSHOT_ARG = "--snapshot=";

  public static class ScanMapper extends TableMapper<ImmutableBytesWritable, Put> {
    public enum COUNTER {
      ROW_COUNTER
    }

    @Override
    public void map(ImmutableBytesWritable row, final Result value, Context context) {
      context.getCounter(COUNTER.ROW_COUNTER).increment(1);
    }
  }

  private Configuration conf;
  private TableName tableName;
  private String snapshotName;

  public RowCounter(Configuration conf, TableName tableName) {
    this.conf = conf;
    this.tableName = tableName;
  }

  public RowCounter(Configuration conf, String snapshotName) {
    this.conf = conf;
    this.snapshotName = snapshotName;
  }

  public long runJob() throws Exception {
    // obtain token from namenode.
    conf.setStrings(MRJobConfig.JOB_NAMENODES, FSUtils.getRootDir(conf).toString());

    Job job = Job.getInstance(conf);
    job.setJarByClass(RowCounter.class);

    Scan scan = new Scan();

    boolean needToCleanSnapshot = false;
    if (this.tableName != null) {
      this.snapshotName = TableMapReduceUtil.initTableSnapshotMapperJob(tableName, scan,
        ScanMapper.class, null, null, job, true);
      needToCleanSnapshot = true;
    } else {
      Path tmpRestoreDir = TableMapReduceUtil.getAndCheckTmpRestoreDir(conf);
      TableMapReduceUtil.initTableSnapshotMapperJob(snapshotName, scan, ScanMapper.class, null,
        null, job, true, tmpRestoreDir);
    }

    try {
      job.setOutputFormatClass(NullOutputFormat.class);
      job.setNumReduceTasks(0);
      if (!job.waitForCompletion(true)) {
        throw new IOException("Failed to wait job finished.");
      }
      return job.getCounters().findCounter(COUNTER.ROW_COUNTER).getValue();
    } finally {
      if (needToCleanSnapshot) {
        TableMapReduceUtil.cleanupTableSnapshotMapperJob(job, snapshotName);
      }
    }
  }

  private static void printUsage() {
    System.err.println("Usage: RowCounter --table=<TABLE_NAME>");
    System.err.println("       RowCounter --snapshot=<snapshot-name>");
    System.exit(-1);
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    if (args.length < 1 || args[0] == null || args[0].length() <= 0) {
      printUsage();
    }

    RowCounter counter = null;
    if (args[0].startsWith(TABLE_ARG)) {
      counter = new RowCounter(conf, TableName.valueOf(args[0].substring(TABLE_ARG.length())));
    } else if (args[0].startsWith(SNAPSHOT_ARG)) {
      counter = new RowCounter(conf, args[0].substring(SNAPSHOT_ARG.length()));
    } else {
      printUsage();
    }
    long rowCount = counter.runJob();
    System.out.println("Total " + rowCount + "  rows in table " + args[0]);
  }
}
