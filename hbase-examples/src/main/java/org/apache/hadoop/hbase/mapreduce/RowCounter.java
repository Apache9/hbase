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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
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

public class RowCounter {

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

  public RowCounter(Configuration conf, TableName tableName) {
    this.conf = conf;
    this.tableName = tableName;
  }

  public long runJob() throws Exception {
    // obtain token from namenode.
    conf.setStrings(MRJobConfig.JOB_NAMENODES, FSUtils.getRootDir(conf).toString());

    Job job = Job.getInstance(conf);
    job.setJarByClass(RowCounter.class);

    Scan scan = new Scan();

    String snapshotName = TableMapReduceUtil.initTableSnapshotMapperJob(tableName, scan,
      ScanMapper.class, null, null, job, true);

    try {
      job.setOutputFormatClass(NullOutputFormat.class);
      job.setNumReduceTasks(0);
      if (!job.waitForCompletion(true)) {
        throw new IOException("Failed to wait job finished.");
      }
      return job.getCounters().findCounter(COUNTER.ROW_COUNTER).getValue();
    } finally {
      TableMapReduceUtil.cleanupTableSnapshotMapperJob(job, snapshotName);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    if (args.length < 1) {
      System.err.println("Only " + args.length + " arguments supplied, required: 1");
      System.err.println("Usage: RowCounter <TABLE_NAME>");
      System.exit(-1);
    }

    RowCounter counter = new RowCounter(conf, TableName.valueOf(args[0]));
    long rowCount = counter.runJob();
    System.out.println("Total " + rowCount + "  rows in table " + args[0]);
  }
}
