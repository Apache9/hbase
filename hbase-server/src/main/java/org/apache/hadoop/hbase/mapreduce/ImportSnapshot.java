/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */
package org.apache.hadoop.hbase.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.RegionSplitter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class ImportSnapshot {
  public final static String TABLE_NAME = "import.table.name";
  final static String NAME = "importSnapshot";
  public final static String BULK_OUTPUT_CONF_KEY = "import.bulk.output";
  public final static String SNAPSHOTSCANNER_SPLIT_ALGO = "UniformSplit";
  public final static String SNAPSHOT_RESTORE_PATH = "snapshot.restore.path";
  public final static String SNAPSHOTSCANNER_SPLIT_NUM = "snapshotScanner.split.num";
  public static final String IS_REPARTITION_PROCESS = "hbase.repartition.process";

  public static Job createSubmittableJob(Configuration conf, String[] args) throws IOException {
    String tableName = args[0];
    conf.set(TABLE_NAME, tableName);
    String snapshotName = args[1];
    Job job = new Job(conf, NAME + "_" + tableName);

    // set InputFormat
    String hfileOutPath = conf.get(BULK_OUTPUT_CONF_KEY);
    String restorePath = conf.get(SNAPSHOT_RESTORE_PATH);
    String splitAlgo = SNAPSHOTSCANNER_SPLIT_ALGO;
    int numsplitsPerRegion = conf.getInt(SNAPSHOTSCANNER_SPLIT_NUM, 10);
    conf.setBoolean(IS_REPARTITION_PROCESS, true);

    // make sure we do not miss any data
    Scan rawScan = new Scan();
    rawScan.setCaching(100);
    rawScan.setRaw(true);

    RegionSplitter.SplitAlgorithm algorithm = RegionSplitter.newSplitAlgoInstance(conf, splitAlgo);

    TableMapReduceUtil.initTableSnapshotMapperJob(snapshotName, rawScan,
      Import.KeyValueImporter.class, ImmutableBytesWritable.class, KeyValue.class, job, true,
      new Path(restorePath), algorithm, numsplitsPerRegion);

    // set reducer
    job.setReducerClass(KeyValueSortReducer.class);
    HTable table = new HTable(conf, tableName);
    HFileOutputFormat2.configureIncrementalLoad(job, table);
    Path outputDir = new Path(hfileOutPath);
    FileOutputFormat.setOutputPath(job, outputDir);

    TableMapReduceUtil.addDependencyJars(job.getConfiguration(),
      com.google.common.base.Preconditions.class);

    return job;
  }
}
