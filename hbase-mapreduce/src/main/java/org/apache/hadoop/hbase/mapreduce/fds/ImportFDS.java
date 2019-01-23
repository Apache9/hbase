/**
 * Copyright The Apache Software Foundation
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
package org.apache.hadoop.hbase.mapreduce.fds;

import com.xiaomi.infra.thirdparty.galaxy.client.authentication.signature.SignAlgorithm;
import com.xiaomi.infra.thirdparty.galaxy.fds.client.exception.GalaxyFDSClientException;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.Import;
import org.apache.hadoop.hbase.mapreduce.KeyValueSortReducer;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;
import org.apache.http.config.SocketConfig;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hbase.mapreduce.fds.FDSInputFormat.FDS_ACCESS_KEY;
import static org.apache.hadoop.hbase.mapreduce.fds.FDSInputFormat.FDS_ACCESS_SECRET;
import static org.apache.hadoop.hbase.mapreduce.fds.FDSInputFormat.FDS_BUCKET_NAME;
import static org.apache.hadoop.hbase.mapreduce.fds.FDSInputFormat.FDS_ENDPOINT;
import static org.apache.hadoop.hbase.mapreduce.fds.FDSInputFormat.FDS_START_TIME;
import static org.apache.hadoop.hbase.mapreduce.fds.FDSInputFormat.FDS_STOP_TIME;
import static org.apache.hadoop.hbase.mapreduce.Import.FILTER_CLASS_CONF_KEY;
import static org.apache.hadoop.hbase.mapreduce.ImportSnapshot.BULK_OUTPUT_CONF_KEY;

@InterfaceAudience.Public
public class ImportFDS {
  private static final Logger LOG = LoggerFactory.getLogger(ImportFDS.class);
  private static final String FDS_OUTPUT_TABLE_NAME = "fds.output.table.name";

  public static Job createSubmittable(Configuration conf, String[] args) throws IOException {
    conf = parseConfigs(conf, args);
    checkConfiguration(conf);
    conf.set("mapreduce.job.user.classpath.first", "true");
    TableMapReduceUtil.addHBaseDependencyJars(conf);
    TableMapReduceUtil.addDependencyJars(conf, com.google.common.base.Preconditions.class,
        GalaxyFDSClientException.class, org.apache.http.conn.socket.ConnectionSocketFactory.class,
        SocketConfig.class, SignAlgorithm.class);
    Job job = new Job(conf, "Import_FDS_" + conf.get(FDS_BUCKET_NAME));
    job.setJarByClass(Import.Importer.class);
    job.setInputFormatClass(FDSInputFormat.class);
    String hfileOutPath = conf.get(BULK_OUTPUT_CONF_KEY);

    // make sure we get the filter in the jars
    try {
      Class<? extends Filter> filter = conf.getClass(FILTER_CLASS_CONF_KEY, null, Filter.class);
      if (filter != null) {
        TableMapReduceUtil.addDependencyJars(conf, filter);
      }
    } catch (Exception e) {
      throw new IOException(e);
    }

    job.setMapperClass(Import.KeyValueImporter.class);
    try(Connection conn = ConnectionFactory.createConnection(conf)) {
      TableName formattedTableName = TableName.valueOf(conf.get(FDS_OUTPUT_TABLE_NAME));
      Table table = conn.getTable(formattedTableName);
      job.setReducerClass(KeyValueSortReducer.class);
      Path outputDir = new Path(hfileOutPath);
      FileOutputFormat.setOutputPath(job, outputDir);
      job.setMapOutputKeyClass(ImmutableBytesWritable.class);
      job.setMapOutputValueClass(KeyValue.class);
      RegionLocator regionLocator = conn.getRegionLocator(formattedTableName);
      HFileOutputFormat2.configureIncrementalLoad(job, table, regionLocator);
    }
    return job;
  }

  private static void removeMapreduceJar(Configuration conf) {
    Set<String> jars = new HashSet<>();
    jars.addAll(conf.getStringCollection("tmpjars"));
    Iterator<String> jarIterator = jars.iterator();
    while (jarIterator.hasNext()) {
      String jar = jarIterator.next();
      LOG.debug("current jar " + jar);
      if (jar.contains("hadoop-mapreduce-client")) {
        jarIterator.remove();
        LOG.info("remove " + jar + " from tmpjars");
      }
    }
    conf.set("tmpjars", StringUtils.arrayToString(jars.toArray(new String[jars.size()])));
  }

  private static void checkConfiguration(Configuration conf) {
    if (conf.get(FDS_ENDPOINT) == null) {
      printUsageAndExit("endpoint missed");
    }
    if (conf.get(FDS_ACCESS_KEY) == null) {
      printUsageAndExit("accessKey missed");
    }
    if (conf.get(FDS_ACCESS_SECRET) == null) {
      printUsageAndExit("accessSecret missed");
    }
    if (conf.get(FDS_BUCKET_NAME) == null) {
      printUsageAndExit("bucketName missed");
    }
    if (conf.get(FDS_OUTPUT_TABLE_NAME) == null) {
      printUsageAndExit("table missed");
    }
    if (conf.get(BULK_OUTPUT_CONF_KEY) == null) {
      printUsageAndExit("outputPath missed");
    }
  }

  private static void printUsageAndExit(String cause) {
    System.err.println(cause);
    System.err.println("Usage: ./hbase org.apache.hadoop.hbase.mapreduce.fds.ImportFDS ");
    System.err.println("-endpoint [required] FDS endpoint");
    System.err.println("-accessKey [required] FDS accessKey");
    System.err.println("-accessSecret [required] FDS accessSecret");
    System.err.println("-bucket [required] FDS bucket name");
    System.err.println("-table [required] output table name");
    System.err.println("-startTime <optional> filter data before the time");
    System.err.println("-endTime <optional> filter data after the time");
    System.err.println("Example: ./hbase org.apache.hadoop.hbase.mapreduce.ImportTalos "
        + "-endpoint staging-cnbj2-fds.api.xiaomi.net"
        + "-accessKey xxxxxx -accessSecret xxxxxx"
        + "-bucket test2" + "-startTime 1"
        + "-endTime 9999999999999"
        + "-table fds:test2 "
        + "-outputPath hdfs://clusterName/hbase/tmp-output/test");
    System.exit(1);
  }

  private static Configuration parseConfigs(Configuration conf, String[] args) throws IOException {
    for (int i = 0; i < args.length; i++) {
      String cmd = args[i];
      if (cmd.equals("-endpoint")) {
        conf.set(FDS_ENDPOINT, args[++i]);
      } else if (cmd.equals("-accessKey")) {
        conf.set(FDS_ACCESS_KEY, args[++i]);
      } else if (cmd.equals("-accessSecret")) {
        conf.set(FDS_ACCESS_SECRET, args[++i]);
      } else if (cmd.equals("-bucket")) {
        conf.set(FDS_BUCKET_NAME, args[++i]);
      } else if (cmd.equals("-startTime")) {
        conf.setLong(FDS_START_TIME, Long.parseLong(args[++i]));
      } else if (cmd.equals("-endTime")) {
        conf.setLong(FDS_STOP_TIME, Long.parseLong(args[++i]));
      } else if (cmd.equals("-table")) {
        conf.set(FDS_OUTPUT_TABLE_NAME, args[++i]);
      } else if (cmd.equals("-outputPath")) {
        conf.set(BULK_OUTPUT_CONF_KEY, args[++i]);
      } else {
        printUsageAndExit("no such parameter" + cmd);
      }
    }
    return conf;
  }

  public static void main(String[] args)
      throws IOException, ClassNotFoundException, InterruptedException {
    Configuration conf = HBaseConfiguration.create();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    Job job = createSubmittable(conf, otherArgs);
    boolean isJobSuccessful = job.waitForCompletion(true);
    System.exit(isJobSuccessful ? 0 : 1);
  }
}
