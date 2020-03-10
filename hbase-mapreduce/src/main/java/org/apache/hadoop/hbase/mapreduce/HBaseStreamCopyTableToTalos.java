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
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.Import.Importer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.HBaseStreamUtil;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tool used to copy a table to Talos
 */
@InterfaceAudience.Public
public class HBaseStreamCopyTableToTalos extends Configured implements Tool {
  private static final Logger LOG = LoggerFactory.getLogger(HBaseStreamCopyTableToTalos.class);

  final static String NAME = "CopyTableToTalos";

  final static String TALOS_ENDPOINT = "hbase.stream.talos.endpoint";
  final static String TALOS_ACCESSKEY = "hbase.stream.talos.accesskey";
  final static String TALOS_ACCESSSECRET = "hbase.stream.talos.accesssecret";
  final static String TALOS_TOPIC_NAME = "hbase.stream.topic.name";
  final static String FIELDS_CONTROL = "hbase.stream.fieldscontrol";

  long startTime = 0;
  long endTime = 0;
  int versions = -1;
  String tableName = null;
  String startRow = null;
  String stopRow = null;
  String families = null;

  String endpoint;
  String accessKey;
  String accessSecret;

  String fieldsControl;

  private final static String JOB_NAME_CONF_KEY = "mapreduce.job.name";

  public HBaseStreamCopyTableToTalos(Configuration conf) {
    super(conf);
  }

  private void initCopyTableMapperJob(Scan scan, Job job) throws IOException {
    Class<? extends TableMapper> mapper = Importer.class;
    TableMapReduceUtil.initTableMapperJob(tableName, scan, mapper, ImmutableBytesWritable.class, Mutation.class, job);
  }

  private void initCopyTableReducerJob(Job job) throws IOException {
    Configuration conf = job.getConfiguration();
    conf.setStrings(TALOS_ENDPOINT, endpoint);
    conf.setStrings(TALOS_ACCESSKEY, accessKey);
    conf.setStrings(TALOS_ACCESSSECRET, accessSecret);
    conf.setStrings(TALOS_TOPIC_NAME, HBaseStreamUtil.encodeTopicName(tableName));
    conf.setStrings(FIELDS_CONTROL, fieldsControl);

    job.setNumReduceTasks(1);
    job.setMapOutputKeyClass(ImmutableBytesWritable.class);
    job.setMapOutputValueClass(Put.class);
    job.setOutputKeyClass(ImmutableBytesWritable.class);
    job.setOutputValueClass(Writable.class);
    job.setReducerClass(HBaseStreamCopyTableToTalosReducer.class);
    job.setOutputFormatClass(NullOutputFormat.class);
  }

  /**
   * Sets up the actual job.
   *
   * @param args  The command line parameters.
   * @return The newly created job.
   * @throws IOException When setting up the job fails.
   */
  public Job createSubmittableJob(String[] args) throws IOException {
    if (!doCommandLine(args)) {
      return null;
    }

    String jobSuffix = tableName;
    Job job = Job.getInstance(getConf(), getConf().get(JOB_NAME_CONF_KEY, NAME + "_" + jobSuffix));
    job.setJarByClass(HBaseStreamCopyTableToTalos.class);
    Scan scan = new Scan();
    scan.setCacheBlocks(false);
    if (startTime != 0) {
      scan.setTimeRange(startTime, endTime == 0 ? HConstants.LATEST_TIMESTAMP : endTime);
    }
    if (versions >= 0) {
      scan.setMaxVersions(versions);
    }

    if (startRow != null) {
      scan.withStartRow(Bytes.toBytes(startRow));
    }

    if (stopRow != null) {
      scan.withStopRow(Bytes.toBytes(stopRow));
    }

    if (families != null) {
      String[] fams = families.split(",");
      for (String fam : fams) {
        scan.addFamily(Bytes.toBytes(fam));
      }
    }
    job.setNumReduceTasks(0);

    initCopyTableMapperJob(scan, job);
    initCopyTableReducerJob(job);
    return job;
  }

  /*
   * @param errorMsg Error message.  Can be null.
   */
  private static void printUsage(final String errorMsg) {
    if (errorMsg != null && errorMsg.length() > 0) {
      System.err.println("ERROR: " + errorMsg);
    }
    System.err.println("Usage: HBaseStreamCopyTableToTalos [general options] [--starttime=X] [--endtime=Y] "
        + "");
    System.err.println();
    System.err.println("Options:");
    System.err.println(" startrow      the start row");
    System.err.println(" stoprow       the stop row");
    System.err.println(" starttime     beginning of the time range (unixtime in millis)");
    System.err.println("               without endtime means from starttime to forever");
    System.err.println(" endtime       end of the time range.  Ignored if no starttime specified.");
    System.err.println(" families      comma-separated list of families to copy");
    System.err.println(" scanrate      the scan rate limit: rows per second for each region.");
    System.err.println();
    System.err.println(" endpoint      Talos cluster endpoint.");
    System.err.println(" accesskey     Talos accessKey.");
    System.err.println(" accesssecret  Talos accessSecret.");
    System.out.println();
    System.err.println(" fieldscontrol Fields output finally.");
    System.err.println("               MUTATION_LOG: complete data.");
    System.err.println("               KEYS_ONLY: missing value. consider this if your value is large and unnecessary");
    System.out.println();
    System.err.println("Args:");
    System.err.println(" tablename     Name of the table to copy");
    System.err.println();
    System.err.println("Examples:");
    System.err.println(" To copy 'TestTable' to a cluster that uses replication for a 1 hour window:");
    System.err.println(" $ bin/hbase "
        + "org.apache.hadoop.hbase.mapreduce.HBaseStreamCopyTableToTalos "
        + "--starttime=1265875194289 --endtime=1265878794289 --families=cf1,cf2 "
        + "--endpoint=http://staging-cnbj2-talos.api.xiaomi.net --accesskey=your_access_key "
        + "--accesssecret=your_access_secret TestTable ");
    System.err.println("For performance consider the following general options:\n"
        + "-Dhbase.client.scanner.caching=100\n"
        + "-Dmapred.map.tasks.speculative.execution=false");
  }

  private boolean doCommandLine(final String[] args) {
    if (args.length < 1) {
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

        final String endpointArgKey = "--endpoint=";
        if (cmd.startsWith(endpointArgKey)) {
          endpoint = cmd.substring(endpointArgKey.length());
          continue;
        }

        final String accessKeyArgKey = "--accesskey=";
        if (cmd.startsWith(accessKeyArgKey)) {
          accessKey = cmd.substring(accessKeyArgKey.length());
          continue;
        }

        final String accessSecretArgKey = "--accesssecret=";
        if (cmd.startsWith(accessSecretArgKey)) {
          accessSecret = cmd.substring(accessSecretArgKey.length());
          continue;
        }

        final String fieldscontrolArgKey = "--fieldscontrol=";
        if (cmd.startsWith(fieldscontrolArgKey)) {
          fieldsControl = cmd.substring(fieldscontrolArgKey.length());
          continue;
        }

        if (i == args.length - 1) {
          tableName = cmd;
        } else {
          printUsage("Invalid argument '" + cmd + "'");
          return false;
        }
      }
      if (endpoint == null || accessKey == null || accessSecret == null) {
        printUsage("Invalid Talos config: endpoint, accesskey or accesssecret is null");
        return false;
      }
      if ((endTime != 0) && (startTime > endTime)) {
        printUsage("Invalid time range filter: starttime=" + startTime + " >  endtime=" + endTime);
        return false;
      }
    } catch (Exception e) {
      e.printStackTrace();
      printUsage("Can't start because " + e.getMessage());
      return false;
    }
    return true;
  }

  /**
   * Main entry point.
   *
   * @param args  The command line parameters.
   * @throws Exception When running the job fails.
   */
  public static void main(String[] args) throws Exception {
    int ret = ToolRunner.run(new HBaseStreamCopyTableToTalos(HBaseConfiguration.create()), args);
    System.exit(ret);
  }

  @Override
  public int run(String[] args) throws Exception {
    String[] otherArgs = new GenericOptionsParser(getConf(), args).getRemainingArgs();
    Job job = createSubmittableJob(otherArgs);
    if (job == null) return 1;
    if (!job.waitForCompletion(true)) {
      LOG.info("Map-reduce job failed!");
      return 1;
    }
    return 0;
  }
}
