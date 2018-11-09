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
package org.apache.hadoop.hbase.mapred;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestSaltedTableOutputFormat {

  private final static HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private final static byte[] TABLE_NAME = Bytes.toBytes("testSaltedTable");
  private static final byte[] FAMILY = Bytes.toBytes("family");
  private static final byte[] NEW_COLUMN = Bytes.toBytes("new_column");
  private static final byte[] VALUE = Bytes.toBytes("value");

  @BeforeClass
  public static void beforeClass() throws Exception {
    UTIL.startMiniCluster();
    UTIL.startMiniMapReduceCluster();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    UTIL.shutdownMiniMapReduceCluster();
    UTIL.shutdownMiniCluster();
  }

  static class SaltedMapper extends MapReduceBase implements TableMap<ImmutableBytesWritable, Put> {
    @Override
    public void map(ImmutableBytesWritable key, Result value,
        OutputCollector<ImmutableBytesWritable, Put> output, Reporter reporter) throws IOException {
      output.collect(key, new Put(key.copyBytes()).add(FAMILY, NEW_COLUMN, VALUE));
    }
  }

  static class SaltedReduce extends MapReduceBase
      implements TableReduce<ImmutableBytesWritable, Put> {
    @Override
    public void reduce(ImmutableBytesWritable key, Iterator<Put> values,
        OutputCollector<ImmutableBytesWritable, Put> output, Reporter reporter) throws IOException {
      while (values.hasNext()) {
        output.collect(key, values.next());
      }
    }
  }

  @Test
  public void testSaltedTableOutputFormat() throws Exception {
    HTableInterface table = UTIL.createSaltedTable(TABLE_NAME, FAMILY, 4);
    UTIL.loadTable(table, FAMILY, false);
    Configuration cfg = UTIL.getConfiguration();
    JobConf jobConf = new JobConf(cfg);
    try {
      jobConf.setJobName("process row task");
      jobConf.setNumReduceTasks(2);
      TableMapReduceUtil.initTableMapJob(Bytes.toString(TABLE_NAME), new String(FAMILY),
        SaltedMapper.class, ImmutableBytesWritable.class, Put.class, jobConf);

      TableMapReduceUtil.initTableReduceJob(Bytes.toString(TABLE_NAME), SaltedReduce.class, jobConf,
        HRegionPartitioner.class);
      RunningJob job = JobClient.runJob(jobConf);
      assertTrue(job.isSuccessful());

      Scan scan = new Scan();
      scan.addColumn(FAMILY, NEW_COLUMN);
      ResultScanner scanner = table.getScanner(scan);
      int count = 0;
      while (scanner.next() != null) {
        count++;
      }
      Assert.assertEquals(HBaseTestingUtility.ROWS.length, count);
    } finally {
      if (jobConf != null) {
        FileUtil.fullyDelete(new File(jobConf.get("hadoop.tmp.dir")));
      }
    }
  }
}
