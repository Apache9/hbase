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
package org.apache.hadoop.hbase.mapreduce.salted;

import java.io.File;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.LargeTests;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.MultiTableInputFormatBase;
import org.apache.hadoop.hbase.mapreduce.TestMultiTableInputFormatBase;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.xiaomi.infra.hbase.salted.SaltedHTable;

/**
 * Tests various scan start and stop row scenarios. This is set in a scan and
 * tested in a MapReduce job to see if that is handed over and done properly
 * too.
 */
@Category(LargeTests.class)
public class TestSaltedMultiTableInputFormat extends TestMultiTableInputFormatBase {

  static final Log LOG = LogFactory.getLog(TestSaltedMultiTableInputFormat.class);
  
  public static HTableDescriptor getTableDescriptor(byte[] tableName, byte[][] families) {
    HTableDescriptor desc = new HTableDescriptor(tableName);
    for (byte[] family : families) {
      desc.addFamily(new HColumnDescriptor(family));
    }
    return desc;
  }
  
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // switch TIF to log at DEBUG level
    TEST_UTIL.enableDebug(SaltedMultiTableInputFormat.class);
    TEST_UTIL.enableDebug(MultiTableInputFormatBase.class);
    // start mini hbase cluster
    TEST_UTIL.startMiniCluster(3);
    // create and fill table
    // two formal tables plus one salted table
    for (int i = 0; i < 3; i++) {
      if (i == 2) {
        // create and load salted table
        HTableDescriptor desc = getTableDescriptor(Bytes.toBytes(TABLE_NAME + i),
          new byte[][] { INPUT_FAMILY });
        desc.setSlotsCount(4);
        TEST_UTIL.getHBaseAdmin().createTable(desc);
        loadTable(new SaltedHTable(new HTable(TEST_UTIL.getConfiguration(), desc.getName())),
          INPUT_FAMILY);
      } else {
        HTable table =
            TEST_UTIL.createTable(Bytes.toBytes(TABLE_NAME + String.valueOf(i)),
                INPUT_FAMILY);
        TEST_UTIL.createMultiRegions(table, INPUT_FAMILY);
        TEST_UTIL.loadTable(table, INPUT_FAMILY);
      }
    }
    // start MR cluster
    TEST_UTIL.startMiniMapReduceCluster();
  }
  
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniMapReduceCluster();
    TEST_UTIL.shutdownMiniCluster();
  }

  public static int loadTable(final HTableInterface t, final byte[] f) throws IOException {
    t.setAutoFlush(false);
    byte[] k = new byte[3];
    int rowCount = 0;
    for (byte b1 = 'a'; b1 <= 'z'; b1++) {
      for (byte b2 = 'a'; b2 <= 'z'; b2++) {
        for (byte b3 = 'a'; b3 <= 'z'; b3++) {
          k[0] = b1;
          k[1] = b2;
          k[2] = b3;
          Put put = new Put(k);
          put.add(f, null, k);
          t.put(put);
          rowCount++;
        }
      }
    }
    t.flushCommits();
    return rowCount;
  }

  @After
  public void tearDown() throws Exception {
    Configuration c = TEST_UTIL.getConfiguration();
    FileUtil.fullyDelete(new File(c.get("hadoop.tmp.dir")));
  }

  @Test
  public void testScanEmptyToEmpty() throws IOException, InterruptedException,
      ClassNotFoundException {
    testScan(null, null, null);
  }
  
  @Test
  public void testScanEmptyToAPP() throws IOException, InterruptedException,
      ClassNotFoundException {
    testScan(null, "app", "apo");
  }

  @Test
  public void testScanOBBToOPP() throws IOException, InterruptedException,
      ClassNotFoundException {
    testScan("obb", "opp", "opo");
  }

  @Test
  public void testScanOPPToEmpty() throws IOException, InterruptedException,
      ClassNotFoundException {
    testScan("opp", null, "zzz");
  }

  @Test
  public void testScanYZYToEmpty() throws IOException, InterruptedException,
      ClassNotFoundException {
    testScan("yzy", null, "zzz");
  }
}
