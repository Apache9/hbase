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
package org.apache.hadoop.hbase.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.master.snapshot.SnapshotManager;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MD5Hash;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(LargeTests.class)
public class TestRepartition {
  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static final String OUTPUT_DIR = "outputdir";
  private static final String RESTORE_DIR = "restoredir";

  private static final String FAMILY_STRING = "a";
  private static final byte[] FAMILY = Bytes.toBytes(FAMILY_STRING);
  private static final byte[] QUAL = Bytes.toBytes("q");
  private static String TABLE = "testTable";
  private static String FQ_OUTPUT_DIR;
  private static String FQ_RESTORE_DIR;

  @BeforeClass
  public static void beforeClass() throws Exception {
    UTIL.getConfiguration().setBoolean(SnapshotManager.HBASE_SNAPSHOT_ENABLED, true);
    UTIL.startMiniCluster();
    UTIL.startMiniMapReduceCluster();
    FQ_OUTPUT_DIR =
        new Path(OUTPUT_DIR).makeQualified(FileSystem.get(UTIL.getConfiguration())).toString();
    FQ_RESTORE_DIR =
        new Path(RESTORE_DIR).makeQualified(FileSystem.get(UTIL.getConfiguration())).toString();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    UTIL.shutdownMiniMapReduceCluster();
    UTIL.shutdownMiniCluster();
  }

  @Before
  @After
  public void cleanup() throws Exception {
    FileSystem fs = FileSystem.get(UTIL.getConfiguration());
    fs.delete(new Path(OUTPUT_DIR), true);
    fs.delete(new Path(RESTORE_DIR), true);
  }

  @Test
  public void testRepartition() throws Exception {
    Configuration conf = UTIL.getConfiguration();
    // create a table
    HTable t = UTIL.createTable(TABLE, FAMILY_STRING);

    // put some data
    loadData(t, 10000);

    // repartition
    String[] args = constructArgs();
    Repartition repartition = new Repartition(conf, args);
    repartition.setAdmin(UTIL.getHBaseAdmin());
    boolean isSuccess = repartition.repatitionProcess();
    Assert.assertTrue(isSuccess);

    HConnection connection = HConnectionManager.createConnection(conf);
    // check number of region
    NavigableMap<HRegionInfo, ServerName> regions =
        MetaScanner.allTableRegions(conf, connection, TableName.valueOf(TABLE), true);
    Assert.assertEquals("number of regions of new table: ", 15, regions.size());

    // check data
    HTableInterface newTable = connection.getTable(TableName.valueOf(TABLE));
    ResultScanner scanner = newTable.getScanner(new Scan());
    int count = 0;
    while (scanner.next() != null) {
      count++;
    }

    Assert.assertEquals("number of rowkeys", 10000, count);
    connection.close();
    t.close();
    newTable.close();
    scanner.close();
  }

  private String[] constructArgs() {
    List<String> args = new ArrayList();
    args.add("--table");
    args.add(TABLE);
    args.add("--export-to");
    args.add(FQ_OUTPUT_DIR);
    args.add("--restorePath");
    args.add(FQ_RESTORE_DIR);
    args.add("--splitAlgo");
    args.add("HexStringSplit");
    args.add("--numRegion");
    args.add("15");
    args.add("--snapshotSplitNum");
    args.add("3");
    return args.toArray(new String[args.size()]);
  }

  private void loadData(HTable t, int count) throws IOException {
    long now = System.currentTimeMillis();
    for (int i = 0; i < count; i++) {
      String key = MD5Hash.getMD5AsHex(Bytes.toBytes(i));
      Put p = new Put(Bytes.toBytes(key));
      p.add(FAMILY, QUAL, now, QUAL);
      t.put(p);
    }
  }
}
