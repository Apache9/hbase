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
package org.apache.hadoop.hbase.master.snapshot;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.protobuf.generated.SnapshotProtos.SnapshotDescription;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test that the snapshot cleaner
 */
@Category(SmallTests.class)
public class TestSnapshotCleaner {

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static HBaseAdmin admin = null;
  private static TableName TABLE_NAME = TableName.valueOf("TestSnapshotCleaner");
  private static byte[] FAMILY = Bytes.toBytes("cf");

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setInt("hbase.master.cleaner.interval", 1000);
    TEST_UTIL.startMiniCluster();
    admin = TEST_UTIL.getHBaseAdmin();
    TEST_UTIL.createTable(TABLE_NAME.getName(), FAMILY);
  }

  @AfterClass
  public static void cleanup() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testSnapshotCleaner() throws Exception {
    String snapshot1 = "s1";
    admin.snapshot(snapshot1, TABLE_NAME);
    String snapshot2 = "s2";
    admin.snapshot(snapshot2, TABLE_NAME, 1000);
    String snapshot3 = "s3";
    admin.snapshot(snapshot3, TABLE_NAME, 8000);
    Thread.sleep(4000);
    List<SnapshotDescription> snapshots = admin.listSnapshots();
    Assert.assertEquals(2, snapshots.size());
    Assert.assertEquals(snapshot1, snapshots.get(0).getName());
    Assert.assertEquals(snapshot3, snapshots.get(1).getName());
    Thread.sleep(6000);
    snapshots = admin.listSnapshots();
    Assert.assertEquals(1, snapshots.size());
    Assert.assertEquals(snapshot1, snapshots.get(0).getName());
  }
}
