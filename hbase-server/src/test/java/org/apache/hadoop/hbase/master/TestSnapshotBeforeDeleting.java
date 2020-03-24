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
package org.apache.hadoop.hbase.master;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.master.cleaner.SnapshotCleaner;
import org.apache.hadoop.hbase.protobuf.generated.SnapshotProtos.SnapshotDescription;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(LargeTests.class)
public class TestSnapshotBeforeDeleting {

  private static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static TableName TABLE_NAME = TableName.valueOf("TestSnapshotBeforeDeleting");

  private static byte[] FAMILY = Bytes.toBytes("cf");

  private static byte[] QUALIFIER = Bytes.toBytes("cq");

  @BeforeClass
  public static void setUp() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setBoolean(HConstants.SNAPSHOT_BEFORE_DELETE, true);
    conf.setInt("hbase.master.cleaner.interval", 1000);
    conf.setLong(SnapshotCleaner.SNAPSHOT_FOR_DELETED_TABLE_TTL_MS, 20000);
    TEST_UTIL.startMiniCluster(1);

  }

  @AfterClass
  public static void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void test() throws IOException, InterruptedException {
    int count = 10;
    HTable table = TEST_UTIL.createTable(TABLE_NAME.getName(), FAMILY);
    for (int i = 0; i < count; i++) {
      table.put(new Put(Bytes.toBytes(i)).add(FAMILY, QUALIFIER, Bytes.toBytes(i)));
    }
    HBaseAdmin admin = TEST_UTIL.getHBaseAdmin();
    admin.disableTable(TABLE_NAME);
    admin.deleteTable(TABLE_NAME);
    long timeAfterDelete = System.currentTimeMillis();
    try {
      table.get(new Get(Bytes.toBytes(0)));
      fail("Should fail because we have already deleted the table.");
    } catch (TableNotFoundException e) {
      // expected
    }
    // we should have a snapshot for the deleted table
    List<SnapshotDescription> snapshots = admin.listSnapshots();
    assertEquals(1, snapshots.size());
    assertTrue(snapshots.get(0).getName()
        .startsWith(SnapshotDescriptionUtils.SNAPSHOT_FOR_DELETED_TABLE_PREFIX));
    assertTrue(snapshots.get(0).getName().contains(TABLE_NAME.getNameAsString()));
    admin.cloneSnapshot(snapshots.get(0).getName(), TABLE_NAME);
    // assert that the data is still there
    for (int i = 0; i < count; i++) {
      Result result = table.get(new Get(Bytes.toBytes(i)));
      assertEquals(i, Bytes.toInt(result.getValue(FAMILY, QUALIFIER)));
    }
    // sleep until the snapshot is expired
    Thread.sleep(25000 - (System.currentTimeMillis() - timeAfterDelete));
    // the expired snapshot should be removed.
    assertTrue(admin.listSnapshots().isEmpty());
  }
}
