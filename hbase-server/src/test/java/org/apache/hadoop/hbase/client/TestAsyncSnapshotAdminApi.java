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

package org.apache.hadoop.hbase.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;

@RunWith(Parameterized.class)
@Category({ LargeTests.class })
public class TestAsyncSnapshotAdminApi extends TestAsyncAdminBase {

  private static final int COUNT = 3000;

  String snapshotName1 = "snapshotName1";
  String snapshotName2 = "snapshotName2";
  String snapshotName3 = "snapshotName3";

  @After
  public void cleanup() throws Exception {
    admin.deleteSnapshots(Pattern.compile(".*")).get();
    admin.disableTables(Pattern.compile(".*")).get();
    admin.deleteTables(Pattern.compile(".*")).get();
  }

  @Test
  public void testTakeSnapshot() throws Exception {
    createTableAndLoadData();

    admin.snapshot(snapshotName1, tableName).get();
    admin.snapshot(snapshotName2, tableName).get();
    List<SnapshotDescription> snapshots = admin.listSnapshots().get();
    Collections.sort(snapshots, (snap1, snap2) -> {
      assertNotNull(snap1);
      assertNotNull(snap1.getName());
      assertNotNull(snap2);
      assertNotNull(snap2.getName());
      return snap1.getName().compareTo(snap2.getName());
    });

    assertEquals(snapshotName1, snapshots.get(0).getName());
    assertEquals(tableName, snapshots.get(0).getTableName());
    assertEquals(SnapshotType.FLUSH, snapshots.get(0).getType());
    assertEquals(snapshotName2, snapshots.get(1).getName());
    assertEquals(tableName, snapshots.get(1).getTableName());
    assertEquals(SnapshotType.FLUSH, snapshots.get(1).getType());
  }

  @Test
  public void testCloneSnapshot() throws Exception {
    TableName tableName2 = TableName.valueOf("testCloneSnapshot2");
    createTableAndLoadData();

    admin.snapshot(snapshotName1, tableName).get();
    List<SnapshotDescription> snapshots = admin.listSnapshots().get();
    assertEquals(snapshots.size(), 1);
    assertEquals(snapshotName1, snapshots.get(0).getName());
    assertEquals(tableName, snapshots.get(0).getTableName());
    assertEquals(SnapshotType.FLUSH, snapshots.get(0).getType());

    // cloneSnapshot into a existed table.
    boolean failed = false;
    try {
      admin.cloneSnapshot(snapshotName1, tableName).get();
    } catch (Exception e) {
      failed = true;
    }
    assertTrue(failed);

    // cloneSnapshot into a new table.
    assertFalse(admin.tableExists(tableName2).get());
    admin.cloneSnapshot(snapshotName1, tableName2).get();
    long startWait = System.currentTimeMillis();
    while (!admin.tableExists(tableName2).get()) {
      assertTrue("Timed out waiting for table to exist " + tableName, System.currentTimeMillis()
          - startWait < 10000);
      Thread.sleep(1000);
    }
  }

  @Test
  public void testRestoreSnapshot() throws Exception {
    createTableAndLoadData();
    assertEquals(admin.listSnapshots().get().size(), 0);

    admin.snapshot(snapshotName1, tableName).get();
    admin.snapshot(snapshotName2, tableName).get();
    assertEquals(admin.listSnapshots().get().size(), 2);

    admin.disableTable(tableName).get();
    TEST_UTIL.waitTableDisabled(tableName.getName(), 10000);
    admin.restoreSnapshot(snapshotName1, true).get();
    admin.enableTable(tableName).get();
    TEST_UTIL.waitTableEnabled(tableName.getName(), 10000);
    assertLoadData();

    admin.disableTable(tableName).get();
    TEST_UTIL.waitTableDisabled(tableName.getName(), 10000);
    admin.restoreSnapshot(snapshotName2, false).get();
    admin.enableTable(tableName).get();
    TEST_UTIL.waitTableEnabled(tableName.getName(), 10000);
    assertLoadData();
  }

  @Test
  public void testListSnapshots() throws Exception {
    createTableAndLoadData();
    assertEquals(admin.listSnapshots().get().size(), 0);

    admin.snapshot(snapshotName1, tableName).get();
    admin.snapshot(snapshotName2, tableName).get();
    admin.snapshot(snapshotName3, tableName).get();
    assertEquals(3, admin.listSnapshots().get().size());

    assertEquals(3, admin.listSnapshots(Optional.of(Pattern.compile("(.*)"))).get().size());
    assertEquals(3, admin.listSnapshots(Optional.of(Pattern.compile("snapshotName(\\d+)"))).get()
        .size());
    assertEquals(2, admin.listSnapshots(Optional.of(Pattern.compile("snapshotName[1|3]"))).get()
        .size());
    assertEquals(3, admin.listSnapshots(Optional.of(Pattern.compile("snapshot(.*)"))).get().size());
    assertEquals(3,
      admin.listTableSnapshots(Pattern.compile("testListSnapshots"), Pattern.compile("s(.*)"))
          .get().size());
    assertEquals(0,
      admin.listTableSnapshots(Pattern.compile("fakeTableName"), Pattern.compile("snap(.*)")).get()
          .size());
    assertEquals(2,
      admin.listTableSnapshots(Pattern.compile("test(.*)"), Pattern.compile("snap(.*)[1|3]")).get()
          .size());
  }

  @Test
  public void testDeleteSnapshots() throws Exception {
    createTableAndLoadData();
    assertEquals(admin.listSnapshots().get().size(), 0);

    admin.snapshot(snapshotName1, tableName).get();
    admin.snapshot(snapshotName2, tableName).get();
    admin.snapshot(snapshotName3, tableName).get();
    assertEquals(admin.listSnapshots().get().size(), 3);

    admin.deleteSnapshot(snapshotName1).get();
    assertEquals(admin.listSnapshots().get().size(), 2);

    admin.deleteSnapshots(Pattern.compile("(.*)abc")).get();
    assertEquals(admin.listSnapshots().get().size(), 2);

    admin.deleteSnapshots(Pattern.compile("(.*)1")).get();
    assertEquals(admin.listSnapshots().get().size(), 2);

    admin.deleteTableSnapshots(Pattern.compile("(.*)"), Pattern.compile("(.*)1")).get();
    assertEquals(admin.listSnapshots().get().size(), 2);

    admin.deleteTableSnapshots(Pattern.compile("(.*)"), Pattern.compile("(.*)2")).get();
    assertEquals(admin.listSnapshots().get().size(), 1);

    admin.deleteTableSnapshots(Pattern.compile("(.*)"), Pattern.compile("(.*)3")).get();
    assertEquals(admin.listSnapshots().get().size(), 0);
  }

  private void createTableAndLoadData() throws Exception {
    createTableWithDefaultConf(tableName, Optional.empty(), FAMILY);
    RawAsyncTable table = ASYNC_CONN.getRawTable(tableName);
    for (int i = 0; i < COUNT; i++) {
      table.put(new Put(Bytes.toBytes(i)).add(FAMILY, Bytes.toBytes("cq"),
        Bytes.toBytes(i))).join();
    }
  }

  private void assertLoadData() throws Exception {
    RawAsyncTable table = ASYNC_CONN.getRawTable(tableName);
    assertEquals(table.scanAll(new Scan()).get().size(), COUNT);
  }
}