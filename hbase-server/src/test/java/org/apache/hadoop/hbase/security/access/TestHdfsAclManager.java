/*
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

package org.apache.hadoop.hbase.security.access;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.TableSnapshotScanner;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ SmallTests.class })
public class TestHdfsAclManager {
  private static Log LOG = LogFactory.getLog(TestHdfsAclManager.class);

  private static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static Configuration conf = TEST_UTIL.getConfiguration();
  private static HBaseAdmin admin = null;
  private static FileSystem fs = null;
  private static Path rootDir = null;
  private static User grantUser = null;
  private static User unGrantUser = null;

  private static final String NAMESPACE_PREFIX = "ns_";
  private static final String SNAPSHOT_POSTFIX = "_snapshot";
  private static final String GRANT_USER = "grant_user";
  private static final String UN_GRANT_USER = "un_grant_user";

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    conf.setBoolean("dfs.namenode.acls.enabled", true);
    conf.set("fs.permissions.umask-mode", "027");
    conf.setBoolean(HConstants.ENABLE_DATA_FILE_UMASK, true);
    conf.setBoolean(HConstants.HDFS_ACL_ENABLE, true);
    conf.set(User.HBASE_SECURITY_CONF_KEY, "simple");
    SecureTestUtil.enableSecurity(conf);
    TEST_UTIL.startMiniCluster();

    admin = TEST_UTIL.getHBaseAdmin();
    rootDir = TEST_UTIL.getHBaseCluster().getMaster().getMasterFileSystem().getRootDir();
    fs = rootDir.getFileSystem(conf);
    grantUser = User.createUserForTesting(conf, GRANT_USER, new String[] {});
    unGrantUser = User.createUserForTesting(conf, UN_GRANT_USER, new String[] {});

    Path path = rootDir;
    while (path != null) {
      fs.setPermission(path, new FsPermission((short) 0755));
      path = path.getParent();
    }
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testGrantNamespace() throws Exception {
    String namespace = NAMESPACE_PREFIX + "testGrantNamespace";
    String table = namespace + ":" + "testGrantNamespace";
    String table2 = namespace + ":" + "testGrantNamespace2";
    String snapshot = "testGrantNamespace" + SNAPSHOT_POSTFIX;
    String snapshot2 = "testGrantNamespace2" + SNAPSHOT_POSTFIX;

    createNamespace(admin, namespace);

    // create table -> grant ns
    HTable hTable = createTable(TEST_UTIL, table);
    put(hTable);
    admin.snapshot(snapshot, table);
    SecureTestUtil.grantOnNamespace(TEST_UTIL, GRANT_USER, namespace, Permission.Action.READ);

    // grant ns -> create table
    HTable hTable2 = createTable(TEST_UTIL, table2);
    put(hTable2);
    admin.snapshot(snapshot2, table2);

    Assert.assertTrue(canUserScanSnapshot(conf, fs, grantUser, snapshot));
    Assert.assertFalse(canUserScanSnapshot(conf, fs, unGrantUser, snapshot));

    Assert.assertTrue(canUserScanSnapshot(conf, fs, grantUser, snapshot2));
    Assert.assertFalse(canUserScanSnapshot(conf, fs, unGrantUser, snapshot2));
  }

  @Test
  public void testRevokeNamespace() throws Exception {
    String namespace = NAMESPACE_PREFIX + "testRevokeNamespace";
    String table = namespace + ":" + "testRevokeNamespace";
    String table2 = namespace + ":" + "testRevokeNamespace2";
    String snapshot = "testRevokeNamespace" + SNAPSHOT_POSTFIX;
    String snapshot2 = "testRevokeNamespace2" + SNAPSHOT_POSTFIX;
    String snapshot3 = "testRevokeNamespace3" + SNAPSHOT_POSTFIX;

    // snapshot -> revoke ns
    createNamespace(admin, namespace);
    HTable hTable = createTable(TEST_UTIL, table);
    put(hTable);
    admin.snapshot(snapshot, table);
    SecureTestUtil.grantOnNamespace(TEST_UTIL, GRANT_USER, namespace, Permission.Action.READ);
    SecureTestUtil.revokeFromNamespace(TEST_UTIL, GRANT_USER, namespace, Permission.Action.READ);

    // revoke ns -> snapshot
    HTable hTable2 = createTable(TEST_UTIL, table2);
    put(hTable2);
    admin.snapshot(snapshot2, table2);
    admin.snapshot(snapshot3, table);

    Assert.assertFalse(canUserScanSnapshot(conf, fs, grantUser, snapshot));
    Assert.assertFalse(canUserScanSnapshot(conf, fs, unGrantUser, snapshot));

    Assert.assertFalse(canUserScanSnapshot(conf, fs, grantUser, snapshot2));
    Assert.assertFalse(canUserScanSnapshot(conf, fs, unGrantUser, snapshot2));

    Assert.assertFalse(canUserScanSnapshot(conf, fs, grantUser, snapshot3));
  }

  @Test
  public void testGrantTable() throws Exception {
    String namespace = NAMESPACE_PREFIX + "testGrantTable";
    String table = namespace + ":" + "testGrantTable";
    String snapshot = "testGrantTable" + SNAPSHOT_POSTFIX;
    String snapshot2 = "testGrantTable2" + SNAPSHOT_POSTFIX;

    // snapshot -> grant table
    createNamespace(admin, namespace);
    HTable hTable = createTable(TEST_UTIL, table);
    put(hTable);
    admin.snapshot(snapshot, table);
    SecureTestUtil.grantOnTable(TEST_UTIL, GRANT_USER, TableName.valueOf(table), null, null,
      Permission.Action.READ);

    // grant table -> snapshot
    put(hTable);
    admin.snapshot(snapshot2, table);

    Assert.assertTrue(canUserScanSnapshot(conf, fs, grantUser, snapshot));
    Assert.assertFalse(canUserScanSnapshot(conf, fs, unGrantUser, snapshot));

    Assert.assertTrue(canUserScanSnapshot(conf, fs, grantUser, snapshot2));
    Assert.assertFalse(canUserScanSnapshot(conf, fs, unGrantUser, snapshot2));
  }

  @Test
  public void testGrantTableFamily() throws Exception {
    String table = "testGrantTableFamily";
    String snapshot = "testGrantTableFamily" + SNAPSHOT_POSTFIX;

    HTable hTable = createTable(TEST_UTIL, table);
    put(hTable);
    admin.snapshot(snapshot, table);
    SecureTestUtil.grantOnTable(TEST_UTIL, GRANT_USER, TableName.valueOf(table), Bytes.toBytes("f"),
      null, Permission.Action.READ);

    Assert.assertFalse(canUserScanSnapshot(conf, fs, grantUser, snapshot));
  }

  @Test
  public void testGrantTableQualifier() throws Exception {
    String table = "testGrantTableQualifier";
    String snapshot = "testGrantTableQualifier" + SNAPSHOT_POSTFIX;

    HTable hTable = createTable(TEST_UTIL, table);
    put(hTable);
    admin.snapshot(snapshot, table);
    SecureTestUtil.grantOnTable(TEST_UTIL, GRANT_USER, TableName.valueOf(table), null,
      Bytes.toBytes("q"), Permission.Action.READ);

    Assert.assertFalse(canUserScanSnapshot(conf, fs, grantUser, snapshot));
  }

  @Test
  public void testRevokeTable() throws Exception {
    String namespace = NAMESPACE_PREFIX + "testRevokeTable";
    String table = namespace + ":" + "testRevokeTable";
    String snapshot = "testRevokeTable" + SNAPSHOT_POSTFIX;
    String snapshot2 = "testRevokeTable2" + SNAPSHOT_POSTFIX;

    // snapshot -> revoke table
    createNamespace(admin, namespace);
    HTable hTable = createTable(TEST_UTIL, table);
    SecureTestUtil.grantOnTable(TEST_UTIL, GRANT_USER, TableName.valueOf(table), null, null,
      Permission.Action.READ);
    put(hTable);
    admin.snapshot(snapshot, table);
    SecureTestUtil.revokeFromTable(TEST_UTIL, GRANT_USER, TableName.valueOf(table), null, null,
      Permission.Action.READ);

    // revoke table -> snapshot
    admin.snapshot(snapshot2, table);

    Assert.assertFalse(canUserScanSnapshot(conf, fs, grantUser, snapshot));
    Assert.assertFalse(canUserScanSnapshot(conf, fs, grantUser, snapshot2));
  }

  @Test
  public void testDeleteTable() throws Exception {
    String table = "testDeleteTable";
    String snapshot = "testDeleteTable" + SNAPSHOT_POSTFIX;
    String filePrefix = "grant_";
    String snapshot2 = "testDeleteTable2" + SNAPSHOT_POSTFIX;
    String user2 = "u2";
    User userUser2 = User.createUserForTesting(conf, user2, new String[] {});

    // create table, grant table, snapshot -> delete table => can scan snapshot
    HTable hTable = createTable(TEST_UTIL, table);
    put(hTable);
    SecureTestUtil.grantOnTable(TEST_UTIL, GRANT_USER, TableName.valueOf(table), null, null,
      Permission.Action.READ);
    admin.snapshot(snapshot, table);
    admin.disableTable(table);
    admin.deleteTable(table);
    Assert.assertTrue(canUserScanSnapshot(conf, fs, grantUser, snapshot));

    // create same name table, snapshot => can not scan new snapshot
    hTable = createTable(TEST_UTIL, table);
    put2(hTable);
    admin.snapshot(snapshot2, table);
    SecureTestUtil.grantOnTable(TEST_UTIL, user2, TableName.valueOf(table), null, null,
      Permission.Action.READ);

    Assert.assertTrue(canUserScanSnapshot(conf, fs, grantUser, snapshot));
    Assert.assertFalse(canUserScanSnapshot(conf, fs, grantUser, snapshot2));
    Assert.assertTrue(canUserScanSnapshot(conf, fs, userUser2, snapshot2));
    //Assert.assertFalse(canUserScanSnapshot(userUser2, snapshot)); //TODO return true
  }

  @Test
  public void testDeleteNamespace() throws Exception {
    String namespace = NAMESPACE_PREFIX + "testDeleteNamespace";
    String table = namespace + ":" + "testDeleteNamespace";
    String table2 = namespace + ":" + "testDeleteNamespace2";
    String snapshot = "testDeleteNamespace" + SNAPSHOT_POSTFIX;
    String snapshot2 = "testDeleteNamespace2" + SNAPSHOT_POSTFIX;

    // create ns, grant ns, snapshot -> ns table => can scan snapshot
    createNamespace(admin, namespace);
    HTable hTable = createTable(TEST_UTIL, table);
    put(hTable);
    SecureTestUtil.grantOnNamespace(TEST_UTIL, GRANT_USER, namespace, Permission.Action.READ);
    admin.snapshot(snapshot, table);
    admin.disableTable(table);
    admin.deleteTable(table);
    admin.deleteNamespace(namespace);
    Assert.assertTrue(canUserScanSnapshot(conf, fs, grantUser, snapshot));

    // create same name ns, snapshot => can not scan new snapshot
    createNamespace(admin, namespace);
    createTable(TEST_UTIL, table2);
    admin.snapshot(snapshot2, table2);

    Assert.assertTrue(canUserScanSnapshot(conf, fs, grantUser, snapshot));
    Assert.assertFalse(canUserScanSnapshot(conf, fs, grantUser, snapshot2));
  }

  @Test
  public void testTruncateTable() throws Exception {
    String namespace = "testTruncateTable_ns";
    String table = namespace+":"+"testTruncateTable";
    String snapshot = "testTruncateTable" + SNAPSHOT_POSTFIX;
    String snapshot2 = "testTruncateTable2" + SNAPSHOT_POSTFIX;
    String grantUser2 = "grantUser2";
    User user2 = User.createUserForTesting(conf, grantUser2, new String[] {});

    createNamespace(admin, namespace);
    HTable htable = createTable(TEST_UTIL, table);
    put(htable);
    admin.snapshot(snapshot, table);

    SecureTestUtil.grantOnNamespace(TEST_UTIL, grantUser2, namespace, Permission.Action.READ);
    SecureTestUtil.grantOnTable(TEST_UTIL, GRANT_USER, TableName.valueOf(table), null, null,
      Permission.Action.READ);
    admin.disableTable(table);
    admin.truncateTable(TableName.valueOf(table), true);
    Assert.assertTrue(canUserScanSnapshot(conf, fs, grantUser, snapshot));
    Assert.assertTrue(canUserScanSnapshot(conf, fs, user2, snapshot));

    put(htable);
    put2(htable);
    admin.snapshot(snapshot2, table);
    admin.flush(table);
    admin.majorCompact(table);
    Assert.assertTrue(canUserScanSnapshot(conf, fs, grantUser, snapshot));
    Assert.assertTrue(canUserScanSnapshot(conf, fs, grantUser, snapshot2));
    Assert.assertTrue(canUserScanSnapshot(conf, fs, user2, snapshot));
    Assert.assertTrue(canUserScanSnapshot(conf, fs, user2, snapshot2));
  }

  @Test
  public void testCompactTable() throws Exception {
    String table = "testCompactTable";
    String snapshot = "testCompactTable" + SNAPSHOT_POSTFIX;
    String snapshot2 = "testCompactTable2" + SNAPSHOT_POSTFIX;

    HTable htable = createTable(TEST_UTIL, table);
    put(htable);
    admin.snapshot(snapshot, table);
    SecureTestUtil.grantOnTable(TEST_UTIL, GRANT_USER, TableName.valueOf(table), null, null,
      Permission.Action.READ);

    put2(htable);
    admin.snapshot(snapshot2, table);
    admin.flush(table);
    admin.majorCompact(table);

    Assert.assertTrue(canUserScanSnapshot(conf, fs, grantUser, snapshot));
    Assert.assertTrue(canUserScanSnapshot(conf, fs, grantUser, snapshot2));
  }

  @Test
  public void testSplitTable() throws Exception {
    String table = "testSplitTable";
    String snapshot = "testSplitTable" + SNAPSHOT_POSTFIX;
    String snapshot2 = "testSplitTable2" + SNAPSHOT_POSTFIX;

    HTable htable = createTable(TEST_UTIL, table);
    put(htable);
    admin.snapshot(snapshot, table);
    SecureTestUtil.grantOnTable(TEST_UTIL, GRANT_USER, TableName.valueOf(table), null, null,
      Permission.Action.READ);

    put2(htable);
    admin.snapshot(snapshot2, table);
    admin.split(table, "3");
    admin.split(table, "4");
    admin.flush(table);
    admin.majorCompact(table);

    Assert.assertTrue(canUserScanSnapshot(conf, fs, grantUser, snapshot));
    Assert.assertTrue(canUserScanSnapshot(conf, fs, grantUser, snapshot2));
  }

  @Test
  public void testGrantAndRevokeGlobal() throws Exception {
    String namespace = NAMESPACE_PREFIX + "testGrantAndRevokeGlobal";
    String namespace2 = NAMESPACE_PREFIX + "testGrantAndRevokeGlobal2";
    String table = namespace + ":" + "testGrantAndRevokeGlobal";
    String table2 = namespace2 + ":" + "testGrantAndRevokeGlobal";
    String snapshot = "testGrantAndRevokeGlobal" + SNAPSHOT_POSTFIX;
    String snapshot2 = "testGrantAndRevokeGlobal2" + SNAPSHOT_POSTFIX;

    createNamespace(admin, namespace);
    HTable hTable = createTable(TEST_UTIL, table);
    SecureTestUtil.grantGlobal(TEST_UTIL, GRANT_USER, Permission.Action.READ);
    put(hTable);
    admin.snapshot(snapshot, table);

    createNamespace(admin, namespace2);
    HTable hTable2 = createTable(TEST_UTIL, table2);
    put2(hTable2);
    admin.snapshot(snapshot2, table2);

    Assert.assertTrue(canUserScanSnapshot(conf, fs, grantUser, snapshot));
    Assert.assertFalse(canUserScanSnapshot(conf, fs, unGrantUser, snapshot));
    Assert.assertTrue(canUserScanSnapshot(conf, fs, grantUser, snapshot2));
    Assert.assertFalse(canUserScanSnapshot(conf, fs, unGrantUser, snapshot2));

    SecureTestUtil.revokeGlobal(TEST_UTIL, GRANT_USER, Permission.Action.READ);
    Assert.assertFalse(canUserScanSnapshot(conf, fs, grantUser, snapshot));
    Assert.assertFalse(canUserScanSnapshot(conf, fs, grantUser, snapshot2));
  }

  @Test
  public void testSnapshotScannerReadRestoredTable() throws Exception {
    String tableName = "testSnapshotScannerReadRestoredTable";
    try (HTable table = createTable(TEST_UTIL, tableName)) {
      put(table);
    }
    String snapshotName = "snapshot-" + tableName;
    admin.snapshot(snapshotName, tableName);
    admin.disableTable(tableName);
    admin.deleteTable(tableName);
    Assert.assertEquals(false, admin.tableExists(TableName.valueOf(tableName)));
    Assert.assertFalse(canUserScanSnapshot(conf, fs, grantUser, snapshotName));

    admin.restoreSnapshot(snapshotName);
    Assert.assertEquals(true, admin.tableExists(TableName.valueOf(tableName)));

    waitTableRegionsOnline(TEST_UTIL, TableName.valueOf(tableName));
    SecureTestUtil.grantOnTable(TEST_UTIL, GRANT_USER, TableName.valueOf(tableName), null, null,
      Permission.Action.READ);

    Assert.assertTrue(canUserScanSnapshot(conf, fs, grantUser, snapshotName));
  }

  private static final int WAIT_TIME = 10000;
  private void waitTableRegionsOnline(final HBaseTestingUtility util, TableName tableName)
      throws IOException {
    int regionNum = admin.getTableRegions(tableName).size();
    util.waitFor(WAIT_TIME, 100, new Waiter.Predicate<IOException>() {
      @Override
      public boolean evaluate() throws IOException {
        int onlineRegionNum = util.getHBaseCluster().getLiveRegionServerThreads().stream()
            .mapToInt(t -> t.getRegionServer().getOnlineRegions(tableName).size()).sum();
        if (regionNum == onlineRegionNum) {
          return true;
        } else {
          LOG.info("Table:" + tableName.getNameAsString() + ", region num:" + regionNum
              + ", online region num:" + onlineRegionNum);
          return false;
        }
      }
    });
  }

  public static boolean canUserScanSnapshot(Configuration conf, FileSystem fs, User user, String snapshot)
    throws IOException, InterruptedException {
    PrivilegedExceptionAction<Boolean> action = getScanSnapshotAction(conf, fs, snapshot);
    return user.runAs(action);
  }

  private static PrivilegedExceptionAction<Boolean> getScanSnapshotAction(Configuration conf, FileSystem fs,
                                                                          String snapshotName) throws IOException {
    Path restoreDir = new Path(HConstants.SNAPSHOT_RESTORE_TMP_DIR_DEFAULT, snapshotName);
    fs.mkdirs(restoreDir);
    Path path = restoreDir;
    while (path != null) {
      fs.setPermission(path, new FsPermission((short) 0757));
      path = path.getParent();
    }

    PrivilegedExceptionAction<Boolean> action = () -> {
      try {
        Scan scan = new Scan();
        TableSnapshotScanner scanner =
          new TableSnapshotScanner(conf, restoreDir, snapshotName, scan);
        while (true) {
          Result result = scanner.next();
          if (result == null) {
            break;
          }
          getRowCnt(result);
        }
        scanner.close();
        return true;
      } catch (IOException e) {
        e.printStackTrace();
      }
      return false;
    };
    return action;
  }

  private static int getRowCnt(Result result) throws IOException {
    try {
      int cnt = 0;
      CellScanner scanner = result.cellScanner();
      while (scanner.advance()) {
        Cell cell = scanner.current();
        if (cell != null) {
          cnt++;
        }
      }
      return cnt;
    } catch (Exception e) {
      e.printStackTrace();
    }
    return 0;
  }

  public static void createNamespace(HBaseAdmin admin, String namespace) throws IOException {
    NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(namespace).build();
    admin.createNamespace(namespaceDescriptor);
  }

  private static final String COLUMN1 = "A";
  private static final String COLUMN2 = "B";

  public static HTable createTable(HBaseTestingUtility testingUtility, String table) throws IOException {
    TableName tableName = TableName.valueOf(table);
    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(new HColumnDescriptor(COLUMN1));
    desc.addFamily(new HColumnDescriptor(COLUMN2));
    byte[][] splits = new byte[2][];
    splits[0] = Bytes.toBytes("1");
    splits[1] = Bytes.toBytes("2");
    return testingUtility.createTable(desc, splits);
  }

  public static void put(HTable hTable) throws Exception { // row cnt is 12
    List<Put> puts = new ArrayList<>();
    for (int i = 0; i < 6; i++) {
      Put put = new Put(Bytes.toBytes(i));
      put.add(Bytes.toBytes(COLUMN1), null, Bytes.toBytes(i));
      put.add(Bytes.toBytes(COLUMN2), null, Bytes.toBytes(i + 1));
      puts.add(put);
    }
    hTable.put(puts);
  }

  // put and put2 row cnt is 16
  public static void put2(HTable hTable) throws Exception { // row cnt is 14
    List<Put> puts = new ArrayList<>();
    for (int i = 0; i < 8; i++) {
      if (i == 5) {
        break;
      }
      Put put = new Put(Bytes.toBytes(i));
      put.add(Bytes.toBytes(COLUMN1), null, Bytes.toBytes(i + 2));
      put.add(Bytes.toBytes(COLUMN2), null, Bytes.toBytes(i + 3));
      puts.add(put);
    }
    hTable.put(puts);
  }
}
