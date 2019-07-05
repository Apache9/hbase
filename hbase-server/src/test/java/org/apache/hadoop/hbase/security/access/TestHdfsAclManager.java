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

import static org.apache.hadoop.hbase.security.access.Permission.Action.READ;
import static org.apache.hadoop.hbase.security.access.Permission.Action.WRITE;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclEntryScope;
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
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Category({ MediumTests.class })
public class TestHdfsAclManager {
  private static Log LOG = LogFactory.getLog(TestHdfsAclManager.class);
  @Rule
  public TestName name = new TestName();

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
    conf.setBoolean(HConstants.HDFS_ACL_ENABLE, true);
    conf.set(User.HBASE_SECURITY_CONF_KEY, "simple");
    SecureTestUtil.enableSecurity(conf);
    TEST_UTIL.startMiniCluster();

    admin = TEST_UTIL.getHBaseAdmin();
    rootDir = TEST_UTIL.getHBaseCluster().getMaster().getMasterFileSystem().getRootDir();
    fs = rootDir.getFileSystem(conf);
    grantUser = User.createUserForTesting(conf, GRANT_USER, new String[] {});
    unGrantUser = User.createUserForTesting(conf, UN_GRANT_USER, new String[] {});

    FsPermission publicFilePermission = HdfsAclManager.getHdfsAclEnabledPublicFilePermission(conf);
    Path path = rootDir;
    while (path != null) {
      fs.setPermission(path, publicFilePermission);
      path = path.getParent();
    }

    Path restoreDir = new Path(HConstants.SNAPSHOT_RESTORE_TMP_DIR_DEFAULT);
    if (!fs.exists(restoreDir)) {
      fs.mkdirs(restoreDir);
      fs.setPermission(restoreDir, HdfsAclManager.getHdfsAclEnabledRestoreFilePermission(conf));
    }
    path = restoreDir.getParent();
    while (path != null) {
      fs.setPermission(path, publicFilePermission);
      path = path.getParent();
    }
    TEST_UTIL.waitTableAvailable(AccessControlLists.ACL_GLOBAL_NAME);
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

    createNamespace(namespace);

    // create table -> grant ns
    HTable hTable = createTable(table);
    put(hTable);
    admin.snapshot(snapshot, table);
    SecureTestUtil.grantOnNamespace(TEST_UTIL, GRANT_USER, namespace, READ);

    // grant ns -> create table
    HTable hTable2 = createTable(table2);
    put(hTable2);
    admin.snapshot(snapshot2, table2);
    hTable.close();
    hTable2.close();

    Assert.assertTrue(canUserScanSnapshot(grantUser, snapshot));
    Assert.assertFalse(canUserScanSnapshot(unGrantUser, snapshot));

    Assert.assertTrue(canUserScanSnapshot(grantUser, snapshot2));
    Assert.assertFalse(canUserScanSnapshot(unGrantUser, snapshot2));
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
    createNamespace(namespace);
    HTable hTable = createTable(table);
    put(hTable);
    admin.snapshot(snapshot, table);
    SecureTestUtil.grantOnNamespace(TEST_UTIL, GRANT_USER, namespace, READ);
    SecureTestUtil.revokeFromNamespace(TEST_UTIL, GRANT_USER, namespace, READ);

    // revoke ns -> snapshot
    HTable hTable2 = createTable(table2);
    put(hTable2);
    admin.snapshot(snapshot2, table2);
    admin.snapshot(snapshot3, table);
    hTable.close();
    hTable2.close();

    Assert.assertFalse(canUserScanSnapshot(grantUser, snapshot));
    Assert.assertFalse(canUserScanSnapshot(unGrantUser, snapshot));

    Assert.assertFalse(canUserScanSnapshot(grantUser, snapshot2));
    Assert.assertFalse(canUserScanSnapshot(unGrantUser, snapshot2));

    Assert.assertFalse(canUserScanSnapshot(grantUser, snapshot3));
  }

  @Test
  public void testGrantTable() throws Exception {
    String namespace = NAMESPACE_PREFIX + "testGrantTable";
    String table = namespace + ":" + "testGrantTable";
    String snapshot = "testGrantTable" + SNAPSHOT_POSTFIX;
    String snapshot2 = "testGrantTable2" + SNAPSHOT_POSTFIX;

    // snapshot -> grant table
    createNamespace(namespace);
    HTable hTable = createTable(table);
    put(hTable);
    admin.snapshot(snapshot, table);
    SecureTestUtil.grantOnTable(TEST_UTIL, GRANT_USER, TableName.valueOf(table), Bytes.toBytes("f"), Bytes.toBytes("q"), READ);
    grantOnTable(GRANT_USER, table, READ);

    // grant table -> snapshot
    put(hTable);
    admin.snapshot(snapshot2, table);
    hTable.close();

    Assert.assertTrue(canUserScanSnapshot(grantUser, snapshot));
    Assert.assertFalse(canUserScanSnapshot(unGrantUser, snapshot));

    Assert.assertTrue(canUserScanSnapshot(grantUser, snapshot2));
    Assert.assertFalse(canUserScanSnapshot(unGrantUser, snapshot2));
  }

  @Test
  public void testGrantTableFamily() throws Exception {
    String table = "testGrantTableFamily";
    String snapshot = "testGrantTableFamily" + SNAPSHOT_POSTFIX;

    HTable hTable = createTable(table);
    put(hTable);
    admin.snapshot(snapshot, table);
    SecureTestUtil.grantOnTable(TEST_UTIL, GRANT_USER, TableName.valueOf(table), Bytes.toBytes("f"),
      null, Permission.Action.READ);
    hTable.close();

    Assert.assertFalse(canUserScanSnapshot(grantUser, snapshot));
  }

  @Test
  public void testGrantTableQualifier() throws Exception {
    String table = "testGrantTableQualifier";
    String snapshot = "testGrantTableQualifier" + SNAPSHOT_POSTFIX;

    HTable hTable = createTable(table);
    put(hTable);
    admin.snapshot(snapshot, table);
    hTable.close();
    SecureTestUtil.grantOnTable(TEST_UTIL, GRANT_USER, TableName.valueOf(table), null,
      Bytes.toBytes("q"), READ);

    Assert.assertFalse(canUserScanSnapshot(grantUser, snapshot));
  }

  @Test
  public void testRevokeTable() throws Exception {
    String namespace = NAMESPACE_PREFIX + "testRevokeTable";
    String table = namespace + ":" + "testRevokeTable";
    String snapshot = "testRevokeTable" + SNAPSHOT_POSTFIX;
    String snapshot2 = "testRevokeTable2" + SNAPSHOT_POSTFIX;

    // snapshot -> revoke table
    createNamespace(namespace);
    HTable hTable = createTable(table);
    grantOnTable(GRANT_USER, table, READ);
    put(hTable);
    hTable.close();
    admin.snapshot(snapshot, table);
    SecureTestUtil.revokeFromTable(TEST_UTIL, GRANT_USER, TableName.valueOf(table), null, null,
      READ);

    // revoke table -> snapshot
    admin.snapshot(snapshot2, table);

    Assert.assertFalse(canUserScanSnapshot(grantUser, snapshot));
    Assert.assertFalse(canUserScanSnapshot(grantUser, snapshot2));
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
    HTable hTable = createTable(table);
    put(hTable);
    grantOnTable(GRANT_USER, table, READ);
    admin.snapshot(snapshot, table);
    admin.disableTable(table);
    admin.deleteTable(table);
    Assert.assertTrue(canUserScanSnapshot(grantUser, snapshot));

    // create same name table, snapshot => can not scan new snapshot
    hTable = createTable(table);
    put2(hTable);
    admin.snapshot(snapshot2, table);
    hTable.close();
    grantOnTable(user2, table, READ);

    Assert.assertTrue(canUserScanSnapshot(grantUser, snapshot));
    Assert.assertFalse(canUserScanSnapshot(grantUser, snapshot2));
    Assert.assertTrue(canUserScanSnapshot(userUser2, snapshot2));
    //Assert.assertFalse(canUserScanSnapshot(userUser2, snapshot)); // return true
  }

  @Test
  public void testDeleteNamespace() throws Exception {
    String namespace = NAMESPACE_PREFIX + "testDeleteNamespace";
    String table = namespace + ":" + "testDeleteNamespace";
    String table2 = namespace + ":" + "testDeleteNamespace2";
    String snapshot = "testDeleteNamespace" + SNAPSHOT_POSTFIX;
    String snapshot2 = "testDeleteNamespace2" + SNAPSHOT_POSTFIX;

    // create ns, grant ns, snapshot -> ns table => can scan snapshot
    createNamespace(namespace);
    HTable hTable = createTable(table);
    put(hTable);
    SecureTestUtil.grantOnNamespace(TEST_UTIL, GRANT_USER, namespace, READ);
    admin.snapshot(snapshot, table);
    admin.disableTable(table);
    admin.deleteTable(table);
    admin.deleteNamespace(namespace);
    hTable.close();
    Assert.assertTrue(canUserScanSnapshot(grantUser, snapshot));

    // create same name ns, snapshot => can not scan new snapshot
    createNamespace(namespace);
    createTable(table2);
    admin.snapshot(snapshot2, table2);

    Assert.assertTrue(canUserScanSnapshot(grantUser, snapshot));
    Assert.assertFalse(canUserScanSnapshot(grantUser, snapshot2));
  }

  @Test
  public void testTruncateTable() throws Exception {
    String namespace = "testTruncateTable_ns";
    String table = namespace+":"+"testTruncateTable";
    String snapshot = "testTruncateTable" + SNAPSHOT_POSTFIX;
    String snapshot2 = "testTruncateTable2" + SNAPSHOT_POSTFIX;
    String grantUser2 = "grantUser2";
    User user2 = User.createUserForTesting(conf, grantUser2, new String[] {});

    createNamespace(namespace);
    HTable htable = createTable(table);
    put(htable);
    admin.snapshot(snapshot, table);

    SecureTestUtil.grantOnNamespace(TEST_UTIL, grantUser2, namespace, READ);
    grantOnTable(GRANT_USER, table, READ);
    admin.disableTable(table);
    admin.truncateTable(TableName.valueOf(table), true);
    Assert.assertTrue(canUserScanSnapshot(grantUser, snapshot));
    Assert.assertTrue(canUserScanSnapshot(user2, snapshot));

    put(htable);
    put2(htable);
    admin.snapshot(snapshot2, table);
    admin.flush(table);
    admin.majorCompact(table);
    htable.close();
    Assert.assertTrue(canUserScanSnapshot(grantUser, snapshot));
    Assert.assertTrue(canUserScanSnapshot(grantUser, snapshot2));
    Assert.assertTrue(canUserScanSnapshot(user2, snapshot));
    Assert.assertTrue(canUserScanSnapshot(user2, snapshot2));
  }

  @Test
  public void testCompactTable() throws Exception {
    String table = "testCompactTable";
    String snapshot = "testCompactTable" + SNAPSHOT_POSTFIX;
    String snapshot2 = "testCompactTable2" + SNAPSHOT_POSTFIX;

    HTable htable = createTable(table);
    put(htable);
    admin.snapshot(snapshot, table);
    grantOnTable(GRANT_USER, table, READ);

    put2(htable);
    admin.snapshot(snapshot2, table);
    admin.flush(table);
    admin.majorCompact(table);
    htable.close();

    Assert.assertTrue(canUserScanSnapshot(grantUser, snapshot));
    Assert.assertTrue(canUserScanSnapshot(grantUser, snapshot2));
  }

  @Test
  public void testSplitTable() throws Exception {
    String table = "testSplitTable";
    String snapshot = "testSplitTable" + SNAPSHOT_POSTFIX;
    String snapshot2 = "testSplitTable2" + SNAPSHOT_POSTFIX;

    HTable htable = createTable(table);
    put(htable);
    admin.snapshot(snapshot, table);
    grantOnTable(GRANT_USER, table, READ);

    put2(htable);
    admin.snapshot(snapshot2, table);
    admin.split(table, "3");
    admin.split(table, "4");
    admin.flush(table);
    admin.majorCompact(table);
    htable.close();

    Assert.assertTrue(canUserScanSnapshot(grantUser, snapshot));
    Assert.assertTrue(canUserScanSnapshot(grantUser, snapshot2));
  }

  @Test
  public void testGrantAndRevokeGlobal() throws Exception {
    String namespace = NAMESPACE_PREFIX + "testGrantAndRevokeGlobal";
    String namespace2 = NAMESPACE_PREFIX + "testGrantAndRevokeGlobal2";
    String table = namespace + ":" + "testGrantAndRevokeGlobal";
    String table2 = namespace2 + ":" + "testGrantAndRevokeGlobal";
    String snapshot = "testGrantAndRevokeGlobal" + SNAPSHOT_POSTFIX;
    String snapshot2 = "testGrantAndRevokeGlobal2" + SNAPSHOT_POSTFIX;

    createNamespace(namespace);
    HTable hTable = createTable(table);
    SecureTestUtil.grantGlobal(TEST_UTIL, GRANT_USER, READ);
    put(hTable);
    admin.snapshot(snapshot, table);

    createNamespace(namespace2);
    HTable hTable2 = createTable(table2);
    put2(hTable2);
    admin.snapshot(snapshot2, table2);
    hTable.close();
    hTable2.close();

    Assert.assertTrue(canUserScanSnapshot(grantUser, snapshot));
    Assert.assertFalse(canUserScanSnapshot(unGrantUser, snapshot));
    Assert.assertTrue(canUserScanSnapshot(grantUser, snapshot2));
    Assert.assertFalse(canUserScanSnapshot(unGrantUser, snapshot2));

    SecureTestUtil.revokeGlobal(TEST_UTIL, GRANT_USER, READ);
    Assert.assertFalse(canUserScanSnapshot(grantUser, snapshot));
    Assert.assertFalse(canUserScanSnapshot(grantUser, snapshot2));
  }

  @Test
  public void testGranNsAndGrantTable() throws Exception {
    String namespace = NAMESPACE_PREFIX + "testGranNsAndGrantNs";
    String table = namespace + ":" + "testGranNsAndGrantNs";
    String snapshot = "testGranNsAndGrantTable" + SNAPSHOT_POSTFIX;

    createNamespace(namespace);
    HTable hTable = createTable(table);
    put(hTable);
    admin.snapshot(snapshot, table);
    SecureTestUtil.grantOnNamespace(TEST_UTIL, GRANT_USER, namespace, WRITE);
    grantOnTable(GRANT_USER, table, READ); // skip set acl because already set ns acl
    hTable.close();

    Assert.assertTrue(canUserScanSnapshot(grantUser, snapshot));
  }

  @Test
  public void testGranNsAndRevokeTable() throws Exception {
    String namespace = NAMESPACE_PREFIX + "testGranNsAndRevokeTable";
    String table = namespace + ":" + "testGranNsAndRevokeTable";
    String snapshot = "testGranNsAndRevokeTable" + SNAPSHOT_POSTFIX;

    createNamespace(namespace);
    HTable hTable = createTable(table);
    put(hTable);
    admin.snapshot(snapshot, table);
    grantOnTable(GRANT_USER, table, READ);
    SecureTestUtil.grantOnNamespace(TEST_UTIL, GRANT_USER, namespace, READ);
    SecureTestUtil.revokeFromTable(TEST_UTIL, GRANT_USER, TableName.valueOf(table), null, null,
            READ); // skip remove acl because ns read perm
    hTable.close();

    Assert.assertTrue(canUserScanSnapshot(grantUser, snapshot));
  }

  @Test
  public void testGrantTableAndRevokeNs() throws Exception {
    String namespace = NAMESPACE_PREFIX + "testGrantTableAndRevokeNs";
    String table = namespace + ":" + "testGrantTableAndRevokeNs";
    String table2 = namespace + ":" + "testGrantTableAndRevokeNs2";
    String snapshot = "testGrantTableAndRevokeNs" + SNAPSHOT_POSTFIX;
    String snapshot2 = "testGrantTableAndRevokeNs2" + SNAPSHOT_POSTFIX;

    createNamespace(namespace);
    HTable hTable = createTable(table);
    put(hTable);
    admin.snapshot(snapshot, table);

    HTable hTable2 = createTable(table2);
    put(hTable2);
    admin.snapshot(snapshot2, table2);

    SecureTestUtil.grantOnNamespace(TEST_UTIL, GRANT_USER, namespace, READ);
    grantOnTable(GRANT_USER, table, READ);

    SecureTestUtil.revokeFromNamespace(TEST_UTIL, GRANT_USER, namespace, READ);
    hTable.close();

    Assert.assertTrue(canUserScanSnapshot(grantUser, snapshot));
    Assert.assertFalse(canUserScanSnapshot(grantUser, snapshot2));
  }

  @Test
  public void testSnapshotScannerReadRestoredTable() throws Exception {
    String tableName = "testSnapshotScannerReadRestoredTable";
    try (HTable table = createTable(tableName)) {
      put(table);
      table.close();
    }
    String snapshotName = "snapshot-" + tableName;
    admin.snapshot(snapshotName, tableName);
    admin.disableTable(tableName);
    admin.deleteTable(tableName);
    Assert.assertEquals(false, admin.tableExists(TableName.valueOf(tableName)));
    Assert.assertFalse(canUserScanSnapshot(grantUser, snapshotName));

    admin.restoreSnapshot(snapshotName);
    Assert.assertEquals(true, admin.tableExists(TableName.valueOf(tableName)));

    waitTableRegionsOnline(TEST_UTIL, TableName.valueOf(tableName));
    grantOnTable(GRANT_USER, tableName, READ);

    Assert.assertTrue(canUserScanSnapshot(grantUser, snapshotName));
  }

  @Test
  public void testSameUserForNsAndTable() throws Exception {
    String namespace = NAMESPACE_PREFIX + "testSameUser";
    String table = namespace + ":" + "testSameUser";
    String snapshot = "testSameUser" + SNAPSHOT_POSTFIX;

    createNamespace(namespace);
    try (HTable hTable = createTable(table)) {
      put(hTable);
    }
    SecureTestUtil.grantOnNamespace(TEST_UTIL, GRANT_USER, namespace, READ);
    grantOnTable(GRANT_USER, table, READ);
    admin.snapshot(snapshot, table);
    Assert.assertTrue(canUserScanSnapshot(grantUser, snapshot));
  }

  @Test
  public void testDeleteTmpDir() throws Exception {
    String namespace = name.getMethodName();
    String globalUserName = name.getMethodName();
    String nsUserName = "ns-" + name.getMethodName();
    String tableUserName = "t-" + name.getMethodName();
    String tableUserName2 = "t2-" + name.getMethodName();
    TableName table = TableName.valueOf(namespace, namespace);
    String snapshot = namespace + SNAPSHOT_POSTFIX;
    User globalUser = User.createUserForTesting(conf, globalUserName, new String[] {});
    User nsUser = User.createUserForTesting(conf, nsUserName, new String[] {});
    User tableUser = User.createUserForTesting(conf, tableUserName, new String[] {});
    // create namespace
    createNamespace(namespace);
    // grant global, namespace, table
    SecureTestUtil.grantOnNamespace(TEST_UTIL, nsUserName, namespace, READ);
    SecureTestUtil.grantGlobal(TEST_UTIL, globalUserName, READ);
    SecureTestUtil.grantOnTable(TEST_UTIL, tableUserName, table, null, null, READ);
    SecureTestUtil.grantOnTable(TEST_UTIL, tableUserName2, table, Bytes.toBytes("A"), null, READ);
    // force delete tmp directory
    Path tmpDir = new Path(rootDir, HConstants.HBASE_TEMP_DIRECTORY);
    fs.delete(tmpDir, true);
    // use tool to fix tmp dir acl
    PresetHdfsAclTool tool = new PresetHdfsAclTool(conf);
    tool.fixTmpDirectoryAcls(conf, fs);
    // check acl exist
    Path tmpDataDir = new Path(tmpDir, HConstants.BASE_NAMESPACE_DIR);
    Path tmpNsDir = new Path(tmpDataDir, namespace);
    Path tmpTableDir = new Path(tmpNsDir, table.getQualifierAsString());
    checkAclEntryExists(tmpDataDir, globalUserName, true, true);
    checkAclEntryExists(tmpNsDir, globalUserName, true, true);
    checkAclEntryExists(tmpNsDir, nsUserName, true, true);
    checkAclEntryExists(tmpNsDir, tableUserName, true, false);
    checkAclEntryExists(tmpNsDir, tableUserName2, false, false);
    checkAclEntryExists(tmpTableDir, tableUserName, true, true);
    checkAclEntryExists(tmpTableDir, tableUserName2, false, false);
    // create table and global, namespace, table acl can be inherited
    HTable hTable = createTable(table.getNameAsString());
    put(hTable);
    admin.snapshot(snapshot, table);
    Assert.assertTrue(canUserScanSnapshot(globalUser, snapshot));
    Assert.assertTrue(canUserScanSnapshot(nsUser, snapshot));
    Assert.assertTrue(canUserScanSnapshot(tableUser, snapshot));
  }

  @Test
  public void testRestartMaster() throws Exception {
    // create
    String namespace = name.getMethodName();
    createNamespace(namespace);
    // grant user with namespace read permission
    String nsUserName = "ns-"+name.getMethodName();
    SecureTestUtil.grantOnNamespace(TEST_UTIL, nsUserName, namespace, READ);
    // restart master
    TEST_UTIL.shutdownMiniHBaseCluster();
    TEST_UTIL.startMiniHBaseCluster(1, 1);
    TEST_UTIL.waitTableAvailable(AccessControlLists.ACL_GLOBAL_NAME);
    // check tmp dir exists
    Path tmpDir = new Path(rootDir, HConstants.HBASE_TEMP_DIRECTORY);
    Path tmpDataDir = new Path(tmpDir, HConstants.BASE_NAMESPACE_DIR);
    Path tmpNsDir = new Path(tmpDataDir, namespace);
    Assert.assertTrue(fs.exists(tmpNsDir));
    // check tmp ns dir acl exists
    checkAclEntryExists(tmpNsDir, nsUserName, true, true);
  }

  private void checkAclEntryExists(Path path, String userName, boolean requireAccessAcl,
      boolean requireDefaultAcl) throws IOException {
    boolean accessAclEntry = !requireAccessAcl;
    boolean defaultAclEntry = !requireDefaultAcl;
    for (AclEntry aclEntry : fs.getAclStatus(path).getEntries()) {
      String user = aclEntry.getName();
      if (user != null && user.equals(userName)) {
        if (aclEntry.getScope() == AclEntryScope.DEFAULT) {
          defaultAclEntry = true;
        } else if (aclEntry.getScope() == AclEntryScope.ACCESS) {
          accessAclEntry = true;
        }
      }
    }
    Assert.assertTrue(accessAclEntry);
    Assert.assertTrue(defaultAclEntry);
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

  private void grantOnTable(String user, String table, Permission.Action... actions) throws Exception{
    SecureTestUtil.grantOnTable(TEST_UTIL, user, TableName.valueOf(table), null, null, actions);
  }

  private void createNamespace(String namespace) throws IOException{
    createNamespace(admin, namespace);
  }

  private HTable createTable(String table) throws IOException{
    return createTable(TEST_UTIL, conf, table);
  }

  private boolean canUserScanSnapshot(User user, String snapshot) throws IOException, InterruptedException{
    return TestHdfsAclManager.canUserScanSnapshot(conf, fs, user, snapshot);
  }

  public static boolean canUserScanSnapshot(Configuration conf, FileSystem fs, User user, String snapshot)
    throws IOException, InterruptedException {
    PrivilegedExceptionAction<Boolean> action = getScanSnapshotAction(conf, fs, snapshot, user.getName());
    return user.runAs(action);
  }

  private static PrivilegedExceptionAction<Boolean> getScanSnapshotAction(Configuration conf, FileSystem fs,
                                                                          String snapshotName, String userName) {
    PrivilegedExceptionAction<Boolean> action = () -> {
      try {
        Path restoreDir = new Path(HConstants.SNAPSHOT_RESTORE_TMP_DIR_DEFAULT, snapshotName);
        fs.mkdirs(restoreDir);
        fs.setOwner(restoreDir, userName, fs.getFileStatus(restoreDir).getGroup());

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

  private static int getRowCnt(Result result) {
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

  public static HTable createTable(HBaseTestingUtility utility, Configuration conf, String table) throws IOException {
    TableName tableName = TableName.valueOf(table);
    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.setOwner(User.createUserForTesting(conf, "hbase_tst_admin", new String[] {}));
    desc.addFamily(new HColumnDescriptor(COLUMN1));
    desc.addFamily(new HColumnDescriptor(COLUMN2));
    byte[][] splits = new byte[2][];
    splits[0] = Bytes.toBytes("1");
    splits[1] = Bytes.toBytes("2");
    return utility.createTable(desc, splits);
  }

  public static void put(HTable hTable) throws IOException { // row cnt is 12
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
  public static void put2(HTable hTable) throws IOException { // row cnt is 14
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
