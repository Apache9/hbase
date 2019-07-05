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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.Map;

@Category({ SmallTests.class })
public class TestPresetHdfsAclTool {
  private static Log LOG = LogFactory.getLog(TestPresetHdfsAclTool.class);

  private static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static Configuration conf = TEST_UTIL.getConfiguration();
  private static HBaseAdmin admin = null;
  private static FileSystem fs = null;
  private static Path rootDir = null;
  private static User grantUser = null;

  private static final String NAMESPACE_PREFIX = "ns_";
  private static final String SNAPSHOT_POSTFIX = "_snapshot";
  private static final String GRANT_USER = "grant_user";

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    conf.setBoolean("dfs.namenode.acls.enabled", true);
    conf.set("fs.permissions.umask-mode", "027");
    conf.setBoolean(HConstants.ENABLE_DATA_FILE_UMASK, true);
    conf.set(User.HBASE_SECURITY_CONF_KEY, "simple");
    SecureTestUtil.enableSecurity(conf);
    TEST_UTIL.startMiniCluster();

    admin = TEST_UTIL.getHBaseAdmin();
    rootDir = TEST_UTIL.getHBaseCluster().getMaster().getMasterFileSystem().getRootDir();
    fs = rootDir.getFileSystem(conf);
    grantUser = User.createUserForTesting(conf, GRANT_USER, new String[] {});

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
    Path tmpDir = new Path(rootDir, HConstants.HBASE_TEMP_DIRECTORY);
    Assert.assertTrue(fs.exists(tmpDir));
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testGrantNamespace() throws Exception {
    String namespace = NAMESPACE_PREFIX + "testGrantNamespace";
    String table = namespace + ":" + "testGrantNamespace";
    String snapshot = "testGrantNamespace" + SNAPSHOT_POSTFIX;
    String snapshot2 = "testGrantNamespace2" + SNAPSHOT_POSTFIX;

    createNamespace(namespace);
    HTable hTable = createTable(table);
    put(hTable);
    admin.snapshot(snapshot, table);
    SecureTestUtil.grantOnNamespace(TEST_UTIL, GRANT_USER, namespace, READ);
    admin.snapshot(snapshot2, table);
    hTable.close();

    presetHdfsAcl();

    Assert.assertTrue(canUserScanSnapshot(grantUser, snapshot));
    Assert.assertTrue(canUserScanSnapshot(grantUser, snapshot2));
  }

  @Test
  public void testGrantTable() throws Exception {
    String namespace = NAMESPACE_PREFIX + "testGrantTable";
    String table = namespace + ":" + "testGrantTable";
    String snapshot = "testGrantTable" + SNAPSHOT_POSTFIX;
    String snapshot2 = "testGrantTable2" + SNAPSHOT_POSTFIX;

    createNamespace(namespace);
    HTable hTable = createTable(table);
    put(hTable);
    admin.snapshot(snapshot, table);
    grantOnTable(GRANT_USER, table, READ);
    put2(hTable);
    admin.snapshot(snapshot2, table);
    hTable.close();

    presetHdfsAcl();

    Assert.assertTrue(canUserScanSnapshot(grantUser, snapshot));
    Assert.assertTrue(canUserScanSnapshot(grantUser, snapshot2));
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

    presetHdfsAcl();

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

    presetHdfsAcl();

    Assert.assertTrue(canUserScanSnapshot(grantUser, snapshot));
    Assert.assertTrue(canUserScanSnapshot(grantUser, snapshot2));
  }

  @Test
  public void testCheckDuplicateHBaseGrants() throws Exception {
    String ns = "ns_testCheckDuplicateHBaseGrants";
    TableName tbName = TableName.valueOf(ns, "testCheckDuplicateHBaseGrants");
    createNamespace(ns);
    try (HTable table = createTable(tbName.toString())) {
      PresetHdfsAclTool tool = new PresetHdfsAclTool(conf);
      Assert.assertEquals(0, tool.checkDuplicateGrantInHBaseACL());

      SecureTestUtil.grantOnNamespace(TEST_UTIL, GRANT_USER, ns, READ, WRITE);
      SecureTestUtil.grantOnTable(TEST_UTIL, GRANT_USER, tbName, null, null, READ, WRITE);
      Assert.assertEquals(1, tool.checkDuplicateGrantInHBaseACL());

      SecureTestUtil.grantOnTable(TEST_UTIL, GRANT_USER, tbName, Bytes.toBytes("A"), null, WRITE);
      Assert.assertEquals(3, tool.checkDuplicateGrantInHBaseACL());

      SecureTestUtil.grantOnTable(TEST_UTIL, GRANT_USER, tbName, Bytes.toBytes("A"),
        Bytes.toBytes("Q"), WRITE);
      Assert.assertEquals(6, tool.checkDuplicateGrantInHBaseACL());
    }
  }

  @Test
  public void testCheckHdfsAclExceeded() throws Exception {
    // 2 global
    String colon = ":";
    String defaultNs = "default"; // 1 -> 3
    String ns1 = "ns1"; // 2 -> 4
    String ns2 = "ns2"; // 0 -> 2
    String t1 = "t1"; // 1 -> 4
    String t2 = "t2"; // 0 -> 3
    String ns1T1 = ns1 + colon + t1; // 2 -> 6
    String ns1T2 = ns1 + colon + t2; // 1 -> 5
    String ns2T1 = ns2 + colon + t1; // 1 -> 3

    String globalUser1 = "h_g1";
    String globalUser2 = "u_g2";
    String globalUser3 = "hbase_tst_admin";
    String nsUser1 = "h_n1";
    String nsUser2 = "h_n2";
    String nsUser3 = "h_n3";
    String tableUser1 = "u_t1";
    String tableUser2 = "h_t2";
    String tableUser3 = "h_t3";

    createNamespace(ns1);
    createNamespace(ns2);
    createTable(t1);
    createTable(t2);
    createTable(ns1T1);
    createTable(ns1T2);
    createTable(ns2T1);

    SecureTestUtil.grantGlobal(TEST_UTIL, globalUser1, READ);
    SecureTestUtil.grantGlobal(TEST_UTIL, globalUser2, READ);
    SecureTestUtil.grantGlobal(TEST_UTIL, globalUser3, READ);
    SecureTestUtil.grantOnNamespace(TEST_UTIL, nsUser3, defaultNs, READ);
    SecureTestUtil.grantOnNamespace(TEST_UTIL, nsUser1, ns1, READ);
    SecureTestUtil.grantOnNamespace(TEST_UTIL, nsUser2, ns1, READ);
    grantOnTable(tableUser1, t1, READ);
    grantOnTable(tableUser1, ns1T1, READ);
    grantOnTable(tableUser2, ns1T1, READ);
    grantOnTable(tableUser3, ns1T2, READ);
    grantOnTable(nsUser1, ns1T2, READ);
    grantOnTable(tableUser1, ns2T1, READ);
    SecureTestUtil.grantOnTable(TEST_UTIL, GRANT_USER, TableName.valueOf(t1), Bytes.toBytes("A"), null, WRITE);

    conf.setBoolean(HConstants.HDFS_ACL_ENABLE, true);
    PresetHdfsAclTool tool = new PresetHdfsAclTool(conf);
    Map<String, Integer> userNumMap = tool.checkHdfsAclEntryExceeded();
    Assert.assertEquals(3, userNumMap.get(defaultNs).intValue());
    Assert.assertEquals(4, userNumMap.get(ns1).intValue());
    Assert.assertEquals(4, userNumMap.get(t1).intValue());
    Assert.assertEquals(6, userNumMap.get(ns1T1).intValue());
    Assert.assertEquals(5, userNumMap.get(ns1T2).intValue());
    Assert.assertEquals(3, userNumMap.get(ns2T1).intValue());

    SecureTestUtil.revokeGlobal(TEST_UTIL, globalUser1);
    SecureTestUtil.revokeGlobal(TEST_UTIL, globalUser2);
    SecureTestUtil.revokeGlobal(TEST_UTIL, globalUser3);
    SecureTestUtil.revokeFromNamespace(TEST_UTIL, nsUser3, defaultNs, READ);
    SecureTestUtil.revokeFromNamespace(TEST_UTIL, nsUser1, ns1, READ);
    SecureTestUtil.revokeFromNamespace(TEST_UTIL, nsUser2, ns1, READ);
    revokeFromTable(tableUser1, t1, READ);
    revokeFromTable(tableUser1, ns1T1, READ);
    revokeFromTable(tableUser2, ns1T1, READ);
    revokeFromTable(tableUser3, ns1T2, READ);
    revokeFromTable(nsUser1, ns1T2, READ);
    revokeFromTable(tableUser1, ns2T1, READ);
  }

  @Test
  public void testPresetHdfsAclWithInclude() throws Exception{
    String table1 = "testPresetHdfsAclWithInclude1";
    String table2 = "testPresetHdfsAclWithInclude2";
    String snapshot1 = "testPresetHdfsAclWithInclude1" + SNAPSHOT_POSTFIX;
    String snapshot2 = "testPresetHdfsAclWithInclude2" + SNAPSHOT_POSTFIX;
    createTable(table1);
    createTable(table2);
    grantOnTable(GRANT_USER, table1, READ);
    grantOnTable(GRANT_USER, table2, READ);
    admin.snapshot(snapshot1, table1);
    admin.snapshot(snapshot2, table2);
    conf.setBoolean(HConstants.HDFS_ACL_ENABLE, true);
    PresetHdfsAclTool tool = new PresetHdfsAclTool(conf);
    tool.presetHdfsAcl(new String[]{"", "include", table1});

    Assert.assertTrue(canUserScanSnapshot(grantUser, snapshot1));
    Assert.assertFalse(canUserScanSnapshot(grantUser, snapshot2));
  }

  @Test
  public void testPresetHdfsAclWithIncludeNsPerm() throws Exception {
    String ns = "testPresetHdfsAclWithIncludeNsPerm";
    String table = TableName.valueOf(ns, "testPresetHdfsAclWithIncludeNsPerm").toString();
    String snapshot = "testPresetHdfsAclWithIncludeNsPerm" + SNAPSHOT_POSTFIX;
    String nsUser = "nsUser";
    String cfUser = "cfUser";

    createNamespace(ns);
    HTable hTable = createTable(table);
    hTable.close();
    grantOnTable(GRANT_USER, table, READ);
    SecureTestUtil.grantOnTable(TEST_UTIL, cfUser, TableName.valueOf(table), Bytes.toBytes("f"), Bytes.toBytes("q"), READ);
    SecureTestUtil.grantOnNamespace(TEST_UTIL, nsUser, ns, READ);
    admin.snapshot(snapshot, table);
    conf.setBoolean(HConstants.HDFS_ACL_ENABLE, true);
    PresetHdfsAclTool tool = new PresetHdfsAclTool(conf);
    tool.presetHdfsAcl(new String[]{"", "include", table});
    Assert.assertTrue(canUserScanSnapshot(grantUser, snapshot));
    Assert.assertTrue(canUserScanSnapshot(User.createUserForTesting(conf, nsUser, new String[]{}), snapshot));
    Assert.assertFalse(canUserScanSnapshot(User.createUserForTesting(conf, cfUser, new String[]{}), snapshot));
  }

  private void presetHdfsAcl() {
    conf.setBoolean(HConstants.HDFS_ACL_ENABLE, true);
    PresetHdfsAclTool tool = new PresetHdfsAclTool(conf);
    tool.presetHdfsAcl(null);
  }

  private void revokeFromTable(String user, String table, Permission.Action... actions) throws Exception{
    SecureTestUtil.revokeFromTable(TEST_UTIL, user, TableName.valueOf(table), null, null, actions);
  }

  private void grantOnTable(String user, String table, Permission.Action... actions) throws Exception{
    SecureTestUtil.grantOnTable(TEST_UTIL, user, TableName.valueOf(table), null, null, actions);
  }

  private void createNamespace(String namespace) throws IOException{
    TestHdfsAclManager.createNamespace(admin, namespace);
  }

  private HTable createTable(String table) throws IOException{
    return TestHdfsAclManager.createTable(TEST_UTIL, conf, table);
  }

  private void put(HTable hTable) throws IOException{
    TestHdfsAclManager.put(hTable);
  }

  private void put2(HTable hTable) throws IOException{
    TestHdfsAclManager.put2(hTable);
  }

  private boolean canUserScanSnapshot(User user, String snapshot) throws IOException, InterruptedException{
    return TestHdfsAclManager.canUserScanSnapshot(conf, fs, user, snapshot);
  }
}
