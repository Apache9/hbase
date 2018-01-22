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
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ SmallTests.class })
public class TestPresetHdfsAclTool {
	private static Log LOG = LogFactory.getLog(TestHdfsAclManager.class);

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
		String snapshot = "testGrantNamespace" + SNAPSHOT_POSTFIX;
		String snapshot2 = "testGrantNamespace2" + SNAPSHOT_POSTFIX;

		TestHdfsAclManager.createNamespace(admin, namespace);
		HTable hTable = TestHdfsAclManager.createTable(TEST_UTIL, table);
		TestHdfsAclManager.put(hTable);
		admin.snapshot(snapshot, table);
		SecureTestUtil.grantOnNamespace(TEST_UTIL, GRANT_USER, namespace, Permission.Action.READ);
		admin.snapshot(snapshot2, table);

		presetHdfsAcl();

		Assert.assertTrue(TestHdfsAclManager.canUserScanSnapshot(conf, fs, grantUser, snapshot));
		Assert.assertTrue(TestHdfsAclManager.canUserScanSnapshot(conf, fs, grantUser, snapshot2));
	}

	@Test
	public void testGrantTable() throws Exception {
		String namespace = NAMESPACE_PREFIX + "testGrantTable";
		String table = namespace + ":" + "testGrantTable";
		String snapshot = "testGrantTable" + SNAPSHOT_POSTFIX;
		String snapshot2 = "testGrantTable2" + SNAPSHOT_POSTFIX;

		TestHdfsAclManager.createNamespace(admin, namespace);
		HTable hTable = TestHdfsAclManager.createTable(TEST_UTIL, table);
		TestHdfsAclManager.put(hTable);
		admin.snapshot(snapshot, table);
		SecureTestUtil.grantOnTable(TEST_UTIL, GRANT_USER, TableName.valueOf(table), null, null,
			Permission.Action.READ);
		TestHdfsAclManager.put2(hTable);
		admin.snapshot(snapshot2, table);

		presetHdfsAcl();

		Assert.assertTrue(TestHdfsAclManager.canUserScanSnapshot(conf, fs, grantUser, snapshot));
		Assert.assertTrue(TestHdfsAclManager.canUserScanSnapshot(conf, fs, grantUser, snapshot2));
	}

	@Test
	public void testCompactTable() throws Exception {
		String table = "testCompactTable";
		String snapshot = "testCompactTable" + SNAPSHOT_POSTFIX;
		String snapshot2 = "testCompactTable2" + SNAPSHOT_POSTFIX;

		HTable htable = TestHdfsAclManager.createTable(TEST_UTIL, table);
		TestHdfsAclManager.put(htable);
		admin.snapshot(snapshot, table);
		SecureTestUtil.grantOnTable(TEST_UTIL, GRANT_USER, TableName.valueOf(table), null, null,
			Permission.Action.READ);

		TestHdfsAclManager.put2(htable);
		admin.snapshot(snapshot2, table);
		admin.flush(table);
		admin.majorCompact(table);

		presetHdfsAcl();

		Assert.assertTrue(TestHdfsAclManager.canUserScanSnapshot(conf, fs, grantUser, snapshot));
		Assert.assertTrue(TestHdfsAclManager.canUserScanSnapshot(conf, fs, grantUser, snapshot2));
	}

	@Test
	public void testSplitTable() throws Exception {
		String table = "testSplitTable";
		String snapshot = "testSplitTable" + SNAPSHOT_POSTFIX;
		String snapshot2 = "testSplitTable2" + SNAPSHOT_POSTFIX;

		HTable htable = TestHdfsAclManager.createTable(TEST_UTIL, table);
		TestHdfsAclManager.put(htable);
		admin.snapshot(snapshot, table);
		SecureTestUtil.grantOnTable(TEST_UTIL, GRANT_USER, TableName.valueOf(table), null, null,
			Permission.Action.READ);

		TestHdfsAclManager.put2(htable);
		admin.snapshot(snapshot2, table);
		admin.split(table, "3");
		admin.split(table, "4");
		admin.flush(table);
		admin.majorCompact(table);

		presetHdfsAcl();

		Assert.assertTrue(TestHdfsAclManager.canUserScanSnapshot(conf, fs, grantUser, snapshot));
		Assert.assertTrue(TestHdfsAclManager.canUserScanSnapshot(conf, fs, grantUser, snapshot2));
	}

	private void presetHdfsAcl(){
		conf.setBoolean(HConstants.HDFS_ACL_ENABLE, true);
		PresetHdfsAclTool tool = new PresetHdfsAclTool(conf);
		tool.run();
	}
}