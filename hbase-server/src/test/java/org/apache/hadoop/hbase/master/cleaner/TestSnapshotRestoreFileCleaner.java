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
package org.apache.hadoop.hbase.master.cleaner;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.catalog.CatalogTracker;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestSnapshotRestoreFileCleaner {

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static Configuration conf = TEST_UTIL.getConfiguration();
  private Path restoreDir =
      new Path(HConstants.SNAPSHOT_RESTORE_TMP_DIR, HConstants.SNAPSHOT_RESTORE_TMP_DIR_DEFAULT);
  private static DirScanPool POOL;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniDFSCluster(1);
    POOL = new DirScanPool(TEST_UTIL.getConfiguration());
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniDFSCluster();
  }

  @Test
  public void testCleaner() throws Exception {
    FileSystem fs = FileSystem.get(conf);

    long ttl = TimeToLiveSnapshotRestoreFileCleaner.SNAPSHOT_RESTORE_FILE_CLEANER_TTL_DEFAULT;
    conf.set(SnapshotRestoreFileCleaner.MASTER_SNAPSHOT_RESTORE_FILE_CLEANER_PLUGINS,
      "org.apache.hadoop.hbase.master.cleaner.TimeToLiveSnapshotRestoreFileCleaner");

    long createTime = System.currentTimeMillis();
    long modifyTime = createTime - ttl - 1;
    // create dirs and files
    Path dirExist = new Path(restoreDir, "dirExist");
    fs.mkdirs(dirExist);

    Path dirDelete = new Path(restoreDir, "dirDelete");
    fs.mkdirs(dirDelete);
    fs.setTimes(dirDelete, modifyTime, -1);

    Path dirWithFile = new Path(restoreDir, "dirWithFile");
    Path subDir = new Path(dirWithFile, "subDir");
    fs.mkdirs(subDir);
    Path file1 = new Path(subDir, "file1");
    fs.create(file1);
    fs.setTimes(file1, modifyTime, -1);
    Path file2 = new Path(subDir, "file2");
    fs.create(file2);

    Server server = new DummyServer();
    SnapshotRestoreFileCleaner cleaner =
        new SnapshotRestoreFileCleaner(1000, server, conf, fs, restoreDir, POOL);
    cleaner.chore();

    Assert.assertTrue(fs.exists(dirExist));
    Assert.assertFalse(fs.exists(dirDelete));
    Assert.assertTrue(fs.exists(dirWithFile));
    Assert.assertTrue(fs.exists(subDir));
    Assert.assertFalse(fs.exists(file1));
    Assert.assertTrue(fs.exists(file2));
  }

  @Test
  public void testDaemonCleaner() throws Exception {
    FileSystem fs = FileSystem.get(conf);

    long ttl = 1000L;
    conf.set(SnapshotRestoreFileCleaner.MASTER_SNAPSHOT_RESTORE_FILE_CLEANER_PLUGINS,
      "org.apache.hadoop.hbase.master.cleaner.TimeToLiveSnapshotRestoreFileCleaner");
    conf.setLong(SnapshotRestoreFileCleaner.SNAPSHOT_RESTORE_FILE_CLEANER_INTERVAL, ttl / 4);
    conf.setLong(TimeToLiveSnapshotRestoreFileCleaner.SNAPSHOT_RESTORE_FILE_CLEANER_TTL, ttl);

    // create dirs and files
    Path dirExist = new Path(restoreDir, "dirExist");
    fs.mkdirs(dirExist);

    Path dirDelete = new Path(restoreDir, "dirDelete");
    fs.mkdirs(dirDelete);

    Path dirWithFile = new Path(restoreDir, "dirWithFile");
    Path subDir = new Path(dirWithFile, "subDir");
    fs.mkdirs(subDir);
    Path file1 = new Path(subDir, "file1");
    fs.create(file1);
    Path file2 = new Path(subDir, "file2");
    fs.create(file2);

    Server server = new DummyServer();
    SnapshotRestoreFileCleaner cleaner =
        new SnapshotRestoreFileCleaner(1000, server, conf, fs, restoreDir, POOL);
    Threads.setDaemonThreadRunning(cleaner.getThread(),
      Thread.currentThread().getName() + ".restoredHFileCleaner");

    Thread.sleep(ttl * 2);
    Assert.assertFalse(fs.exists(dirExist));
    Assert.assertFalse(fs.exists(dirDelete));
    Assert.assertFalse(fs.exists(dirWithFile));
    Assert.assertFalse(fs.exists(subDir));
    Assert.assertFalse(fs.exists(file1));
    Assert.assertFalse(fs.exists(file2));
  }

  static class DummyServer implements Server {
    @Override
    public Configuration getConfiguration() {
      return TEST_UTIL.getConfiguration();
    }

    @Override
    public ZooKeeperWatcher getZooKeeper() {
      try {
        return new ZooKeeperWatcher(getConfiguration(), "dummy server", this);
      } catch (IOException e) {
        e.printStackTrace();
      }
      return null;
    }

    @Override
    public CatalogTracker getCatalogTracker() {
      return null;
    }

    @Override
    public ServerName getServerName() {
      return ServerName.valueOf("regionserver,60020,000000");
    }

    @Override
    public void abort(String why, Throwable e) {
    }

    @Override
    public boolean isAborted() {
      return false;
    }

    @Override
    public void stop(String why) {
    }

    @Override
    public boolean isStopped() {
      return false;
    }
  }

}
