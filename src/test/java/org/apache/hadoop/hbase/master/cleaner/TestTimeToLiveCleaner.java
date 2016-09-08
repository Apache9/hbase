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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.regionserver.compactions.OffPeakHours;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestTimeToLiveCleaner {

  private final static HBaseTestingUtility UTIL = new HBaseTestingUtility();

  @BeforeClass
  public static void setupCluster() throws Exception {
    // have to use a minidfs cluster because the localfs doesn't modify file times correctly
    UTIL.startMiniDFSCluster(1);
  }

  @AfterClass
  public static void shutdownCluster() throws Exception {
    UTIL.shutdownMiniDFSCluster();
  }

  @Test
  public void testTTL() throws Exception {
    FileSystem fs = UTIL.getDFSCluster().getFileSystem();
    Path root = UTIL.getDataTestDir();
    Path file = new Path(root, "file");
    fs.createNewFile(file);
    long createTime = System.currentTimeMillis();
    assertTrue("Test file not created!", fs.exists(file));
    
    Configuration conf = UTIL.getConfiguration();
    TimeToLiveHFileCleaner fileCleaner = new TimeToLiveHFileCleaner();
    conf.setInt(TimeToLiveHFileCleaner.TTL_CONF_KEY, 7200000);
    conf.setInt(TimeToLiveHFileCleaner.TTL_OFFPEAK_CONF_KEY, 3600000);
    fileCleaner.setConf(conf);
    
    // update the time info for the file
    fs.setTimes(file, createTime - 5400000, -1);
    OffPeakHours offPeakHours = OffPeakHours.getInstance(conf);
    if (offPeakHours.isOffPeakHour()) {
      assertTrue("File should be delete and offpeak ttl is 3600000 - check mod time:"
          + getFileStats(file, fs) + " with create time:" + createTime,
          fileCleaner.isFileDeletable(fs.getFileStatus(file)));
    } else {
      assertFalse("File should not be delete and ttl is 7200000 - - check mod time:"
          + getFileStats(file, fs) + " with create time:" + createTime,
          fileCleaner.isFileDeletable(fs.getFileStatus(file)));
    }

    fileCleaner.decreaseTTL();
    assertTrue("File should be delete because ttl is 3600000 and offpeak ttl is 1800000 "
        + "- check mod time:" + getFileStats(file, fs) + " with create time:" + createTime,
        fileCleaner.isFileDeletable(fs.getFileStatus(file)));
 
    for (int i = 0; i < 10; i++) {
      fileCleaner.increaseTTL();
    }
    if (offPeakHours.isOffPeakHour()) {
      assertTrue("File should be delete and offpeak ttl is 3600000 - check mod time:"
          + getFileStats(file, fs) + " with create time:" + createTime,
          fileCleaner.isFileDeletable(fs.getFileStatus(file)));
    } else {
      assertFalse("File should not be delete and ttl is 7200000 - - check mod time:"
          + getFileStats(file, fs) + " with create time:" + createTime,
          fileCleaner.isFileDeletable(fs.getFileStatus(file)));
    }
  }

  @Test
  public void testMinTTL() throws Exception {
    FileSystem fs = UTIL.getDFSCluster().getFileSystem();
    Path root = UTIL.getDataTestDir();
    Path file = new Path(root, "file");
    fs.createNewFile(file);
    long createTime = System.currentTimeMillis();
    assertTrue("Test file not created!", fs.exists(file));

    Configuration conf = UTIL.getConfiguration();
    TimeToLiveHFileCleaner fileCleaner = new TimeToLiveHFileCleaner();
    conf.setInt(TimeToLiveHFileCleaner.TTL_CONF_KEY, (int) TimeToLiveHFileCleaner.MIN_TTL / 6);
    conf.setInt(TimeToLiveHFileCleaner.TTL_OFFPEAK_CONF_KEY, (int) TimeToLiveHFileCleaner.MIN_TTL / 6);
    fileCleaner.setConf(conf);

    // update the time info for the file
    fs.setTimes(file, createTime - (TimeToLiveHFileCleaner.MIN_TTL / 2), -1);
    assertFalse("File should not be delete because min ttl is 60000 "
        + "- check mod time:" + getFileStats(file, fs) + " with create time:" + createTime,
        fileCleaner.isFileDeletable(fs.getFileStatus(file)));

    conf.setInt(TimeToLiveHFileCleaner.TTL_CONF_KEY, (int) TimeToLiveHFileCleaner.MIN_TTL * 10);
    conf.setInt(TimeToLiveHFileCleaner.TTL_OFFPEAK_CONF_KEY, (int) TimeToLiveHFileCleaner.MIN_TTL * 10);
    fileCleaner.setConf(conf);

    for (int i = 0; i < 20; i++) {
      fileCleaner.decreaseTTL();
    }

    assertFalse("File should not be delete because min ttl is 60000 "
        + "- check mod time:" + getFileStats(file, fs) + " with create time:" + createTime,
        fileCleaner.isFileDeletable(fs.getFileStatus(file)));
  }

  @Test
  public void testUpperLimit() {
    Configuration conf = UTIL.getConfiguration();
    TimeToLiveHFileCleaner fileCleaner = new TimeToLiveHFileCleaner();
    conf.setLong(TimeToLiveHFileCleaner.ARCHIVE_LIMIT_CONF_KEY, 2 * 1024 * 1024 * 1024L);
    fileCleaner.setConf(conf);
    assertTrue(fileCleaner.isExceedSizeLimit(4 * 1024 * 1024 * 1024L));
    assertFalse(fileCleaner.isExceedSizeLimit(1024 * 1024 * 1024L));
  }

  /**
   * @param file to check
   * @return loggable information about the file
   */
  private String getFileStats(Path file, FileSystem fs) throws IOException {
    FileStatus status = fs.getFileStatus(file);
    return "File" + file + ", mtime:" + status.getModificationTime() + ", atime:"
        + status.getAccessTime();
  }
}
