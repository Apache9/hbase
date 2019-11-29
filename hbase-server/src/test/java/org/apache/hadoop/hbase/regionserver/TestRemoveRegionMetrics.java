/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.testclassification.LargeTests;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.net.NetUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Category({LargeTests.class})
public class TestRemoveRegionMetrics {
  private static final Logger LOG = LoggerFactory.getLogger(TestRemoveRegionMetrics.class);
  private static MiniHBaseCluster cluster;
  private static Configuration conf;
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static HBaseAdmin admin;

  private static final TableName TABLE_NAME = TableName.valueOf("jmx");
  private static final String CF = "CF";

  @BeforeClass
  public static void startCluster() throws Exception {
    conf = TEST_UTIL.getConfiguration();
    conf.setInt(HConstants.REGIONSERVER_INFO_PORT, -1);

    TEST_UTIL.startMiniCluster(1, 1);
    cluster = TEST_UTIL.getHBaseCluster();
    cluster.waitForActiveAndReadyMaster();
    admin = TEST_UTIL.getHBaseAdmin();

    TEST_UTIL.createTable(TABLE_NAME, Bytes.toBytes(CF));
    TEST_UTIL.waitUntilAllRegionsAssigned(TABLE_NAME);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    admin.disableTable(TABLE_NAME);
    admin.deleteTable(TABLE_NAME);
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testDisableTable() throws Exception {
    for (int i = 0; i < 5; i++) {
      List<HRegionInfo> regionInfos = admin.getTableRegions(TABLE_NAME);
      for (HRegionInfo regionInfo : regionInfos) {
        String jmx = getRegionsJMX(cluster.getRegionServer(0));
        LOG.debug("JMX before disable table: " + jmx);
        assertTrue(jmx.contains(regionInfo.getEncodedName()));
      }
      admin.disableTable(TABLE_NAME);
      // Wait to clear the jmx cache
      Thread.sleep(6000);
      for (HRegionInfo regionInfo : regionInfos) {
        String jmx = getRegionsJMX(cluster.getRegionServer(0));
        LOG.debug("JMX after disable table: " + jmx);
        assertFalse(jmx.contains(regionInfo.getEncodedName()));
      }
      admin.enableTable(TABLE_NAME);
    }
  }

  private String getRegionsJMX(HRegionServer rs) throws IOException {
    URL url = new URL(
        "http://" + NetUtils.getHostPortString(rs.getInfoServer().getListenerAddress()) +
            "/jmx?qry=Hadoop:service=HBase,name=RegionServer,sub=Regions");
    StringBuilder out = new StringBuilder();
    InputStream in = url.openConnection().getInputStream();
    byte[] buffer = new byte[65536];
    for (int len = in.read(buffer); len > 0; len = in.read(buffer)) {
      out.append(new String(buffer, 0, len));
    }
    return out.toString();
  }
}