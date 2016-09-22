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
package org.apache.hadoop.hbase.client.async;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.zookeeper.MasterAddressTracker;
import org.apache.hadoop.hbase.zookeeper.MetaTableLocator;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MediumTests.class, ClientTests.class })
public class TestZKClusterRegistry {

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  @BeforeClass
  public static void setUp() throws Exception {
    TEST_UTIL.startMiniCluster(1);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void test() throws InterruptedException, IOException, KeeperException {
    try (ZKClusterRegistry registry = new ZKClusterRegistry(TEST_UTIL.getConfiguration());
        ZooKeeperWatcher zkw = new ZooKeeperWatcher(TEST_UTIL.getConfiguration(), "test", null)) {
      Thread.sleep(2000);
      Connection conn = TEST_UTIL.getConnection();
      assertEquals(conn.getAdmin().getClusterStatus().getClusterId(), registry.getClusterId());
      assertEquals(MasterAddressTracker.getMasterAddress(zkw), registry.getMasterAddress());
      assertEquals(MasterAddressTracker.getMasterInfoPort(zkw), registry.getMasterInfoPort());
      assertEquals(2, registry.getCurrentNrHRS());
      List<ServerName> expected = new MetaTableLocator().blockUntilAvailable(zkw, 5000,
        TEST_UTIL.getConfiguration());
      RegionLocations locs = registry.getMetaRegionLocation();
      assertEquals(expected.size(), locs.getRegionLocations().length);

      for (HRegionLocation loc : locs.getRegionLocations()) {
        assertThat(expected, hasItem(loc.getServerName()));
      }
    }
  }
}
