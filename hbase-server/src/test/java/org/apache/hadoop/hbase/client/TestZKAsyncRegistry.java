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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import org.apache.commons.io.IOUtils;
import org.apache.curator.CuratorZookeeperClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestZKAsyncRegistry {

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static ZKAsyncRegistry REGISTRY;

  @BeforeClass
  public static void setUp() throws Exception {
    TEST_UTIL.startMiniCluster(1);
    REGISTRY = new ZKAsyncRegistry(TEST_UTIL.getConfiguration());
  }

  @AfterClass
  public static void tearDown() throws Exception {
    IOUtils.closeQuietly(REGISTRY);
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void test() throws InterruptedException, ExecutionException, IOException {
    assertEquals(TEST_UTIL.getHBaseCluster().getClusterStatus().getClusterId(),
      REGISTRY.getClusterId().get());
    assertEquals(TEST_UTIL.getHBaseCluster().getClusterStatus().getServersSize(),
      REGISTRY.getCurrentNrHRS().get().intValue());
    assertEquals(TEST_UTIL.getHBaseCluster().getMaster().getServerName(),
      REGISTRY.getMasterAddress().get());
    HRegionLocation loc = REGISTRY.getMetaRegionLocation().get();
    assertTrue(loc.getRegionInfo().getTable().equals(TableName.META_TABLE_NAME));
    assertEquals(TEST_UTIL.getHBaseCluster().getRegionServer(0).getServerName(),
      loc.getServerName());
  }

  @Test
  public void testIndependentZKConnections() throws IOException {
    final CuratorZookeeperClient zk1 = REGISTRY.getCuratorFramework().getZookeeperClient();

    final Configuration otherConf = new Configuration(TEST_UTIL.getConfiguration());
    otherConf.set(HConstants.ZOOKEEPER_QUORUM, "127.0.0.1");
    try (final ZKAsyncRegistry otherRegistry = new ZKAsyncRegistry(otherConf)) {
      final CuratorZookeeperClient zk2 = otherRegistry.getCuratorFramework().getZookeeperClient();

      assertNotSame("Using a different configuration / quorum should result in different backing "
          + "zk connection.",
        zk1, zk2);
      assertNotEquals(
        "Using a different configrution / quorum should be reflected in the " + "zk connection.",
        zk1.getCurrentConnectionString(), zk2.getCurrentConnectionString());
    }
  }
}
