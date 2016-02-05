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
package org.apache.hadoop.hbase.master;

import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.hbase.SmallTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category(SmallTests.class)
public class TestServerCrashStatistics {
  @Test public void testMarkRegionAsRecovered() throws InterruptedException {
    String serverName = "rs01";
    long crashTs = System.currentTimeMillis();
    Set<String> regions = new TreeSet<String>();
    regions.add("region01");
    regions.add("region02");
    ServerCrashStatistics stat = new ServerCrashStatistics(serverName, crashTs, regions);
    Thread.sleep(1);
    stat.markRegionAsRecovered("region01");
    Thread.sleep(1);
    stat.markRegionAsRecovered("region02");
    long recoverTs = System.currentTimeMillis();
    assertTrue(stat.getPendingRegions().isEmpty());
    assertEquals(crashTs, stat.getCrashTs());
    assertTrue(stat.getAvgRegionRecoverTime() >= (1 + 2) / 2.0 &&
        stat.getAvgRegionRecoverTime() < recoverTs);
    assertTrue(stat.getMinRegionRecoverTime() >= 1);
    assertTrue(stat.getMaxRegionRecoverTime() <= recoverTs);
    assertEquals(stat.getMaxRegionRecoverTime(),
        stat.getRegionRecoverTime().get("region02").longValue());
  }

  @org.junit.Rule
  public org.apache.hadoop.hbase.ResourceCheckerJUnitRule cu =
    new org.apache.hadoop.hbase.ResourceCheckerJUnitRule();
}

