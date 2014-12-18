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
package org.apache.hadoop.hbase.regionserver.compactions;

import static org.junit.Assert.*;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.ManualEnvironmentEdge;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestCompactionsThrottle {
  private static HBaseTestingUtility testUtil;
  private static ManualEnvironmentEdge edge;
  private Configuration conf;

  @BeforeClass
  public static void setUpClass() {
    testUtil = new HBaseTestingUtility();
    edge = new ManualEnvironmentEdge();
    EnvironmentEdgeManager.injectEdge(edge);
  }

  @Before
  public void setUp() {
    conf = testUtil.getConfiguration();
  }

  @Test
  public void testOffPeak() throws IOException, InterruptedException {
    conf.set(CompactionConfiguration.HBASE_HSTORE_OFFPEAK_START_HOUR, "0");
    conf.set(CompactionConfiguration.HBASE_HSTORE_OFFPEAK_END_HOUR, "23");
    conf.setLong(CompactionConfiguration.HBASE_HSTORE_PEAK_COMPACTION_SPEED_ALLOWED,
      1024L * 1024 * 1024);
    conf.setLong(CompactionConfiguration.HBASE_HSTORE_OFFPEAK_COMPACTION_SPEED_ALLOWED,
      5L * 1024 * 1024);
    conf.setLong(CompactionConfiguration.HBASE_HSTORE_COMPACTION_SPEED_CHECK_INTERVAL,
      1L * 1024 * 1024);
    CompactionsThrottle peakCompactionsThrottle = new CompactionsThrottle(conf);
    edge.setValue(1000);
    peakCompactionsThrottle.startCompaction();
    long numOfBytes = 20 * 1024 * 1024;
    edge.setValue(10000);
    peakCompactionsThrottle.throttle(numOfBytes);
    assertEquals(0, peakCompactionsThrottle.getNumberOfThrottles());
    edge.setValue(12000);
    peakCompactionsThrottle.throttle(numOfBytes);
    assertEquals(1, peakCompactionsThrottle.getNumberOfThrottles());
    peakCompactionsThrottle.finishCompaction("region", "family");

  }

  @Test
  public void testPeak() throws IOException {
    conf.set(CompactionConfiguration.HBASE_HSTORE_OFFPEAK_START_HOUR, "-1");
    conf.set(CompactionConfiguration.HBASE_HSTORE_OFFPEAK_END_HOUR, "-1");
    conf.setLong(CompactionConfiguration.HBASE_HSTORE_PEAK_COMPACTION_SPEED_ALLOWED,
      5L * 1024 * 1024);
    conf.setLong(CompactionConfiguration.HBASE_HSTORE_OFFPEAK_COMPACTION_SPEED_ALLOWED,
      1024L * 1024 * 1024);
    conf.setLong(CompactionConfiguration.HBASE_HSTORE_COMPACTION_SPEED_CHECK_INTERVAL,
      1L * 1024 * 1024);
    CompactionsThrottle peakCompactionsThrottle = new CompactionsThrottle(conf);
    edge.setValue(1000);
    peakCompactionsThrottle.startCompaction();
    long numOfBytes = 20 * 1024 * 1024;
    edge.setValue(10000);
    peakCompactionsThrottle.throttle(numOfBytes);
    assertEquals(0, peakCompactionsThrottle.getNumberOfThrottles());
    edge.setValue(12000);
    peakCompactionsThrottle.throttle(numOfBytes);
    assertEquals(1, peakCompactionsThrottle.getNumberOfThrottles());
    peakCompactionsThrottle.finishCompaction("region", "family");
  }
}
