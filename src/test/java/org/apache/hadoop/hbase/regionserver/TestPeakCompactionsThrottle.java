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
package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.regionserver.compactions.PeakCompactionsThrottle;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestPeakCompactionsThrottle {
  private static HBaseTestingUtility testUtil;
  private Configuration conf;

  @BeforeClass
  public static void setUpClass() {
    testUtil = new HBaseTestingUtility();
  }

  @Before
  public void setUp() {
    conf = testUtil.getConfiguration();
  }

  @Test
  public void testSetPeakHour() throws IOException {
    conf.setInt("hbase.offpeak.start.hour", -1);
    conf.setInt("hbase.offpeak.end.hour", -1);
    conf.setLong("hbase.regionserver.compaction.peak.maxspeed", 50 * 1024 * 1024L);
    conf.setLong("hbase.regionserver.compaction.speed.check.interval", 50 * 1024 * 1024L);
    PeakCompactionsThrottle peakCompactionsThrottle = new PeakCompactionsThrottle(conf, null);
    peakCompactionsThrottle.startCompaction();
    long numOfBytes = 60 * 1024 * 1024;
    peakCompactionsThrottle.throttle(numOfBytes);
    peakCompactionsThrottle.finishCompaction("region", "family");
    assertTrue(peakCompactionsThrottle.getNumberOfThrottles() == 1);
    conf.setInt("hbase.offpeak.start.hour", 0);
    conf.setInt("hbase.offpeak.end.hour", 23);
    peakCompactionsThrottle = new PeakCompactionsThrottle(conf, null);
    peakCompactionsThrottle.startCompaction();
    numOfBytes = 60 * 1024 * 1024;
    peakCompactionsThrottle.throttle(numOfBytes);
    peakCompactionsThrottle.finishCompaction("region", "family");
    assertTrue(peakCompactionsThrottle.getNumberOfThrottles() == 0);
  }
}
