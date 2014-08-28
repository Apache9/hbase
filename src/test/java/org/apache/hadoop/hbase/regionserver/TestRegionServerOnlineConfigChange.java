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

package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.experimental.categories.Category;

/**
 * Verify that the Online Config Changes on the HRegionServer side are actually
 * happening. We should add tests for important configurations which will be
 * changed online.
 */
@Category(MediumTests.class)
public class TestRegionServerOnlineConfigChange extends TestCase {
  static final Log LOG =
          LogFactory.getLog(TestRegionServerOnlineConfigChange.class.getName());
  HBaseTestingUtility hbaseTestingUtility = new HBaseTestingUtility();
  Configuration conf = null;

  HTable t1 = null;
  HRegionServer rs1 = null;

  final String table1Str = "table1";
  final String columnFamily1Str = "columnFamily1";
  final byte[] TABLE1 = Bytes.toBytes(table1Str);
  final byte[] COLUMN_FAMILY1 = Bytes.toBytes(columnFamily1Str);

  @Override
  public void setUp() throws Exception {
    conf = hbaseTestingUtility.getConfiguration();
    hbaseTestingUtility.startMiniCluster(1,1);
    t1 = hbaseTestingUtility.createTable(TABLE1, COLUMN_FAMILY1);
    rs1 = hbaseTestingUtility.getRSForFirstRegionInTable(TABLE1);
  }

  @Override
  public void tearDown() throws Exception {
    hbaseTestingUtility.shutdownMiniCluster();
  }

  /**
   * Check if the number of compaction threads changes online
   * @throws IOException
   */
  public void testNumCompactionThreadsOnlineChange() throws IOException {
    assertTrue(rs1.compactSplitThread != null);
    int newNumSmallThreads =
            rs1.compactSplitThread.getSmallCompactionThreadNum() + 1;
    int newNumLargeThreads =
            rs1.compactSplitThread.getLargeCompactionThreadNum() + 1;

    conf.setInt("hbase.regionserver.thread.compaction.small",
            newNumSmallThreads);
    conf.setInt("hbase.regionserver.thread.compaction.large",
            newNumLargeThreads);
    HRegionServer.configurationManager.notifyAllObservers(conf);

    assertEquals(newNumSmallThreads,
                  rs1.compactSplitThread.getSmallCompactionThreadNum());
    assertEquals(newNumLargeThreads,
                  rs1.compactSplitThread.getLargeCompactionThreadNum());
  }

}
