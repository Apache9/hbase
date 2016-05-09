/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.client;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import junit.framework.Assert;

import static junit.framework.TestCase.assertEquals;

/**
 * This test sets the multi size WAAAAAY low and then checks to make sure that gets will still make
 * progress.
 */
@Category({MediumTests.class})
public class TestMultiRespectsLimits {
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private final static byte[] FAMILY = Bytes.toBytes("D");
  public static final int MAX_SIZE = 500;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setLong(
        HConstants.HBASE_SERVER_SCANNER_MAX_RESULT_SIZE_KEY,
        MAX_SIZE);

    // Only start on regionserver so that all regions are on the same server.
    TEST_UTIL.startMiniCluster(1);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testMultiLimits() throws Exception {
    final byte[] name = Bytes.toBytes("testMultiLimits");
    HTable t = TEST_UTIL.createTable(name, FAMILY, new byte[][]{Bytes.toBytes("m")});
    TEST_UTIL.loadTable(t, FAMILY);

    // more than one region
    Assert.assertEquals(2, t.getStartKeys().length);

    // get loaded row
    List<Result> rows = new ArrayList<Result>();
    ResultScanner scanner = t.getScanner(new Scan().setFilter(new FirstKeyOnlyFilter()));
    Result r = null;
    while ((r = scanner.next()) != null) {
      rows.add(r);
    }
    scanner.close();
    // make sure multi request will cross-region
    Collections.shuffle(rows);
    
    List<Get> gets = new ArrayList<Get>(MAX_SIZE);
    for (int i = 0; i < MAX_SIZE && i < rows.size(); i++) {
      gets.add(new Get(rows.get(i).getRow()));
    }
    Result[] results = t.get(gets);
    assertEquals(MAX_SIZE, results.length);
  }
}
