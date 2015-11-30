/**
 *
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
package org.apache.hadoop.hbase.mapreduce.salted;

import java.io.IOException;

import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.LargeTests;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableInputFormatBase;
import org.apache.hadoop.hbase.mapreduce.TestTableInputFormatScanBase;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.xiaomi.infra.hbase.salted.SaltedHTable;

@Category(LargeTests.class)
public class TestSaltedTableInputFormatScan extends TestTableInputFormatScanBase {

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // switch TIF to log at DEBUG level
    TEST_UTIL.enableDebug(TableInputFormat.class);
    TEST_UTIL.enableDebug(TableInputFormatBase.class);
    // start mini hbase cluster
    TEST_UTIL.startMiniCluster(3);
    
    // create and fill table
    HTableDescriptor desc = TestSaltedMultiTableInputFormat.getTableDescriptor(TABLE_NAME,
      new byte[][] { INPUT_FAMILY });
    desc.setSlotsCount(4);
    TEST_UTIL.getHBaseAdmin().createTable(desc);
    TestSaltedMultiTableInputFormat.loadTable(
      new SaltedHTable(new HTable(TEST_UTIL.getConfiguration(), desc.getName())), INPUT_FAMILY);
    // start MR cluster
    
    TEST_UTIL.startMiniMapReduceCluster();
  }
  
  /**
   * Tests a MR scan using specific start and stop rows.
   *
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   */
  @Test
  public void testScanEmptyToEmpty()
  throws IOException, InterruptedException, ClassNotFoundException {
    testScan(null, null, null);
  }

  /**
   * Tests a MR scan using specific start and stop rows.
   *
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   */
  @Test
  public void testScanEmptyToAPP()
  throws IOException, InterruptedException, ClassNotFoundException {
    testScan(null, "app", "apo");
  }

  /**
   * Tests a MR scan using specific start and stop rows.
   *
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   */
  @Test
  public void testScanEmptyToBBA()
  throws IOException, InterruptedException, ClassNotFoundException {
    testScan(null, "bba", "baz");
  }

  /**
   * Tests a MR scan using specific start and stop rows.
   *
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   */
  @Test
  public void testScanEmptyToBBB()
  throws IOException, InterruptedException, ClassNotFoundException {
    testScan(null, "bbb", "bba");
  }

  /**
   * Tests a MR scan using specific start and stop rows.
   *
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   */
  @Test
  public void testScanEmptyToOPP()
  throws IOException, InterruptedException, ClassNotFoundException {
    testScan(null, "opp", "opo");
  }

  /**
   * Tests a MR scan using specific start and stop rows.
   *
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   */
  @Test
  public void testScanOBBToOPP()
  throws IOException, InterruptedException, ClassNotFoundException {
    testScan("obb", "opp", "opo");
  }

  /**
   * Tests a MR scan using specific start and stop rows.
   *
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   */
  @Test
  public void testScanOBBToQPP()
  throws IOException, InterruptedException, ClassNotFoundException {
    testScan("obb", "qpp", "qpo");
  }

  /**
   * Tests a MR scan using specific start and stop rows.
   *
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   */
  @Test
  public void testScanOPPToEmpty()
  throws IOException, InterruptedException, ClassNotFoundException {
    testScan("opp", null, "zzz");
  }

  /**
   * Tests a MR scan using specific start and stop rows.
   *
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   */
  @Test
  public void testScanYYXToEmpty()
  throws IOException, InterruptedException, ClassNotFoundException {
    testScan("yyx", null, "zzz");
  }

  /**
   * Tests a MR scan using specific start and stop rows.
   *
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   */
  @Test
  public void testScanYYYToEmpty()
  throws IOException, InterruptedException, ClassNotFoundException {
    testScan("yyy", null, "zzz");
  }

  /**
   * Tests a MR scan using specific start and stop rows.
   *
   * @throws IOException
   * @throws ClassNotFoundException
   * @throws InterruptedException
   */
  @Test
  public void testScanYZYToEmpty()
  throws IOException, InterruptedException, ClassNotFoundException {
    testScan("yzy", null, "zzz");
  }

  @Test
  public void testScanFromConfiguration()
  throws IOException, InterruptedException, ClassNotFoundException {
    testScanFromConfiguration("bba", "bbd", "bbc");
  }
}
