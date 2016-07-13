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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeepDeletedCells;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionConfiguration;
import org.apache.hadoop.hbase.regionserver.compactions.RatioBasedCompactionPolicy;
import org.apache.hadoop.hbase.regionserver.throttle.CompactionThroughputControllerFactory;
import org.apache.hadoop.hbase.regionserver.throttle.NoLimitThroughputController;
import org.apache.hadoop.hbase.regionserver.throttle.ThroughputController;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WAL;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Category({ RegionServerTests.class, MediumTests.class })
public class TestReclaimDeleteMarkersWithMinorCompaction {

  private static final HBaseTestingUtility UTIL = HBaseTestingUtility.createLocalHTU();

  private static byte[] FAMILY = HBaseTestingUtility.fam1;

  private HTableDescriptor htd;

  private HRegion r;

  @Rule
  public TestName name = new TestName();

  public static final class FakePolicy extends RatioBasedCompactionPolicy {

    public FakePolicy(Configuration conf, StoreConfigInformation storeConfigInfo) {
      super(conf, storeConfigInfo);
    }

    @Override
    protected ArrayList<StoreFile> applyCompactionPolicy(ArrayList<StoreFile> candidates,
        boolean mayUseOffPeak, boolean mayBeStuck) throws IOException {
      if (candidates.size() > 2) {
        candidates.subList(0, candidates.size() - 2).clear();
      }
      return candidates;
    }
  }

  @BeforeClass
  public static void setUpBeforeClass() throws IOException {
    Configuration conf = UTIL.getConfiguration();
    conf.setInt(CompactionConfiguration.HBASE_HSTORE_COMPACTION_MIN_KEY, 2);
    conf.setLong(CompactionConfiguration.HBASE_HSTORE_COMPACTION_MIN_SIZE_KEY, 0);
    conf.setClass(CompactionThroughputControllerFactory.HBASE_THROUGHPUT_CONTROLLER_KEY,
      NoLimitThroughputController.class, ThroughputController.class);
    conf.setClass(DefaultStoreEngine.DEFAULT_COMPACTION_POLICY_CLASS_KEY, FakePolicy.class,
      RatioBasedCompactionPolicy.class);

  }

  @AfterClass
  public static void tearDownAfterClass() throws IOException {
    UTIL.cleanupTestDir();
  }

  @Before
  public void setUp() throws IOException {
    htd = UTIL.createTableDescriptor(TableName.valueOf(name.getMethodName()), 0, 3,
      HConstants.FOREVER, KeepDeletedCells.FALSE);
    r = UTIL.createLocalHRegion(htd, null, null);
  }

  @After
  public void tearDown() throws IOException {
    WAL wal = r.getWAL();
    this.r.close();
    wal.close();
  }

  private void assertDeleteCells(List<Cell> cells, int size) {
    assertEquals(size, cells.size());
    for (int i = 0; i < cells.size(); i++) {
      assertTrue("The " + i + "th element is not a delele cell", CellUtil.isDelete(cells.get(i)));
    }
  }

  @Test
  public void test() throws IOException, InterruptedException {
    byte[] cq = Bytes.toBytes("cq");
    byte[] val = Bytes.toBytes("value");
    // write several store files
    for (int i = 0; i < 5; i++) {
      Put put = new Put(Bytes.toBytes(100 + i)).addColumn(FAMILY, cq, val);
      put.setDurability(Durability.SKIP_WAL);
      r.put(put);
      r.flush(true);
    }
    // write two store files with lots of duplicated delete markers.
    for (int i = 0; i < 2; i++) {
      byte[] row = Bytes.toBytes(i);
      for (int j = 0; j < 20; j++) {
        Delete d = new Delete(row).addFamily(FAMILY);
        d.setDurability(Durability.SKIP_WAL);
        r.delete(d);
        Thread.sleep(5);
      }
      r.flush(true);
    }
    assertEquals(7, r.getStore(FAMILY).getStorefilesCount());
    Scan scan = new Scan().addFamily(FAMILY).setRaw(true);
    // before minor compaction
    RegionScanner scanner = r.getScanner(scan);
    try {
      List<Cell> results = new ArrayList<>();
      scanner.next(results);
      assertDeleteCells(results, 20);
      results.clear();
      scanner.next(results);
      assertDeleteCells(results, 20);
    } finally {
      scanner.close();
    }

    // compact the two files
    r.compact(false);
    assertEquals(6, r.getStore(FAMILY).getStorefilesCount());

    // after minor compaction
    scanner = r.getScanner(scan);
    try {
      List<Cell> results = new ArrayList<>();
      scanner.next(results);
      System.out.println(results.size());
      results.clear();
      scanner.next(results);
      System.out.println(results.size());
    } finally {
      scanner.close();
    }
  }
}
