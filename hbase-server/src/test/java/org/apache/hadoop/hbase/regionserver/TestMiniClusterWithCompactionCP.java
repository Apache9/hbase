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
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category(MediumTests.class)
public class TestMiniClusterWithCompactionCP {

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static final Configuration CONF = UTIL.getConfiguration();
  private static final TableName TABLE_NAME = TableName.valueOf("testCompactionCP");
  private static final byte[] FAMILY = Bytes.toBytes("f");
  private static final byte[] QUALIFIER = Bytes.toBytes("q");
  private static final Logger LOG = LoggerFactory.getLogger(TestMiniClusterWithCompactionCP.class);

  @BeforeClass
  public static void setUp() throws Exception {
    CONF.setStrings(CoprocessorHost.USER_REGION_COPROCESSOR_CONF_KEY,
      MyCompactionCP.class.getName());
    UTIL.startMiniCluster();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void test() throws IOException, InterruptedException {
    HTable table = UTIL.createTable(TABLE_NAME, FAMILY);
    HBaseAdmin admin = UTIL.getHBaseAdmin();
    final Path storePath =
        HStore.getStoreHomedir(FSUtils.getTableDir(FSUtils.getRootDir(CONF), TABLE_NAME),
          admin.getTableRegions(TABLE_NAME).get(0), FAMILY);
    Assert.assertEquals(admin.getTableRegions(TABLE_NAME).size(), 1);
    FileSystem fs = UTIL.getDFSCluster().getFileSystem();

    for (int i = 0; i < 10; i++) {
      Put put = new Put(Bytes.toBytes(i)).add(FAMILY, QUALIFIER, Bytes.toBytes(i));
      table.put(put);
    }
    Assert.assertEquals(UTIL.countRows(table), 10);
    Delete delete = new Delete(Bytes.toBytes(0));
    table.delete(delete);
    Assert.assertEquals(UTIL.countRows(table), 9);
    Assert.assertEquals(fs.listStatus(storePath).length, 0);

    admin.flush(TABLE_NAME.toBytes());
    Assert.assertEquals(UTIL.countRows(table), 9);
    Assert.assertEquals(fs.listStatus(storePath).length, 1);

    for (int i = 10; i < 20; i++) {
      Put put = new Put(Bytes.toBytes(i)).add(FAMILY, QUALIFIER, Bytes.toBytes(i));
      table.put(put);
    }
    Assert.assertEquals(UTIL.countRows(table), 19);
    admin.flush(TABLE_NAME.toBytes());
    Assert.assertEquals(fs.listStatus(storePath).length, 2);

    admin.majorCompact(TABLE_NAME.toBytes());
    while (true) {
      if (fs.listStatus(storePath).length == 1) {
        break;
      }
      Thread.sleep(1000);
    }

    Assert.assertEquals(UTIL.countRows(table), 19);
  }

  public static class MyCompactionCP extends BaseRegionObserver {

    @Override
    public InternalScanner preCompactScannerOpen(
        final ObserverContext<RegionCoprocessorEnvironment> c, Store store,
        List<? extends KeyValueScanner> scanners, ScanType scanType, long earliestPutTs,
        InternalScanner s) throws IOException {

      Scan scan = new Scan();
      scan.setMaxVersions(store.getScanInfo().getMaxVersions());
      scan.setFilter(new FilterBase() {

        @Override
        public ReturnCode filterKeyValue(Cell ignored) throws IOException {
          LOG.info("filterKeyValue: {}", ignored);
          return super.filterKeyValue(ignored);
        }

        @Override
        public void reset() throws IOException {
          super.reset();
        }
      });
      InternalScanner scanner = new StoreScanner(store, store.getScanInfo(), scan, scanners,
          scanType, ((HStore) store).getHRegion().getSmallestReadPoint(), earliestPutTs);
      LOG.info("Initialize compact scanner from MyCompactionCP");
      return scanner;
    }
  }
}
