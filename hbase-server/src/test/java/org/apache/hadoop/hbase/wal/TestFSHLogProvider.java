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
package org.apache.hadoop.hbase.wal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.regionserver.MultiVersionConcurrencyControl;
// imports for things that haven't moved from regionserver.wal yet.
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Category({ RegionServerTests.class, MediumTests.class })
public class TestFSHLogProvider {
  private static final Log LOG = LogFactory.getLog(TestFSHLogProvider.class);

  protected static Configuration conf;
  protected static FileSystem fs;
  protected final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  protected MultiVersionConcurrencyControl mvcc;

  @Rule
  public final TestName currentTest = new TestName();

  @Before
  public void setUp() throws Exception {
    mvcc = new MultiVersionConcurrencyControl();
    FileStatus[] entries = fs.listStatus(new Path("/"));
    for (FileStatus dir : entries) {
      fs.delete(dir.getPath(), true);
    }
  }

  @After
  public void tearDown() throws Exception {
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // Make block sizes small.
    TEST_UTIL.getConfiguration().setInt("dfs.blocksize", 1024 * 1024);
    // quicker heartbeat interval for faster DN death notification
    TEST_UTIL.getConfiguration().setInt("dfs.namenode.heartbeat.recheck-interval", 5000);
    TEST_UTIL.getConfiguration().setInt("dfs.heartbeat.interval", 1);
    TEST_UTIL.getConfiguration().setInt("dfs.client.socket-timeout", 5000);

    // faster failover with cluster.shutdown();fs.close() idiom
    TEST_UTIL.getConfiguration().setInt("hbase.ipc.client.connect.max.retries", 1);
    TEST_UTIL.getConfiguration().setInt("dfs.client.block.recovery.retries", 1);
    TEST_UTIL.getConfiguration().setInt("hbase.ipc.client.connection.maxidletime", 500);
    TEST_UTIL.startMiniDFSCluster(3);

    // Set up a working space for our tests.
    TEST_UTIL.createRootDir();
    conf = TEST_UTIL.getConfiguration();
    fs = TEST_UTIL.getDFSCluster().getFileSystem();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  static String getName() {
    return "TestDefaultWALProvider";
  }

  @Test
  public void testGetServerNameFromWALDirectoryName() throws IOException {
    ServerName sn = ServerName.valueOf("hn", 450, 1398);
    String hl =
        FSUtils.getRootDir(conf) + "/" + AbstractFSWALProvider.getWALDirectoryName(sn.toString());

    // Must not throw exception
    assertNull(AbstractFSWALProvider.getServerNameFromWALDirectoryName(conf, null));
    assertNull(AbstractFSWALProvider.getServerNameFromWALDirectoryName(conf,
      FSUtils.getRootDir(conf).toUri().toString()));
    assertNull(AbstractFSWALProvider.getServerNameFromWALDirectoryName(conf, ""));
    assertNull(AbstractFSWALProvider.getServerNameFromWALDirectoryName(conf, "                  "));
    assertNull(AbstractFSWALProvider.getServerNameFromWALDirectoryName(conf, hl));
    assertNull(AbstractFSWALProvider.getServerNameFromWALDirectoryName(conf, hl + "qdf"));
    assertNull(AbstractFSWALProvider.getServerNameFromWALDirectoryName(conf, "sfqf" + hl + "qdf"));

    final String wals = "/WALs/";
    ServerName parsed = AbstractFSWALProvider.getServerNameFromWALDirectoryName(conf,
      FSUtils.getRootDir(conf).toUri().toString() + wals + sn +
          "/localhost%2C32984%2C1343316388997.1343316390417");
    assertEquals("standard", sn, parsed);

    parsed = AbstractFSWALProvider.getServerNameFromWALDirectoryName(conf, hl + "/qdf");
    assertEquals("subdir", sn, parsed);

    parsed = AbstractFSWALProvider.getServerNameFromWALDirectoryName(conf,
      FSUtils.getRootDir(conf).toUri().toString() + wals + sn +
          "-splitting/localhost%3A57020.1340474893931");
    assertEquals("split", sn, parsed);
  }

  private long addEdits(WAL log, HRegionInfo hri, HTableDescriptor htd, int times,
      NavigableMap<byte[], Integer> scopes) throws IOException {
    long sequenceId = HConstants.NO_SEQNUM;
    final byte[] row = Bytes.toBytes("row");
    for (int i = 0; i < times; i++) {
      long timestamp = System.currentTimeMillis();
      WALEdit cols = new WALEdit();
      cols.add(new KeyValue(row, row, row, timestamp, row));
      WALKey key = getWalKey(hri.getEncodedNameAsBytes(), htd.getTableName(), timestamp, scopes);
      log.append(hri, key, cols, true);
      sequenceId = key.getSequenceId();
    }
    log.sync();
    return sequenceId;
  }

  /**
   * used by TestDefaultWALProviderWithHLogKey
   * @param scopes
   */
  WALKey getWalKey(final byte[] info, final TableName tableName, final long timestamp,
      NavigableMap<byte[], Integer> scopes) {
    return new WALKey(info, tableName, timestamp, mvcc, scopes);
  }

  /**
   * helper method to simulate region flush for a WAL.
   * @param wal
   * @param encodedRegionName
   */
  private void flushRegion(WAL wal, byte[] encodedRegionName, Set<byte[]> flushedFamilyNames,
      long sequenceId) {
    wal.startCacheFlush(encodedRegionName);
    Map<byte[], Long> family2LowestUnflushedSequenceId =
        flushedFamilyNames.stream().map(f -> Pair.newPair(f, sequenceId))
            .collect(Collectors.toMap(Pair::getFirst, Pair::getSecond, (u, v) -> {
              throw new IllegalStateException("family should be unique");
            }, () -> new TreeMap<>(Bytes.BYTES_COMPARATOR)));
    wal.completeCacheFlush(encodedRegionName, family2LowestUnflushedSequenceId);
  }

  private static final byte[] UNSPECIFIED_REGION = new byte[] {};

  @Test
  public void testLogCleaning() throws Exception {
    LOG.info(currentTest.getMethodName());
    final HTableDescriptor htd =
        new HTableDescriptor(TableName.valueOf(currentTest.getMethodName()))
            .addFamily(new HColumnDescriptor("row"));
    final HTableDescriptor htd2 =
        new HTableDescriptor(TableName.valueOf(currentTest.getMethodName() + "2"))
            .addFamily(new HColumnDescriptor("row"));
    NavigableMap<byte[], Integer> scopes = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    for (byte[] fam : htd.getFamiliesKeys()) {
      scopes.put(fam, 0);
    }
    NavigableMap<byte[], Integer> scopes2 = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    for (byte[] fam : htd2.getFamiliesKeys()) {
      scopes2.put(fam, 0);
    }
    final Configuration localConf = new Configuration(conf);
    localConf.set(WALFactory.WAL_PROVIDER, FSHLogProvider.class.getName());
    final WALFactory wals = new WALFactory(localConf, null, currentTest.getMethodName());
    try {
      HRegionInfo hri =
          new HRegionInfo(htd.getTableName(), HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW);
      HRegionInfo hri2 = new HRegionInfo(htd2.getTableName(), HConstants.EMPTY_START_ROW,
          HConstants.EMPTY_END_ROW);

      // we want to mix edits from regions, so pick our own identifier.
      final WAL log = wals.getWAL(UNSPECIFIED_REGION, null);
      htd.getFamiliesKeys().forEach(f -> log.updateStore(hri.getEncodedNameAsBytes(), f, 0));
      htd2.getFamiliesKeys().forEach(f -> log.updateStore(hri2.getEncodedNameAsBytes(), f, 0));
      // Add a single edit and make sure that rolling won't remove the file
      // Before HBASE-3198 it used to delete it
      long seqId = addEdits(log, hri, htd, 1, scopes);
      log.rollWriter();
      assertEquals(1, AbstractFSWALProvider.getNumRolledLogFiles(log));

      // See if there's anything wrong with more than 1 edit
      seqId = addEdits(log, hri, htd, 2, scopes);
      log.rollWriter();
      assertEquals(2, FSHLogProvider.getNumRolledLogFiles(log));

      // Now mix edits from 2 regions, still no flushing
      seqId = addEdits(log, hri, htd, 1, scopes);
      long seqId2 = addEdits(log, hri2, htd2, 1, scopes2);
      seqId = addEdits(log, hri, htd, 1, scopes);
      seqId2 = addEdits(log, hri2, htd2, 1, scopes2);
      log.rollWriter();
      assertEquals(3, AbstractFSWALProvider.getNumRolledLogFiles(log));

      // Flush the first region, we expect to see the first two files getting
      // archived. We need to append something or writer won't be rolled.
      seqId2 = addEdits(log, hri2, htd2, 1, scopes2);
      flushRegion(log, hri.getEncodedNameAsBytes(), htd.getFamiliesKeys(), seqId + 1);
      log.rollWriter();
      assertEquals(2, AbstractFSWALProvider.getNumRolledLogFiles(log));

      // Flush the second region, which removes all the remaining output files
      // since the oldest was completely flushed and the two others only contain
      // flush information
      seqId2 = addEdits(log, hri2, htd2, 1, scopes2);
      flushRegion(log, hri2.getEncodedNameAsBytes(), htd2.getFamiliesKeys(), seqId2 + 1);
      log.rollWriter();
      assertEquals(0, AbstractFSWALProvider.getNumRolledLogFiles(log));
    } finally {
      if (wals != null) {
        wals.close();
      }
    }
  }

  /**
   * Tests wal archiving by adding data, doing flushing/rolling and checking we archive old logs and
   * also don't archive "live logs" (that is, a log with un-flushed entries).
   * <p>
   * This is what it does: It creates two regions, and does a series of inserts along with log
   * rolling. Whenever a WAL is rolled, HLogBase checks previous wals for archiving. A wal is
   * eligible for archiving if for all the regions which have entries in that wal file, have flushed
   * - past their maximum sequence id in that wal file.
   * <p>
   * @throws IOException
   */
  @Test
  public void testWALArchiving() throws IOException {
    LOG.debug(currentTest.getMethodName());
    HTableDescriptor table1 =
        new HTableDescriptor(TableName.valueOf(currentTest.getMethodName() + "1"))
            .addFamily(new HColumnDescriptor("row"));
    HTableDescriptor table2 =
        new HTableDescriptor(TableName.valueOf(currentTest.getMethodName() + "2"))
            .addFamily(new HColumnDescriptor("row"));
    NavigableMap<byte[], Integer> scopes1 = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    for (byte[] fam : table1.getFamiliesKeys()) {
      scopes1.put(fam, 0);
    }
    NavigableMap<byte[], Integer> scopes2 = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    for (byte[] fam : table2.getFamiliesKeys()) {
      scopes2.put(fam, 0);
    }
    final Configuration localConf = new Configuration(conf);
    localConf.set(WALFactory.WAL_PROVIDER, FSHLogProvider.class.getName());
    final WALFactory wals = new WALFactory(localConf, null, currentTest.getMethodName());
    try {
      final WAL wal = wals.getWAL(UNSPECIFIED_REGION, null);
      assertEquals(0, AbstractFSWALProvider.getNumRolledLogFiles(wal));
      HRegionInfo hri1 = new HRegionInfo(table1.getTableName(), HConstants.EMPTY_START_ROW,
          HConstants.EMPTY_END_ROW);
      HRegionInfo hri2 = new HRegionInfo(table2.getTableName(), HConstants.EMPTY_START_ROW,
          HConstants.EMPTY_END_ROW);
      // ensure that we don't split the regions.
      hri1.setSplit(false);
      hri2.setSplit(false);
      // init sequence id accounting
      table1.getFamiliesKeys().forEach(f -> wal.updateStore(hri1.getEncodedNameAsBytes(), f, 0));
      table2.getFamiliesKeys().forEach(f -> wal.updateStore(hri2.getEncodedNameAsBytes(), f, 0));
      // variables to mock region sequenceIds.
      // start with the testing logic: insert a waledit, and roll writer
      long seqId1 = addEdits(wal, hri1, table1, 1, scopes1);
      wal.rollWriter();
      // assert that the wal is rolled
      assertEquals(1, AbstractFSWALProvider.getNumRolledLogFiles(wal));
      // add edits in the second wal file, and roll writer.
      addEdits(wal, hri1, table1, 1, scopes1);
      wal.rollWriter();
      // assert that the wal is rolled
      assertEquals(2, AbstractFSWALProvider.getNumRolledLogFiles(wal));
      // add a waledit to table1, and flush the region.
      seqId1 = addEdits(wal, hri1, table1, 3, scopes1);
      flushRegion(wal, hri1.getEncodedNameAsBytes(), table1.getFamiliesKeys(), seqId1 + 1);
      // roll log; all old logs should be archived.
      wal.rollWriter();
      assertEquals(0, AbstractFSWALProvider.getNumRolledLogFiles(wal));
      // add an edit to table2, and roll writer
      long seqId2 = addEdits(wal, hri2, table2, 1, scopes2);
      wal.rollWriter();
      assertEquals(1, AbstractFSWALProvider.getNumRolledLogFiles(wal));
      // add edits for table1, and roll writer
      seqId1 = addEdits(wal, hri1, table1, 2, scopes1);
      wal.rollWriter();
      assertEquals(2, AbstractFSWALProvider.getNumRolledLogFiles(wal));
      // add edits for table2, and flush hri1.
      seqId2 = addEdits(wal, hri2, table2, 2, scopes2);
      flushRegion(wal, hri1.getEncodedNameAsBytes(), table1.getFamiliesKeys(), seqId1 + 1);
      // the log : region-sequenceId map is
      // log1: region2 (unflushed)
      // log2: region1 (flushed)
      // log3: region2 (unflushed)
      // roll the writer; log2 should be archived.
      wal.rollWriter();
      assertEquals(2, AbstractFSWALProvider.getNumRolledLogFiles(wal));
      // flush region2, and all logs should be archived.
      seqId2 = addEdits(wal, hri2, table2, 2, scopes2);
      flushRegion(wal, hri2.getEncodedNameAsBytes(), table2.getFamiliesKeys(), seqId2 + 1);
      wal.rollWriter();
      assertEquals(0, AbstractFSWALProvider.getNumRolledLogFiles(wal));
    } finally {
      if (wals != null) {
        wals.close();
      }
    }
  }

  /**
   * Write to a log file with three concurrent threads and verifying all data is written.
   * @throws Exception
   */
  @Test
  public void testConcurrentWrites() throws Exception {
    // Run the WPE tool with three threads writing 3000 edits each concurrently.
    // When done, verify that all edits were written.
    int errCode =
        WALPerformanceEvaluation.innerMain(new Configuration(TEST_UTIL.getConfiguration()),
          new String[] { "-threads", "3", "-verify", "-noclosefs", "-iterations", "3000" });
    assertEquals(0, errCode);
  }

  /**
   * Ensure that we can use Set.add to deduplicate WALs
   */
  @Test
  public void setMembershipDedups() throws IOException {
    final Configuration localConf = new Configuration(conf);
    localConf.set(WALFactory.WAL_PROVIDER, FSHLogProvider.class.getName());
    final WALFactory wals = new WALFactory(localConf, null, currentTest.getMethodName());
    try {
      final Set<WAL> seen = new HashSet<>(1);
      final Random random = new Random();
      assertTrue("first attempt to add WAL from default provider should work.",
        seen.add(wals.getWAL(Bytes.toBytes(random.nextInt()), null)));
      for (int i = 0; i < 1000; i++) {
        assertFalse(
          "default wal provider is only supposed to return a single wal, which should " +
              "compare as .equals itself.",
          seen.add(wals.getWAL(Bytes.toBytes(random.nextInt()), null)));
      }
    } finally {
      wals.close();
    }
  }
}
