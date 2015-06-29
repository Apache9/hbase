/*
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
package org.apache.hadoop.hbase.replication.thrift;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos;
import org.apache.hadoop.hbase.replication.regionserver.ReplicationSink;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.ByteStringer;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
@Category(MediumTests.class)
public class TestThriftReplicationSink extends TestThriftReplicationBase {

  private static final Log LOG = LogFactory.getLog(TestThriftReplicationSink.class);
  private static final int BATCH_SIZE = 10;

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static ReplicationSink SINK;

  private static final byte[] TABLE_NAME1 = Bytes.toBytes("table1");
  private static final byte[] TABLE_NAME2 = Bytes.toBytes("table2");

  private static final byte[] FAM_NAME1 = Bytes.toBytes("info1");
  private static final byte[] FAM_NAME2 = Bytes.toBytes("info2");

  private static HTable table1;
  private static HTable table2;
  private static Stoppable STOPPABLE = new Stoppable() {
    final AtomicBoolean stop = new AtomicBoolean(false);

    @Override
    public boolean isStopped() {
      return this.stop.get();
    }

    @Override
    public void stop(String why) {
      LOG.info("STOPPING BECAUSE: " + why);
      this.stop.set(true);
    }

  };


  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setBoolean("dfs.support.append", true);
    TEST_UTIL.getConfiguration().setBoolean(HConstants.REPLICATION_ENABLE_KEY, true);
    ReplicationTestUtils.setupConfiguration(TEST_UTIL, HBaseTestingUtility.randomFreePort());
    TEST_UTIL.startMiniCluster(1);

    Configuration sinkConfiguration = new Configuration(TEST_UTIL.getConfiguration());
    sinkConfiguration.setInt("hbase.replication.thrift.server.port", HBaseTestingUtility.randomFreePort());
    SINK = new ReplicationSink(sinkConfiguration, STOPPABLE, UUID.randomUUID().toString());
    table1 = TEST_UTIL.createTable(TABLE_NAME1, FAM_NAME1);
    table2 = TEST_UTIL.createTable(TABLE_NAME2, FAM_NAME2);
  }

  /**
   * @throws java.lang.Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    STOPPABLE.stop("Shutting down");
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception {
    table1 = TEST_UTIL.truncateTable(TABLE_NAME1);
    table2 = TEST_UTIL.truncateTable(TABLE_NAME2);
  }

  /**
   * Insert a whole batch of entries
   * @throws Exception
   */
  @Test
  public void testBatchSink() throws Exception {
    List<AdminProtos.WALEntry> entries = new ArrayList<AdminProtos.WALEntry>(BATCH_SIZE);
    List<Cell> cells = new ArrayList<Cell>();
    for(int i = 0; i < BATCH_SIZE; i++) {
      entries.add(createEntry(TABLE_NAME1, i, KeyValue.Type.Put, cells));
    }
    SINK.replicateEntries(entries, CellUtil.createCellScanner(cells.iterator()));
    Scan scan = new Scan();
    ResultScanner scanRes = table1.getScanner(scan);
    assertEquals(BATCH_SIZE, scanRes.next(BATCH_SIZE).length);
  }

  /**
   * Insert a mix of puts and deletes
   * @throws Exception
   */
  @Test
  public void testMixedPutDelete() throws Exception {
    List<AdminProtos.WALEntry> entries = new ArrayList<AdminProtos.WALEntry>(BATCH_SIZE/2);
    List<Cell> cells = new ArrayList<Cell>();
    for(int i = 0; i < BATCH_SIZE/2; i++) {
      entries.add(createEntry(TABLE_NAME1, i, KeyValue.Type.Put, cells));
    }
    SINK.replicateEntries(entries, CellUtil.createCellScanner(cells));

    entries = new ArrayList<AdminProtos.WALEntry>(BATCH_SIZE);
    cells = new ArrayList<Cell>();
    for(int i = 0; i < BATCH_SIZE; i++) {
      entries.add(createEntry(TABLE_NAME1, i,
          i % 2 != 0 ? KeyValue.Type.Put: KeyValue.Type.DeleteColumn, cells));
    }

    SINK.replicateEntries(entries, CellUtil.createCellScanner(cells.iterator()));
    Scan scan = new Scan();
    ResultScanner scanRes = table1.getScanner(scan);
    assertEquals(BATCH_SIZE/2, scanRes.next(BATCH_SIZE).length);
  }

  /**
   * Insert to 2 different tables
   * @throws Exception
   */
  @Test
  public void testMixedPutTables() throws Exception {
    List<AdminProtos.WALEntry> entries = new ArrayList<AdminProtos.WALEntry>(BATCH_SIZE/2);
    List<Cell> cells = new ArrayList<Cell>();
    for(int i = 0; i < BATCH_SIZE; i++) {
      entries.add(createEntry( i % 2 == 0 ? TABLE_NAME2 : TABLE_NAME1,
          i, KeyValue.Type.Put, cells));
    }

    SINK.replicateEntries(entries, CellUtil.createCellScanner(cells.iterator()));
    Scan scan = new Scan();
    ResultScanner scanRes = table2.getScanner(scan);
    for(Result res : scanRes) {
      assertTrue(Bytes.toInt(res.getRow()) % 2 == 0);
    }
  }

  /**
   * Insert then do different types of deletes
   * @throws Exception
   */
  @Test
  public void testMixedDeletes() throws Exception {
    List<AdminProtos.WALEntry> entries = new ArrayList<AdminProtos.WALEntry>(3);
    List<Cell> cells = new ArrayList<Cell>();
    for(int i = 0; i < 3; i++) {
      entries.add(createEntry(TABLE_NAME1, i, KeyValue.Type.Put, cells));
    }
    SINK.replicateEntries(entries, CellUtil.createCellScanner(cells.iterator()));
    entries = new ArrayList<AdminProtos.WALEntry>(3);
    cells = new ArrayList<Cell>();
    entries.add(createEntry(TABLE_NAME1, 0, KeyValue.Type.DeleteColumn, cells));
    entries.add(createEntry(TABLE_NAME1, 1, KeyValue.Type.DeleteFamily, cells));
    entries.add(createEntry(TABLE_NAME1, 2, KeyValue.Type.DeleteColumn, cells));

    SINK.replicateEntries(entries, CellUtil.createCellScanner(cells.iterator()));

    Scan scan = new Scan();
    ResultScanner scanRes = table1.getScanner(scan);
    assertEquals(0, scanRes.next(3).length);
  }

  /**
   * Puts are buffered, but this tests when a delete (not-buffered) is applied
   * before the actual Put that creates it.
   * @throws Exception
   */
  @Test
  public void testApplyDeleteBeforePut() throws Exception {
    List<AdminProtos.WALEntry> entries = new ArrayList<AdminProtos.WALEntry>(5);
    List<Cell> cells = new ArrayList<Cell>();
    for(int i = 0; i < 2; i++) {
      entries.add(createEntry(TABLE_NAME1, i, KeyValue.Type.Put, cells));
    }
    entries.add(createEntry(TABLE_NAME1, 1, KeyValue.Type.DeleteFamily, cells));
    for(int i = 3; i < 5; i++) {
      entries.add(createEntry(TABLE_NAME1, i, KeyValue.Type.Put, cells));
    }
    SINK.replicateEntries(entries, CellUtil.createCellScanner(cells.iterator()));
    Get get = new Get(Bytes.toBytes(1));
    Result res = table1.get(get);
    assertEquals(0, res.size());
  }

  private AdminProtos.WALEntry createEntry(byte [] table, int row,  KeyValue.Type type, List<Cell> cells) {
    byte[] fam = Bytes.equals(table, TABLE_NAME1) ? FAM_NAME1 : FAM_NAME2;
    byte[] rowBytes = Bytes.toBytes(row);
    // Just make sure we don't get the same ts for two consecutive rows with
    // same key
    try {
      Thread.sleep(1);
    } catch (InterruptedException e) {
      LOG.info("Was interrupted while sleep, meh", e);
    }
    final long now = System.currentTimeMillis();
    KeyValue kv = null;
    if(type.getCode() == KeyValue.Type.Put.getCode()) {
      kv = new KeyValue(rowBytes, fam, fam, now,
          KeyValue.Type.Put, Bytes.toBytes(row));
    } else if (type.getCode() == KeyValue.Type.DeleteColumn.getCode()) {
      kv = new KeyValue(rowBytes, fam, fam,
          now, KeyValue.Type.DeleteColumn);
    } else if (type.getCode() == KeyValue.Type.DeleteFamily.getCode()) {
      kv = new KeyValue(rowBytes, fam, null,
          now, KeyValue.Type.DeleteFamily);
    }
    AdminProtos.WALEntry.Builder builder = AdminProtos.WALEntry.newBuilder();
    builder.setAssociatedCellCount(1);
    WALProtos.WALKey.Builder keyBuilder = WALProtos.WALKey.newBuilder();
    HBaseProtos.UUID.Builder uuidBuilder = HBaseProtos.UUID.newBuilder();
    uuidBuilder.setLeastSigBits(HConstants.DEFAULT_CLUSTER_ID.getLeastSignificantBits());
    uuidBuilder.setMostSigBits(HConstants.DEFAULT_CLUSTER_ID.getMostSignificantBits());
    keyBuilder.setClusterId(uuidBuilder.build());
    keyBuilder.setTableName(ByteStringer.wrap(table));
    keyBuilder.setWriteTime(now);
    keyBuilder.setEncodedRegionName(ByteStringer.wrap(HConstants.EMPTY_BYTE_ARRAY));
    keyBuilder.setLogSequenceNumber(-1);
    builder.setKey(keyBuilder.build());
    cells.add(kv);

    return builder.build();
  }
}
