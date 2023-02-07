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
package org.apache.hadoop.hbase.replication.regionserver;

import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CellBuilderFactory;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.HBaseCommonTestingUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.regionserver.MultiVersionConcurrencyControl;
import org.apache.hadoop.hbase.regionserver.wal.ProtobufLogWriter;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.ReplicationTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALKeyImpl;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;

/**
 * Enable compression and reset the WALEntryStream while reading in ReplicationSourceWALReader.
 * <p/>
 * This is used to confirm that we can work well when hitting EOFException in the middle when
 * reading a WAL entry, when compression is enabled.
 */
@Category({ ReplicationTests.class, MediumTests.class })
public class TestWALEntryStreamCompressionReset {

  private static final HBaseCommonTestingUtil UTIL = new HBaseCommonTestingUtil();

  private static TableName TABLE_NAME = TableName.valueOf("reset");

  private static RegionInfo REGION_INFO = RegionInfoBuilder.newBuilder(TABLE_NAME).build();

  private static byte[] FAMILY = Bytes.toBytes("family");

  private static MultiVersionConcurrencyControl MVCC = new MultiVersionConcurrencyControl();

  private static NavigableMap<byte[], Integer> SCOPE;

  private static FileSystem FS;

  private static ReplicationSource SOURCE;

  private static MetricsSource METRICS_SOURCE;

  private static ReplicationSourceLogQueue LOG_QUEUE;

  // return the wal path, and also the end offset of last wal entry
  private static Pair<Path, Long> generateWAL() throws Exception {
    Path path = UTIL.getDataTestDir("wal");
    ProtobufLogWriter writer = new ProtobufLogWriter();
    writer.init(FS, path, UTIL.getConfiguration(), false, FS.getDefaultBlockSize(path), null);
    WALKeyImpl key = new WALKeyImpl(REGION_INFO.getEncodedNameAsBytes(), TABLE_NAME,
      EnvironmentEdgeManager.currentTime(), MVCC, SCOPE);
    WALEdit edit = new WALEdit();
    edit.add(CellBuilderFactory.create(CellBuilderType.DEEP_COPY).setRow(Bytes.toBytes(1))
      .setFamily(FAMILY).setQualifier(Bytes.toBytes("qualifier")).setValue(Bytes.toBytes("v1"))
      .build());
    writer.append(new WAL.Entry(key, edit));

    WALEdit edit2 = new WALEdit();
    edit.add(CellBuilderFactory.create(CellBuilderType.DEEP_COPY).setRow(Bytes.toBytes(2))
      .setFamily(FAMILY).setQualifier(Bytes.toBytes("qualifier1")).setValue(Bytes.toBytes("v2"))
      .build());
    edit.add(CellBuilderFactory.create(CellBuilderType.DEEP_COPY).setRow(Bytes.toBytes(2))
      .setFamily(FAMILY).setQualifier(Bytes.toBytes("qualifier")).setValue(Bytes.toBytes("v3"))
      .build());
    writer.append(new WAL.Entry(key, edit2));
    writer.sync(false);
    long offset = writer.getSyncedLength();
    writer.close();
    return Pair.newPair(path, offset);
  }

  @BeforeClass
  public static void setUp() throws Exception {
    UTIL.getConfiguration().setBoolean(HConstants.ENABLE_WAL_COMPRESSION, true);
    FS = FileSystem.getLocal(UTIL.getConfiguration());
    generateWAL();
  }

  @AfterClass
  public static void tearDown() {
    UTIL.cleanupTestDir();
  }
}
