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
package org.apache.hadoop.hbase.protobuf;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.AdminService.BlockingInterface;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.ReplicateWALEntryResponse;
import org.apache.hadoop.hbase.regionserver.wal.HLog.Entry;
import org.apache.hadoop.hbase.regionserver.wal.HLogKey;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import com.google.protobuf.ServiceException;


@Category(SmallTests.class)
public class TestReplicationProtobuf {

  private HLogKey createFakeHLogKey() {
    long ts = System.currentTimeMillis();
    return new HLogKey(Bytes.toBytes("abc"), TableName.valueOf("Table" + ts), ts, ts,
        UUID.randomUUID());
  }

  @Test
  public void testReplicateSomeEmptyWALEntries() throws IOException, ServiceException {
    BlockingInterface admin = Mockito.mock(BlockingInterface.class);
    Mockito.when(admin.replicateWALEntry(Mockito.any(), Mockito.any()))
        .thenReturn(ReplicateWALEntryResponse.getDefaultInstance());

    Entry[] entries = new Entry[] {};
    Assert.assertEquals(0, ReplicationProtbufUtil.replicateWALEntry(admin, entries));

    WALEdit edit = new WALEdit();
    edit.add(CellUtil.createCell(Bytes.toBytes("row"), Bytes.toBytes("cf"), Bytes.toBytes("cq"),
      System.currentTimeMillis(), Type.Put.getCode(), Bytes.toBytes("value")));
    Entry nonEmptyEdit = new Entry(createFakeHLogKey(), edit);

    Entry emptyEdit1 = new Entry(createFakeHLogKey(), new WALEdit());
    Entry emtpyEdit2 = new Entry(createFakeHLogKey(), new WALEdit());

    entries = new Entry[] { nonEmptyEdit, emptyEdit1, emtpyEdit2 };
    Assert.assertEquals(1, ReplicationProtbufUtil.replicateWALEntry(admin, entries));
  }

  /**
   * Little test to check we can basically convert list of a list of KVs into a CellScanner
   * @throws IOException
   */
  @Test
  public void testGetCellScanner() throws IOException {
    List<Cell> a = new ArrayList<Cell>();
    KeyValue akv = new KeyValue(Bytes.toBytes("a"), -1L);
    a.add(akv);
    // Add a few just to make it less regular.
    a.add(new KeyValue(Bytes.toBytes("aa"), -1L));
    a.add(new KeyValue(Bytes.toBytes("aaa"), -1L));
    List<Cell> b = new ArrayList<Cell>();
    KeyValue bkv = new KeyValue(Bytes.toBytes("b"), -1L);
    a.add(bkv);
    List<Cell> c = new ArrayList<Cell>();
    KeyValue ckv = new KeyValue(Bytes.toBytes("c"), -1L);
    c.add(ckv);
    List<List<? extends Cell>> all = new ArrayList<List<? extends Cell>>();
    all.add(a);
    all.add(b);
    all.add(c);
    CellScanner scanner = ReplicationProtbufUtil.getCellScanner(all, 0);
    testAdvancetHasSameRow(scanner, akv);
    // Skip over aa
    scanner.advance();
    // Skip over aaa
    scanner.advance();
    testAdvancetHasSameRow(scanner, bkv);
    testAdvancetHasSameRow(scanner, ckv);
    assertFalse(scanner.advance());
  }

  private void testAdvancetHasSameRow(CellScanner scanner, final KeyValue kv) throws IOException {
    scanner.advance();
    assertTrue(Bytes.equals(scanner.current().getRowArray(), scanner.current().getRowOffset(),
        scanner.current().getRowLength(),
      kv.getRowArray(), kv.getRowOffset(), kv.getRowLength()));
  }
}
