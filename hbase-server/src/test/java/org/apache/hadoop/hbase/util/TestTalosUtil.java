/**
 * Copyright The Apache Software Foundation
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
package org.apache.hadoop.hbase.util;

import com.xiaomi.infra.thirdparty.galaxy.talos.thrift.Message;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.FDSMessageScanner;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALKeyImpl;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.apache.hadoop.hbase.HConstants.EMPTY_BYTE_ARRAY;
import static org.apache.hadoop.hbase.util.TalosUtil.HEADER_SIZE;

@Category(SmallTests.class)
public class TestTalosUtil {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestTalosUtil.class);
  private static final String REGION_NAME = "testRegion";
  private static final TableName TABLE_NAME = TableName.valueOf("test:testTable");
  private static final byte[] FAMILY = Bytes.toBytes("family");
  private static final byte[] QUALIFIER = Bytes.toBytes("quailifier");

  @Test
  public void testEntrytoMessageThenToResult() throws IOException {
    WAL.Entry entry = getEntry(1, 100);
    List<Message> messages = TalosUtil.constructMessages(entry);
    List<Result> results = parseResultFromMessages(messages, new TimeRange());
    Assert.assertEquals("size of results", 100, results.size());
    int count = 0;
    for (Result result : results) {
      Assert.assertEquals("is the rowkey same: ", 0,
          Bytes.compareTo(result.getRow(), Bytes.toBytes(String.valueOf(count))));
      count++;
    }
  }

  @Test
  public void testMultipleKvs() throws IOException {
    WALKeyImpl key = new WALKeyImpl(Bytes.toBytes(REGION_NAME), TABLE_NAME, 1, 100, UUID.randomUUID());
    WALEdit edit = new WALEdit();
    for (int i = 0; i < 10; i++) {
      addOneBigPut(edit, i);
    }
    WAL.Entry entry = new WAL.Entry(key, edit);
    List<Message> messages = TalosUtil.constructMessages(entry);
    List<Result> results = parseResultFromMessages(messages, TimeRange.allTime());
    results.stream().forEach(result -> System.out.println(Bytes.toStringBinary(result.getRow())));
    Assert.assertEquals("size of results", 10, results.size());
    Result exampleResult = results.get(0);
    Assert.assertEquals("size of kv", 100, exampleResult.size());
  }

  private void addOneBigPut(WALEdit edit, int count) {
    for (int i = 0; i < 100; i++) {
      Cell cell = CellUtil.createCell(Bytes.toBytesBinary("test" + count), FAMILY, Bytes.toBytes(i),
          System.currentTimeMillis(), KeyValue.Type.Put.getCode(), Bytes.toBytes("test" + i));
      edit.add(cell);
    }
  }

  @Test
  public void testTimerange() throws IOException {
    WAL.Entry entry = getEntry(1, 100);
    List<Message> messages = TalosUtil.constructMessages(entry);
    List<Result> results = parseResultFromMessages(messages, new TimeRange(1, 99));
    Assert.assertEquals("size of results", 0, results.size());
  }

  private List<Result> parseResultFromMessages(List<Message> messages, TimeRange timeRange)
      throws IOException {
    List<Result> results = new ArrayList<>();
    for (Message message : messages) {
      long seqNum = Bytes.toLong(message.getMessage(), 0, Bytes.SIZEOF_LONG);
      int index = Bytes.toInt(message.getMessage(), Bytes.SIZEOF_LONG, Bytes.SIZEOF_INT);
      int totalSlices =
          Bytes.toInt(message.getMessage(), Bytes.SIZEOF_LONG + Bytes.SIZEOF_INT, Bytes.SIZEOF_INT);
      byte[] messageBytes =
          Bytes.copy(message.getMessage(), HEADER_SIZE, message.getMessage().length - HEADER_SIZE);
      results.addAll(TalosUtil.convertMessageToResult(TalosUtil.mergeChunksToMessage(
          Arrays.asList(new TalosUtil.MessageChunk(HEADER_SIZE + messageBytes.length, seqNum, index,
              totalSlices, messageBytes))),
          timeRange));
    }
    return results;
  }

  @Test
  public void testEncodeTableName() {
    String tableA = "miui:vip-test_a";
    String tableB = "miui-vip:test_a";
    String encodedA = TalosUtil.encodeTableName(tableA);
    String encodedB = TalosUtil.encodeTableName(tableB);
    Assert.assertNotEquals(encodedA, encodedB);
    Assert.assertEquals(TalosUtil.parseFromTopicName(encodedA), tableA);
    Assert.assertEquals(TalosUtil.parseFromTopicName(encodedB), tableB);
  }

  @Test
  public void testSmallEntry() throws Exception {
    WAL.Entry entry = getEntry(1, 1000);
    List<Message> messages = TalosUtil.constructMessages(entry);
    Assert.assertTrue(messages.size() == 1);
    DataInputStream inputStream = getDataInputStreamFromMessages(messages);
    FDSMessageScanner scanner = new FDSMessageScanner(new LinkedList<>(), inputStream);
    Assert.assertTrue(scanner.loadNextMessage());
    List<Result> results = TalosUtil.convertMessageToResult(
        TalosUtil.mergeChunksToMessage(scanner.getMessageChunks()), new TimeRange());
    Assert.assertEquals("size of result ", 1000, results.size());
  }

  @Test
  public void testHugeEntry() throws Exception {
    WAL.Entry entry = getEntry(1, 800000);
    List<Message> messages = TalosUtil.constructMessages(entry);
    Assert.assertTrue(messages.size() > 1);
    DataInputStream inputStream = getDataInputStreamFromMessages(messages);
    FDSMessageScanner scanner = new FDSMessageScanner(new LinkedList<>(), inputStream);
    Assert.assertTrue(scanner.loadNextMessage());
    List<Result> results = TalosUtil.convertMessageToResult(
        TalosUtil.mergeChunksToMessage(scanner.getMessageChunks()), new TimeRange());
    Assert.assertEquals("size of result ", 800000, results.size());
  }

  @Test
  public void testUncompleteMessages() throws Exception {
    LinkedList<TalosUtil.MessageChunk> chunks  = new LinkedList<>();
    WAL.Entry entry = getEntry(1, 800000);
    List<Message> messages = TalosUtil.constructMessages(entry);
    Assert.assertTrue(messages.size() > 1);
    DataInputStream inputStream =
        getDataInputStreamFromMessages(messages.subList(0, messages.size() - 1));
    FDSMessageScanner scanner = new FDSMessageScanner(chunks, inputStream);
    Throwable e = null;
    try {
      scanner.loadNextMessage();
    } catch (Throwable eof) {
      e = eof;
    }
    Assert.assertNotNull(e);
    Assert.assertFalse(scanner.getMessageChunks().getLast().isLastChunk());
    WAL.Entry smallEntry = getEntry(2, 1000);
    List<Message> completeMessages = TalosUtil.constructMessages(smallEntry);
    List<Message> testMessages = new ArrayList<>();
    testMessages.addAll(messages.subList(0, messages.size() - 2));
    testMessages.addAll(completeMessages);
    DataInputStream inputStream2 = getDataInputStreamFromMessages(testMessages);
    scanner = new FDSMessageScanner(chunks, inputStream2);
    Assert.assertTrue(scanner.loadNextMessage());
    List<Result> results = TalosUtil
        .convertMessageToResult(TalosUtil.mergeChunksToMessage(chunks), TimeRange.allTime());
    Assert.assertEquals("size of result ", 1000, results.size());
  }

  @Test
  public void testDuplicateMessages() throws Exception {
    WAL.Entry entry = getEntry(1, 800000);
    List<Message> messages = TalosUtil.constructMessages(entry);
    Assert.assertTrue(messages.size() > 1);
    List<Message> duplicateMessageList = new ArrayList<>();
    duplicateMessageList.addAll(messages.subList(0, messages.size() / 2));
    duplicateMessageList.addAll(messages);
    Assert.assertTrue(duplicateMessageList.size() > messages.size());
    DataInputStream inputStream = getDataInputStreamFromMessages(duplicateMessageList);
    FDSMessageScanner scanner = new FDSMessageScanner(new LinkedList<>(), inputStream);
    Assert.assertTrue(scanner.loadNextMessage());
    List<Result> results = TalosUtil.convertMessageToResult(
        TalosUtil.mergeChunksToMessage(scanner.getMessageChunks()), new TimeRange());
    Assert.assertEquals("size of result ", 800000, results.size());
  }

  private WAL.Entry getEntry(long seqNum, int count) {
    WALKeyImpl key =
        new WALKeyImpl(Bytes.toBytes(REGION_NAME), TABLE_NAME, seqNum, 100, UUID.randomUUID());
    WALEdit edit = new WALEdit();
    for (int i = 0; i < count; i++) {
      Cell cell = CellUtil.createCell(Bytes.toBytesBinary(String.valueOf(i)), FAMILY, QUALIFIER,
          System.currentTimeMillis(), KeyValue.Type.Put.getCode(), Bytes.toBytes("test" + i));
      edit.add(cell);
    }
    WAL.Entry entry = new WAL.Entry(key, edit);
    return entry;
  }

  private DataInputStream getDataInputStreamFromMessages(List<Message> messages) {
    byte[] result = EMPTY_BYTE_ARRAY;
    for (Message message : messages) {
      result = Bytes.add(result, Bytes.toBytes(message.getMessage().length), message.getMessage());
    }
    DataInputStream inputStream = new DataInputStream(new ByteArrayInputStream(result));
    return inputStream;
  }

}
