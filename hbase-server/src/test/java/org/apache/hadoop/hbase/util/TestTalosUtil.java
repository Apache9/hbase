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

import com.xiaomi.infra.galaxy.talos.thrift.Message;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.wal.HLogKey;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestTalosUtil {
  private static final String REGION_NAME = "testRegion";
  private static final TableName TABLE_NAME = TableName.valueOf("test:testTable");
  private static final byte[] FAMILY = Bytes.toBytes("family");
  private static final byte[] QUALIFIER = Bytes.toBytes("quailifier");

  @Test
  public void testEntrytoMessageThenToResult() throws IOException {
    HLogKey key = new HLogKey(Bytes.toBytes(REGION_NAME), TABLE_NAME, 1, System.currentTimeMillis(),
        UUID.randomUUID());
    WALEdit edit = new WALEdit();
    for (int i = 0; i < 100; i++) {
      Cell cell = CellUtil.createCell(Bytes.toBytesBinary(String.valueOf(i)), FAMILY, QUALIFIER,
        System.currentTimeMillis(), KeyValue.Type.Put.getCode(), Bytes.toBytes("test" + i));
      edit.add(cell);
    }
    HLog.Entry entry = new HLog.Entry(key, edit);
    Message message = TalosUtil.constructSingleMessage(entry);
    List<Result> results =
        TalosUtil.convertMessageToResult(message, new TimeRange());
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
    HLogKey key = new HLogKey(Bytes.toBytes(REGION_NAME), TABLE_NAME, 1, 100, UUID.randomUUID());
    WALEdit edit = new WALEdit();
    for (int i = 0; i < 10; i++) {
      addOneBigPut(edit, i);
    }
    HLog.Entry entry = new HLog.Entry(key, edit);
    Message message = TalosUtil.constructSingleMessage(entry);
    List<Result> results =
        TalosUtil.convertMessageToResult(message, new TimeRange());
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
    HLogKey key = new HLogKey(Bytes.toBytes(REGION_NAME), TABLE_NAME, 1, 100, UUID.randomUUID());
    WALEdit edit = new WALEdit();
    for (int i = 0; i < 100; i++) {
      Cell cell = CellUtil.createCell(Bytes.toBytesBinary(String.valueOf(i)), FAMILY, QUALIFIER,
        System.currentTimeMillis(), KeyValue.Type.Put.getCode(), Bytes.toBytes("test" + i));
      edit.add(cell);
    }
    HLog.Entry entry = new HLog.Entry(key, edit);
    Message message = TalosUtil.constructSingleMessage(entry);
    List<Result> results =
        TalosUtil.convertMessageToResult(message, new TimeRange(1, 99));
    Assert.assertEquals("size of results", 0, results.size());
  }

  @Test
  public void testEncodeTableName() throws IOException {
    String tableA = "miui:vip-test_a";
    String tableB = "miui-vip:test_a";
    String encodedA = TalosUtil.encodeTableName(tableA);
    String encodedB = TalosUtil.encodeTableName(tableB);
    Assert.assertNotEquals(encodedA, encodedB);
    Assert.assertEquals(TalosUtil.parseFromTopicName(encodedA), tableA);
    Assert.assertEquals(TalosUtil.parseFromTopicName(encodedB), tableB);

  }

}
