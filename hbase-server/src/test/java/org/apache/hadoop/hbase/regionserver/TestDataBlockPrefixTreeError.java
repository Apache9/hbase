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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.primitives.Ints;

@Category(MediumTests.class)
public class TestDataBlockPrefixTreeError {

  private HBaseTestingUtility util;

  private HConnection conn;

  private HTableInterface table;

  @Before
  public void setUp() throws Exception {
    util = new HBaseTestingUtility();
    util.startMiniCluster(1);
    HBaseAdmin admin = util.getHBaseAdmin();
    NamespaceDescriptor tableSpace = NamespaceDescriptor.create("Test").build();
    admin.createNamespace(tableSpace);
    TableName tableName = TableName.valueOf(tableSpace.getName(), "PrefixTreeError");
    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(new HColumnDescriptor("F").setDataBlockEncoding(DataBlockEncoding.PREFIX_TREE));
    admin.createTable(desc);
    conn = HConnectionManager.createConnection(util.getConfiguration());
    table = conn.getTable(tableName);
  }

  @After
  public void tearDown() throws Exception {
    table.close();
    conn.close();
    util.shutdownMiniCluster();
  }

  private byte[] createRowKey(int keyPart1, int keyPart2) throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream(16);
    DataOutputStream dos = new DataOutputStream(bos);
    dos.writeInt(keyPart1);
    dos.writeInt(keyPart2);
    return bos.toByteArray();
  }

  @Test
  public void test() throws IOException, InterruptedException {
    table.put(new Put(createRowKey(1, 12345)).add(Bytes.toBytes("F"), Bytes.toBytes("Q"),
        new byte[] {
          1
        }));
    int keyPart1 = 12345;
    table.put(new Put(createRowKey(keyPart1, 0x01000000)).add(Bytes.toBytes("F"),
        Bytes.toBytes("Q"), new byte[] {
          1
        }));
    table.put(new Put(createRowKey(keyPart1, 0x01010000)).add(Bytes.toBytes("F"),
        Bytes.toBytes("Q"), new byte[] {
          1
        }));
    table.put(new Put(createRowKey(keyPart1, 0x02000000)).add(Bytes.toBytes("F"),
        Bytes.toBytes("Q"), new byte[] {
          1
        }));
    table.put(new Put(createRowKey(keyPart1, 0x02020000)).add(Bytes.toBytes("F"),
        Bytes.toBytes("Q"), new byte[] {
          1
        }));
    table.put(new Put(createRowKey(keyPart1, 0x03000000)).add(Bytes.toBytes("F"),
        Bytes.toBytes("Q"), new byte[] {
          1
        }));
    table.put(new Put(createRowKey(keyPart1, 0x03030000)).add(Bytes.toBytes("F"),
        Bytes.toBytes("Q"), new byte[] {
          1
        }));
    table.put(new Put(createRowKey(keyPart1, 0x04000000)).add(Bytes.toBytes("F"),
        Bytes.toBytes("Q"), new byte[] {
          1
        }));
    table.put(new Put(createRowKey(keyPart1, 0x04040000)).add(Bytes.toBytes("F"),
        Bytes.toBytes("Q"), new byte[] {
          1
        }));
    util.flush();
    ResultScanner scanner = table.getScanner(new Scan(Bytes.toBytes(keyPart1)));
    Result result = scanner.next();
    assertEquals(keyPart1, Bytes.toInt(result.getRow()));
    assertEquals(0x01000000, Bytes.toInt(result.getRow(), Ints.BYTES));
    scanner.close();
  }
}
