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
package com.xiaomi.infra.hbase.salted;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestSaltedHTable {
  final Log LOG = LogFactory.getLog(getClass());
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static HConnection connection;
  
  private static final byte[] TEST_TABLE = Bytes.toBytes("test");
  private static final byte[] TEST_FAMILY = Bytes.toBytes("f1");

  private static final byte[] ROW_A = Bytes.toBytes("aaa");
  private static final byte[] ROW_B = Bytes.toBytes("bbb");
  private static final byte[] ROW_C = Bytes.toBytes("ccc");

  private static final byte[] qualifierCol1 = Bytes.toBytes("col1");

  private static final byte[] bytes1 = Bytes.toBytes(1);
  private static final byte[] bytes2 = Bytes.toBytes(2);
  private static final byte[] bytes3 = Bytes.toBytes(3);
  private static final byte[] bytes4 = Bytes.toBytes(4);
  private static final byte[] bytes5 = Bytes.toBytes(5);
  private static final byte[] bytes6 = Bytes.toBytes(6);
  private static final byte[] bytes7 = Bytes.toBytes(7);

  private HTableInterface saltedHTable;
  private HBaseAdmin admin;
  private List<Put> puts = new ArrayList<Put>();

  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster();
    connection = HConnectionManager.createConnection(TEST_UTIL.getConfiguration());
  }

  /**
   * @throws java.lang.Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    if (connection != null) {
      connection.close();
    }
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void before() throws Exception {
    admin = new HBaseAdmin(TEST_UTIL.getConfiguration());

    saltedHTable = createSaltedTable();
    Assert.assertTrue(saltedHTable instanceof SaltedHTable);
    saltedHTable.setAutoFlush(false);

    Put puta = new Put(ROW_A);
    puta.add(TEST_FAMILY, qualifierCol1, bytes1);
    puts.add(puta);

    Put putb = new Put(ROW_B);
    putb.add(TEST_FAMILY, qualifierCol1, bytes2);
    puts.add(putb);

    Put putc = new Put(ROW_C);
    putc.add(TEST_FAMILY, qualifierCol1, bytes3);
    puts.add(putc);

    saltedHTable.put(puts);
    saltedHTable.flushCommits();
  }

  @After
  public void after() throws Exception {
    try {
      if (admin != null) {
        admin.close();
      }
      if (saltedHTable != null) {
        saltedHTable.close();
      }
    } finally {
      TEST_UTIL.deleteTable(TEST_TABLE);
    }
  }

  protected HTableInterface createSaltedTable() throws IOException {
    return createSaltedTable(null);
  }
  
  protected HTableInterface createSaltedTable(Integer slotCounts) throws IOException {
    admin.createTable(getSaltedHTableDescriptor(slotCounts));
    return connection.getTable(TEST_TABLE);
  }
  
  protected HTableInterface createUnSaltedTable() throws IOException {
    admin.createTable(getUnSaltedHTableDescriptor());
    return connection.getTable(TEST_TABLE);
  }
  
  protected HTableDescriptor getUnSaltedHTableDescriptor() {
    return getUnSaltedHTableDescriptor(TEST_TABLE);
  }
  
  protected HTableDescriptor getUnSaltedHTableDescriptor(byte[] tableName) {
    HTableDescriptor desc = new HTableDescriptor(tableName);
    HColumnDescriptor column = new HColumnDescriptor(TEST_FAMILY);
    desc.addFamily(column);
    return desc;
  }
  
  protected HTableDescriptor getSaltedHTableDescriptor(Integer slotCounts) {
    HTableDescriptor desc = getUnSaltedHTableDescriptor();
    if (slotCounts != null) {
      desc.setSalted(OneBytePrefixKeySalter.class.getName(), slotCounts);
    } else {
      desc.setSalted(OneBytePrefixKeySalter.class.getName());
    }
    return desc;
  }
  
  protected HTable toHTable(HTableInterface hTableInterface) throws IOException {
    if (hTableInterface instanceof HTable) {
      return (HTable)hTableInterface;
    } else if (hTableInterface instanceof SaltedHTable) {
      return (HTable)((SaltedHTable)hTableInterface).getRawTable();
    }
    throw new IOException("error HTableInterface implementation");
  }
  
  @Test
  public void testCreateSaltedTable() throws IOException {
    TEST_UTIL.deleteTable(TEST_TABLE);
    
    // pre-split by salted-slots
    HTable table = toHTable(createSaltedTable());
    byte[][] stopKeys = table.getEndKeys();
    byte[][] slots = new OneBytePrefixKeySalter().getAllSalts();
    Assert.assertEquals(slots.length, stopKeys.length);
    for (int i = 0; i < slots.length - 1; ++i) {
      Assert.assertArrayEquals(slots[i + 1], stopKeys[i]);
    }
    table.close();
    
    // set slots count
    TEST_UTIL.deleteTable(TEST_TABLE);
    int slotCount = 10;
    table = toHTable(createSaltedTable(slotCount));
    slots = new OneBytePrefixKeySalter(slotCount).getAllSalts();
    Assert.assertEquals(slotCount, slots.length);
    stopKeys = table.getEndKeys();
    Assert.assertEquals(slots.length, stopKeys.length);
    for (int i = 0; i < slots.length - 1; ++i) {
      Assert.assertArrayEquals(slots[i + 1], stopKeys[i]);
    }
    table.close();
    
    // won't pre-split by salted-slots if splitKeys are set
    TEST_UTIL.deleteTable(TEST_TABLE);
    byte[] splitKey = Bytes.toBytes("aa");
    HTableDescriptor desc = getSaltedHTableDescriptor(null);
    admin.createTable(desc, new byte[][]{splitKey});
    table = toHTable(connection.getTable(TEST_TABLE));
    stopKeys = table.getEndKeys();
    Assert.assertEquals(2, stopKeys.length);
    Assert.assertArrayEquals(splitKey, stopKeys[0]);
    table.close();
  }
  
  @Test
  public void testModifyKeySalter() throws IOException {
    TEST_UTIL.deleteTable(TEST_TABLE);
    createSaltedTable();
    HTableDescriptor desc = admin.getTableDescriptor(TEST_TABLE);

    // unset KeySalter
    desc.setSalted(null);
    try {
      admin.modifyTable(TEST_TABLE, desc);
      Assert.fail();
    } catch (IOException e) {
      Assert
          .assertTrue(e.getMessage().indexOf("can not modify the KeySalter attribute of table") >= 0);
    }
    
    // use another Salter class
    desc.setSalted("abc");
    try {
      admin.modifyTable(TEST_TABLE, desc);
      Assert.fail();
    } catch (IOException e) {
      Assert
          .assertTrue(e.getMessage().indexOf("can not modify the KeySalter attribute of table") >= 0);
    }
    
    // set KeySalter
    TEST_UTIL.deleteTable(TEST_TABLE);
    createUnSaltedTable();
    desc = admin.getTableDescriptor(TEST_TABLE);
    desc.setSalted(OneBytePrefixKeySalter.class.getName());
    try {
      admin.modifyTable(TEST_TABLE, desc);
      Assert.fail();
    } catch (IOException e) {
      Assert
          .assertTrue(e.getMessage().indexOf("can not modify the KeySalter attribute of table") >= 0);
    }
  }
  
  @Test
  public void testGetNonSaltedHTableFromHConnection() throws IOException {
    byte[] testTable = Bytes.toBytes("unsalted_test_table");
    HTableDescriptor desc = getUnSaltedHTableDescriptor(testTable);
    admin.createTable(desc);
    for (int i = 0; i < 10; ++i) {
      HTableInterface table = connection.getTable(testTable);
      Assert.assertTrue(table instanceof HTable);
      table.close();
      
      table = connection.getTable(TEST_TABLE);
      Assert.assertTrue(table instanceof SaltedHTable);
      table.close();
    }
  }
  
  @Test
  public void testGetHTableInstance() throws IOException {
    TEST_UTIL.deleteTable(TEST_TABLE);
    createUnSaltedTable();
    HTableInterface table = connection.getTable(TEST_TABLE);
    Assert.assertTrue(table instanceof SaltedHTable);
  }
  
  /**
   *test the methods of put,delete and scan.
   * @throws Exception
   */
  @Test
  public void TestPutDeleteScan()  throws Exception {
    Scan scan = new Scan();
    int count = 0;
    ResultScanner scanner = saltedHTable.getScanner(scan);
    Result result = null;
    List<Result> results = new ArrayList<Result>();
    while(null != (result = scanner.next()) ) {
      results.add(result);
      count++;
    }
    assertEquals(count, puts.size());
    Assert.assertArrayEquals(ROW_A, results.get(0).getRow());
    Assert.assertArrayEquals(ROW_B, results.get(1).getRow());
    Assert.assertArrayEquals(ROW_C, results.get(2).getRow());
    scanner.close();

    // test order scan
    scanner = ((SaltedHTable)saltedHTable).getScanner(scan, null, false, false);
    results.clear();
    count = 0;
    while(null != (result = scanner.next()) ) {
      results.add(result);
      count++;
    }
    Assert.assertEquals(3, count);
    List<byte[]> saltedRowkeys = new ArrayList<byte[]>();
    KeySalter salter = ((SaltedHTable)saltedHTable).getKeySalter();
    for (int i = 0; i < results.size(); ++i) {
      saltedRowkeys.add(salter.salt(results.get(i).getRow()));
    }
    // check order
    Assert.assertTrue(Bytes.compareTo(saltedRowkeys.get(0), saltedRowkeys.get(1)) < 0);
    Assert.assertTrue(Bytes.compareTo(saltedRowkeys.get(1), saltedRowkeys.get(2)) < 0);
    scanner.close();
    
    count = 0;
    List<Delete> deletes = new ArrayList<Delete>();
    deletes.add( new Delete(ROW_A) );
    deletes.add( new Delete(ROW_B) );
    deletes.add( new Delete(ROW_C) );

    saltedHTable.delete(deletes);

    scan = new Scan();
    scan.addColumn(TEST_FAMILY, qualifierCol1);
    count = 0;
    scanner = saltedHTable.getScanner(scan);
    result = null;
    while(null != (result = scanner.next()) ) {
      count++;
    }
    assertEquals(count, 0);
    scanner.close();
  }

  @Test
  public void TestCheckAndPut() throws IOException {
    Put putA = new Put(ROW_A).add(TEST_FAMILY, qualifierCol1, bytes5);
    assertFalse(saltedHTable.checkAndPut(ROW_A, TEST_FAMILY, qualifierCol1, /* expect */bytes2,
        putA/* newValue */));
    assertTrue(saltedHTable.checkAndPut(ROW_A, TEST_FAMILY, qualifierCol1, /* expect */bytes1,
        putA/* newValue */));
    checkRowValue(ROW_A, bytes5);

    Put putB = new Put(ROW_B).add(TEST_FAMILY, qualifierCol1, bytes6);
    assertFalse(saltedHTable.checkAndPut(ROW_B, TEST_FAMILY, qualifierCol1, CompareOp.EQUAL,
        bytes3, putB/* newValue */));
    assertTrue(saltedHTable.checkAndPut(ROW_B, TEST_FAMILY, qualifierCol1, CompareOp.EQUAL,
        bytes2, putB/* newValue */));
    checkRowValue(ROW_B, bytes6);

    Put putC = new Put(ROW_C).add(TEST_FAMILY, qualifierCol1, bytes7);
    assertFalse(saltedHTable.checkAndPut(ROW_C, TEST_FAMILY, qualifierCol1, CompareOp.LESS,
        bytes4, putC/* newValue */));
    assertTrue(saltedHTable.checkAndPut(ROW_C, TEST_FAMILY, qualifierCol1, CompareOp.LESS,
        bytes2, putC/* newValue */));
    checkRowValue(ROW_C, bytes7);
  }

  @Test
  public void TestCheckAndDelete() throws IOException {
    Delete deleteA = new Delete(ROW_A);
    assertFalse(saltedHTable.checkAndDelete(ROW_A, TEST_FAMILY, qualifierCol1, bytes2, deleteA));
    assertTrue(saltedHTable.checkAndDelete(ROW_A, TEST_FAMILY, qualifierCol1, bytes1, deleteA));
    checkRowValue(ROW_A, null);

    Delete deleteB = new Delete(ROW_B);
    assertFalse(saltedHTable.checkAndDelete(ROW_B, TEST_FAMILY, qualifierCol1, CompareOp.EQUAL,
        bytes3, deleteB));
    assertTrue(saltedHTable.checkAndDelete(ROW_B, TEST_FAMILY, qualifierCol1, CompareOp.EQUAL,
        bytes2, deleteB));
    checkRowValue(ROW_B, null);

    Delete deleteC = new Delete(ROW_C);
    assertFalse(saltedHTable.checkAndDelete(ROW_C, TEST_FAMILY, qualifierCol1, CompareOp.GREATER,
        bytes2, deleteC));
    assertTrue(saltedHTable.checkAndDelete(ROW_C, TEST_FAMILY, qualifierCol1, CompareOp.GREATER,
        bytes4, deleteC));
    checkRowValue(ROW_C, null);
  }

    private void checkRowValue(byte[] row, byte[] expectedValue) throws IOException {
    Get get = new Get(row).addColumn(TEST_FAMILY, qualifierCol1);
    Result result = saltedHTable.get(get);
    byte[] actualValue = result.getValue(TEST_FAMILY, qualifierCol1);
    assertArrayEquals(expectedValue, actualValue);
  }
}

