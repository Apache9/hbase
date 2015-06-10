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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
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
import org.apache.hadoop.hbase.client.replication.ReplicationAdmin;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
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
  private static HBaseTestingUtility TEST_UTIL;
  private static HBaseTestingUtility utility1;
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

  private SaltedHTable saltedHTable;
  private static final int defaultSlotsCount = 10;
  private HBaseAdmin admin;
  private List<Put> puts = new ArrayList<Put>();

  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    conf.setBoolean(HConstants.REPLICATION_ENABLE_KEY, true);
    TEST_UTIL = new HBaseTestingUtility(conf);
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
    if (utility1 != null) {
      utility1.shutdownMiniCluster();
    }
    TEST_UTIL.shutdownMiniCluster();
  }

  @Before
  public void before() throws Exception {
    admin = new HBaseAdmin(TEST_UTIL.getConfiguration());
    saltedHTable = createSaltedTable(defaultSlotsCount);
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

  protected SaltedHTable createSaltedTable() throws IOException {
    return createSaltedTable(null);
  }
  
  protected SaltedHTable createSaltedTable(Integer slotCounts) throws IOException {
    admin.createTable(getSaltedHTableDescriptor(slotCounts));
    return (SaltedHTable)connection.getTable(TEST_TABLE);
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
    scanner = saltedHTable.getScanner(scan, null, false, false);
    results.clear();
    count = 0;
    while(null != (result = scanner.next()) ) {
      results.add(result);
      count++;
    }
    Assert.assertEquals(3, count);
    List<byte[]> saltedRowkeys = new ArrayList<byte[]>();
    KeySalter salter = saltedHTable.getKeySalter();
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
  public void TestScanWithSalts()  throws Exception {
    byte[][] rows = new byte[][]{ROW_A, ROW_B, ROW_C};
    KeySalter salter = saltedHTable.getKeySalter();
    byte[][] salts = new byte[][] { salter.getSalt(ROW_A), salter.getSalt(ROW_B),
        salter.getSalt(ROW_C) };
    for (int i = 0; i < rows.length; ++i) {
      byte[] salt = salts[i];
      Scan scan = new Scan();
      ResultScanner scanner = saltedHTable.getScanner(scan, new byte[][]{salt});
      Result result = scanner.next();
      Assert.assertNull(scanner.next());
      scanner.close();
      Assert.assertArrayEquals(rows[i], result.getRow());
    }
    
    ResultScanner scanner = saltedHTable
        .getScanner(new Scan(), new byte[][] { salts[0], salts[2] });
    List<Result> results = new ArrayList<Result>();
    Result result = null;
    while ((result = scanner.next()) != null) {
      results.add(result);
    }
    scanner.close();
    Assert.assertEquals(2, results.size());
    Assert.assertArrayEquals(ROW_A, results.get(0).getRow());
    Assert.assertArrayEquals(ROW_C, results.get(1).getRow());
  }

  @Test
  public void TestCheckAndPut() throws IOException {
    Put putA = new Put(ROW_A).add(TEST_FAMILY, qualifierCol1, bytes5);
    assertFalse(saltedHTable.checkAndPut(ROW_A, TEST_FAMILY, qualifierCol1, /* expect */bytes2,
        putA/* newValue */));
    assertTrue(saltedHTable.checkAndPut(ROW_A, TEST_FAMILY, qualifierCol1, /* expect */bytes1,
        putA/* newValue */));
    checkRowValue(ROW_A, bytes5);
  }

  @Test
  public void TestCheckAndDelete() throws IOException {
    Delete deleteA = new Delete(ROW_A);
    assertFalse(saltedHTable.checkAndDelete(ROW_A, TEST_FAMILY, qualifierCol1, bytes2, deleteA));
    assertTrue(saltedHTable.checkAndDelete(ROW_A, TEST_FAMILY, qualifierCol1, bytes1, deleteA));
    checkRowValue(ROW_A, null);
  }

    private void checkRowValue(byte[] row, byte[] expectedValue) throws IOException {
    Get get = new Get(row).addColumn(TEST_FAMILY, qualifierCol1);
    Result result = saltedHTable.get(get);
    byte[] actualValue = result.getValue(TEST_FAMILY, qualifierCol1);
    assertArrayEquals(expectedValue, actualValue);
  }

  @Test
  public void testReplication() throws Exception {
    Configuration conf1 = HBaseConfiguration.create();
    conf1.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/1");
    conf1.setBoolean(HConstants.REPLICATION_ENABLE_KEY, true);

    utility1 = new HBaseTestingUtility(conf1);
    utility1.setZkCluster(TEST_UTIL.getZkCluster());
//    utility1.startMiniZKCluster();
    utility1.startMiniCluster(2);
    
    TEST_UTIL.deleteTable(TEST_TABLE);
    HTableDescriptor tableDesc = getSaltedHTableDescriptor(10);
    tableDesc.getFamily(TEST_FAMILY).setScope(1);
    admin.createTable(tableDesc);
    
    HBaseAdmin admin1 = new HBaseAdmin(utility1.getConfiguration());
    admin1.createTable(tableDesc);
    ReplicationAdmin repAdmin = new ReplicationAdmin(TEST_UTIL.getConfiguration());
    repAdmin.addPeer("10", utility1.getClusterKey(), Bytes.toString(TEST_TABLE));
    
    // write data to source cluster
    HTableInterface table = connection.getTable(TEST_TABLE);
    HConnection connection1 = HConnectionManager.createConnection(utility1.getConfiguration());
    HTableInterface table1 = connection1.getTable(TEST_TABLE);
    
    Put put = new Put(ROW_A).add(TEST_FAMILY, qualifierCol1, bytes1);
    table.put(put);
    Threads.sleep(10000); // wait to replicate to peer cluster
    Get get = new Get(ROW_A);
    Result result = table1.get(get);
    Assert.assertTrue(!result.isEmpty());    
    
    // delete data from source cluster
    Delete delete = new Delete(ROW_A).deleteColumns(TEST_FAMILY, qualifierCol1);
    table.delete(delete);
    Threads.sleep(10000); // wait to replicate to peer cluster
    get = new Get(ROW_A);
    result = table1.get(get);
    Assert.assertTrue(result.isEmpty());    
    
    table.close();
    table1.close();
    admin1.close();
    connection1.close();
    repAdmin.close();
  }
}

