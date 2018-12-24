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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.client.replication.ReplicationAdmin;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SkipFilter;
import org.apache.hadoop.hbase.filter.WhileMatchFilter;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category(LargeTests.class)
public class TestSaltedHTable {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestSaltedHTable.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestSaltedHTable.class);
  private static HBaseTestingUtility TEST_UTIL;
  private static HBaseTestingUtility utility1;
  private static Configuration conf;
  private static Connection connection;

  private static final TableName TEST_TABLE = TableName.valueOf(Bytes.toBytes("test"));
  private static final byte[] TEST_FAMILY = Bytes.toBytes("f1");

  private static final byte[] ROW_A = Bytes.toBytes("aaa");
  private static final byte[] ROW_B = Bytes.toBytes("bbb");
  private static final byte[] ROW_C = Bytes.toBytes("ccc");

  private static final byte[] qualifierCol1 = Bytes.toBytes("col1");
  private static final byte[] incrementQual = Bytes.toBytes("inc");
  private static final byte[] deleteQual = Bytes.toBytes("deleteQal");

  private static final byte[] bytes1 = Bytes.toBytes(1);
  private static final byte[] bytes2 = Bytes.toBytes(2);
  private static final byte[] bytes3 = Bytes.toBytes(3);
  private static final byte[] bytes4 = Bytes.toBytes(4);
  private static final byte[] bytes5 = Bytes.toBytes(5);
  private static final byte[] bytes6 = Bytes.toBytes(6);
  private static final byte[] bytes7 = Bytes.toBytes(7);

  private SaltedHTable saltedHTable;
  private static final int defaultSlotsCount = 10;
  private Admin admin;
  private List<Put> puts = new ArrayList<Put>();

  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    conf = HBaseConfiguration.create();
    conf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/1");
    TEST_UTIL = new HBaseTestingUtility(conf);
    TEST_UTIL.startMiniZKCluster();
    TEST_UTIL.startMiniCluster(2);
    connection = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration());
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
    admin = connection.getAdmin();
    saltedHTable = createSaltedTable(defaultSlotsCount);

    Put puta = new Put(ROW_A);
    puta.addColumn(TEST_FAMILY, qualifierCol1, bytes1);
    puts.add(puta);

    Put putb = new Put(ROW_B);
    putb.addColumn(TEST_FAMILY, qualifierCol1, bytes2);
    puts.add(putb);

    Put putc = new Put(ROW_C);
    putc.addColumn(TEST_FAMILY, qualifierCol1, bytes3);
    puts.add(putc);

    saltedHTable.put(puts);
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
    return createSaltedTable(defaultSlotsCount);
  }

  protected SaltedHTable createSaltedTable(Integer slotCounts) throws IOException {
    admin.createTable(getSaltedTableDescriptor(slotCounts));
    return (SaltedHTable)(connection.getTable(TEST_TABLE));
  }

  protected Table createUnSaltedTable() throws IOException {
    admin.createTable(getUnSaltedTableDescriptor());
    return connection.getTable(TEST_TABLE);
  }

  protected TableDescriptor getUnSaltedTableDescriptor() {
    return getUnSaltedTableDescriptor(TEST_TABLE);
  }

  protected TableDescriptor getUnSaltedTableDescriptor(TableName tableName) {
    TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(tableName);
    builder.setColumnFamily(ColumnFamilyDescriptorBuilder.of(TEST_FAMILY));
    TableDescriptor desc = builder.build();
    return desc;
  }

  protected TableDescriptor getSaltedTableDescriptor(Integer slotCounts) {
    TableDescriptorBuilder builder = TableDescriptorBuilder.newBuilder(TEST_TABLE);
    builder.setColumnFamily(ColumnFamilyDescriptorBuilder.of(TEST_FAMILY));
    builder.setSlotsCount(slotCounts);
    TableDescriptor desc = builder.build();
    return desc;
  }

  @Test
  public void testCreateSaltedTable() throws IOException {
    TEST_UTIL.deleteTable(TEST_TABLE);
    TableDescriptorBuilder.ModifyableTableDescriptor
        desc = ((TableDescriptorBuilder.ModifyableTableDescriptor)
        getUnSaltedTableDescriptor(TEST_TABLE));
    desc.setValue(TableDescriptorBuilder.KEY_SALTER, NBytePrefixKeySalter.class.getName());
    desc.setValue(TableDescriptorBuilder.SLOTS_COUNT, null);
    try {
      admin.createTable(desc);
      Assert.fail();
    } catch (IOException e) {
      Assert.assertTrue(e.getMessage().indexOf("must specify SLOTS_COUNT when KEY_SALTER is set") >= 0);
    }

    // set slots count
    int slotCount = 10;
    Table table = createSaltedTable(slotCount);
    byte[][] slots = new NBytePrefixKeySalter(slotCount).getAllSalts();
    Assert.assertEquals(slotCount, slots.length);
    RegionLocator rl = TEST_UTIL.getConnection().getRegionLocator(table.getName());
    byte[][] endKeys = rl.getEndKeys();
    Assert.assertEquals(slots.length, endKeys.length);
    for (int i = 0; i < slots.length - 1; ++i) {
      Assert.assertArrayEquals(slots[i + 1], endKeys[i]);
    }
    table.close();

    // won't pre-split by salted-slots if splitKeys are set
    TEST_UTIL.deleteTable(TEST_TABLE);
    byte[] splitKey = Bytes.toBytes("aa");
    desc = ((TableDescriptorBuilder.ModifyableTableDescriptor)
        getSaltedTableDescriptor(defaultSlotsCount));
    admin.createTable(desc, new byte[][]{splitKey});
    table = connection.getTable(TEST_TABLE);
    rl = TEST_UTIL.getConnection().getRegionLocator(table.getName());
    endKeys = rl.getEndKeys();
    Assert.assertEquals(2, endKeys.length);
    Assert.assertArrayEquals(splitKey, endKeys[0]);
    table.close();
  }

  @Test
  public void testModifyKeySalter() throws IOException {
    TEST_UTIL.deleteTable(TEST_TABLE);
    createSaltedTable();
    TableDescriptorBuilder.ModifyableTableDescriptor
        desc = ((TableDescriptorBuilder.ModifyableTableDescriptor) admin.getDescriptor(TEST_TABLE));

    // unset slots count
    desc.setValue(TableDescriptorBuilder.SLOTS_COUNT, null);
    try {
      admin.modifyTable(TEST_TABLE, desc);
      Assert.fail();
    } catch (IOException e) {
      Assert
          .assertTrue(e.getMessage().indexOf("can not modify the salted attribute of table") >= 0);
    }

    // use another Salter class
    TableDescriptorBuilder descriptorBuilder = TableDescriptorBuilder.newBuilder(desc);
    descriptorBuilder.setSalted("abc", 1);
    TableDescriptor tableDescriptor = descriptorBuilder.build();
    try {
      admin.modifyTable(TEST_TABLE, tableDescriptor);
      Assert.fail();
    } catch (IOException e) {
      Assert
          .assertTrue(e.getMessage().indexOf("can not modify the salted attribute of table") >= 0);
    }

    // set KeySalter in unsalted table
    TEST_UTIL.deleteTable(TEST_TABLE);
    createUnSaltedTable();
    desc = ((TableDescriptorBuilder.ModifyableTableDescriptor) admin.getDescriptor(TEST_TABLE));
    descriptorBuilder = TableDescriptorBuilder.newBuilder(desc);
    descriptorBuilder.setSlotsCount(1);
    tableDescriptor = descriptorBuilder.build();
    try {
      admin.modifyTable(TEST_TABLE, tableDescriptor);
      Assert.fail();
    } catch (IOException e) {
      Assert
          .assertTrue(e.getMessage().indexOf("can not modify the salted attribute of table") >= 0);
    }
  }

  @Test
  public void testGetNonSaltedHTableFromHConnection() throws IOException {
    byte[] testTable = Bytes.toBytes("unsalted_test_table");
    TableDescriptor desc = getUnSaltedTableDescriptor(TableName.valueOf(testTable));
    admin.createTable(desc);
    for (int i = 0; i < 10; ++i) {
      Table table = connection.getTable(TableName.valueOf(testTable));
      Assert.assertTrue(table instanceof HTable);
      table.close();
    }
  }

  @Test
  public void testGetHTableInstance() throws IOException {
    TEST_UTIL.deleteTable(TEST_TABLE);
    createUnSaltedTable();
    Table table = connection.getTable(TEST_TABLE);
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
    for (int i = 0; i < 1; ++i) {
      byte[] salt = salts[i];
      Scan scan = new Scan();
      ResultScanner scanner = saltedHTable.getScanner(scan, new byte[][]{salt});
      Result result = scanner.next();
      Assert.assertNotNull(result);
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
    Put putA = new Put(ROW_A).addColumn(TEST_FAMILY, qualifierCol1, bytes5);
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
  public void TestIncrement() throws IOException {
    Increment increment = new Increment(ROW_A);
    increment.addColumn(TEST_FAMILY, incrementQual, 0);
    Result result = saltedHTable.increment(increment);
    Assert.assertArrayEquals(ROW_A, result.getRow());
    Assert.assertEquals(0, Bytes.toLong(result.getValue(TEST_FAMILY, incrementQual)));
    increment = new Increment(ROW_A);
    increment.addColumn(TEST_FAMILY, incrementQual, 10);
    result = saltedHTable.increment(increment);
    Assert.assertArrayEquals(ROW_A, result.getRow());
    Assert.assertEquals(10, Bytes.toLong(result.getValue(TEST_FAMILY, incrementQual)));
  }

  @Test
  public void TestBatch() throws Exception {
    Result result = saltedHTable.get(new Get(ROW_A));
    byte[] origBytes = result.getValue(TEST_FAMILY, incrementQual);
    long incrValue = origBytes == null ? 0 : Bytes.toLong(origBytes);
    byte[] testValue = Bytes.toBytes("testValue");
    Put put = new Put(ROW_A).addColumn(TEST_FAMILY, deleteQual, testValue);
    saltedHTable.put(put);

    // do batch
    List actions = new ArrayList();
    put = new Put(ROW_A).addColumn(TEST_FAMILY, qualifierCol1, testValue);
    Delete delete = new Delete(ROW_A).addColumn(TEST_FAMILY, deleteQual);
    Increment increment = new Increment(ROW_A).addColumn(TEST_FAMILY, incrementQual, 10);
    actions.add(put);
    actions.add(delete);
    actions.add(increment);
    Object[] results = new Object[3];
    saltedHTable.batch(actions, results);
    result = saltedHTable.get(new Get(ROW_A));
    Assert.assertArrayEquals(testValue, result.getValue(TEST_FAMILY, qualifierCol1));
    Assert.assertNull(result.getValue(TEST_FAMILY, deleteQual));
    Assert.assertEquals(incrValue + 10, Bytes.toLong(result.getValue(TEST_FAMILY, incrementQual)));
  }

  @Test
  public void testReadWithFilter() throws Exception {
    Get get = new Get(ROW_A);
    // no filter, ok
    saltedHTable.get(get);
    // no-row filter, ok
    get = new Get(ROW_A);
    SingleColumnValueFilter scvf =
        new SingleColumnValueFilter(TEST_FAMILY, qualifierCol1, CompareFilter.CompareOp.EQUAL, bytes1);
    get.setFilter(scvf);
    saltedHTable.get(get);
    // filter list with no-row filters, ok
    FilterList filterList = new FilterList();
    filterList.addFilter(scvf);
    filterList.addFilter(scvf);
    get.setFilter(filterList);
    saltedHTable.get(get);
    // filter list with wrapper filter, no-row filters, ok
    filterList.addFilter(new WhileMatchFilter(scvf));
    filterList.addFilter(new SkipFilter(scvf));
    saltedHTable.get(get);

    // row-level filter, fail
    PrefixFilter prefixFilter = new PrefixFilter(Bytes.toBytes("prefix"));
    get.setFilter(prefixFilter);
    try {
      saltedHTable.get(get);
      Assert.fail("should fail when using PrefixFilter");
    } catch (DoNotRetryIOException e) {
    }
    // wrapped filter contains row-level filter
    get.setFilter(new WhileMatchFilter(prefixFilter));
    try {
      saltedHTable.get(get);
      Assert.fail("should fail when WhileMatchFilter wrapped PrefixFilter");
    } catch (DoNotRetryIOException e) {
    }

    // filter list, contains row-level filter
    filterList = new FilterList();
    filterList.addFilter(scvf);
    filterList.addFilter(prefixFilter);
    get.setFilter(filterList);
    try {
      saltedHTable.get(get);
      Assert.fail("should fail when contains PrefixFilter in FilterList");
    } catch (DoNotRetryIOException e) {
    }

    // filter list, contains wrapper filter which wrapped row-level filter
    filterList = new FilterList();
    filterList.addFilter(scvf);
    filterList.addFilter(new SkipFilter(scvf));
    filterList.addFilter(new WhileMatchFilter(prefixFilter));
    get.setFilter(filterList);
    try {
      saltedHTable.get(get);
      Assert.fail("should fail when contains FilterList contains PrefixFilter");
    } catch (DoNotRetryIOException e) {
    }
  }

  @Test
  public void testReplication() throws Exception {
    Configuration conf1 = HBaseConfiguration.create(TEST_UTIL.getConfiguration());
    conf1.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/2");


    utility1 = new HBaseTestingUtility(conf1);
    utility1.setZkCluster(TEST_UTIL.getZkCluster());
    utility1.startMiniCluster(4);

    TEST_UTIL.deleteTable(TEST_TABLE);
    HTableDescriptor tableDesc = new HTableDescriptor(getSaltedTableDescriptor(10));
    tableDesc.getFamily(TEST_FAMILY).setScope(1);
    admin.createTable(tableDesc);


    Admin admin1 = ConnectionFactory.createConnection(utility1.getConfiguration()).getAdmin();
    admin1.createTable(tableDesc);
    ReplicationAdmin repAdmin = new ReplicationAdmin(TEST_UTIL.getConfiguration());
    ReplicationPeerConfig pc = new ReplicationPeerConfig();
    pc.setClusterKey(utility1.getClusterKey());
    pc.setReplicateAllUserTables(false);
    Map<TableName, List<String>> tableCf = new HashMap<TableName, List<String>>();
    tableCf.put(TEST_TABLE, null);
    repAdmin.addPeer("10", pc, tableCf);

    // write data to source cluster
    Table table = connection.getTable(TEST_TABLE);
    Connection connection1 = ConnectionFactory.createConnection(utility1.getConfiguration());
    Table table1 = connection1.getTable(TEST_TABLE);

    Put put = new Put(ROW_A).addColumn(TEST_FAMILY, qualifierCol1, bytes1);
    table.put(put);
    Threads.sleep(10000); // wait to replicate to peer cluster
    Get get = new Get(ROW_A);
    Result result = table1.get(get);
    Assert.assertTrue(!result.isEmpty());

    // delete data from source cluster
    Delete delete = new Delete(ROW_A).addColumn(TEST_FAMILY, qualifierCol1);
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

