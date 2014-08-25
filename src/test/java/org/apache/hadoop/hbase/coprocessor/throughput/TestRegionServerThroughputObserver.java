package org.apache.hadoop.hbase.coprocessor.throughput;

import java.util.ArrayList;
import junit.framework.Assert;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.test.GenericTestUtils;


@Category(MediumTests.class)
public class TestRegionServerThroughputObserver {

  private static final byte[] TEST_TABLE = Bytes.toBytes("TestTable");
  private static final byte[] TEST_FAMILY = Bytes.toBytes("TestFamily");
  private static final byte[] TEST_QUALIFIER = Bytes.toBytes("TestQualifier");

  private static final int ROWSIZE = 20;
  private static byte[] ROW = Bytes.toBytes("testRow");
  private static byte[][] ROWS = makeN(ROW, ROWSIZE);


  private static HBaseTestingUtility util;
  private static HTable table;

  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    util = new HBaseTestingUtility();
    Configuration conf = util.getConfiguration();
    conf.setLong("hbase.regionserver.qps.window.size.in.ms", 1000);
    conf.setLong("hbase.regionserver.qps.read.max", 10);
    conf.setLong("hbase.regionserver.qps.write.max", 10);
    conf.setStrings("hbase.coprocessor.user.region.classes",
        "org.apache.hadoop.hbase.coprocessor.throughput.RegionServerThroughputObserver");
    util.startMiniCluster(1);
    table = util.createTable(TEST_TABLE, TEST_FAMILY);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    util.shutdownMiniCluster();
  }

  @Before
  public void resetEnv() throws Exception
  {
    RegionServerThroughputObserver.resetThroughputWindow();
  }


  private static byte[][] makeN(byte[] base, int n) {
    byte[][] ret = new byte[n][];
    for (int i = 0; i < n; i++) {
      ret[i] = Bytes.add(base, Bytes.toBytes(String.format("%02d", i)));
    }
    return ret;
  }

  private byte[] makeData(int n) {
    byte[] data = new byte[n];
    for(int i=0; i<n; ++i) {
      data[i] = 0;
    }
    return data;
  }

  @Test
  public void testPut() throws Exception
  {
    Put put = null;
    for (int i = 0; i < 8; ++i) {
      put = new Put(ROWS[0]);
      put.add(TEST_FAMILY, TEST_QUALIFIER, makeData(512));
      table.put(put);
    }
    put = new Put(ROWS[8]);
    put.add(TEST_FAMILY, TEST_QUALIFIER, makeData(1024));
    table.put(put);

    boolean hit = false;
    try {
      put = new Put(ROWS[0]);
      put.add(TEST_FAMILY, TEST_QUALIFIER, Bytes.toBytes(0));
      table.put(put);
    }catch(Exception e) {
      GenericTestUtils.assertExceptionContains("ThroughputExceededException", e);
      hit = true;
    }
    Assert.assertTrue(hit);
  }

  @Test
  public void testMultiPut() throws Exception
  {
    Put put = null;
    ArrayList<Put> puts = new ArrayList<Put>();

    //9个请求，size加起来大于9×1024, 所以下一个请求会抛出ThroughputExceededException
    for (int i = 0; i < 9; ++i) {
      put = new Put(ROWS[i]);
      put.add(TEST_FAMILY, TEST_QUALIFIER, makeData(1000));
      puts.add(put);
    }

    table.put(puts);
    boolean hit = false;
    try {
      put = new Put(ROWS[0]);
      put.add(TEST_FAMILY, TEST_QUALIFIER, Bytes.toBytes(0));
      table.put(put);
    }catch(Exception e) {
      GenericTestUtils.assertExceptionContains("ThroughputExceededException", e);
      hit = true;
    }
    Assert.assertTrue(hit);
  }

  @Test
  public void testGet() throws Exception {
    Put put = null;
    for (int i = 0; i < 8; ++i) {
      put = new Put(ROWS[i]);
      put.add(TEST_FAMILY, TEST_QUALIFIER, makeData(512));
      table.put(put);
    }

    put = new Put(ROWS[8]);
    put.add(TEST_FAMILY, TEST_QUALIFIER, makeData(1024));
    table.put(put);

    Get get = null;
    for (int i = 0; i < 9; ++i) {
      get = new Get(ROWS[i]);
      table.get(get);
    }

    boolean hit = false;
    try {
      get = new Get(ROWS[0]);
      table.get(get);
    }catch(Exception e) {
      GenericTestUtils.assertExceptionContains("ThroughputExceededException", e);
      hit = true;
    }
    Assert.assertTrue(hit);
  }

  @Test
  public void testScan() throws Exception {
    Put put = null;
    for (int i = 0; i < 5; ++i) {
      put = new Put(ROWS[i]);
      put.add(TEST_FAMILY, TEST_QUALIFIER, makeData(1024));
      table.put(put);
    }

    RegionServerThroughputObserver.resetThroughputWindow();
    for (int i = 5; i < 10; ++i) {
      put = new Put(ROWS[i]);
      put.add(TEST_FAMILY, TEST_QUALIFIER, makeData(1024));
      table.put(put);
    }

    //每个Result占用1083个字节，相当于两个请求
    Result result = null;
    ResultScanner scanner = table.getScanner(TEST_FAMILY);
    for (int i = 0; i < 5; ++i) {
      result = scanner.next();
    }

    boolean hit = false;
    try {
      result = scanner.next();
    }catch (Exception e) {
      GenericTestUtils.assertExceptionContains("ThroughputExceededException", e);
      hit = true;
    }
    Assert.assertTrue(hit);

  }


  @Test
  public void testExists() throws Exception
  {
    Put put = null;
    for (int i = 0; i < 8; ++i) {
      put = new Put(ROWS[i]);
      put.add(TEST_FAMILY, TEST_QUALIFIER, makeData(512));
      table.put(put);
    }

    put = new Put(ROWS[8]);
    put.add(TEST_FAMILY, TEST_QUALIFIER, makeData(1024));
    table.put(put);

    Get get = null;
    for (int i = 0; i < 9; ++i) {
      get = new Get(ROWS[i]);
      table.exists(get);
    }

    boolean hit = false;
    try {
      get = new Get(ROWS[0]);
      table.exists(get);
    }catch(Exception e) {
      GenericTestUtils.assertExceptionContains("ThroughputExceededException", e);
      hit = true;
    }
    Assert.assertTrue(hit);
  }

  @Test
  public void testeDelete() throws Exception
  {
    Delete delete = null;
    for (int i = 0; i < 10; ++i) {
      delete = new Delete(ROWS[i]);
      table.delete(delete);
    }

    boolean hit = false;
    try {
      delete = new Delete(ROWS[0]);
      table.delete(delete);
    }catch(Exception e) {
      GenericTestUtils.assertExceptionContains("ThroughputExceededException", e);
      hit = true;
    }
    Assert.assertTrue(hit);
  }

  @Test
  public void testMultiDelete() throws Exception
  {
    ArrayList<Delete> deletes = new ArrayList<Delete>();
    Delete delete = null;
    for (int i = 0; i < 10; ++i) {
      delete = new Delete(ROWS[i]);
      deletes.add(delete);
    }
    table.delete(deletes);

    boolean hit = false;
    try {
      delete = new Delete(ROWS[0]);
      table.delete(delete);
    }catch(Exception e) {
      GenericTestUtils.assertExceptionContains("ThroughputExceededException", e);
      hit = true;
    }
    Assert.assertTrue(hit);
  }

  @Test
  public void testAppend() throws Exception
  {
    Append append = null;
    for (int i = 0; i < 10; ++i) {
      append = new Append(ROWS[i]);
      append.add(TEST_FAMILY, TEST_QUALIFIER, makeData(512));
      table.append(append);
    }

    boolean hit = false;
    try {
      append = new Append(ROWS[0]);
      append.add(TEST_FAMILY, TEST_QUALIFIER, Bytes.toBytes(0));
      table.append(append);
    }catch(Exception e) {
      GenericTestUtils.assertExceptionContains("ThroughputExceededException", e);
      hit = true;
    }
    Assert.assertTrue(hit);
  }


  @Test
  public void testCheckAndPut() throws Exception
  {
    Put put = null;
    for (int i = 0; i < 8; ++i) {
      put = new Put(ROWS[i]);
      put.add(TEST_FAMILY, TEST_QUALIFIER, makeData(512));
      table.checkAndPut(ROWS[i], TEST_FAMILY, TEST_QUALIFIER, Bytes.toBytes(0), put);
    }

    put = new Put(ROWS[8]);
    put.add(TEST_FAMILY, TEST_QUALIFIER, makeData(1024));
    table.checkAndPut(ROWS[8], TEST_FAMILY, TEST_QUALIFIER, Bytes.toBytes(0), put);

    boolean hit = false;
    try {
      put = new Put(ROWS[0]);
      put.add(TEST_FAMILY, TEST_QUALIFIER, Bytes.toBytes(0));
      table.checkAndPut(ROWS[0], TEST_FAMILY, TEST_QUALIFIER, Bytes.toBytes(0), put);
    }catch(Exception e) {
      GenericTestUtils.assertExceptionContains("ThroughputExceededException", e);
      hit = true;
    }
    Assert.assertTrue(hit);
  }

  @Test
  public void testCheckAndDelete() throws Exception
  {
    Delete delete = null;
    for (int i = 0; i < 10; ++i) {
      delete = new Delete(ROWS[i]);
      table.checkAndDelete(ROWS[i], TEST_FAMILY, TEST_QUALIFIER, Bytes.toBytes(0), delete);
    }

    boolean hit = false;
    try {
      delete = new Delete(ROWS[0]);
      table.checkAndDelete(ROWS[0], TEST_FAMILY, TEST_QUALIFIER, Bytes.toBytes(0), delete);
    }catch(Exception e)
    {
      GenericTestUtils.assertExceptionContains("ThroughputExceededException", e);
      hit = true;
    }
    Assert.assertTrue(hit);
  }

  private void initTableData() throws Exception{
    RegionServerThroughputObserver.resetThroughputWindow();
    Put put = null;
    for (long i = 0; i < 10; ++i) {
      put = new Put(ROWS[(int)i]);
      put.add(TEST_FAMILY, TEST_QUALIFIER, Bytes.toBytes(i));
      table.put(put);
    }
    RegionServerThroughputObserver.resetThroughputWindow();
  }

  @Test
  public void testIncrement() throws Exception{
    initTableData();

    Increment increment = null;
    for (int i = 0; i < 10; ++i) {
      increment = new Increment(ROWS[0]);
      increment.addColumn(TEST_FAMILY, TEST_QUALIFIER, 1);
      table.increment(increment);
    }

    boolean hit = false;
    try {
      increment = new Increment(ROWS[0]);
      increment.addColumn(TEST_FAMILY, TEST_QUALIFIER, 1);
      table.increment(increment);
    }catch(Exception e) {
      GenericTestUtils.assertExceptionContains("ThroughputExceededException", e);
      hit = true;
    }
    Assert.assertTrue(hit);
  }

  @Test
  public void testIncrementColumnValue() throws Exception{
    initTableData();

    for (int i = 0; i < 10; ++i) {
      table.incrementColumnValue(ROWS[0], TEST_FAMILY, TEST_QUALIFIER, 1);
    }

    boolean hit = false;
    try {
      table.incrementColumnValue(ROWS[0], TEST_FAMILY, TEST_QUALIFIER, 1);
    }catch(Exception e) {
      GenericTestUtils.assertExceptionContains("ThroughputExceededException", e);
      hit = true;
    }
    Assert.assertTrue(hit);
  }

  @Test
  public void testGetRowOrBefore() throws Exception {
    Put put = null;
    for (int i = 0; i < 8; ++i) {
      put = new Put(ROWS[0]);
      put.add(TEST_FAMILY, TEST_QUALIFIER, makeData(512));
      table.put(put);
    }

    put = new Put(ROWS[8]);
    put.add(TEST_FAMILY, TEST_QUALIFIER, makeData(1024));
    table.put(put);

    for (int i = 0; i < 9; ++i) {
      table.getRowOrBefore(ROWS[i], TEST_FAMILY);
    }

    boolean hit = false;
    try {
      table.getRowOrBefore(ROWS[0], TEST_FAMILY);
    }catch(Exception e) {
      GenericTestUtils.assertExceptionContains("ThroughputExceededException", e);
      hit = true;
    }
    Assert.assertTrue(hit);
  }

}
