package org.apache.hadoop.hbase.client;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestParallelGet {
  final Log LOG = LogFactory.getLog(getClass());
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static byte[] ROW = Bytes.toBytes("testRow");
  private static byte[] FAMILY = Bytes.toBytes("testFamily");
  private static byte[] QUALIFIER = Bytes.toBytes("testQualifier");
  private static byte[] VALUE = Bytes.toBytes("testValue");

  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster();
  }

  /**
   * @throws java.lang.Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  private Put createPut(String row) {
    Put put = new Put( Bytes.toBytes(row));
    put.add(FAMILY, QUALIFIER, VALUE);
    return put;
  }
  
  /**
   * @throws Exception
   */
  @Test
  public void testParallelGet() throws Exception {
    byte[] TABLE = Bytes.toBytes("testBucketPut");
    HTable ht = TEST_UTIL.createTable(TABLE, FAMILY);
    ht.setAutoFlush(false);

    List<Put> puts = new ArrayList<Put>();
    for (int i = 0; i < 10; i++) {
      puts.add(createPut("row" + Integer.toString(i)));
    }
    HTableUtil.bucketRsPut(ht, puts);
    
    List<Get> gets = new ArrayList<Get>();
    for (int i = 0; i < 10; i++) {
      gets.add(new Get(Bytes.toBytes("row" + Integer.toString(i))));
    }
    
    Result[] results = ht.parallelGet(gets);

    assertEquals(results.length, puts.size());
    for (int i = 0; i < 10 ; i++){
      assertEquals(Bytes.toString(results[i].getRow()), "row" + Integer.toString(i));
    }

    ht.close();
  }
}
