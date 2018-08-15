package org.apache.hadoop.hbase.regionserver;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TooManyRegionScannersException;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test limit of opened region scanners
 */
@Category(SmallTests.class)
public class TestTooManyRegionScanners {
  private static final Log LOG = LogFactory.getLog(TestTooManyRegionScanners.class.getName());
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final int LIMITED_OPENED_REGION_SCANNERS = 10;

  @BeforeClass
  public static void beforeClass() throws Exception {
    //set limit to 10
    TEST_UTIL.getConfiguration().setInt("hbase.regionserver.maximum.opened.region.scanner.limit",
        LIMITED_OPENED_REGION_SCANNERS);
    TEST_UTIL.getConfiguration().setInt("hbase.client.scanner.caching", 2);
    TEST_UTIL.getConfiguration().setInt("hbase.client.retries.number", 1);
    TEST_UTIL.startMiniCluster();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testLimitedOpenedRegionScanners() throws Exception {
    //create table
    byte[] cf = Bytes.toBytes("cf");
    byte[] tableName = Bytes.toBytes(this.getClass().getSimpleName());
    HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));
    HColumnDescriptor hcd = new HColumnDescriptor(cf);
    desc.addFamily(hcd);
    TEST_UTIL.getHBaseAdmin().createTable(desc);
    HTable ht = new HTable(TEST_UTIL.getConfiguration(), tableName);

    //put 10 rows
    for (int i = 0; i < 100; i++) {
      byte[] rowkey = Bytes.toBytes("row_" + i);
      Put put = new Put(rowkey);
      put.add(cf, Bytes.toBytes("col"), Bytes.toBytes("val_" + i));
      ht.put(put);
    }

    //open 11 result scanners
    int opened = 0;
    for (int i = 0; i < LIMITED_OPENED_REGION_SCANNERS + 1; i++) {
      try {
        Scan scan = new Scan();
        scan.addFamily(cf);
        scan.setMaxResultSize(2);
        ResultScanner result_scanner = ht.getScanner(scan);
        result_scanner.next();
        opened++;
      } catch (Exception e) {
        assertEquals(i, LIMITED_OPENED_REGION_SCANNERS);
        assertTrue(e.getCause() instanceof TooManyRegionScannersException);
      }
    }
    assertEquals(opened, LIMITED_OPENED_REGION_SCANNERS);
  }

}
