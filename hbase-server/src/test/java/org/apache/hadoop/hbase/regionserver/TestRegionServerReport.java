package org.apache.hadoop.hbase.regionserver;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos.RegionServerReportResponse;
import org.apache.hadoop.hbase.protobuf.generated.RegionServerStatusProtos.TableRegionCount;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;

@Category(MediumTests.class)
public class TestRegionServerReport {
  final Log LOG = LogFactory.getLog(getClass());
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static int SLAVES = 3;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(SLAVES);
  }

  /**
   * @throws java.lang.Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testGetTableRegionsNum() throws Exception {
    TableName TABLE = TableName.valueOf("testTable");
    final int NUM_REGIONS = 10;
    TEST_UTIL.createTable(TABLE, new byte[][] { Bytes.toBytes("testFamily") }, 3,
      Bytes.toBytes("aaaaa"), Bytes.toBytes("zzzzz"), NUM_REGIONS);
    TEST_UTIL.waitUntilAllRegionsAssigned(TABLE);
    HRegionServer rs = TEST_UTIL.getMiniHBaseCluster().getRegionServer(0);
    Thread
        .sleep(TEST_UTIL.getConfiguration().getInt("hbase.regionserver.msginterval", 3 * 1000) * 2);
    RegionServerReportResponse response = rs.getRegionServerReportResponse();
    int tableRegionsNum = 0;
    for (TableRegionCount entry : response.getRegionCountsList()) {
      if (ProtobufUtil.toTableName(entry.getTableName()).equals(TABLE)) {
        tableRegionsNum = entry.getRegionNum();
        break;
      }
    }
    assertEquals(NUM_REGIONS, tableRegionsNum);
  }

  @Test
  public void testGetRegionServerNum() throws Exception {
    HRegionServer rs = TEST_UTIL.getMiniHBaseCluster().getRegionServer(0);
    Thread
        .sleep(TEST_UTIL.getConfiguration().getInt("hbase.regionserver.msginterval", 3 * 1000) * 2);
    RegionServerReportResponse response = rs.getRegionServerReportResponse();
    assertEquals(SLAVES, response.getServerNum());
  }
}
