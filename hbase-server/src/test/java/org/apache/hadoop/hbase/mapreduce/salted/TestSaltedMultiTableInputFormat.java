package org.apache.hadoop.hbase.mapreduce.salted;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.mapreduce.MultiTableInputFormat;
import org.apache.hadoop.hbase.mapreduce.MultiTableInputFormatBase;
import org.apache.hadoop.hbase.mapreduce.TestMultiTableInputFormat;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;

@Category(LargeTests.class)
public class TestSaltedMultiTableInputFormat extends TestMultiTableInputFormat {
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // switch TIF to log at DEBUG level
    TEST_UTIL.enableDebug(MultiTableInputFormat.class);
    TEST_UTIL.enableDebug(MultiTableInputFormatBase.class);
    // start mini hbase cluster
    TEST_UTIL.startMiniCluster(3);
    // create and fill table
    for (int i = 0; i < 3; i++) {
      if (i == 2) {
        // create and load salted table
        HTableInterface table = TEST_UTIL.createSaltedTable(Bytes.toBytes(TABLE_NAME + i), INPUT_FAMILY, 4);
        TEST_UTIL.loadTable(table, INPUT_FAMILY, false);
      } else {
        HTable table =
            TEST_UTIL.createTable(TableName.valueOf(TABLE_NAME + String.valueOf(i)), INPUT_FAMILY);
        TEST_UTIL.createMultiRegions(TEST_UTIL.getConfiguration(), table, INPUT_FAMILY, 4);
        TEST_UTIL.loadTable(table, INPUT_FAMILY, false);
      }
    }
    // start MR cluster
    TEST_UTIL.startMiniMapReduceCluster();
  }
}
