package org.apache.hadoop.hbase.mapreduce.salted;

import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableInputFormatBase;
import org.apache.hadoop.hbase.mapreduce.TestTableInputFormatScan1;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;

@Category(LargeTests.class)
public class TestSaltedTableInputFormatScan1 extends TestTableInputFormatScan1 {
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // test intermittently fails under hadoop2 (2.0.2-alpha) if shortcircuit-read (scr) is on.
    // this turns it off for this test.  TODO: Figure out why scr breaks recovery. 
    System.setProperty("hbase.tests.use.shortcircuit.reads", "false");

    // switch TIF to log at DEBUG level
    TEST_UTIL.enableDebug(TableInputFormat.class);
    TEST_UTIL.enableDebug(TableInputFormatBase.class);
    // start mini hbase cluster
    TEST_UTIL.startMiniCluster(3);
    // create and fill table
    table = TEST_UTIL.createSaltedTable(TABLE_NAME, INPUT_FAMILY, 4);
    TEST_UTIL.loadTable(table, INPUT_FAMILY, false);
    // start MR cluster
    TEST_UTIL.startMiniMapReduceCluster();
  }
}
