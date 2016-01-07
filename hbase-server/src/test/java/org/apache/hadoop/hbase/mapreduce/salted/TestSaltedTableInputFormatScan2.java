package org.apache.hadoop.hbase.mapreduce.salted;

import org.apache.hadoop.hbase.mapreduce.TestTableInputFormatScan2;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;

@Category(LargeTests.class)
public class TestSaltedTableInputFormatScan2 extends TestTableInputFormatScan2 {
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TestSaltedTableInputFormatScan1.setUpBeforeClass();
  }
}
