package org.apache.hadoop.hbase.mapreduce.salted;

import org.apache.hadoop.hbase.mapreduce.TestTableInputFormatScan2;
import org.junit.BeforeClass;

public class TestSaltedTableInputFormatScan2 extends TestTableInputFormatScan2 {
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TestSaltedTableInputFormatScan1.setUpBeforeClass();
  }
}
