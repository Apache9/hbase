package org.apache.hadoop.hbase.mapreduce;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.stream.IntStream;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Basic test for the HBaseStreamCopyTableToTalos M/R tool
 */
@Category(LargeTests.class)
public class TestHBaseStreamCopyTableToTalos {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestHBaseStreamCopyTableToTalos.class);
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static byte[][] rowkeys;

  @BeforeClass
  public static void beforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(3);
    TEST_UTIL.startMiniMapReduceCluster();

    rowkeys = IntStream.range(0, 10).boxed().map(i -> "row" + i).map(Bytes::toBytes).toArray(byte[][]::new);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    TEST_UTIL.shutdownMiniMapReduceCluster();
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void doCopyTableTest() throws Exception {
    TableName tableName = TableName.valueOf("testCopyToTalos");
    final byte[] FAMILY = Bytes.toBytes("C");
    final byte[] COLUMN = Bytes.toBytes("c1");

    Table table = TEST_UTIL.createTable(tableName, FAMILY);
    loadData(table, FAMILY, COLUMN);
    deleteData(table, FAMILY, COLUMN);

    HBaseStreamCopyTableToTalos copy = new HBaseStreamCopyTableToTalos(TEST_UTIL.getConfiguration());
    int code = copy.run(new String[] {
        "--endpoint=http://staging-cnbj2-talos.api.xiaomi.net",
        "--accesskey=AKKVSCLYLT4I4BZUZC",
        "--accesssecret=RP9k7Q3rZ4W+4oyfpBmx6NyY7YzyljigP67MUX9i",
        tableName.getNameAsString() });
    assertEquals("copy job failed", 0, code);
  }

  private void loadData(Table t, byte[] family, byte[] column) throws IOException {
    for (byte[] row : rowkeys) {
      Put p = new Put(row);
      p.addColumn(family, column, row);
      t.put(p);
    }
  }

  private void deleteData(Table t, byte[] family, byte[] column) throws IOException {
    Delete d1 = new Delete(rowkeys[0]);
    t.delete(d1);
    Delete d2 = new Delete(rowkeys[1]).addFamily(family);
    t.delete(d2);
    Delete d3 = new Delete((rowkeys[2])).addColumns(family, column);
    t.delete(d3);
  }
}
