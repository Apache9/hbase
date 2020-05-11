package org.apache.hadoop.hbase.mapreduce;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.HBaseStreamUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Basic test for the HBaseStreamCopyTableToTalos M/R tool
 */
@Category(LargeTests.class)
public class TestHBaseStreamCopyTableToTalos {
  private static final Logger LOG = LoggerFactory.getLogger(TestHBaseStreamCopyTableToTalos.class);

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestHBaseStreamCopyTableToTalos.class);


  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static byte[][] rowkeys;

  private static final String TALOS_ENDPOINT = "http://staging-cnbj2-talos.api.xiaomi.net";
  private static final String ACCESS_KEY = "ACCESS_KEY";
  private static final String ACCESS_SECRET = "ACCESS_SECRET";

  private static final TableName TABLE_NAME = TableName.valueOf("testCopyToTalos");
  private static final byte[] FAMILY = Bytes.toBytes("C");
  private static final byte[] COLUMN = Bytes.toBytes("c1");

  private static String UT_OUTPUT_FILE = System.getProperty("user.dir")
      + "/target/hbase-stream-ut.txt";

  @BeforeClass
  public static void beforeClass() throws Exception {
    TEST_UTIL.startMiniCluster();
    TEST_UTIL.startMiniMapReduceCluster();

    rowkeys = IntStream.range(0, 5).boxed().map(i -> "row" + i).map(Bytes::toBytes).toArray(byte[][]::new);

    LOG.info("UT_OUTPUT_FILE: {}", UT_OUTPUT_FILE);
    File file = new File(UT_OUTPUT_FILE);
    if (file.exists()) {
      file.delete();
    }
    file.getParentFile().mkdirs();
    file.createNewFile();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    File file = new File(UT_OUTPUT_FILE);
    if (file.exists()) {
      file.delete();
    }
    TEST_UTIL.shutdownMiniMapReduceCluster();
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void doCopyTableTest() throws Exception {
    Table table = TEST_UTIL.createTable(TABLE_NAME, FAMILY);
    putData(table);

    HBaseStreamCopyTableToTalos copy = new HBaseStreamCopyTableToTalos(TEST_UTIL.getConfiguration());
    int code = copy.run(new String[] {
        "--endpoint=" + TALOS_ENDPOINT,
        "--accesskey=" + ACCESS_KEY,
        "--accesssecret=" + ACCESS_SECRET,
        "--utoutput=" + UT_OUTPUT_FILE,
        TABLE_NAME.getNameAsString() });
    assertEquals("copy job failed", 0, code);
    checkData();
  }

  private void putData(Table table) throws IOException {
    // put data
    for (byte[] row : rowkeys) {
      Put p = new Put(row);
      p.addColumn(FAMILY, COLUMN, row);
      table.put(p);
    }
    // delete data
    Delete d1 = new Delete(rowkeys[0]);
    table.delete(d1);
    Delete d2 = new Delete(rowkeys[1]).addFamily(FAMILY);
    table.delete(d2);
    Delete d3 = new Delete((rowkeys[2])).addColumns(FAMILY, COLUMN);
    table.delete(d3);
  }

  private void checkData() throws Exception {
    HBaseStreamMessageMerger messageMerger = new HBaseStreamMessageMerger();
    for (HBaseStreamMessageSlice hBaseStreamMessageSlice : readFile()) {
      messageMerger.append(hBaseStreamMessageSlice);
    }
    List<CellJson> cells = messageMerger.cells;
    assertEquals(2, cells.size());
    // cell0 put
    CellJson cell0 = cells.get(0);
    assertEquals(Bytes.toString(FAMILY), cell0.f);
    assertEquals(Bytes.toString(COLUMN), cell0.q);
    assertEquals(Type.Put.toString(), cell0.op);
    assertEquals(HBaseStreamUtil.toBase64String(rowkeys[3]), cell0.v);
  }

  private List<HBaseStreamMessageSlice> readFile() throws IOException {
    FileInputStream reader = new FileInputStream(UT_OUTPUT_FILE);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    byte[] buf = new byte[1024];
    for (int size; (size = reader.read(buf)) > 0; ) {
      out.write(buf, 0, size);
    }
    List<HBaseStreamMessageSlice> slices = new ArrayList<>();
    byte[] bytes = out.toByteArray();
    byte[] separator = "\n".getBytes();
    for (int idx; bytes.length > 0 && (idx = Bytes.indexOf(bytes, separator)) > -1; ) {
      byte[] msg = Bytes.copy(bytes, 0, idx);
      slices.add(new HBaseStreamMessageSlice(msg));
      bytes = Bytes.copy(bytes, idx + separator.length, bytes.length - separator.length - idx);
    }
    return slices;
  }

  static class HBaseStreamMessageSlice {

    private int index;
    private int total;
    private byte[] data;

    public HBaseStreamMessageSlice(byte[] message) {
      this(bytesToInt(message, 0), bytesToInt(message, 4),
          Arrays.copyOfRange(message, 8, message.length));
    }

    public HBaseStreamMessageSlice(int index, int total, byte[] data) {
      this.index = index;
      this.total = total;
      this.data = data;
    }

    public boolean isLast() {
      return total == index + 1;
    }

    private static int bytesToInt(byte[] bytes, int offset) {
      int result = 0;
      for (int i = 0; i < 4; i++) {
        result <<= 8;
        result |= bytes[offset + i] & 0xFF;
      }
      return result;
    }
  }

  public static class CellJson {
    private String r;
    private String op;
    private String f;
    private String q;
    private String v;
    private long t;

    public void setR(String r) {
      this.r = r;
    }

    public void setOp(String op) {
      this.op = op;
    }

    public void setF(String f) {
      this.f = f;
    }

    public void setQ(String q) {
      this.q = q;
    }

    public void setV(String v) {
      this.v = v;
    }

    public void setT(long t) {
      this.t = t;
    }
  }

  public class HBaseStreamMessageMerger {

    private List<HBaseStreamMessageSlice> slices = new ArrayList<>();

    private ObjectMapper objectMapper;

    ByteArrayOutputStream os;

    List<CellJson> cells = new ArrayList<>();

    public HBaseStreamMessageMerger() {
      objectMapper = new ObjectMapper();
      os = new ByteArrayOutputStream();
    }

    public boolean append(HBaseStreamMessageSlice slice) throws Exception {
      slices.add(slice);
      if (slice.isLast()) {
        Optional.ofNullable(merge()).ifPresent(cells::addAll);
        return true;
      }
      return false;
    }

    private List<CellJson> merge() throws Exception {
      try {
        for (HBaseStreamMessageSlice s : slices) {
          os.write(s.data);
        }
        String json = new String(os.toByteArray(), Charset.forName("UTF-8"));
        os.reset();
        LOG.info("merged json: {}", json);
        return objectMapper.readValue(json, objectMapper.getTypeFactory()
            .constructParametricType(ArrayList.class, CellJson.class));
      } catch (Exception e) {
        LOG.error("merge message slices exceptionally, slices size: {}", slices.size(), e);
        throw e;
      } finally {
        slices = new ArrayList<>();
        os.reset();
      }
    }
  }

}
