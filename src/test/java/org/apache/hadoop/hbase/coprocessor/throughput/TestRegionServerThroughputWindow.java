package org.apache.hadoop.hbase.coprocessor.throughput;

import junit.framework.Assert;
import org.apache.hadoop.hbase.SmallTests;
import org.junit.Before;
import org.junit.Test;

import org.apache.hadoop.hbase.throughput.ThroughputExceededException;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.experimental.categories.Category;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

@Category(SmallTests.class)
public class TestRegionServerThroughputWindow {

  private static final long WINDOW_SIZE = 50;
  private static final long MAX_READ_QPS = 200;
  private static final long MAX_WRITE_QPS = 200;
  private static final byte[] ACL_TABLE_BYTES = Bytes.toBytes("_acl_");

  RegionServerThroughputWindow window;

  @Before
  public void setup() throws Exception{
    window = new RegionServerThroughputWindow(WINDOW_SIZE, MAX_READ_QPS, MAX_WRITE_QPS);
  }


  @Test
  public void testNormal() throws Exception {
    testNormalTableRead(Bytes.toBytes("test"), WINDOW_SIZE, MAX_READ_QPS);
    Thread.sleep(WINDOW_SIZE);
    testNormalTableWrite(Bytes.toBytes("test"), WINDOW_SIZE, MAX_WRITE_QPS);
  }

  @Test
  public void testRootOrMeta() throws Exception {
    testSystemTableRead(ACL_TABLE_BYTES, WINDOW_SIZE, MAX_READ_QPS);
    Thread.sleep(WINDOW_SIZE);
    testSystemTableWrite(ACL_TABLE_BYTES, WINDOW_SIZE, MAX_WRITE_QPS);
  }

  @Test
  public void testGenerateTableStatStr() throws Exception {
    ConcurrentHashMap<String, AtomicLong> a = new ConcurrentHashMap<String, AtomicLong>();
    a.put("a", new AtomicLong(3L));
    a.put("b", new AtomicLong(2L));
    a.put("c", new AtomicLong(4L));
    a.put("d", new AtomicLong(0L));
    a.put("e", new AtomicLong(10L));
    Assert.assertEquals(window.generateTableStatStr(a), "e:10;c:4;a:3;b:2;d:0;");
  }

  private void testNormalTableRead(byte[] tableName, long windowSize, long targetQps) throws Exception
  {
    for(long i=0; i<targetQps*WINDOW_SIZE/1000; ++i) {
      window.check(tableName, RegionServerThroughputWindow.RequestType.BEFORE_READ);
      window.check(tableName, RegionServerThroughputWindow.RequestType.AFTER_READ);
    }

    boolean isExceeded = false;
    try {
      window.check(tableName, RegionServerThroughputWindow.RequestType.BEFORE_READ);
    }
    catch (ThroughputExceededException e ) {
      isExceeded = true;
    }
    Assert.assertTrue(isExceeded);

    Thread.sleep(windowSize);

    for(long i=0; i<targetQps*WINDOW_SIZE/1000; ++i) {
      window.check(tableName, RegionServerThroughputWindow.RequestType.BEFORE_READ);
      window.check(tableName, RegionServerThroughputWindow.RequestType.AFTER_READ);
    }

    isExceeded = false;
    try {
      window.check(tableName, RegionServerThroughputWindow.RequestType.BEFORE_READ);
    }
    catch (ThroughputExceededException e ) {
      isExceeded = true;
    }
    Assert.assertTrue(isExceeded);
  }


  private void testNormalTableWrite(byte[] tableName, long windowSize, long targetQps) throws Exception
  {
    for(long i=0; i<targetQps*WINDOW_SIZE/1000; ++i) {
      window.check(tableName, RegionServerThroughputWindow.RequestType.WRITE);
    }

    boolean isExceeded = false;
    try {
      window.check(tableName, RegionServerThroughputWindow.RequestType.WRITE);
    }
    catch (ThroughputExceededException e ) {
      isExceeded = true;
    }
    Assert.assertTrue(isExceeded);

    Thread.sleep(windowSize);

    for(long i=0; i<targetQps*WINDOW_SIZE/1000; ++i) {
      window.check(tableName, RegionServerThroughputWindow.RequestType.WRITE);
    }

    isExceeded = false;
    try {
      window.check(tableName, RegionServerThroughputWindow.RequestType.WRITE);
    }
    catch (ThroughputExceededException e ) {
      isExceeded = true;
    }
    Assert.assertTrue(isExceeded);
  }

  private void testSystemTableWrite(byte[] tableName, long windowSize, long targetQps) throws Exception {
    for(long i=0; i<targetQps*windowSize/1000*10; ++i) {
      window.check(tableName, RegionServerThroughputWindow.RequestType.WRITE);
    }

    Thread.sleep(windowSize);
    for(long i=0; i<targetQps*windowSize/1000*10; ++i) {
      window.check(tableName, RegionServerThroughputWindow.RequestType.WRITE);
    }
  }

  private void testSystemTableRead(byte[] tableName, long windowSize, long targetQps) throws Exception {
    window.reset();
    for(long i=0; i<targetQps*windowSize/1000*10; ++i) {
      window.check(tableName, RegionServerThroughputWindow.RequestType.BEFORE_READ);
      window.check(tableName, RegionServerThroughputWindow.RequestType.AFTER_READ);
    }
    window.check(tableName, RegionServerThroughputWindow.RequestType.BEFORE_READ);
    window.check(tableName, RegionServerThroughputWindow.RequestType.AFTER_READ);

    window.reset();

    for(long i=0; i<targetQps*windowSize/1000*10; ++i) {
      window.check(tableName, RegionServerThroughputWindow.RequestType.BEFORE_READ);
      window.check(tableName, RegionServerThroughputWindow.RequestType.AFTER_READ);
    }
    window.check(tableName, RegionServerThroughputWindow.RequestType.BEFORE_READ);
    window.check(tableName, RegionServerThroughputWindow.RequestType.AFTER_READ);
  }

}
