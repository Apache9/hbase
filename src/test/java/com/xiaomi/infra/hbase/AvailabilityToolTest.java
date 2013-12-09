package com.xiaomi.infra.hbase;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

public class AvailabilityToolTest {
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster();
    HBaseAdmin admin = TEST_UTIL.getHBaseAdmin();
    admin.createTable(getTestTableDesc());
  }

  public static HTableDescriptor getTestTableDesc() {
    HTableDescriptor desc = new HTableDescriptor("Test");
    HColumnDescriptor family = new HColumnDescriptor("M");
    desc.addFamily(family);
    return desc;
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.cleanupTestDir();
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void testSimple() throws IOException, InterruptedException {
    ExecutorService exec = new ScheduledThreadPoolExecutor(2);
    AvailabilityTool tool = new AvailabilityTool(TEST_UTIL.getConfiguration(),
        exec);
    String args[] = { "Test" };
    tool.exec(args);
    assertEquals(tool.getReportor().GetAvailability("Test"), 1.0, 0.01);
  }
}
