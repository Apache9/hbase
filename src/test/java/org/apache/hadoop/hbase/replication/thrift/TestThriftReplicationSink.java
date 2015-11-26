package org.apache.hadoop.hbase.replication.thrift;

import com.google.common.base.Throwables;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.catalog.CatalogTracker;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.wal.HLogKey;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.replication.regionserver.ReplicationSink;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
@Category(MediumTests.class)
public class TestThriftReplicationSink extends TestThriftReplicationBase {

  private static final Log LOG = LogFactory.getLog(TestThriftReplicationSink.class);
  private static final int BATCH_SIZE = 10;

  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static ReplicationSink SINK;

  private static final byte[] TABLE_NAME1 = Bytes.toBytes("table1");
  private static final byte[] TABLE_NAME2 = Bytes.toBytes("table2");

  private static final byte[] FAM_NAME1 = Bytes.toBytes("info1");
  private static final byte[] FAM_NAME2 = Bytes.toBytes("info2");

  private static HTable table1;
  private static HTable table2;

  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.getConfiguration().setBoolean("dfs.support.append", true);
    TEST_UTIL.getConfiguration().setBoolean(HConstants.REPLICATION_ENABLE_KEY, true);
    ReplicationTestUtils.setupConfiguration(TEST_UTIL, HBaseTestingUtility.randomFreePort());
    TEST_UTIL.startMiniCluster(1);

    Configuration sinkConfiguration = new Configuration(TEST_UTIL.getConfiguration());
    sinkConfiguration.setInt("hbase.replication.thrift.server.port", HBaseTestingUtility.randomFreePort());
    SINK = new ReplicationSink(sinkConfiguration, SERVER, UUID.randomUUID().toString());
    table1 = TEST_UTIL.createTable(TABLE_NAME1, FAM_NAME1);
    table2 = TEST_UTIL.createTable(TABLE_NAME2, FAM_NAME2);
  }

  /**
   * @throws java.lang.Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    SERVER.stop("Shutting down");
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception {
    table1 = TEST_UTIL.truncateTable(TABLE_NAME1);
    table2 = TEST_UTIL.truncateTable(TABLE_NAME2);
  }

  /**
   * Insert a whole batch of entries
   * @throws Exception
   */
  @Test
  public void testBatchSink() throws Exception {
    HLog.Entry[] entries = new HLog.Entry[BATCH_SIZE];
    for(int i = 0; i < BATCH_SIZE; i++) {
      entries[i] = createEntry(TABLE_NAME1, i, KeyValue.Type.Put);
    }
    SINK.replicate(ThriftAdaptors.REPLICATION_BATCH_ADAPTOR.toThrift(entries));
    Scan scan = new Scan();
    ResultScanner scanRes = table1.getScanner(scan);
    assertEquals(BATCH_SIZE, scanRes.next(BATCH_SIZE).length);
  }

  /**
   * Insert a mix of puts and deletes
   * @throws Exception
   */
  @Test
  public void testMixedPutDelete() throws Exception {
    HLog.Entry[] entries = new HLog.Entry[BATCH_SIZE/2];
    for(int i = 0; i < BATCH_SIZE/2; i++) {
      entries[i] = createEntry(TABLE_NAME1, i, KeyValue.Type.Put);
    }
    SINK.replicate(ThriftAdaptors.REPLICATION_BATCH_ADAPTOR.toThrift(entries));

    entries = new HLog.Entry[BATCH_SIZE];
    for(int i = 0; i < BATCH_SIZE; i++) {
      entries[i] = createEntry(TABLE_NAME1, i,
          i % 2 != 0 ? KeyValue.Type.Put: KeyValue.Type.DeleteColumn);
    }

    SINK.replicate(ThriftAdaptors.REPLICATION_BATCH_ADAPTOR.toThrift(entries));
    Scan scan = new Scan();
    ResultScanner scanRes = table1.getScanner(scan);
    assertEquals(BATCH_SIZE/2, scanRes.next(BATCH_SIZE).length);
  }

  /**
   * Insert to 2 different tables
   * @throws Exception
   */
  @Test
  public void testMixedPutTables() throws Exception {
    HLog.Entry[] entries = new HLog.Entry[BATCH_SIZE];
    for(int i = 0; i < BATCH_SIZE; i++) {
      entries[i] =
          createEntry( i % 2 == 0 ? TABLE_NAME2 : TABLE_NAME1,
              i, KeyValue.Type.Put);
    }

    SINK.replicate(ThriftAdaptors.REPLICATION_BATCH_ADAPTOR.toThrift(entries));
    Scan scan = new Scan();
    ResultScanner scanRes = table2.getScanner(scan);
    for(Result res : scanRes) {
      assertTrue(Bytes.toInt(res.getRow()) % 2 == 0);
    }
  }

  /**
   * Insert then do different types of deletes
   * @throws Exception
   */
  @Test
  public void testMixedDeletes() throws Exception {
    HLog.Entry[] entries = new HLog.Entry[3];
    for(int i = 0; i < 3; i++) {
      entries[i] = createEntry(TABLE_NAME1, i, KeyValue.Type.Put);
    }
    SINK.replicate(ThriftAdaptors.REPLICATION_BATCH_ADAPTOR.toThrift(entries));
    entries = new HLog.Entry[3];

    entries[0] = createEntry(TABLE_NAME1, 0, KeyValue.Type.DeleteColumn);
    entries[1] = createEntry(TABLE_NAME1, 1, KeyValue.Type.DeleteFamily);
    entries[2] = createEntry(TABLE_NAME1, 2, KeyValue.Type.DeleteColumn);

    SINK.replicate(ThriftAdaptors.REPLICATION_BATCH_ADAPTOR.toThrift(entries));

    Scan scan = new Scan();
    ResultScanner scanRes = table1.getScanner(scan);
    assertEquals(0, scanRes.next(3).length);
  }

  /**
   * Puts are buffered, but this tests when a delete (not-buffered) is applied
   * before the actual Put that creates it.
   * @throws Exception
   */
  @Test
  public void testApplyDeleteBeforePut() throws Exception {
    HLog.Entry[] entries = new HLog.Entry[5];
    for(int i = 0; i < 2; i++) {
      entries[i] = createEntry(TABLE_NAME1, i, KeyValue.Type.Put);
    }
    entries[2] = createEntry(TABLE_NAME1, 1, KeyValue.Type.DeleteFamily);
    for(int i = 3; i < 5; i++) {
      entries[i] = createEntry(TABLE_NAME1, i, KeyValue.Type.Put);
    }
    SINK.replicate(ThriftAdaptors.REPLICATION_BATCH_ADAPTOR.toThrift(entries));
    Get get = new Get(Bytes.toBytes(1));
    Result res = table1.get(get);
    assertEquals(0, res.size());
  }

  private HLog.Entry createEntry(byte [] table, int row,  KeyValue.Type type) {
    byte[] fam = Bytes.equals(table, TABLE_NAME1) ? FAM_NAME1 : FAM_NAME2;
    byte[] rowBytes = Bytes.toBytes(row);
    // Just make sure we don't get the same ts for two consecutive rows with
    // same key
    try {
      Thread.sleep(1);
    } catch (InterruptedException e) {
      LOG.info("Was interrupted while sleep, meh", e);
    }
    final long now = System.currentTimeMillis();
    KeyValue kv = null;
    if(type.getCode() == KeyValue.Type.Put.getCode()) {
      kv = new KeyValue(rowBytes, fam, fam, now,
          KeyValue.Type.Put, Bytes.toBytes(row));
    } else if (type.getCode() == KeyValue.Type.DeleteColumn.getCode()) {
      kv = new KeyValue(rowBytes, fam, fam,
          now, KeyValue.Type.DeleteColumn);
    } else if (type.getCode() == KeyValue.Type.DeleteFamily.getCode()) {
      kv = new KeyValue(rowBytes, fam, null,
          now, KeyValue.Type.DeleteFamily);
    }

    HLogKey key = new HLogKey(table, table, now, now,
        HConstants.DEFAULT_CLUSTER_ID);

    WALEdit edit = new WALEdit();
    edit.add(kv);

    return new HLog.Entry(key, edit);
  }

  @org.junit.Rule
  public org.apache.hadoop.hbase.ResourceCheckerJUnitRule cu =
      new org.apache.hadoop.hbase.ResourceCheckerJUnitRule();

  private static Server SERVER = new Server() {
    final AtomicBoolean stop = new AtomicBoolean(false);

    @Override
    public boolean isStopped() {
      return this.stop.get();
    }

    @Override
    public void stop(String why) {
      LOG.info("STOPPING BECAUSE: " + why);
      this.stop.set(true);
    }

    @Override
    public Configuration getConfiguration() {
      return null;
    }

    @Override
    public ZooKeeperWatcher getZooKeeper() {
      return null;
    }

    @Override
    public CatalogTracker getCatalogTracker() {
      return null;
    }

    @Override
    public ServerName getServerName() {
      try {
        return new ServerName(InetAddress.getLocalHost().getHostName(),9,-1);
      } catch (UnknownHostException e) {
        throw Throwables.propagate(e);
      }
    }

    @Override
    public void abort(String why, Throwable e) {
      //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean isAborted() {
      return false;  //To change body of implemented methods use File | Settings | File Templates.
    }
  };


}
