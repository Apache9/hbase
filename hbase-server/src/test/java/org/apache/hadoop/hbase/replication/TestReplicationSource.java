/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.replication;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.EOFException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.Waiter.Predicate;
import org.apache.hadoop.hbase.fs.HFileSystem;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.wal.HLogFactory;
import org.apache.hadoop.hbase.regionserver.wal.HLogKey;
import org.apache.hadoop.hbase.regionserver.wal.SequenceFileLogReader;
import org.apache.hadoop.hbase.regionserver.wal.SequenceFileLogWriter;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.replication.regionserver.HBaseInterClusterReplicationEndpoint;
import org.apache.hadoop.hbase.replication.regionserver.ReplicationSource;
import org.apache.hadoop.hbase.replication.regionserver.ReplicationSourceManager;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

@Category(MediumTests.class)
public class TestReplicationSource {

  private static final Log LOG =
      LogFactory.getLog(TestReplicationSource.class);
  private final static HBaseTestingUtility TEST_UTIL =
      new HBaseTestingUtility();
  private static FileSystem FS;
  private static Path oldLogDir;
  private static Path logDir;
  private static Configuration conf = HBaseConfiguration.create();

  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(1, 3);
  }

  /**
   * Sanity check that we can move logs around while we are reading
   * from them. Should this test fail, ReplicationSource would have a hard
   * time reading logs that are being archived.
   * @throws Exception
   */
  @Test
  public void testLogMoving() throws Exception{
    FS = TEST_UTIL.getDFSCluster().getFileSystem();
    oldLogDir = new Path(FS.getHomeDirectory(),
        HConstants.HREGION_OLDLOGDIR_NAME);
    if (FS.exists(oldLogDir)) FS.delete(oldLogDir, true);
    logDir = new Path(FS.getHomeDirectory(),
        HConstants.HREGION_LOGDIR_NAME);
    if (FS.exists(logDir)) FS.delete(logDir, true);
    
    Path logPath = new Path(logDir, "log");
    if (!FS.exists(logDir)) FS.mkdirs(logDir);
    if (!FS.exists(oldLogDir)) FS.mkdirs(oldLogDir);
    HLog.Writer writer = HLogFactory.createWALWriter(FS,
      logPath, conf);
    for(int i = 0; i < 3; i++) {
      byte[] b = Bytes.toBytes(Integer.toString(i));
      KeyValue kv = new KeyValue(b,b,b);
      WALEdit edit = new WALEdit();
      edit.add(kv);
      HLogKey key = new HLogKey(b, TableName.valueOf(b), 0, 0,
          HConstants.DEFAULT_CLUSTER_ID);
      writer.append(new HLog.Entry(key, edit));
      writer.sync(false);
    }
    writer.close();

    HLog.Reader reader = HLogFactory.createReader(FS, 
        logPath, conf);
    HLog.Entry entry = reader.next();
    assertNotNull(entry);

    Path oldLogPath = new Path(oldLogDir, "log");
    FS.rename(logPath, oldLogPath);

    entry = reader.next();
    assertNotNull(entry);

    entry = reader.next();
    entry = reader.next();

    assertNull(entry);

  }

  @Test
  public void testRecoverLeaseFromNotClosedLog() throws Exception {
    Configuration conf = new Configuration(TEST_UTIL.getMiniHBaseCluster().getConfiguration());
    conf.setClass("hbase.regionserver.hlog.writer.impl", SequenceFileLogWriter.class,
      HLog.Writer.class);
    HFileSystem FS = new HFileSystem(conf, false);
    assertEquals(FS.getBackingFs().getClass().getName(),
        "org.apache.hadoop.hdfs.DistributedFileSystem");

    Path logDir = new Path(TEST_UTIL.getDFSCluster().getFileSystem().getHomeDirectory(),
        HConstants.HREGION_LOGDIR_NAME);

    Path logPath = new Path(logDir, "log");
    if (!FS.exists(logDir))
      FS.mkdirs(logDir);

    HLogFactory.resetLogWriterClass();
    HLog.Writer writer = HLogFactory.createWALWriter(FS, logPath, conf);
    for(int i = 0; i < 5; i++) {
      byte[] b = Bytes.toBytes(Integer.toString(i));
      KeyValue kv = new KeyValue(b,b,b);
      WALEdit edit = new WALEdit();
      edit.add(kv);
      HLogKey key = new HLogKey(b, TableName.valueOf(b), 0, 0, HConstants.DEFAULT_CLUSTER_ID);
      HLog.Entry entry = new HLog.Entry(key, edit);
      writer.append(entry);
    }
    SequenceFileLogWriter sequenceFileLogWriter = (SequenceFileLogWriter) writer;
    sequenceFileLogWriter.getWriterFSDataOutputStream().hflush();
    long fileLen0 = FS.getFileStatus(logPath).getLen();
    assertTrue(fileLen0 > 0);

    sequenceFileLogWriter.getWriterFSDataOutputStream().writeInt(10);

    sequenceFileLogWriter.getWriterFSDataOutputStream().hflush();
    long fileLen1 = FS.getFileStatus(logPath).getLen();
    assertEquals(fileLen0, fileLen1);

    HLogFactory.resetLogReaderClass();
    conf.setClass("hbase.regionserver.hlog.reader.impl", SequenceFileLogReader.class,
      HLog.Reader.class);
    HLog.Reader reader = HLogFactory.createReader(FS.getBackingFs(), logPath, conf);
    for(int i = 0; i< 5; ++i) {
      HLog.Entry entry = reader.next();
      assertNotNull(entry);
    }

    try {
      HLog.Entry entry = reader.next();

      fail("not catch EOFException");
    }catch (EOFException eofException) {
    }

    ReplicationSource.recoverFileLease(FS, logPath, conf);
    long fileLen = FS.getFileStatus(logPath).getLen();
    assertTrue(fileLen > fileLen0);
    assertEquals(fileLen, reader.getPosition());
  }

  /**
   * Tests that {@link ReplicationSource#terminate(String)} will timeout properly
   */
  @Test
  public void testTerminateTimeout() throws Exception {
    final ReplicationSource source = new ReplicationSource();
    ReplicationEndpoint replicationEndpoint = new HBaseInterClusterReplicationEndpoint() {
      @Override
      protected void doStart() {
        notifyStarted();
      }

      @Override
      protected void doStop() {
        // not calling notifyStopped() here causes the caller of stop() to get a Future that never
        // completes
      }
    };
    replicationEndpoint.start();
    ReplicationPeers mockPeers = Mockito.mock(ReplicationPeers.class);
    ReplicationPeer mockPeer = Mockito.mock(ReplicationPeer.class);
    Mockito.when(mockPeer.getPeerBandwidth()).thenReturn(0L);
    Mockito.when(mockPeers.getPeer("testPeer")).thenReturn(mockPeer);
    Configuration testConf = HBaseConfiguration.create();
    testConf.setInt("replication.source.maxretriesmultiplier", 1);
    ReplicationSourceManager manager = Mockito.mock(ReplicationSourceManager.class);
    Mockito.when(manager.getTotalBufferUsed()).thenReturn(new AtomicLong());
    source.init(testConf, null, manager, null, mockPeers, null, "testPeer", null, replicationEndpoint,
      null);
    ExecutorService executor = Executors.newSingleThreadExecutor();
    final Future<?> future = executor.submit(new Runnable() {

      @Override
      public void run() {
        source.terminate("testing source termination");
      }
    });
    long sleepForRetries = testConf.getLong("replication.source.sleepforretries", 1000);
    Waiter.waitFor(testConf, sleepForRetries * 2, new Predicate<Exception>() {

      @Override
      public boolean evaluate() throws Exception {
        return future.isDone();
      }

    });
  }
}

