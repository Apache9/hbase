/**
 * Copyright 2010 The Apache Software Foundation
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

package org.apache.hadoop.hbase.regionserver.wal;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

@Category(MediumTests.class)
public class TestHLogCompactor {
  private static Configuration conf;
  private static HLog hlog;
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static MiniHBaseCluster cluster;

  private final static String table1 = "HLogCompactor1";
  private final static String table2 = "HLogCompactor2";

  private final static byte[] cf = Bytes.toBytes("cf");
  private final static byte[] qualifier = Bytes.toBytes("qualifier");
  private final static byte[] value = Bytes.toBytes("value");

  @Before
  public void setUp() throws Exception {
    conf = TEST_UTIL.getConfiguration();
    // disable regionserver hlog compact
    conf.set(HLogCompactor.HLOG_COMPACT_ENABLE, "false");

    cluster = TEST_UTIL.startMiniCluster(1);
    hlog = cluster.getRegionServer(0).getWAL();
  }

  private void insertData(int hlogNum) throws Exception {
    HBaseAdmin admin = new HBaseAdmin(conf);
    HColumnDescriptor family = new HColumnDescriptor(cf);
    
    if (admin.tableExists(table1)) {
      admin.deleteTable(table1);
    }
    HTableDescriptor desc = new HTableDescriptor(table1);
    desc.addFamily(family);
    admin.createTable(desc);

    if (admin.tableExists(table2)) {
      admin.deleteTable(table2);
    }
    desc = new HTableDescriptor(table2);
    desc.addFamily(family);
    admin.createTable(desc);

    HTable t1 = new HTable(conf, table1);
    HTable t2 = new HTable(conf, table2);

    for (int i = 0; i < hlogNum; i++) {
      Put put = new Put(Bytes.toBytes("row-" + i));
      put.add(cf, qualifier, value);
      t1.put(put);
      t2.put(put);
      hlog.rollWriter(true);
    }
    admin.flush(Bytes.toBytes(table2));
    
    Put put = new Put(Bytes.toBytes("row-" + hlogNum));
    put.add(cf, qualifier, value);
    t1.put(put);
    t2.put(put);

    t1.close();
    t2.close();
    admin.close();
  }
  
  private void flushAllAndRollWriter(HRegionServer server) throws Exception {
    // flush all region and archive all hlogs
    HBaseAdmin admin = new HBaseAdmin(conf);
    admin.flush(HConstants.ROOT_TABLE_NAME);
    admin.flush(HConstants.META_TABLE_NAME);
    admin.flush(Bytes.toBytes(table1));
    admin.flush(Bytes.toBytes(table2));
    server.rollHLogWriter();
    assertEquals(1, server.getWAL().getNumLogFiles());
    admin.close();
  }

  @Test
  public void testCompactHLogs() throws Exception {
    insertData(5);
    assertEquals(6, hlog.getNumLogFiles());
    HLogCompactor compactor = new HLogCompactor(cluster.getRegionServer(0), 10);
    assertTrue(compactor.compact());
    assertEquals(2, hlog.getNumLogFiles());
  }

  @Test
  public void testFSFailure() throws Exception {
    HRegionServer server = cluster.getRegionServer(0);
    FileSystem mockFS = Mockito.mock(FileSystem.class);
    HLogCompactor compactor =
        new HLogCompactor(server, mockFS, server.getWAL(), 10);

    when(mockFS.exists(Mockito.any(Path.class))).thenReturn(false);
    when(mockFS.mkdirs(Mockito.any(Path.class))).thenThrow(
      new IOException("File System error"));
    when(mockFS.listStatus(Mockito.any(Path.class))).thenThrow(
      new IOException("File System error"));

    assertFalse(compactor.compact());
    
    verify(mockFS, atLeastOnce()).exists(Mockito.any(Path.class));
    verify(mockFS, atLeastOnce()).mkdirs(Mockito.any(Path.class));
    verify(mockFS, atLeastOnce()).listStatus(Mockito.any(Path.class));
  }

  @Test
  public void testNoHLogToCompact() throws Exception {
    HRegionServer server = cluster.getRegionServer(0);
    HLog mockHlog = Mockito.spy(server.getWAL());
    HLogCompactor compactor = new HLogCompactor(server, server.getFileSystem(), mockHlog, 10);
    when(mockHlog.getCompactHLogFiles()).thenReturn(new TreeMap<Long, Path>());
    assertFalse(compactor.compact());
    
    verify(mockHlog).getCompactHLogFiles();
    verify(mockHlog, never()).completeHLogCompaction(Mockito.any(Map.class),
      Mockito.any(Map.class));
  }

  @Test
  public void testCompactNoExistedHLog() throws Exception {
    HRegionServer server = cluster.getRegionServer(0);
    HLog mockHlog = Mockito.spy(server.getWAL());
    HLogCompactor compactor = new HLogCompactor(server, server.getFileSystem(), mockHlog, 10);
    
    SortedMap<Long, Path> fakeLogs = new TreeMap<Long, Path>();
    
    fakeLogs.put(mockHlog.getSequenceNumber(), mockHlog.computeNewFilename());
    fakeLogs.put(mockHlog.getSequenceNumber(), mockHlog.computeNewFilename());
    fakeLogs.put(mockHlog.getSequenceNumber(), mockHlog.computeNewFilename());
    
    when(mockHlog.getCompactHLogFiles()).thenReturn(fakeLogs);

    assertFalse(compactor.compact());

    verify(mockHlog).getCompactHLogFiles();
    verify(mockHlog, never()).completeHLogCompaction(Mockito.any(Map.class),
      Mockito.any(Map.class));
  }

  @Test
  public void testCompactArchivedHLog() throws Exception {
    insertData(6);
    final HRegionServer server = cluster.getRegionServer(0);
    HLog mockHlog = Mockito.spy(server.getWAL());

    HLogCompactor compactor = new HLogCompactor(server, server.getFileSystem(), mockHlog, 10);
    
    when(mockHlog.getCompactHLogFiles()).thenAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        Object ret = invocation.callRealMethod();
        flushAllAndRollWriter(server);
        return ret;
      }
    });
    assertFalse(compactor.compact());

    verify(mockHlog).getCompactHLogFiles();
    verify(mockHlog, never()).completeHLogCompaction(Mockito.any(Map.class),
      Mockito.any(Map.class));
  }

  @Test
  public void testCompactFailedWhenLogArchived() throws Exception {
    insertData(13);

    final HRegionServer server = cluster.getRegionServer(0);
    HLog mockHlog = Mockito.spy(server.getWAL());
    HLogCompactor compactor = new HLogCompactor(server, server.getFileSystem(), mockHlog, 10);

    Mockito.doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        flushAllAndRollWriter(server);
        invocation.callRealMethod();
        return null;
      }
    }).when(mockHlog)
        .completeHLogCompaction(Mockito.any(Map.class), Mockito.any(Map.class));

    assertFalse(compactor.compact());

    verify(mockHlog).getCompactHLogFiles();
    verify(mockHlog).completeHLogCompaction(Mockito.any(Map.class),
      Mockito.any(Map.class));
  }

  @Test
  public void testDataDurability() throws Exception {
    HLogCompactor compactor = new HLogCompactor(cluster.getRegionServer(0), 10);
    insertData(20);
    HTable t1 = new HTable(conf, table1);
    HTable t2 = new HTable(conf, table2);

    for (int i = 0; i < 20; i++) {
      Get get = new Get(Bytes.toBytes("row-" + i));
      assertEquals(Bytes.toString(value), Bytes.toString(t1.get(get).getValue(cf, qualifier)));
      assertEquals(Bytes.toString(value), Bytes.toString(t2.get(get).getValue(cf, qualifier)));
    }

    assertTrue(compactor.compact());
    TEST_UTIL.shutdownMiniHBaseCluster();
    TEST_UTIL.restartHBaseCluster(1);
    
    for (int i = 0; i < 20; i++) {
      Get get = new Get(Bytes.toBytes("row-" + i));
      assertEquals(Bytes.toString(value), Bytes.toString(t1.get(get).getValue(cf, qualifier)));
      assertEquals(Bytes.toString(value), Bytes.toString(t2.get(get).getValue(cf, qualifier)));
    }
    t1.close();
    t2.close();
  }

  @After
  public void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }
}
