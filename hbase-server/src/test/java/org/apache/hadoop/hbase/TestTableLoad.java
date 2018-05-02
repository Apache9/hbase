/**
 *
 * Copyright The Apache Software Foundation
 * <p>
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

package org.apache.hadoop.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.protobuf.generated.ClusterStatusProtos;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.protobuf.ByteString;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

@Category(MediumTests.class)
public class TestTableLoad {

  private static HBaseTestingUtility testUtil = new HBaseTestingUtility();;
  private static Configuration conf;
  private static Map<TableName, TableLoad> tableLoadMap;

  private static String testTableLatencyTableName = "TestTableLatency";
  private static final byte[] tableName = "TestFamilyInfo".getBytes();
  private static final byte[] familyName = "f1".getBytes();
  private static final long rowCount = 10;
  private static final long kvCount = 15;
  private static final long deleteFamilyCount = 1;
  private static final long deleteKvCount = 1;

  @BeforeClass
  public static void start() throws Exception {
    conf = testUtil.getConfiguration();
    conf.setInt("hbase.regionserver.msginterval", Integer.MAX_VALUE);
    testUtil.startMiniCluster(1, 1);

    MiniHBaseCluster cluster = testUtil.getHBaseCluster();
    cluster.waitForActiveAndReadyMaster();
    while (cluster.getLiveRegionServerThreads().size() < 1) {
      Threads.sleep(100);
    }
    long start = System.currentTimeMillis();
    makeDataForTestTableLatency();
    makeDataForTestFamilyInfo();
    cluster.getRegionServer(0).tryRegionServerReport(start, System.currentTimeMillis());
    Thread.sleep(2000);
    tableLoadMap = testUtil.getMiniHBaseCluster().getMaster().getTableLoads();
  }

  @AfterClass
  public static void shutdown() throws Exception {
    testUtil.shutdownMiniCluster();
  }

  @Test
  public void testFamilyStatistic() throws IOException, InterruptedException {
    TableLoad tableLoad = tableLoadMap.get(TableName.valueOf(tableName));
    List<FamilyLoad> familyLoads = tableLoad.getFamilyLoads();
    assertEquals(1, familyLoads.size());
    FamilyLoad load = familyLoads.get(0);
    assertEquals(Bytes.toString(familyName), load.getName());
    assertEquals(rowCount, load.getRowCount());
    assertEquals(kvCount, load.getKeyValueCount());
    assertEquals(deleteFamilyCount, load.getDeleteFamilyCount());
    assertEquals(deleteKvCount, load.getDeleteKeyValueCount());
    assertEquals(rowCount - deleteFamilyCount, tableLoad.getApproximateRowCount());
  }

  @Test
  public void testUpdateFamilyInfo(){
    TableLoad tableLoad = new TableLoad("testUpdateTableLoad");

    RegionLoad rl1 = new RegionLoad(createRegionLoad(false));
    RegionLoad rl2 = new RegionLoad(createRegionLoad(true));

    tableLoad.updateTableLoad(rl1);
    // test when family info is null
    tableLoad.updateTableLoad(rl2);
  }

  @Test
  public void testTableLatency() throws Exception {
    TableLoad tableLoad = tableLoadMap.get(TableName.valueOf(testTableLatencyTableName));
    Assert.assertEquals(10, tableLoad.getGetCount());
    Assert.assertEquals(31, tableLoad.getPutCount());
    Assert.assertEquals(1, tableLoad.getScanCount());
    Assert.assertEquals(3, tableLoad.getBatchCount());
    Assert.assertEquals(1, tableLoad.getDeleteCount());
    Assert.assertEquals(1, tableLoad.getAppendCount());
    Assert.assertEquals(1, tableLoad.getIncrementCount());
  }

  private ClusterStatusProtos.RegionLoad createRegionLoad(boolean familyInfoIsNull) {
    HBaseProtos.RegionSpecifier rSpec = HBaseProtos.RegionSpecifier.newBuilder()
        .setType(HBaseProtos.RegionSpecifier.RegionSpecifierType.ENCODED_REGION_NAME)
        .setValue(ByteString.copyFromUtf8("QWERTYUIOP")).build();
    ClusterStatusProtos.RegionLoad.Builder builder = ClusterStatusProtos.RegionLoad.newBuilder();
    ClusterStatusProtos.RegionLoad rl = builder.setRegionSpecifier(rSpec).setStores(3)
        .setStorefiles(13).setStoreUncompressedSizeMB(23).setStorefileSizeMB(300)
        .setStorefileIndexSizeMB(40).setRootIndexSizeKB(303).setReadRequestsCount(Integer.MAX_VALUE)
        .setWriteRequestsCount(Integer.MAX_VALUE).build();
    if (familyInfoIsNull == false) {
      ClusterStatusProtos.FamilyInfo familyInfo = ClusterStatusProtos.FamilyInfo.newBuilder()
          .setFamilyname("f1").setRowCount(123).setKvCount(456).setDelFamilyCount(12)
          .setDelKvCount(45).build();
      builder.addFamilyInfo(0, familyInfo);
      rl = builder.build();
    }
    return rl;
  }

  private static void makeDataForTestFamilyInfo() throws IOException, InterruptedException {
    final byte[] qualifier1 = "col1".getBytes();
    final byte[] qualifier2 = "col2".getBytes();
    final String rowKeyPrefix = "rowKey";

    HBaseAdmin ha = testUtil.getHBaseAdmin();
    byte[][] families = new byte[1][];
    families[0] = familyName;
    HTable ht = testUtil.createTable(tableName, families,1, "rowKey5".getBytes(), "rowKey9".getBytes(), 3);
    // insert rows
    for (int i = 0; i < rowCount; i++) {
      Put put = new Put((rowKeyPrefix + i).getBytes());
      put.add(familyName, qualifier1, Bytes.toBytes(i));
      ht.put(put);
    }
    // add additonal cols, row count stay unchange
    for (int i = 0; i < kvCount - rowCount; i++) {
      Put put = new Put((rowKeyPrefix + i).getBytes());
      put.add(familyName, qualifier2, Bytes.toBytes(i));
      ht.put(put);
    }
    //when one row has a put and a delete action in the same memstore, the delete action will override the put action,
    //the row count has been subtracted, but the delete count is not 0,
    //so flush the memstore, seperate the put and delete action
    ha.flush(tableName);
    // delete rows
    for(int i = 0; i < deleteFamilyCount; i++){
      Delete delete = new Delete((rowKeyPrefix + i).getBytes());
      ht.delete(delete);
    }
    // delete cols
    for(int i = 6; i < deleteKvCount + 6; i++){
      Delete delete2 = new Delete((rowKeyPrefix + i).getBytes());
      delete2.deleteColumns(familyName, qualifier1);
      ht.delete(delete2);
    }
    //make sure memstore flushed
    ha.flush(tableName);
  }

  private static void makeDataForTestTableLatency() throws Exception {
    byte[] tableName = Bytes.toBytes(testTableLatencyTableName);
    byte[][] splits = new byte[][] { Bytes.toBytes("1"), Bytes.toBytes("2"), Bytes.toBytes("3") };
    byte[] family = Bytes.toBytes("f");
    byte[] row = Bytes.toBytes(30);
    byte[] qualifier = Bytes.toBytes("q");
    byte[] qualifier2 = Bytes.toBytes("q2");
    byte[] value = Bytes.toBytes(30);

    testUtil.createTable(tableName, family, splits);
    new HTable(conf, tableName).close(); // wait for the table to come up.

    // put
    HTable table = new HTable(conf, tableName);
    Put put = new Put(row);
    put.add(family, qualifier, value);
    table.put(put);
    table.flushCommits();

    // put 30
    for (int i = 0; i < 30; i++) {
      Put putTmp = new Put(Bytes.toBytes(i));
      putTmp.add(family, qualifier, Bytes.toBytes(i));
      table.put(putTmp);
    }
    table.flushCommits();

    // get 10
    Get get = new Get(row);
    for (int i = 0; i < 10; i++) {
      table.get(get);
    }
    table.flushCommits();

    // scan
    Scan scan = new Scan();
    scan.setStartRow(Bytes.toBytes(0));
    scan.setStopRow(Bytes.toBytes(30));
    Iterator<Result> iterable = table.getScanner(scan).iterator();
    while (iterable.hasNext()) {
      iterable.next();
    }
    table.flushCommits();

    // append
    Append append = new Append(row);
    append.add(family, qualifier2, Bytes.toBytes(1));
    table.append(append);
    table.flushCommits();

    // delete
    Delete delete = new Delete(row);
    table.delete(delete);
    table.flushCommits();

    // increment
    Increment increment = new Increment(row);
    increment.addColumn(family, qualifier2, 10);
    table.increment(increment);
    table.flushCommits();

    // batch
    List<Get> gets = new ArrayList<Get>();
    for (int i = 0; i < 10; i++) {
      gets.add(new Get(row));
    }
    table.get(gets);
    table.flushCommits();

    RowMutations mutations = new RowMutations(row);
    mutations.add(new Delete(row));
    mutations.add(put);
    table.mutateRow(mutations);
    table.flushCommits();

    // batch
    table.setAutoFlushTo(false);
    for (int i = 0; i < 30; i++) {
      table.put(put);
    }
    table.flushCommits();

    table.close();
  }
}
