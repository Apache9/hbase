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

import com.google.protobuf.ByteString;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.protobuf.generated.ClusterStatusProtos;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.Map;

import static org.junit.Assert.assertEquals;

@Category(MediumTests.class)
public class TestTableLoad {

  private HBaseTestingUtility testUtil = new HBaseTestingUtility();;

  private static final byte[] tableName = "TestFmailyInfo".getBytes();
  private static final byte[] familyName = "f1".getBytes();
  private static final byte[] qualifier1 = "col1".getBytes();
  private static final byte[] qualifier2 = "col2".getBytes();
  private static final String rowKeyPrefix = "rowKey";

  private static final long rowCount = 10;
  private static final long kvCount = 15;
  private static final long deleteFamilyCount = 1;
  private static final long deleteKvCount = 1;

  @Before
  public void start() throws Exception {
    testUtil.startMiniCluster(1, 1);
  }

  @After
  public void shutdown() throws Exception {
    testUtil.shutdownMiniCluster();
  }

  @Test
  public void testFamilyStatistic() throws IOException, InterruptedException {
    makeSomeData();
    //make sure metrics has been update
    Thread.sleep(5000);

    TableLoad tableLoad = testUtil.getMiniHBaseCluster().getMaster().getTableLoads().get(TableName.valueOf(tableName));
    Map<String, Long> familyStatistic = tableLoad.getFamilyStastics().get(new String(familyName));
    assertEquals(rowCount, (long)familyStatistic.get("rowCount"));
    assertEquals(kvCount, (long)familyStatistic.get("kvCount"));
    assertEquals(deleteFamilyCount, (long)familyStatistic.get("deleteFamilyCount"));
    assertEquals(deleteKvCount, (long)familyStatistic.get("deleteKvCount"));
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

  private ClusterStatusProtos.RegionLoad createRegionLoad(boolean familyInfoIsNull){
    HBaseProtos.RegionSpecifier rSpec =
            HBaseProtos.RegionSpecifier.newBuilder()
                    .setType(HBaseProtos.RegionSpecifier.RegionSpecifierType.ENCODED_REGION_NAME)
                    .setValue(ByteString.copyFromUtf8("QWERTYUIOP")).build();
    ClusterStatusProtos.RegionLoad.Builder builder = ClusterStatusProtos.RegionLoad.newBuilder();
    ClusterStatusProtos.RegionLoad rl =
            builder.setRegionSpecifier(rSpec).setStores(3)
                    .setStorefiles(13).setStoreUncompressedSizeMB(23).setStorefileSizeMB(300)
                    .setStorefileIndexSizeMB(40).setRootIndexSizeKB(303).setReadRequestsCount(Integer.MAX_VALUE).setWriteRequestsCount(Integer.MAX_VALUE).build();
    if(familyInfoIsNull == false){
      ClusterStatusProtos.FamilyInfo familyInfo=
              ClusterStatusProtos.FamilyInfo.newBuilder().setFamilyname("f1").setRowCount(123).setKvCount(456)
              .setDelFamilyCount(12).setDelKvCount(45).build();
      builder.addFamilyInfo(0, familyInfo);
      rl = builder.build();
    }
    return rl;
  }

  private void makeSomeData() throws IOException, InterruptedException {
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

}
