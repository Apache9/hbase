/**
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.Waiter.ExplainingPredicate;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestSplitMerge {
  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  @BeforeClass
  public static void setUp() throws Exception {
    UTIL.getConfiguration().setInt(HConstants.HBASE_CLIENT_META_OPERATION_TIMEOUT, 1000);
    UTIL.getConfiguration().setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 2);
    UTIL.startMiniCluster(1);
    UTIL.waitTableAvailable(TableName.NAMESPACE_TABLE_NAME.getName());
  }

  @AfterClass
  public static void tearDown() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Test
  public void test() throws Exception {
    TableName tableName = TableName.valueOf("SplitMerge");
    byte[] family = Bytes.toBytes("CF");
    HTableDescriptor td = new HTableDescriptor(tableName);
    td.addFamily(new HColumnDescriptor(family));
    HBaseAdmin admin = UTIL.getHBaseAdmin();
    admin.createTable(td, new byte[][] { Bytes.toBytes(1) });
    UTIL.waitTableAvailable(tableName.getName());

    admin.split(tableName.getName(), Bytes.toBytes(2));
    UTIL.waitFor(30000, new ExplainingPredicate<Exception>() {

      @Override
      public boolean evaluate() throws Exception {
        return UTIL.getMiniHBaseCluster().getRegions(tableName).size() == 3;
      }

      @Override
      public String explainFailure() throws Exception {
        return "Split has not finished yet";
      }
    });
	  UTIL.waitUntilNoRegionsInTransition(TimeUnit.SECONDS.toMillis(30));
    HRegionInfo regionA = null;
    HRegionInfo regionB = null;
    for (HRegionInfo region : admin.getTableRegions(tableName)) {
      if (region.getStartKey().length == 0) {
        regionA = region;
      } else if (Bytes.equals(region.getStartKey(), Bytes.toBytes(1))) {
        regionB = region;
      }
    }
    assertNotNull(regionA);
    assertNotNull(regionB);

    admin.mergeRegions(regionA.getEncodedNameAsBytes(), regionB.getEncodedNameAsBytes(), false);
    UTIL.waitFor(30000, new ExplainingPredicate<Exception>() {
      @Override
      public boolean evaluate() throws Exception {
        return admin.getTableRegions(tableName).size() == 2;
      }

      @Override
      public String explainFailure() throws Exception {
        return "Merge has not finished yet";
      }
    });
    
    HConnection connection = HConnectionManager.createConnection(UTIL.getConfiguration());
    ServerName expected = UTIL.getMiniHBaseCluster().getRegionServer(0).getServerName();
    assertEquals(expected, connection.getRegionLocation(tableName, Bytes.toBytes(1), true)
        .getServerName());
  }
}
