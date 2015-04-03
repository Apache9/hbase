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
package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.regionserver.Region.FlushResult;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ RegionServerTests.class, MediumTests.class })
public class TestPerColumnFamilyFlushOfRegionReplicas {

  private static final Log LOG = LogFactory.getLog(TestPerColumnFamilyFlush.class);

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static final byte[] FAMILY1 = Bytes.toBytes("F1");

  private static final byte[] FAMILY2 = Bytes.toBytes("F2");

  private static final byte[] QUALIFIER = Bytes.toBytes("Q");

  private static final TableName TABLE_NAME = TableName.valueOf("test",
    TestPerColumnFamilyFlushOfRegionReplicas.class.getName());

  @BeforeClass
  public static void setUp() throws Exception {
    TEST_UTIL.startMiniCluster(2);
    TEST_UTIL.getHBaseAdmin().createNamespace(
      NamespaceDescriptor.create(TABLE_NAME.getNamespaceAsString()).build());
    HTableDescriptor htd = new HTableDescriptor(TABLE_NAME);
    htd.addFamily(new HColumnDescriptor(FAMILY1));
    htd.addFamily(new HColumnDescriptor(FAMILY2));
    htd.setFlushPolicyClassName(FlushLargeStoresPolicy.class.getName());
    htd.setValue(FlushLargeStoresPolicy.HREGION_COLUMNFAMILY_FLUSH_SIZE_LOWER_BOUND, "1024");
    htd.setRegionReplication(2);
    TEST_UTIL.getHBaseAdmin().createTable(htd);
    TEST_UTIL.waitTableAvailable(TABLE_NAME);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  @Test
  public void test() throws Exception {
    MiniHBaseCluster cluster = TEST_UTIL.getMiniHBaseCluster();
    List<JVMClusterUtil.RegionServerThread> rsts = cluster.getRegionServerThreads();
    Region pr = null;
    Region sr = null;
    for (int i = 0; i < cluster.getRegionServerThreads().size(); i++) {
      HRegionServer hrs = rsts.get(i).getRegionServer();
      for (Region region : hrs.getOnlineRegions(TABLE_NAME)) {
        if (RegionReplicaUtil.isDefaultReplica(region.getRegionInfo())) {
          pr = region;
        } else {
          sr = region;
        }
      }
    }
    assertNotNull(pr);
    assertNotNull(sr);
    final HRegion primaryReplica = (HRegion) pr;
    final HRegion secondaryReplica = (HRegion) sr;
    try (Table table = TEST_UTIL.getConnection().getTable(TABLE_NAME)) {
      Put put = new Put(Bytes.toBytes(1));
      put.addColumn(FAMILY1, QUALIFIER, Bytes.toBytes("1"));
      table.put(put);

      put = new Put(Bytes.toBytes(2));
      byte[] value = new byte[1200];
      ThreadLocalRandom.current().nextBytes(value);
      put.addColumn(FAMILY2, QUALIFIER, value);
      table.put(put);
    }
    final long maxFlushedSeqIdOfSecondaryReplica = secondaryReplica.getMaxFlushedSeqId();
    FlushResult flushResult = ((HRegion) primaryReplica).flushcache(false, true);
    assertTrue(flushResult.isFlushSucceeded());
    LOG.info("======" + primaryReplica.getMaxFlushedSeqId());
    TEST_UTIL.waitFor(60000, new Waiter.ExplainingPredicate<Exception>() {

      @Override
      public boolean evaluate() throws Exception {
        return secondaryReplica.getMaxFlushedSeqId() != maxFlushedSeqIdOfSecondaryReplica;
      }

      @Override
      public String explainFailure() throws Exception {
        return "Max flushed sequence id of secondary replica is still "
            + secondaryReplica.getMaxFlushedSeqId();
      }
    });
  }
}
