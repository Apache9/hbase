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
package org.apache.hadoop.hbase.client;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.After;
import org.junit.Test;

public abstract class AbstractTestRegionLocator {

  protected static final HBaseTestingUtility UTIL = new HBaseTestingUtility();

  protected static TableName TABLE_NAME = TableName.valueOf("Locator");

  protected static byte[] FAMILY = Bytes.toBytes("family");

  protected static int REGION_REPLICATION = 3;

  protected static byte[][] SPLIT_KEYS;

  protected static void startClusterAndCreateTable() throws Exception {
    UTIL.startMiniCluster(3);
    TableDescriptor td =
      TableDescriptorBuilder.newBuilder(TABLE_NAME).setRegionReplication(REGION_REPLICATION)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY)).build();
    SPLIT_KEYS = new byte[9][];
    for (int i = 0; i < 9; i++) {
      SPLIT_KEYS[i] = Bytes.toBytes(Integer.toString(i + 1));
    }
    UTIL.getAdmin().createTable(td, SPLIT_KEYS);
    UTIL.waitTableAvailable(TABLE_NAME);
    UTIL.getAdmin().balancerSwitch(false, true);
  }

  @After
  public void tearDownAfterTest() throws IOException {
    clearCache();
  }

  private byte[] getStartKey(int index) {
    return index == 0 ? HConstants.EMPTY_START_ROW : SPLIT_KEYS[index - 1];
  }

  private byte[] getEndKey(int index) {
    return index == SPLIT_KEYS.length ? HConstants.EMPTY_END_ROW : SPLIT_KEYS[index];
  }

  private void assertStartKeys(byte[][] startKeys) {
    assertEquals(SPLIT_KEYS.length + 1, startKeys.length);
    for (int i = 0; i < startKeys.length; i++) {
      assertArrayEquals(getStartKey(i), startKeys[i]);
    }
  }

  private void assertEndKeys(byte[][] endKeys) {
    assertEquals(SPLIT_KEYS.length + 1, endKeys.length);
    for (int i = 0; i < endKeys.length; i++) {
      assertArrayEquals(getEndKey(i), endKeys[i]);
    }
  }

  @Test
  public void testStartEndKeys() throws IOException {
    assertStartKeys(getStartKeys());
    assertEndKeys(getEndKeys());
    Pair<byte[][], byte[][]> startEndKeys = getStartEndKeys();
    assertStartKeys(startEndKeys.getFirst());
    assertEndKeys(startEndKeys.getSecond());
  }

  private void assertRegionLocation(HRegionLocation loc, int index, int replicaId) {
    RegionInfo region = loc.getRegion();
    byte[] startKey = getStartKey(index);
    assertArrayEquals(startKey, region.getStartKey());
    assertArrayEquals(getEndKey(index), region.getEndKey());
    assertEquals(replicaId, region.getReplicaId());
    ServerName expected =
      UTIL.getMiniHBaseCluster().getRegionServerThreads().stream().map(t -> t.getRegionServer())
        .filter(rs -> rs.getRegions(TABLE_NAME).stream().map(Region::getRegionInfo)
          .anyMatch(r -> r.containsRow(startKey) && r.getReplicaId() == replicaId))
        .findFirst().get().getServerName();
    assertEquals(expected, loc.getServerName());
  }

  @Test
  public void testGetRegionLocation() throws IOException {
    for (int i = 0; i <= SPLIT_KEYS.length; i++) {
      for (int replicaId = 0; replicaId < REGION_REPLICATION; replicaId++) {
        assertRegionLocation(getRegionLocation(getStartKey(i), replicaId), i, replicaId);
      }
    }
  }

  @Test
  public void testGetRegionLocations() throws IOException {
    for (int i = 0; i <= SPLIT_KEYS.length; i++) {
      List<HRegionLocation> locs = getRegionLocations(getStartKey(i));
      assertEquals(REGION_REPLICATION, locs.size());
      for (int replicaId = 0; replicaId < REGION_REPLICATION; replicaId++) {
        assertRegionLocation(locs.get(replicaId), i, replicaId);
      }
    }
  }

  @Test
  public void testGetAllRegionLocations() throws IOException {
    List<HRegionLocation> locs = getAllRegionLocations();
    assertEquals(REGION_REPLICATION * (SPLIT_KEYS.length + 1), locs.size());
    Collections.sort(locs, (l1, l2) -> {
      int c = Bytes.compareTo(l1.getRegion().getStartKey(), l2.getRegion().getStartKey());
      if (c != 0) {
        return c;
      }
      return Integer.compare(l1.getRegion().getReplicaId(), l2.getRegion().getReplicaId());
    });
    for (int i = 0; i <= SPLIT_KEYS.length; i++) {
      for (int replicaId = 0; replicaId < REGION_REPLICATION; replicaId++) {
        assertRegionLocation(locs.get(i * REGION_REPLICATION + replicaId), i, replicaId);
      }
    }
  }

  protected abstract byte[][] getStartKeys() throws IOException;

  protected abstract byte[][] getEndKeys() throws IOException;

  protected abstract Pair<byte[][], byte[][]> getStartEndKeys() throws IOException;

  protected abstract HRegionLocation getRegionLocation(byte[] row, int replicaId)
      throws IOException;

  protected abstract List<HRegionLocation> getRegionLocations(byte[] row) throws IOException;

  protected abstract List<HRegionLocation> getAllRegionLocations() throws IOException;

  protected abstract void clearCache() throws IOException;
}
