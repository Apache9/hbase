/**
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

package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Lists;

@Category(SmallTests.class)
public class TestSlaveTable {
  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private final byte[] TABLE = Bytes.toBytes("ReplicationSlaveTable");
  private final byte[] FAMILY = Bytes.toBytes("somefamily");
  private final byte[] ROW = Bytes.toBytes("somerow");
  private final byte[] QUALIFIER = Bytes.toBytes("somequalifier");
  private final List<UUID> clusterIds = Lists.newArrayList(UUID.randomUUID());

  private HRegion region;

  private HRegion createRegion(final byte[] table, final byte[] family) throws IOException {
    HTableDescriptor htd = new HTableDescriptor(table);
    htd.addFamily(new HColumnDescriptor(family));
    htd.setSlave(true);
    HRegionInfo hri = new HRegionInfo(htd.getName(), null, null);
    return HRegion.createHRegion(hri, TEST_UTIL.getDataTestDir("TestReplicationSlaveTable"),
      TEST_UTIL.getConfiguration(), htd);
  }

  @Before
  public void setUp() throws Exception {
    this.region = createRegion(TABLE, FAMILY);
  }

  @After
  public void tearDown() throws Exception {
    HRegion.closeHRegion(this.region);
    this.region = null;
  }

  @Test
  public void testMutationWithSlaveTable() throws Exception {
    Append append = new Append(ROW);
    append.add(FAMILY, QUALIFIER, Bytes.toBytes("somevalue"));
    try {
      region.append(append, false);
      assertTrue(false);
    } catch (IOException e) {
    }

    Increment incr = new Increment(ROW);
    incr.addColumn(FAMILY, Bytes.toBytes("data"), 1);
    try {
      region.increment(incr, false);
      assertTrue(false);
    } catch (IOException e) {
    }

    Put put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, Bytes.toBytes("somevalue"));
    try {
      region.put(put, false);
      assertTrue(false);
    } catch (IOException e) {
    }

    Delete del = new Delete(ROW);
    del.deleteColumns(FAMILY, QUALIFIER);
    try {
      region.delete(del, false);
      assertTrue(false);
    } catch (IOException e) {
    }

    RowMutations rows = new RowMutations(ROW);
    rows.add(put);
    rows.add(del);
    try {
      region.mutateRow(rows);
      assertTrue(false);
    } catch (IOException e) {
    }

    // checkAndMutate
    try {
      region.checkAndMutate(ROW, FAMILY, QUALIFIER, CompareOp.EQUAL, new BinaryComparator(), put,
        false);
      assertTrue(false);
    } catch (IOException e) {
    }

    try {
      region.checkAndMutate(ROW, FAMILY, QUALIFIER, CompareOp.EQUAL, new BinaryComparator(), del,
        false);
      assertTrue(false);
    } catch (IOException e) {
    }

    // batchMutate
    List<Pair<Mutation, Integer>> mutationsAndLocks = new LinkedList<Pair<Mutation, Integer>>();
    mutationsAndLocks.add(new Pair(put, null));
    mutationsAndLocks.add(new Pair(del, null));
    try {
      region.batchMutate(mutationsAndLocks.toArray(new Pair[] {}));
      assertTrue(false);
    } catch (IOException e) {
    }
  }

  @Test
  public void testReadWithSlaveTable() throws Exception {
    Get get = new Get(ROW);
    try {
      region.get(get);
    } catch (IOException e) {
      assertTrue(false);
    }
  }

  @Test
  public void testMutationFromReplication() throws Exception {
    Put put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, Bytes.toBytes("somevalue"));
    put.setClusterIds(clusterIds);
    try {
      region.put(put, false);
    } catch (IOException e) {
      assertTrue(false);
    }

    Delete del = new Delete(ROW);
    del.deleteColumns(FAMILY, QUALIFIER);
    del.setClusterIds(clusterIds);
    try {
      region.delete(del, false);
    } catch (IOException e) {
      assertTrue(false);
    }

    RowMutations rows = new RowMutations(ROW);
    rows.add(put);
    rows.add(del);
    try {
      region.mutateRow(rows);
    } catch (IOException e) {
      assertTrue(false);
    }
    // batchMutate
    List<Pair<Mutation, Integer>> mutationsAndLocks = new LinkedList<Pair<Mutation, Integer>>();
    mutationsAndLocks.add(new Pair(put, null));
    mutationsAndLocks.add(new Pair(del, null));
    try {
      region.batchMutate(mutationsAndLocks.toArray(new Pair[] {}));
    } catch (IOException e) {
      assertTrue(false);
    }
  }
}
