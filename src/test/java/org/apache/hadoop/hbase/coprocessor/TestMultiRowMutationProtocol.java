/*
 * Copyright 2011 The Apache Software Foundation
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

package org.apache.hadoop.hbase.coprocessor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.client.Condition;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * A test class to cover multi row mutations protocol
 */
@Category(MediumTests.class)
public class TestMultiRowMutationProtocol {

  private static final byte[] TEST_TABLE = Bytes.toBytes("TestTable");
  private static final byte[] TEST_FAMILY = Bytes.toBytes("TestFamily");
  private static final byte[] TEST_QUALIFIER = Bytes.toBytes("TestQualifier");
  private static byte[] ROW = Bytes.toBytes("testRow");
  
  private static final int ROWSIZE = 20;
  private static final int rowSeperator1 = 5;
  private static final int rowSeperator2 = 12;
  private static byte[][] ROWS = makeN(ROW, ROWSIZE);

  private static HBaseTestingUtility util = new HBaseTestingUtility();
  private static MiniHBaseCluster cluster = null;
  
  private HTable table = null;
  
  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    // set configure to indicate which cp should be loaded
    Configuration conf = util.getConfiguration();
    conf.setStrings(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
      "org.apache.hadoop.hbase.coprocessor.MultiRowMutationEndpoint");

    util.startMiniCluster(2);
    cluster = util.getMiniHBaseCluster();

    HTable table = util.createTable(TEST_TABLE, TEST_FAMILY);
    util.createMultiRegions(util.getConfiguration(), table, TEST_FAMILY,
                            new byte[][] { HConstants.EMPTY_BYTE_ARRAY,
                                ROWS[rowSeperator1], ROWS[rowSeperator2] });

    for (int i = 0; i < ROWSIZE; i++) {
      Put put = new Put(ROWS[i]);
      put.setWriteToWAL(false);
      put.add(TEST_FAMILY, TEST_QUALIFIER, Bytes.toBytes(i));
      table.put(put);
    }

    // sleep here is an ugly hack to allow region transitions to finish
    long timeout = System.currentTimeMillis() + (15 * 1000);
    while ((System.currentTimeMillis() < timeout) &&
      (table.getRegionsInfo().size() != 3)) {
      Thread.sleep(250);
    }
    table.close();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    util.shutdownMiniCluster();
  }
  
  @Before
  public void setup() throws IOException {
    table = new HTable(util.getConfiguration(), TEST_TABLE);
    for (int i = 0; i < ROWSIZE; i++) {
      Put put = new Put(ROWS[i]);
      put.setWriteToWAL(false);
      put.add(TEST_FAMILY, TEST_QUALIFIER, Bytes.toBytes(i));
      table.put(put);
    }
  }
  
  @After
  public void tearDown() throws IOException {
    table.close();
  }
  
  @Test
  public void testMultiRowMutations() throws IOException {
    List<Mutation> mutations = new ArrayList<Mutation>();

    Put put = new Put(ROWS[1]);
    put.add(TEST_FAMILY, TEST_QUALIFIER, Bytes.toBytes(2 * 1));
    mutations.add(put);
    Delete del = new Delete(ROWS[3]);
    del.deleteColumns(TEST_FAMILY, TEST_QUALIFIER);
    mutations.add(del);
    
    MultiRowMutationProtocol p =
        table.coprocessorProxy(MultiRowMutationProtocol.class, mutations.get(0).getRow());
    try {
      p.mutateRows(mutations);
    } catch (IOException e) {
      Assert.assertTrue(false);
    }
    
    Get get = new Get(ROWS[1]);
    get.addColumn(TEST_FAMILY, TEST_QUALIFIER);
    Result result = table.get(get);
    Assert.assertEquals(2, Bytes.toInt(result.getValue(TEST_FAMILY, TEST_QUALIFIER)));
    
    
    get = new Get(ROWS[3]);
    get.addColumn(TEST_FAMILY, TEST_QUALIFIER);
    result = table.get(get);
    Assert.assertNull(result.getValue(TEST_FAMILY, TEST_QUALIFIER));
  }
  
  @Test
  public void testMultiRowMutationsAcrossRegions() throws IOException {
    List<Mutation> mutations = new ArrayList<Mutation>();

    Put put = new Put(ROWS[1]);
    put.add(TEST_FAMILY, TEST_QUALIFIER, Bytes.toBytes(2 * 1));
    mutations.add(put);
    Delete del = new Delete(ROWS[7]);
    del.deleteColumns(TEST_FAMILY, TEST_QUALIFIER);
    mutations.add(del);
    
    MultiRowMutationProtocol p =
        table.coprocessorProxy(MultiRowMutationProtocol.class, mutations.get(0).getRow());
    try {
      p.mutateRows(mutations);
      Assert.assertTrue(false);
    } catch (IOException e) {
    }
  }

  @Test
  public void testMultiRowMutationsWithConditions() throws IOException {
    List<Mutation> mutations = new ArrayList<Mutation>();
    List<Condition> conditions = new ArrayList<Condition>();
    
    conditions.add(new Condition(ROWS[1], TEST_FAMILY, TEST_QUALIFIER, Bytes.toBytes(1)));
    Put put = new Put(ROWS[1]);
    put.add(TEST_FAMILY, TEST_QUALIFIER, Bytes.toBytes(2 * 1));
    mutations.add(put);
    
    conditions.add(new Condition(ROWS[3], TEST_FAMILY, TEST_QUALIFIER, Bytes.toBytes(3)));
    Delete del = new Delete(ROWS[3]);
    del.deleteColumns(TEST_FAMILY, TEST_QUALIFIER);
    mutations.add(del);
    
    MultiRowMutationProtocol p =
        table.coprocessorProxy(MultiRowMutationProtocol.class, mutations.get(0).getRow());
    try {
      List<Condition> results = p.mutateRowsWithConditions(mutations, conditions);
      Assert.assertEquals(0, results.size());
    } catch (IOException e) {
      Assert.assertTrue(false);
    }
    
    Get get = new Get(ROWS[1]);
    get.addColumn(TEST_FAMILY, TEST_QUALIFIER);
    Result result = table.get(get);
    Assert.assertEquals(2, Bytes.toInt(result.getValue(TEST_FAMILY, TEST_QUALIFIER)));
    
    
    get = new Get(ROWS[3]);
    get.addColumn(TEST_FAMILY, TEST_QUALIFIER);
    result = table.get(get);
    Assert.assertNull(result.getValue(TEST_FAMILY, TEST_QUALIFIER));
  }
  
  @Test
  public void testMultiRowMutationsWithConditionsAcrossRegions() throws IOException {
    List<Mutation> mutations = new ArrayList<Mutation>();
    List<Condition> conditions = new ArrayList<Condition>();
    
    conditions.add(new Condition(ROWS[1], TEST_FAMILY, TEST_QUALIFIER, Bytes.toBytes(1)));
    Put put = new Put(ROWS[1]);
    put.add(TEST_FAMILY, TEST_QUALIFIER, Bytes.toBytes(2 * 1));
    mutations.add(put);
    
    conditions.add(new Condition(ROWS[7], TEST_FAMILY, TEST_QUALIFIER, Bytes.toBytes(7)));
    Delete del = new Delete(ROWS[3]);
    del.deleteColumns(TEST_FAMILY, TEST_QUALIFIER);
    mutations.add(del);
    
    MultiRowMutationProtocol p =
        table.coprocessorProxy(MultiRowMutationProtocol.class, mutations.get(0).getRow());
    try {
      List<Condition> results = p.mutateRowsWithConditions(mutations, conditions);
      Assert.assertTrue(false);
    } catch (IOException e) {
    }
    
    
  }
  
  @Test
  public void testMultiRowMutationsWithConditionsFailed() throws IOException {
    List<Mutation> mutations = new ArrayList<Mutation>();
    List<Condition> conditions = new ArrayList<Condition>();
    
    conditions.add(new Condition(ROWS[1], TEST_FAMILY, TEST_QUALIFIER, Bytes.toBytes(1)));
    Put put = new Put(ROWS[1]);
    put.add(TEST_FAMILY, TEST_QUALIFIER, Bytes.toBytes(2 * 1));
    mutations.add(put);
    
    conditions.add(new Condition(ROWS[3], TEST_FAMILY, TEST_QUALIFIER, Bytes.toBytes(4)));
    Delete del = new Delete(ROWS[3]);
    del.deleteColumns(TEST_FAMILY, TEST_QUALIFIER);
    mutations.add(del);
    
    MultiRowMutationProtocol p =
        table.coprocessorProxy(MultiRowMutationProtocol.class, mutations.get(0).getRow());

    try {
      List<Condition> results = p.mutateRowsWithConditions(mutations, conditions);
      Assert.assertEquals(1, results.size());
    } catch (IOException e) {
      Assert.assertTrue(false);
    }
    
    Get get = new Get(ROWS[1]);
    get.addColumn(TEST_FAMILY, TEST_QUALIFIER);
    Result result = table.get(get);
    Assert.assertEquals(1, Bytes.toInt(result.getValue(TEST_FAMILY, TEST_QUALIFIER)));
    
    
    get = new Get(ROWS[3]);
    get.addColumn(TEST_FAMILY, TEST_QUALIFIER);
    result = table.get(get);
    Assert.assertEquals(3, Bytes.toInt(result.getValue(TEST_FAMILY, TEST_QUALIFIER)));
  }
  
  /**
   * an infrastructure method to prepare rows for the testtable.
   * @param base
   * @param n
   * @return
   */
  private static byte[][] makeN(byte[] base, int n) {
    byte[][] ret = new byte[n][];
    for (int i = 0; i < n; i++) {
      ret[i] = Bytes.add(base, Bytes.toBytes(i));
    }
    return ret;
  }
}
