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

package org.apache.hadoop.hbase.client;

import java.io.IOException;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test for checkAndMutate in HTable
 */
@Category(MediumTests.class)
public class TestCheckAndMutate {
  private static final byte[] TEST_TABLE = Bytes.toBytes("TestCheckAndMutate");
  private static final byte[] FAMILY = Bytes.toBytes("TestFamily");
  private static final byte[] QUALIFIER = Bytes.toBytes("qualifier");
  
  private static byte[] ROW = Bytes.toBytes("testRow");

  private static HBaseTestingUtility util = new HBaseTestingUtility();
  
  private static HTable table = null;
  
  @BeforeClass
  public static void setupBeforeClass() throws Exception {
    // set configure to indicate which cp should be loaded
    Configuration conf = util.getConfiguration();
    util.startMiniCluster(2);

    table = util.createTable(TEST_TABLE, FAMILY);
    util.createMultiRegions(table, FAMILY);
    
    // sleep here is an ugly hack to allow region transitions to finish
    long timeout = System.currentTimeMillis() + (15 * 1000);
    while ((System.currentTimeMillis() < timeout) &&
      (table.getRegionsInfo().size() < 1)) {
      Thread.sleep(250);
    }
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    util.shutdownMiniCluster();
    table.close();
  }

  @Test
  public void testSimple() throws IOException {
    // put value1 if qualifier == null
    Check check = new SingleColumnCheck(ROW, FAMILY, QUALIFIER, null);
    Put put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, Bytes.toBytes("value1"));
    Assert.assertTrue(table.checkAndMutate(check, put));
    assertValue(ROW, FAMILY, QUALIFIER, Bytes.toBytes("value1"));
    
    // put value2 if qualifier == null
    check = new SingleColumnCheck(ROW, FAMILY, QUALIFIER, null);
    put = new Put(ROW);
    put.add(FAMILY, QUALIFIER, Bytes.toBytes("value2"));
    Assert.assertFalse(table.checkAndMutate(check, put));
    assertValue(ROW, FAMILY, QUALIFIER, Bytes.toBytes("value1"));
    
    // delete if qualifier = value1
    check = new SingleColumnCheck(ROW, FAMILY, QUALIFIER, Bytes.toBytes("value1"));
    Delete del = new Delete(ROW);
    del.deleteColumn(FAMILY, QUALIFIER);
    Assert.assertTrue(table.checkAndMutate(check, del));
    assertValue(ROW, FAMILY, QUALIFIER, null);
    
    // delete if qualifier = value1
    check = new SingleColumnCheck(ROW, FAMILY, QUALIFIER, Bytes.toBytes("value1"));
    del = new Delete(ROW);
    del.deleteColumn(FAMILY, QUALIFIER);
    Assert.assertFalse(table.checkAndMutate(check, del));
    assertValue(ROW, FAMILY, QUALIFIER, null);
  }
  
  private void assertValue(final byte[] row, final byte[] family,
      final byte[] qualifier, final byte[] expected) throws IOException {
    Get get = new Get(row);
    get.addColumn(family, qualifier);
    Result result = table.get(get);
    byte[] value = result.getValue(family, qualifier);
    if (expected == null || expected.length == 0) {
      Assert.assertNull(value);
    } else {
      Assert.assertEquals(Bytes.toString(expected), Bytes.toString(value));
    }
  }
}
