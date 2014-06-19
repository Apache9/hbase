/*
 * Copyright 2011 The Apache Software Foundation Licensed to the Apache Software Foundation (ASF)
 * under one or more contributor license agreements. See the NOTICE file distributed with this work
 * for additional information regarding copyright ownership. The ASF licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law or agreed to in
 * writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */

package org.apache.hadoop.hbase.client;

import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.filter.LongComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Lists;

@Category(SmallTests.class)
public class TestCheckList {
  private byte[] row = Bytes.toBytes("TestCheck");
  private byte[] family = Bytes.toBytes("family");
  private byte[] qualifier1 = Bytes.toBytes("qualifier1");
  private byte[] qualifier2 = Bytes.toBytes("qualifier2");

  @Test
  public void testAndOperator() throws HBaseIOException {
    //  > value1  && < value 3
    CheckList checkList = new CheckList(row, Operator.MUST_PASS_ALL);
    Check check = new SingleColumnCheck(row, family,
      qualifier1, CompareOp.LESS, Bytes.toBytes("value3"));
    checkList.addCheck(check);
    
    check = new SingleColumnCheck(row, family,
        qualifier1, CompareOp.GREATER, Bytes.toBytes("value1"));
    checkList.addCheck(check);
    
    // value2
    KeyValue kv = new KeyValue(row, family, qualifier1, Bytes.toBytes("value2"));
    Result result = new Result(Lists.newArrayList(kv));
    Assert.assertTrue(checkList.check(result));
    
    // value 1
    kv = new KeyValue(row, family, qualifier1, Bytes.toBytes("value1"));
    result = new Result(Lists.newArrayList(kv));
    Assert.assertFalse(checkList.check(result));
    
    // value 4s
    kv = new KeyValue(row, family, qualifier1, Bytes.toBytes("value4"));
    result = new Result(Lists.newArrayList(kv));
    Assert.assertFalse(checkList.check(result));
  }

  @Test
  public void testOROperator() throws HBaseIOException {
    // < value2 || = value3
    CheckList checkList = new CheckList(row, Operator.MUST_PASS_ONE);
    Check check = new SingleColumnCheck(row, family,
        qualifier1, CompareOp.LESS, Bytes.toBytes("value2"));
    checkList.addCheck(check);
    check =
        new SingleColumnCheck(row, family, qualifier1, CompareOp.EQUAL,
            Bytes.toBytes("value3"));
    checkList.addCheck(check);
    
    // value 1
    KeyValue kv = new KeyValue(row, family, qualifier1, Bytes.toBytes("value1"));
    Result result = new Result(Lists.newArrayList(kv));
    Assert.assertTrue(checkList.check(result));
    
    // value 2
    kv = new KeyValue(row, family, qualifier1, Bytes.toBytes("value2"));
    result = new Result(Lists.newArrayList(kv));
    Assert.assertFalse(checkList.check(result));
    
    //value 3
    kv = new KeyValue(row, family, qualifier1, Bytes.toBytes("value3"));
    result = new Result(Lists.newArrayList(kv));
    Assert.assertTrue(checkList.check(result));
  }

  @Test
  public void testHierarchy() throws HBaseIOException {
    // (qualifier1 < value2 || qualifier1 = value3) && qualifier2 == 10
    CheckList c1 = new CheckList(row, Operator.MUST_PASS_ONE);
    Check check = new SingleColumnCheck(row, family,
        qualifier1, CompareOp.LESS, Bytes.toBytes("value2"));
    c1.addCheck(check);
    check =
        new SingleColumnCheck(row, family, qualifier1, CompareOp.EQUAL,
            Bytes.toBytes("value3"));
    c1.addCheck(check);
    
    Check c2 =
        new SingleColumnCheck(row, family, qualifier2,
            CompareOp.EQUAL, new LongComparator(10));
    
    CheckList c = new CheckList(row, Operator.MUST_PASS_ALL);
    c.addCheck(c1);
    c.addCheck(c2);
    
    // qualifier1 = value2, qualifier2 = null
    KeyValue kv = new KeyValue(row, family, qualifier1, Bytes.toBytes("value2"));
    Result result = new Result(Lists.newArrayList(kv));
    Assert.assertFalse(c.check(result));
    
    // qualifier1 = value2, qualifier2 = 10
    KeyValue kv1 = new KeyValue(row, family, qualifier1, Bytes.toBytes("value2"));
    KeyValue kv2 = new KeyValue(row, family, qualifier2, Bytes.toBytes(10L));
    result = new Result(Lists.newArrayList(kv1, kv2));
    Assert.assertFalse(c.check(result));
    
    // qualifier1 = value3, qualifier2 = 10
    kv1 = new KeyValue(row, family, qualifier1, Bytes.toBytes("value3"));
    kv2 = new KeyValue(row, family, qualifier2, Bytes.toBytes(10L));
    result = new Result(Lists.newArrayList(kv1, kv2));
    Assert.assertTrue(c.check(result));
  }
}
