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

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.LongComparator;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Lists;

@Category(SmallTests.class)
public class TestSingleColumnCheck {
  private byte[]  row = Bytes.toBytes("TestCheck");
  private byte[]  family = Bytes.toBytes("family");
  private byte[]  qualifier = Bytes.toBytes("qualifier");
  private byte[]  value = Bytes.toBytes("value2");
  private byte[]  lessValue = Bytes.toBytes("value1");
  private byte[]  largerValue = Bytes.toBytes("value3");
  
  @Test
  public void testEqualCheck() {
    SingleColumnCheck check = new SingleColumnCheck(row, family, qualifier, value);
    
    Result result = new Result();
    Assert.assertFalse(check.check(result));

    KeyValue kv = new KeyValue(row, family, qualifier, new byte[0]);
    result = new Result(Lists.newArrayList(kv));
    Assert.assertFalse(check.check(result));
    
    kv = new KeyValue(row, family, qualifier, value);
    result = new Result(Lists.newArrayList(kv));
    Assert.assertTrue(check.check(result));
    
    kv = new KeyValue(row, family, qualifier, lessValue);
    result = new Result(Lists.newArrayList(kv));
    Assert.assertFalse(check.check(result));

    kv = new KeyValue(row, family, qualifier, largerValue);
    result = new Result(Lists.newArrayList(kv));
    Assert.assertFalse(check.check(result));
  }
  
  @Test
  public void testLessCheck() {
    SingleColumnCheck check = new SingleColumnCheck(row, family, qualifier, CompareOp.LESS, value);
    
    Result result = new Result();
    Assert.assertFalse(check.check(result));

    KeyValue kv = new KeyValue(row, family, qualifier, new byte[0]);
    result = new Result(Lists.newArrayList(kv));
    Assert.assertFalse(check.check(result));
    
    kv = new KeyValue(row, family, qualifier, value);
    result = new Result(Lists.newArrayList(kv));
    Assert.assertFalse(check.check(result));
    
    kv = new KeyValue(row, family, qualifier, lessValue);
    result = new Result(Lists.newArrayList(kv));
    Assert.assertTrue(check.check(result));

    kv = new KeyValue(row, family, qualifier, largerValue);
    result = new Result(Lists.newArrayList(kv));
    Assert.assertFalse(check.check(result));
  }
  
  @Test
  public void testGreaterCheck() {
    SingleColumnCheck check = new SingleColumnCheck(row, family, qualifier, CompareOp.GREATER, value);
    
    Result result = new Result();
    Assert.assertFalse(check.check(result));

    KeyValue kv = new KeyValue(row, family, qualifier, new byte[0]);
    result = new Result(Lists.newArrayList(kv));
    Assert.assertFalse(check.check(result));
    
    kv = new KeyValue(row, family, qualifier, value);
    result = new Result(Lists.newArrayList(kv));
    Assert.assertFalse(check.check(result));
    
    kv = new KeyValue(row, family, qualifier, lessValue);
    result = new Result(Lists.newArrayList(kv));
    Assert.assertFalse(check.check(result));

    kv = new KeyValue(row, family, qualifier, largerValue);
    result = new Result(Lists.newArrayList(kv));
    Assert.assertTrue(check.check(result));
  }
  
  @Test
  public void testLessCheckInLong() {
    SingleColumnCheck check =
        new SingleColumnCheck(row, family, qualifier, CompareOp.GREATER, new LongComparator(10));
    
    Result result = new Result();
    Assert.assertFalse(check.check(result));
    
    KeyValue kv = new KeyValue(row, family, qualifier, Bytes.toBytes(9L));
    result = new Result(Lists.newArrayList(kv));
    Assert.assertFalse(check.check(result));
    
    kv = new KeyValue(row, family, qualifier, Bytes.toBytes(10L));
    result = new Result(Lists.newArrayList(kv));
    Assert.assertFalse(check.check(result));

    kv = new KeyValue(row, family, qualifier, Bytes.toBytes(11L));
    result = new Result(Lists.newArrayList(kv));
    Assert.assertTrue(check.check(result));
  }
}
