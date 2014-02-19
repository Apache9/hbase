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
public class TestCondition {
  private byte[]  row = Bytes.toBytes("TestCondition");
  private byte[]  family = Bytes.toBytes("family");
  private byte[]  qualifier = Bytes.toBytes("qualifier");
  private byte[]  value = Bytes.toBytes("value2");
  private byte[]  lessValue = Bytes.toBytes("value3");
  private byte[]  largerValue = Bytes.toBytes("value1");
  
  @Test
  public void testEqualCondition() {
    Condition condition = new Condition(row, family, qualifier, value);
    
    Result result = new Result();
    Assert.assertFalse(condition.isMatch(result));

    KeyValue kv = new KeyValue(row, family, qualifier, new byte[0]);
    result = new Result(Lists.newArrayList(kv));
    Assert.assertFalse(condition.isMatch(result));
    
    kv = new KeyValue(row, family, qualifier, value);
    result = new Result(Lists.newArrayList(kv));
    Assert.assertTrue(condition.isMatch(result));
    
    kv = new KeyValue(row, family, qualifier, lessValue);
    result = new Result(Lists.newArrayList(kv));
    Assert.assertFalse(condition.isMatch(result));

    kv = new KeyValue(row, family, qualifier, largerValue);
    result = new Result(Lists.newArrayList(kv));
    Assert.assertFalse(condition.isMatch(result));
  }
  
  @Test
  public void testLessCondition() {
    Condition condition = new Condition(row, family, qualifier, CompareOp.LESS, value);
    
    Result result = new Result();
    Assert.assertFalse(condition.isMatch(result));

    KeyValue kv = new KeyValue(row, family, qualifier, new byte[0]);
    result = new Result(Lists.newArrayList(kv));
    Assert.assertFalse(condition.isMatch(result));
    
    kv = new KeyValue(row, family, qualifier, value);
    result = new Result(Lists.newArrayList(kv));
    Assert.assertFalse(condition.isMatch(result));
    
    kv = new KeyValue(row, family, qualifier, lessValue);
    result = new Result(Lists.newArrayList(kv));
    Assert.assertTrue(condition.isMatch(result));

    kv = new KeyValue(row, family, qualifier, largerValue);
    result = new Result(Lists.newArrayList(kv));
    Assert.assertFalse(condition.isMatch(result));
  }
  
  @Test
  public void testGreaterCondition() {
    Condition condition = new Condition(row, family, qualifier, CompareOp.GREATER, value);
    
    Result result = new Result();
    Assert.assertFalse(condition.isMatch(result));

    KeyValue kv = new KeyValue(row, family, qualifier, new byte[0]);
    result = new Result(Lists.newArrayList(kv));
    Assert.assertFalse(condition.isMatch(result));
    
    kv = new KeyValue(row, family, qualifier, value);
    result = new Result(Lists.newArrayList(kv));
    Assert.assertFalse(condition.isMatch(result));
    
    kv = new KeyValue(row, family, qualifier, lessValue);
    result = new Result(Lists.newArrayList(kv));
    Assert.assertFalse(condition.isMatch(result));

    kv = new KeyValue(row, family, qualifier, largerValue);
    result = new Result(Lists.newArrayList(kv));
    Assert.assertTrue(condition.isMatch(result));
  }
  
  @Test
  public void testLessConditionInLong() {
    Condition condition =
        new Condition(row, family, qualifier, CompareOp.GREATER, new LongComparator(10));
    
    Result result = new Result();
    Assert.assertFalse(condition.isMatch(result));
    
    KeyValue kv = new KeyValue(row, family, qualifier, Bytes.toBytes(9L));
    result = new Result(Lists.newArrayList(kv));
    Assert.assertTrue(condition.isMatch(result));
    
    kv = new KeyValue(row, family, qualifier, Bytes.toBytes(10L));
    result = new Result(Lists.newArrayList(kv));
    Assert.assertFalse(condition.isMatch(result));

    kv = new KeyValue(row, family, qualifier, Bytes.toBytes(11L));
    result = new Result(Lists.newArrayList(kv));
    Assert.assertFalse(condition.isMatch(result));
  }
}
