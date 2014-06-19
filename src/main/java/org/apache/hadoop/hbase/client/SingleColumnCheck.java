/*
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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.WritableByteArrayComparable;
import org.apache.hadoop.hbase.io.HbaseObjectWritable;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.collect.ImmutableSet;

/**
 * Single column value check
 */
public class SingleColumnCheck implements Check {
  private static final Log LOG = LogFactory.getLog(SingleColumnCheck.class);
  private static final byte VERSION = (byte) 1;

  private byte[] row;
  private byte[] family;
  private byte[] qualifier;
  private CompareOp compareOp;
  private WritableByteArrayComparable comparator;

  public SingleColumnCheck() {
  }

  public SingleColumnCheck(final byte[] row, final byte[] family, final byte[] qualifier,
      final CompareOp compareOp, WritableByteArrayComparable comparator) {
    this.row = row;
    this.family = family;
    this.qualifier =
        qualifier == null ? HConstants.EMPTY_BYTE_ARRAY : qualifier;
    this.compareOp = compareOp;
    this.comparator = comparator;
  }

  public SingleColumnCheck(final byte[] row, final byte[] family, final byte[] qualifier, final byte[] value) {
    this(row, family, qualifier, CompareOp.EQUAL, new BinaryComparator(value));
  }

  public SingleColumnCheck(final byte[] row, final byte[] family, final byte[] qualifier,
      final CompareOp compareOp, final byte[] value) {
    this(row, family, qualifier, compareOp, new BinaryComparator(value));
  }
  
  @Override
  public byte[] getRow() {
    return row;
  }
  
  @Override
  public Set<byte[]> getFamilies() {
    return ImmutableSet.of(family);
  }
  
  @Override
  public Map<byte[], NavigableSet<byte[]>> getFamilyMap() {
    Map<byte [], NavigableSet<byte []>> familyMap =
        new TreeMap<byte [], NavigableSet<byte []>>(Bytes.BYTES_COMPARATOR);
    familyMap.put(family, new TreeSet<byte []>(Bytes.BYTES_COMPARATOR));
    familyMap.get(family).add(qualifier);
    return familyMap;
  }
  
  @Override
  public boolean check(Result result) {
    boolean valueIsNull = comparator.getValue() == null || comparator.getValue().length == 0;

    KeyValue kv = result.getColumnLatest(family, qualifier);
    boolean rowIsNull = (result.size() == 0 || kv.getValue().length == 0);

    boolean matches = false;
    if (rowIsNull || valueIsNull) {
      if (compareOp.equals(CompareOp.EQUAL)) {
        matches = (rowIsNull == valueIsNull);
      } else if (compareOp.equals(CompareOp.NOT_EQUAL)) {
        matches = (rowIsNull != valueIsNull);
      } else if (compareOp.equals(CompareOp.LESS) || compareOp.equals(CompareOp.LESS_OR_EQUAL)
          || compareOp.equals(CompareOp.GREATER) || compareOp.equals(CompareOp.GREATER_OR_EQUAL)) {
        LOG.warn("CompareOp : " + compareOp
            + " is not supportted when cell value or comparator value is null, actual rowIsNull : "
            + rowIsNull + ", valueIsNull : " + valueIsNull);
      } else {
        throw new RuntimeException("Unknown Compare op " + compareOp.name());
      }
    } else {
      int compareResult =
          comparator.compareTo(kv.getBuffer(), kv.getValueOffset(), kv.getValueLength());
      switch (compareOp) {
      case LESS:
        matches = compareResult > 0;
        break;
      case LESS_OR_EQUAL:
        matches = compareResult >= 0;
        break;
      case EQUAL:
        matches = compareResult == 0;
        break;
      case NOT_EQUAL:
        matches = compareResult != 0;
        break;
      case GREATER_OR_EQUAL:
        matches = compareResult <= 0;
        break;
      case GREATER:
        matches = compareResult < 0;
        break;
      default:
        throw new RuntimeException("Unknown Compare op " + compareOp.name());
      }
    }
    return matches;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeByte(VERSION);
    Bytes.writeByteArray(out, this.row);
    Bytes.writeByteArray(out, this.family);
    Bytes.writeByteArray(out, this.qualifier);
    out.writeUTF(compareOp.name());
    HbaseObjectWritable.writeObject(out, comparator, WritableByteArrayComparable.class, null);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    byte version = in.readByte();
    this.row = Bytes.readByteArray(in);
    this.family = Bytes.readByteArray(in);
    this.qualifier = Bytes.readByteArray(in);
    this.compareOp = CompareOp.valueOf(in.readUTF());
    this.comparator = (WritableByteArrayComparable) HbaseObjectWritable.readObject(in, null);
  }
  
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("Check row:" + Bytes.toString(row));
    sb.append(" familiy:" + Bytes.toString(family));
    sb.append(" qualifier:" + Bytes.toString(qualifier));
    sb.append(" compareOp:" + compareOp);
    sb.append(" comparator:" + comparator);
    sb.append(". ");
    return sb.toString();
  }
}
