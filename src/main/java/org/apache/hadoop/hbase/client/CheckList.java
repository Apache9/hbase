/*
 * Copyright 2010 The Apache Software Foundation
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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.io.HbaseObjectWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;

/**
 * Implementation of {@link Check} that represents an ordered List of Check
 * which will be evaluated with a specified boolean operator {@link Operator#MUST_PASS_ALL}
 * (<code>AND</code>) or {@link Operator#MUST_PASS_ONE} (<code>OR</code>).
 * Since you can use Check Lists as children of check lists, you can create a
 * hierarchy of Check to be evaluated.
 */

public class CheckList implements Check {
  private static final Log LOG = LogFactory.getLog(CheckList.class);
  private static final byte VERSION = (byte) 1;

  private byte[] row;
  private Operator operator = Operator.MUST_PASS_ALL;
  private List<Check> checkList = new ArrayList<Check>();

  /**
   * Constructor that takes an operator.
   * @param operator Operator to process filter set with.
   */
  public CheckList(final byte[] row) {
    this(row, Operator.MUST_PASS_ALL);
  }
  
  public CheckList(final byte[] row, final Operator operator) {
    this.row = row;
    this.operator = operator;
  }

  public CheckList(byte[] row, final Operator operator,
      final List<Check> checkList) throws HBaseIOException {
    this.row = row;
    this.operator = operator;
    this.checkList = checkList;
    for (Check check : checkList) {
      if (Bytes.compareTo(row, check.getRow()) != 0) {
        throw new HBaseIOException("The row mismatch. The check : " + check
            + " and expected row key is " + Bytes.toString(row));
      }
    }
  }

  public void addCheck(final Check check) throws HBaseIOException {
    checkList.add(check);
    if (Bytes.compareTo(row, check.getRow()) != 0) {
      throw new HBaseIOException("The row mismatch. The check : " + check
          + " and expected row key is " + Bytes.toString(row));
    }
  }

  @Override
  public boolean check(Result result) {
    for (Check check : checkList) {
      if (check.check(result)) {
        if (operator == Operator.MUST_PASS_ONE) {
          return true;
        }
      } else {
        if (operator == Operator.MUST_PASS_ALL) {
          return false;
        }
      }
    }
    return operator == Operator.MUST_PASS_ALL;
  }

  @Override
  public byte[] getRow() {
    return row;
  }

  @Override
  public Set<byte[]> getFamilies() {
    Set<byte[]> families = new HashSet<byte[]>();
    for (Check check : checkList) {
      families.addAll(check.getFamilies());
    }
    return families;
  }

  @Override
  public Map<byte[], NavigableSet<byte[]>> getFamilyMap() {
    Map<byte[], NavigableSet<byte[]>> familyMap =
        new TreeMap<byte[], NavigableSet<byte[]>>(Bytes.BYTES_COMPARATOR);
    for (Check check : checkList) {
      Map<byte[], NavigableSet<byte[]>> other = check.getFamilyMap();
      for (byte[] family : other.keySet()) {
        NavigableSet<byte[]> set = familyMap.get(family);
        if (set == null) {
          set = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
          familyMap.put(family, set);
        }
        set.addAll(other.get(family));
      }
    }
    return familyMap;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeByte(VERSION);
    Bytes.writeByteArray(out, this.row);
    out.writeByte(operator.ordinal());
    out.writeInt(checkList.size());
    for (Check check : checkList) {
      HbaseObjectWritable.writeObject(out, check, Writable.class, null);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    byte version = in.readByte();
    this.row = Bytes.readByteArray(in);
    byte opByte = in.readByte();
    this.operator = Operator.values()[opByte];
    int size = in.readInt();
    if (size > 0) {
      checkList = new ArrayList<Check>(size);
      for (int i = 0; i < size; i++) {
        Check check =
            (Check) HbaseObjectWritable.readObject(in, null);
        checkList.add(check);
      }
    }
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("Check row:" + Bytes.toString(row));
    sb.append(" operator:" + operator);
    sb.append(" qualifier: ").append(checkList.size());
    sb.append("{");
    for (Check check : checkList) {
      sb.append(check).append(", ");
    }
    sb.append(" }");
    return sb.toString();
  }
}
