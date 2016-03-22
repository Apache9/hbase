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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Check Condition
 */

public class Condition {
  private static final Log LOG = LogFactory.getLog(Condition.class);

  private byte[] row;
  private byte[] family;
  private byte[] qualifier;
  private CompareOp compareOp;
  private ByteArrayComparable comparator;

  // to debug
  private boolean failedMatch = false;
  private Result reason = new Result();

  public Condition(final byte[] row, final byte[] family, final byte[] qualifier,
      final CompareOp compareOp, ByteArrayComparable comparator) {
    this.row = row;
    this.family = family;
    this.qualifier = qualifier;
    this.compareOp = compareOp;
    this.comparator = comparator;
  }

  public Condition(final byte[] row, final byte[] family, final byte[] qualifier,
      final byte[] value) {
    this(row, family, qualifier, CompareOp.EQUAL, new BinaryComparator(value));
  }

  public Condition(final byte[] row, final byte[] family, final byte[] qualifier,
      final CompareOp compareOp, final byte[] value) {
    this(row, family, qualifier, compareOp, new BinaryComparator(value));
  }

  public byte[] getRow() {
    return row;
  }

  public byte[] getFamily() {
    return family;
  }

  public byte[] getQualifier() {
    return qualifier;
  }

  public CompareOp getCompareOp() {
    return compareOp;
  }

  public ByteArrayComparable getComparator() {
    return comparator;
  }

  public boolean failedMatch() {
    return failedMatch;
  }

  public boolean isMatch(Result result) {
    // result.size() must be 0 or 1, except that the default max version
    // setting logic of Get changed.
    if (result.size() > 1) {
      throw new RuntimeException(
          "Result size of get in checkAndMutate must be 0 or 1, actual size:" + result.size());
    }

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
        matches = compareResult < 0;
        break;
      case LESS_OR_EQUAL:
        matches = compareResult <= 0;
        break;
      case EQUAL:
        matches = compareResult == 0;
        break;
      case NOT_EQUAL:
        matches = compareResult != 0;
        break;
      case GREATER_OR_EQUAL:
        matches = compareResult >= 0;
        break;
      case GREATER:
        matches = compareResult > 0;
        break;
      default:
        throw new RuntimeException("Unknown Compare op " + compareOp.name());
      }
    }
    if (!matches) {
      this.failedMatch  = true;
      this.reason = result;
    }
    return matches;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("Condition row:" + Bytes.toString(row));
    sb.append(" familiy:" + Bytes.toString(family));
    sb.append(" qualifier:" + Bytes.toString(qualifier));
    sb.append(" compareOp:" + compareOp);
    sb.append(" comparator:" + comparator);
    sb.append(". ");

    if (failedMatch) {
      sb.append("Condition failed. Reason row: " + reason);
    }
    return sb.toString();
  }
}
