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

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.ByteArrayComparable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Check Condition
 */
@InterfaceAudience.Private
public class Condition {
  private static final Logger LOG = LoggerFactory.getLogger(Condition.class);

  private byte[] row;
  private byte[] family;
  private byte[] qualifier;
  private CompareOperator compareOperator;
  private ByteArrayComparable comparator;

  // to debug
  private boolean failedMatch = false;
  private Result reason = new Result();

  public Condition(final byte[] row, final byte[] family, final byte[] qualifier,
      final CompareOperator compareOperator, ByteArrayComparable comparator) {
    this.row = row;
    this.family = family;
    this.qualifier = qualifier;
    this.compareOperator = compareOperator;
    this.comparator = comparator;
  }

  public Condition(final byte[] row, final byte[] family, final byte[] qualifier,
      final byte[] value) {
    this(row, family, qualifier, CompareOperator.EQUAL, new BinaryComparator(value));
  }

  public Condition(final byte[] row, final byte[] family, final byte[] qualifier,
      final CompareOperator compareOperator, final byte[] value) {
    this(row, family, qualifier, compareOperator, new BinaryComparator(value));
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

  public CompareOperator getCompareOp() {
    return compareOperator;
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

    Cell cell = result.getColumnLatestCell(family, qualifier);
    boolean rowIsNull = (result.size() == 0 || cell.getValueLength() == 0);

    boolean matches = false;
    if (rowIsNull || valueIsNull) {
      if (compareOperator.equals(CompareOperator.EQUAL)) {
        matches = (rowIsNull == valueIsNull);
      } else if (compareOperator.equals(CompareOperator.NOT_EQUAL)) {
        matches = (rowIsNull != valueIsNull);
      } else if (compareOperator.equals(CompareOperator.LESS) || compareOperator.equals(CompareOperator.LESS_OR_EQUAL)
          || compareOperator.equals(CompareOperator.GREATER) || compareOperator.equals(CompareOperator.GREATER_OR_EQUAL)) {
        LOG.warn("CompareOperator : " + compareOperator
            + " is not supportted when cell value or comparator value is null, actual rowIsNull : "
            + rowIsNull + ", valueIsNull : " + valueIsNull);
      } else {
        throw new RuntimeException("Unknown Compare op " + compareOperator.name());
      }
    } else {
      int compareResult =
          comparator.compareTo(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
      switch (compareOperator) {
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
        throw new RuntimeException("Unknown Compare op " + compareOperator.name());
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
    sb.append("Condition row:" + Bytes.toStringBinary(row));
    sb.append(" familiy:" + Bytes.toStringBinary(family));
    sb.append(" qualifier:" + Bytes.toStringBinary(qualifier));
    sb.append(" compareOperator:" + compareOperator);
    sb.append(" comparator:" + comparator);
    sb.append(". ");
    if (failedMatch) {
      sb.append("Condition failed. Reason row: " + reason);
    }
    return sb.toString();
  }
}
