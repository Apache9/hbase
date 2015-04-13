package org.apache.hadoop.hbase.mapreduce;

import com.google.common.base.Preconditions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

public class ProgressEstimator {
  static final Log LOG = LogFactory.getLog(ProgressEstimator.class);

  private static final int PRECISION_BYTES_NUM = 4;

  int commonPrefixLength = 0;
  long startValue = 0;
  long endValue = 0;

  public ProgressEstimator(byte[] startRow, byte[] endRow) {
    commonPrefixLength = getCommonPrefixLength(startRow, endRow);
    if (startRow != null && startRow.length != 0) {
      startValue = computeValue(startRow, commonPrefixLength);
    }
    endValue = computeValue(endRow, commonPrefixLength);
  }

  private int getCommonPrefixLength(byte[] startRow, byte[] endRow) {
    if (startRow == null || endRow == null) {
      return 0;
    }

    int i = 0;
    while (i < startRow.length && i < endRow.length) {
      if (startRow[i] != endRow[i]) {
        break;
      }
      ++i;
    }
    return i;
  }


  public float getProgress(ImmutableBytesWritable key) {
    if (key == null) {
      return 0;
    }

    if (endValue == startValue) {
      return 0;
    }

    long currentValue = computeValue(key.get(), key.getOffset(), key.getLength(), commonPrefixLength);
    Preconditions.checkArgument(currentValue >= startValue && currentValue <= endValue);
    return (float)(currentValue - startValue) / (endValue - startValue);
  }

  private long computeValue(byte[] bytes, int start) {
    if (bytes == null) {
      return maxValue();
    }

    return computeValue(bytes, 0, bytes.length, start);
  }

  private long computeValue(byte[] bytes, int offset, int len, int start) {
    if (bytes == null || bytes.length == 0) {
      return maxValue();
    }

    final int bitsPerByte = 8;
    long base = (1L << bitsPerByte);
    long value = 0;
    int i = offset + start;

    while (i < offset + len && i < offset + start + PRECISION_BYTES_NUM) {
      value = value * base + (bytes[i] & 0xFF);
      ++i;
    }

    while (i < offset + start + PRECISION_BYTES_NUM) {
      // fill with 0x00
      value = value * base;
      ++i;
    }
    return value;
  }

  private long maxValue() {
    final int bitsPerByte = 8;
    // all bytes filled with 0xff
    return (1L << (bitsPerByte * PRECISION_BYTES_NUM)) - 1;
  }
}
