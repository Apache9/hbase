package org.apache.hadoop.hbase.client;

import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Thrown when a scanner get a very large row and client do not allow partial
 * result.
 * When catch this exception, please use scan.setBatch or
 * scan.setAllowPartialResults(true) to allow scanner return
 * partial of a row as a result of next() to prevent your client OOM.
 * The default max size is 20% of the max heap size, where the ratio can be
 * configured by Scan.setMaxCompleteRowHeapRatio or Conf.
 */
public class RowTooLargeException extends DoNotRetryIOException {

  RowTooLargeException(byte[] rowKey, long rowSize) {
    super("This row is too large: " + Bytes.toString(rowKey)
        + " with size " + rowSize + ". "
        + "Use scan.setAllowPartialResults(true) to prevent your client OOM");
  }
}
