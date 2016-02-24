package org.apache.hadoop.hbase.client;

import org.apache.hadoop.hbase.DoNotRetryIOException;

/**
 * Thrown when a scanner get a very large row and client do not allow partial
 * result.
 * When catch this exception, please use scan.setBatch or
 * scan.setAllowPartialResults(true) to allow scanner return
 * partial of a row as a result of next() to prevent your client OOM
 */
public class RowTooLargeException extends DoNotRetryIOException {

  RowTooLargeException(){
    super("This row is too large."
        +"Use scan.setAllowPartialResults(true) to prevent your client OOM");
  }
}
