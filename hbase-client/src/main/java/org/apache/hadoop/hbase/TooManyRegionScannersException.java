package org.apache.hadoop.hbase;

/**
 * Thrown by a region server when there are too many opened region scanners on it.
 */
public class TooManyRegionScannersException extends HBaseIOException{
  private static final long serialVersionUID = 1L;

  public TooManyRegionScannersException(String s) {
    super(s);
  }
}
