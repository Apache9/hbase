package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.KeyValue;

/**
 * Scanner status, including whether the scanner is done and scanned key values counter.
 */
public class ScannerStatus {
  public static final ScannerStatus CONTINUED_WITH_NO_STATS = new ScannerStatus(true, null, 0);
  public static final ScannerStatus DONE_WITH_NO_STATS = new ScannerStatus(false, null, 0);

  private final boolean hasNext;
  private final KeyValue next;
  private final int rawValueScanned;

  public ScannerStatus(boolean hasNext, KeyValue next, int rawValueScanned) {
    this.hasNext = hasNext;
    this.next = next;
    this.rawValueScanned = rawValueScanned;
  }

  public static ScannerStatus done(int rawValueScanned) {
    return new ScannerStatus(false, null, rawValueScanned);
  }

  public static ScannerStatus continued(KeyValue next, int rawValueScanned) {
    return new ScannerStatus(true, next, rawValueScanned);
  }

  /**
   * Whether there are (possibly) more key values to scan
   */
  public boolean hasNext() {
    return hasNext;
  }

  /**
   * The next kv to scan, can be an invalid (e.g. filtered out or deleted) key value
   */
  public KeyValue next() {
    return next;
  }

  /**
   * The number of key values scanned
   */
  public int getRawValueScanned() {
    return rawValueScanned;
  }
}
