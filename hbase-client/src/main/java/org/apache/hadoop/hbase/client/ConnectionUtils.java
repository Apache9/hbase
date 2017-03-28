/**
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

import static org.apache.hadoop.hbase.HConstants.EMPTY_END_ROW;
import static org.apache.hadoop.hbase.HConstants.EMPTY_START_ROW;

import java.util.Arrays;
import java.util.Comparator;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Utility used by client connections.
 */
@InterfaceAudience.Private
public class ConnectionUtils {

  /**
   * Calculate pause time. Built on {@link HConstants#RETRY_BACKOFF}.
   * @param pause
   * @param tries
   * @return How long to wait after <code>tries</code> retries
   */
  public static long getPauseTime(final long pause, final int tries) {
    int ntries = tries;
    if (ntries >= HConstants.RETRY_BACKOFF.length) {
      ntries = HConstants.RETRY_BACKOFF.length - 1;
    }
    if (ntries < 0) {
      ntries = 0;
    }

    long normalPause = pause * HConstants.RETRY_BACKOFF[ntries];
    // 1% possible jitter
    long jitter = (long) (normalPause * ThreadLocalRandom.current().nextFloat() * 0.01f);
    return normalPause + jitter;
  }

  /**
   * Adds / subs a 10% jitter to a pause time. Minimum is 1.
   * @param pause the expected pause.
   * @param jitter the jitter ratio, between 0 and 1, exclusive.
   */
  public static long addJitter(final long pause, final float jitter) {
    float lag = pause * (ThreadLocalRandom.current().nextFloat() - 0.5f) * jitter;
    long newPause = pause + (long) lag;
    if (newPause <= 0) {
      return 1;
    }
    return newPause;
  }

  /**
   * Changes the configuration to set the number of retries needed when using HConnection
   * internally, e.g. for  updating catalog tables, etc.
   * Call this method before we create any Connections.
   * @param c The Configuration instance to set the retries into.
   * @param log Used to log what we set in here.
   */
  public static void setServerSideHConnectionRetriesConfig(
      final Configuration c, final String sn, final Log log) {
    // TODO: Fix this. Not all connections from server side should have 10 times the retries.
    int hcRetries = c.getInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER,
      HConstants.DEFAULT_HBASE_CLIENT_RETRIES_NUMBER);
    // Go big.  Multiply by 10.  If we can't get to meta after this many retries
    // then something seriously wrong.
    int serversideMultiplier = c.getInt("hbase.client.serverside.retries.multiplier", 10);
    int retries = hcRetries * serversideMultiplier;
    c.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, retries);
    log.info(sn + " server-side HConnection retries=" + retries);
  }

  // A byte array in which all elements are the max byte, and it is used to
  // construct closest front row
  static final byte[] MAX_BYTE_ARRAY = Bytes.createMaxByteArray(9);

  /**
   * Create the closest row after the specified row
   */
  static byte[] createClosestRowAfter(byte[] row) {
    return Arrays.copyOf(row, row.length + 1);
  }

  /**
   * Create a row before the specified row and very close to the specified row.
   */
  static byte[] createCloseRowBefore(byte[] row) {
    if (row.length == 0) {
      return MAX_BYTE_ARRAY;
    }
    if (row[row.length - 1] == 0) {
      return Arrays.copyOf(row, row.length - 1);
    } else {
      byte[] nextRow = new byte[row.length + MAX_BYTE_ARRAY.length];
      System.arraycopy(row, 0, nextRow, 0, row.length - 1);
      nextRow[row.length - 1] = (byte) ((row[row.length - 1] & 0xFF) - 1);
      System.arraycopy(MAX_BYTE_ARRAY, 0, nextRow, row.length, MAX_BYTE_ARRAY.length);
      return nextRow;
    }
  }

  static boolean isEmptyStartRow(byte[] row) {
    return Bytes.equals(row, EMPTY_START_ROW);
  }

  static boolean isEmptyStopRow(byte[] row) {
    return Bytes.equals(row, EMPTY_END_ROW);
  }

  private static final Comparator<Cell> COMPARE_WITHOUT_ROW = new Comparator<Cell>() {

    @Override
    public int compare(Cell o1, Cell o2) {
      return CellComparator.compareWithoutRow(o1, o2, true);
    }
  };

  static Result filterCells(Result result, Cell keepCellsAfter) {
    if (keepCellsAfter == null) {
      // do not need to filter
      return result;
    }
    // not the same row
    if (!CellUtil.matchingRow(keepCellsAfter, result.getRow(), 0, result.getRow().length)) {
      return result;
    }
    Cell[] rawCells = result.rawCells();
    int index = Arrays.binarySearch(rawCells, keepCellsAfter, COMPARE_WITHOUT_ROW);
    if (index < 0) {
      index = -index - 1;
    } else {
      index++;
    }
    if (index == 0) {
      return result;
    }
    if (index == rawCells.length) {
      return null;
    }
    return Result.create(Arrays.copyOfRange(rawCells, index, rawCells.length), null,
      result.isStale(), result.mayHaveMoreCellsInRow());
  }

  static boolean noMoreResultsForScan(Scan scan, HRegionInfo info) {
    if (isEmptyStopRow(info.getEndKey())) {
      return true;
    }
    if (isEmptyStopRow(scan.getStopRow())) {
      return false;
    }
    int c = Bytes.compareTo(info.getEndKey(), scan.getStopRow());
    // 1. if our stop row is less than the endKey of the region
    // 2. if our stop row is equal to the endKey of the region and we do not include the stop row
    // for scan.
    return c > 0 || (c == 0 && !scan.includeStopRow());
  }

  static boolean noMoreResultsForReverseScan(Scan scan, HRegionInfo info) {
    if (isEmptyStartRow(info.getStartKey())) {
      return true;
    }
    if (isEmptyStopRow(scan.getStopRow())) {
      return false;
    }
    // no need to test the inclusive of the stop row as the start key of a region is included in
    // the region.
    return Bytes.compareTo(info.getStartKey(), scan.getStopRow()) <= 0;
  }

  public static ScanResultCache createScanResultCache(Scan scan) {
    if (scan.getAllowPartialResults()) {
      return new AllowPartialScanResultCache();
    } else if (scan.getBatch() > 0) {
      return new BatchScanResultCache(scan.getBatch());
    } else {
      return new CompleteScanResultCache();
    }
  }
}
