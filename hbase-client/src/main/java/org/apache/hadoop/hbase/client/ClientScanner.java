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

import com.google.common.annotations.VisibleForTesting;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue.MetaComparator;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.UnknownScannerException;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.exceptions.OutOfOrderScannerNextException;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.MapReduceProtos;
import org.apache.hadoop.hbase.regionserver.RegionServerStoppedException;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Implements the scanner interface for the HBase client. If there are multiple regions in a table,
 * this scanner will iterate through them all.
 */
@InterfaceAudience.Private
public class ClientScanner extends AbstractClientScanner {

  private static final Log LOG = LogFactory.getLog(ClientScanner.class);

  // A byte array in which all elements are the max byte, and it is used to
  // construct closest front row
  static byte[] MAX_BYTE_ARRAY = Bytes.createMaxByteArray(9);
  protected Scan scan;
  protected boolean closed = false;
  // Current region scanner is against. Gets cleared if current region goes
  // wonky: e.g. if it splits on us.
  protected HRegionInfo currentRegion = null;

  protected ScannerCallable callable = null;

  protected final LinkedList<Result> cache = new LinkedList<Result>();
  /**
   * A list of partial results that have been returned from the server. This list should only
   * contain results if this scanner does not have enough partial results to form the complete
   * result.
   */
  protected final LinkedList<Result> partialResults = new LinkedList<Result>();

  /**
   * The row for which we are accumulating partial Results (i.e. the row of the Results stored
   * inside partialResults). Changes to partialResultsRow and partialResults are kept in sync via
   * the methods {@link #addToPartialResults(Result)} and {@link #clearPartialResults()}
   */
  protected byte[] partialResultsRow = null;

  /**
   * The last cell from a not full Row which is added to cache
   */
  protected Cell lastCellLoadedToCache = null;
  protected final int caching;
  protected long lastNext;
  // Keep lastResult returned successfully in case we have to reset scanner.
  protected Result lastResult = null;
  protected final long maxScannerResultSize;
  private final HConnection connection;
  private final TableName tableName;
  protected final int scannerTimeout;
  protected boolean scanMetricsPublished = false;
  protected RpcRetryingCaller<Result[]> caller;
  protected Configuration conf;

  /**
   * Create a new ClientScanner for the specified table Note that the passed {@link Scan}'s start
   * row maybe changed changed.
   * @param conf The {@link Configuration} to use.
   * @param scan {@link Scan} to use in this scanner
   * @param tableName The table that we wish to scan
   * @param connection Connection identifying the cluster
   * @throws IOException
   */
  public ClientScanner(final Configuration conf, final Scan scan, final TableName tableName,
      HConnection connection) throws IOException {
    if (LOG.isTraceEnabled()) {
      LOG.trace(
        "Scan table=" + tableName + ", startRow=" + Bytes.toStringBinary(scan.getStartRow()));
    }
    this.scan = scan;
    this.tableName = tableName;
    this.lastNext = System.currentTimeMillis();
    this.connection = connection;
    if (scan.getMaxResultSize() > 0) {
      this.maxScannerResultSize = scan.getMaxResultSize();
    } else {
      this.maxScannerResultSize = conf.getLong(HConstants.HBASE_CLIENT_SCANNER_MAX_RESULT_SIZE_KEY,
        HConstants.DEFAULT_HBASE_CLIENT_SCANNER_MAX_RESULT_SIZE);
    }
    this.scannerTimeout =
        HBaseConfiguration.getInt(conf, HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD,
          HConstants.HBASE_REGIONSERVER_LEASE_PERIOD_KEY,
          HConstants.DEFAULT_HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD);

    // check if application wants to collect scan metrics
    initScanMetrics(scan);

    // Use the caching from the Scan. If not set, use the default cache setting for this table.
    if (this.scan.getCaching() > 0) {
      this.caching = this.scan.getCaching();
    } else {
      this.caching = conf.getInt(HConstants.HBASE_CLIENT_SCANNER_CACHING,
        HConstants.DEFAULT_HBASE_CLIENT_SCANNER_CACHING);
    }

    this.caller = connection.getRpcRetryingCallerFactory().newCaller();

    initializeScannerInConstruction();
  }

  protected void initializeScannerInConstruction() throws IOException {
    // initialize the scanner
    nextScanner(this.caching, false);
  }

  protected HConnection getConnection() {
    return this.connection;
  }

  /**
   * @return Table name
   * @deprecated Since 0.96.0; use {@link #getTable()}
   */
  @Deprecated
  protected byte[] getTableName() {
    return this.tableName.getName();
  }

  protected TableName getTable() {
    return this.tableName;
  }

  protected Scan getScan() {
    return scan;
  }

  protected long getTimestamp() {
    return lastNext;
  }

  // returns true if the passed region endKey
  protected boolean checkScanStopRow(final byte[] endKey) {
    if (this.scan.getStopRow().length > 0) {
      // there is a stop row, check to see if we are past it.
      byte[] stopRow = scan.getStopRow();
      int cmp = Bytes.compareTo(stopRow, 0, stopRow.length, endKey, 0, endKey.length);
      if (cmp <= 0) {
        // stopRow <= endKey (endKey is equals to or larger than stopRow)
        // This is a stop.
        return true;
      }
    }
    return false; // unlikely.
  }

  /*
   * Gets a scanner for the next region. If this.currentRegion != null, then we will move to the
   * endrow of this.currentRegion. Else we will get scanner at the scan.getStartRow(). We will go no
   * further, just tidy up outstanding scanners, if <code>currentRegion != null</code> and
   * <code>done</code> is true.
   * @param nbRows
   * @param done Server-side says we're done scanning.
   */
  protected boolean nextScanner(int nbRows, final boolean done) throws IOException {
    // Close the previous scanner if it's open
    if (this.callable != null) {
      this.callable.setClose();
      call(callable, caller, scannerTimeout);
      this.callable = null;
    }

    // Where to start the next scanner
    byte[] localStartKey;

    // if we're at end of table, close and return false to stop iterating
    if (this.currentRegion != null) {
      byte[] endKey = this.currentRegion.getEndKey();
      if (endKey == null || Bytes.equals(endKey, HConstants.EMPTY_BYTE_ARRAY)
          || checkScanStopRow(endKey) || done) {
        close();
        if (LOG.isTraceEnabled()) {
          LOG.trace("Finished " + this.currentRegion);
        }
        return false;
      }
      localStartKey = endKey;
      // clear mvcc read point if we are going to switch regions
      scan.resetMvccReadPoint();
      if (LOG.isTraceEnabled()) {
        LOG.trace("Finished " + this.currentRegion);
      }
    } else {
      localStartKey = this.scan.getStartRow();
    }

    if (LOG.isDebugEnabled() && this.currentRegion != null) {
      // Only worth logging if NOT first region in scan.
      LOG.debug(
        "Advancing internal scanner to startKey at '" + Bytes.toStringBinary(localStartKey) + "'");
    }
    try {
      callable = getScannerCallable(localStartKey, nbRows);
      // Open a scanner on the region server starting at the
      // beginning of the region
      call(callable, caller, scannerTimeout);
      this.currentRegion = callable.getHRegionInfo();
      if (this.scanMetrics != null) {
        this.scanMetrics.countOfRegions.incrementAndGet();
      }
    } catch (IOException e) {
      close();
      throw e;
    }
    return true;
  }

  private Result[] call(ScannerCallable callable, RpcRetryingCaller<Result[]> caller,
      int scannerTimeout) throws IOException {
    if (Thread.interrupted()) {
      throw new InterruptedIOException();
    }
    // TODO: Ignore the scannerTimeout here to keep the old behavior
    return caller.callWithRetries(callable);
  }

  @InterfaceAudience.Private
  protected ScannerCallable getScannerCallable(byte[] localStartKey, int nbRows) {
    scan.setStartRow(localStartKey);
    ScannerCallable s =
        new ScannerCallable(getConnection(), getTable(), scan, this.scanMetrics, scannerTimeout);
    s.setCaching(nbRows);
    return s;
  }

  /**
   * Publish the scan metrics. For now, we use scan.setAttribute to pass the metrics back to the
   * application or TableInputFormat.Later, we could push it to other systems. We don't use metrics
   * framework because it doesn't support multi-instances of the same metrics on the same machine;
   * for scan/map reduce scenarios, we will have multiple scans running at the same time.
   * <p/>
   * By default, scan metrics are disabled; if the application wants to collect them, this behavior
   * can be turned on by calling calling:
   * <p/>
   * scan.setAttribute(SCAN_ATTRIBUTES_METRICS_ENABLE, Bytes.toBytes(Boolean.TRUE))
   */
  protected void writeScanMetrics() {
    if (this.scanMetrics == null || scanMetricsPublished) {
      return;
    }
    MapReduceProtos.ScanMetrics pScanMetrics = ProtobufUtil.toScanMetrics(scanMetrics);
    scan.setAttribute(Scan.SCAN_ATTRIBUTES_METRICS_DATA, pScanMetrics.toByteArray());
    scanMetricsPublished = true;
  }

  @Override
  public Result next() throws IOException {
    if (cache.size() == 0 && this.closed) {
      return null;
    }
    if (cache.size() == 0) {
      loadCache();
    }
    if (cache.size() > 0) {
      return cache.poll();
    }
    // if we exhausted this scanner before calling close, write out the scan metrics
    writeScanMetrics();
    return null;
  }

  protected void loadCache() throws IOException {
    Result[] values = null;
    long remainingResultSize = maxScannerResultSize;
    int countdown = this.caching;
    // We need to reset it if it's a new callable that was created
    // with a countdown in nextScanner
    callable.setCaching(this.caching);
    boolean retryAfterOutOfOrderException = true;
    // We don't expect that the server will have more results for us if
    // it doesn't tell us otherwise. We rely on the size or count of results
    boolean serverHasMoreResults = false;
    // A flag to make sure we must scan this region in next rpc right now.
    boolean continueScanInCurrentRegion = false;
    do {
      continueScanInCurrentRegion = false;
      try {

        // Server returns a null values if scanning is to stop. Else,
        // returns an empty array if scanning is to go on and we've just
        // exhausted current region.
        values = call(callable, caller, scannerTimeout);

        retryAfterOutOfOrderException = true;
      } catch (DoNotRetryIOException e) {
        // An exception was thrown which makes any partial results that we were collecting
        // invalid. The scanner will need to be reset to the beginning of a row.
        clearPartialResults();

        // DNRIOEs are thrown to make us break out of retries. Some types of DNRIOEs want us
        // to reset the scanner and come back in again.
        // If exception is any but the list below throw it back to the client; else setup
        // the scanner and retry.
        Throwable cause = e.getCause();
        if ((cause != null && cause instanceof NotServingRegionException)
            || (cause != null && cause instanceof RegionServerStoppedException)
            || e instanceof OutOfOrderScannerNextException
            || e instanceof UnknownScannerException) {
          // Pass. It is easier writing the if loop test as list of what is allowed rather than
          // as a list of what is not allowed... so if in here, it means we do not throw.
        } else {
          throw e;
        }
        // Else, its signal from depths of ScannerCallable that we need to reset the scanner.
        if (this.lastResult != null) {
          // The region has moved. We need to open a brand new scanner at
          // the new location.
          // Reset the startRow to the row we've seen last so that the new
          // scanner starts at the correct row. Otherwise we may see previously
          // returned rows again.
          // (ScannerCallable by now has "relocated" the correct region)
          if (!this.lastResult.isPartial() && scan.getBatch() < 0) {
            if (scan.isReversed()) {
              scan.setStartRow(createClosestRowBefore(lastResult.getRow()));
            } else {
              scan.setStartRow(Bytes.add(lastResult.getRow(), new byte[1]));
            }
          } else {
            // we need rescan this row because we only loaded partial row before
            scan.setStartRow(lastResult.getRow());
          }
        }
        if (e instanceof OutOfOrderScannerNextException) {
          if (retryAfterOutOfOrderException) {
            retryAfterOutOfOrderException = false;
          } else {
            // TODO: Why wrap this in a DNRIOE when it already is a DNRIOE?
            throw new DoNotRetryIOException("Failed after retry of "
                + "OutOfOrderScannerNextException: was there a rpc timeout?", e);
          }
        }
        // Clear region.
        this.currentRegion = null;
        // Set this to zero so we don't try and do an rpc and close on remote server when
        // the exception we got was UnknownScanner or the Server is going down.
        callable = null;
        // This continue will take us to while at end of loop where we will set up new scanner.
        continue;
      }
      long currentTime = System.currentTimeMillis();
      if (this.scanMetrics != null) {
        this.scanMetrics.sumOfMillisSecBetweenNexts.addAndGet(currentTime - lastNext);
      }
      lastNext = currentTime;
      if (this.lastCellLoadedToCache != null && values != null && values.length > 0
          && compare(this.lastCellLoadedToCache, values[0].rawCells()[0]) >= 0) {
        // If we will drop some results because we have loaded them to cache, we must continue to
        // scan this region in next rpc.
        // Set this flag to true to prevent doneWithRegion return true.
        continueScanInCurrentRegion = true;
      }
      // Groom the array of Results that we received back from the server before adding that
      // Results to the scanner's cache. If partial results are not allowed to be seen by the
      // caller, all book keeping will be performed within this method.
      List<Result> resultsToAddToCache =
          getResultsToAddToCache(values, callable.isHeartbeatMessage());
      for (Result rs : resultsToAddToCache) {
        rs = filterLoadedCell(rs);
        if (rs == null) {
          continue;
        }
        cache.add(rs);
        long estimatedHeapSizeOfResult = calcEstimatedSize(rs);
        countdown--;
        remainingResultSize -= estimatedHeapSizeOfResult;
        addEstimatedSize(estimatedHeapSizeOfResult);
        this.lastResult = rs;
        if (this.lastResult.isPartial() || scan.getBatch() > 0) {
          updateLastCellLoadedToCache(this.lastResult);
        } else {
          this.lastCellLoadedToCache = null;
        }
      }
      if (cache.isEmpty() && values != null && values.length > 0 && partialResults.isEmpty()) {
        // all result has been seen before, we need scan more.
        continueScanInCurrentRegion = true;
        continue;
      }

      if (callable.isHeartbeatMessage()) {
        if (cache.size() > 0) {
          // Caller of this method just wants a Result. If we see a heartbeat message, it means
          // processing of the scan is taking a long time server side. Rather than continue to
          // loop until a limit (e.g. size or caching) is reached, break out early to avoid causing
          // unnecesary delays to the caller
          if (LOG.isTraceEnabled()) {
            LOG.trace("Heartbeat message received and cache contains Results."
                + " Breaking out of scan loop");
          }
          break;
        }
        continueScanInCurrentRegion = true;
        continue;
      }

      // We expect that the server won't have more results for us when we exhaust
      // the size (bytes or count) of the results returned. If the server *does* inform us that
      // there are more results, we want to avoid possiblyNextScanner(...). Only when we actually
      // get results is the moreResults context valid.
      if (null != values && values.length > 0 && callable.hasMoreResultsContext()) {
        // Only adhere to more server results when we don't have any partialResults
        // as it keeps the outer loop logic the same.
        serverHasMoreResults = callable.getServerHasMoreResults() && partialResults.isEmpty();
      }

      // Values == null means server-side filter has determined we must STOP
      // !partialResults.isEmpty() means that we are still accumulating partial Results for a
      // row. We should not change scanners before we receive all the partial Results for that
      // row.
    } while (continueScanInCurrentRegion
        || (doneWithRegion(remainingResultSize, countdown, serverHasMoreResults)
            && (!partialResults.isEmpty() || nextScanner(countdown, values == null))));
  }

  /**
   * @param remainingResultSize
   * @param remainingRows
   * @param regionHasMoreResults
   * @return true when the current region has been exhausted. When the current region has been
   *         exhausted, the region must be changed before scanning can continue
   */
  private boolean doneWithRegion(long remainingResultSize, int remainingRows,
      boolean regionHasMoreResults) {
    return remainingResultSize > 0 && remainingRows > 0 && !regionHasMoreResults;
  }

  protected long calcEstimatedSize(Result rs) {
    long estimatedHeapSizeOfResult = 0;
    // We don't make Iterator here
    for (Cell cell : rs.rawCells()) {
      estimatedHeapSizeOfResult += CellUtil.estimatedSizeOf(cell);
    }
    return estimatedHeapSizeOfResult;
  }

  protected void addEstimatedSize(long estimatedHeapSizeOfResult) {
    return;
  }

  @VisibleForTesting
  public int getCacheSize() {
    return cache != null ? cache.size() : 0;
  }

  /**
   * This method ensures all of our book keeping regarding partial results is kept up to date. This
   * method should be called once we know that the results we received back from the RPC request do
   * not contain errors. We return a list of results that should be added to the cache. In general,
   * this list will contain all NON-partial results from the input array (unless the client has
   * specified that they are okay with receiving partial results)
   * @return the list of results that should be added to the cache.
   * @throws IOException
   */
  protected List<Result> getResultsToAddToCache(Result[] resultsFromServer,
      boolean heartbeatMessage) throws IOException {
    int resultSize = resultsFromServer != null ? resultsFromServer.length : 0;
    List<Result> resultsToAddToCache = new ArrayList<Result>(resultSize);

    final boolean isBatchSet = scan != null && scan.getBatch() > 0;
    final boolean allowPartials = scan != null && scan.getAllowPartialResults();

    // If the caller has indicated in their scan that they are okay with seeing partial results,
    // then simply add all results to the list. Note that since scan batching also returns results
    // for a row in pieces we treat batch being set as equivalent to allowing partials. The
    // implication of treating batching as equivalent to partial results is that it is possible
    // the caller will receive a result back where the number of cells in the result is less than
    // the batch size even though it may not be the last group of cells for that row.
    if (allowPartials || isBatchSet) {
      addResultsToList(resultsToAddToCache, resultsFromServer, 0,
        (null == resultsFromServer ? 0 : resultsFromServer.length));
      return resultsToAddToCache;
    }

    // If no results were returned it indicates that either we have the all the partial results
    // necessary to construct the complete result or the server had to send a heartbeat message
    // to the client to keep the client-server connection alive
    if (resultsFromServer == null || resultsFromServer.length == 0) {
      // If this response was an empty heartbeat message, then we have not exhausted the region
      // and thus there may be more partials server side that still need to be added to the partial
      // list before we form the complete Result
      if (!partialResults.isEmpty() && !heartbeatMessage) {
        resultsToAddToCache.add(Result.createCompleteResult(partialResults));
        clearPartialResults();
      }

      return resultsToAddToCache;
    }

    // In every RPC response there should be at most a single partial result. Furthermore, if
    // there is a partial result, it is guaranteed to be in the last position of the array.
    Result last = resultsFromServer[resultsFromServer.length - 1];
    Result partial = last.isPartial() ? last : null;

    if (LOG.isTraceEnabled()) {
      StringBuilder sb = new StringBuilder();
      sb.append("number results from RPC: ").append(resultsFromServer.length).append(",");
      sb.append("partial != null: ").append(partial != null).append(",");
      sb.append("number of partials so far: ").append(partialResults.size());
      LOG.trace(sb.toString());
    }

    // There are three possibilities cases that can occur while handling partial results
    //
    // 1. (partial != null && partialResults.isEmpty())
    // This is the first partial result that we have received. It should be added to
    // the list of partialResults and await the next RPC request at which point another
    // portion of the complete result will be received
    //
    // 2. !partialResults.isEmpty()
    // Since our partialResults list is not empty it means that we have been accumulating partial
    // Results for a particular row. We cannot form the complete/whole Result for that row until
    // all partials for the row have been received. Thus we loop through all of the Results
    // returned from the server and determine whether or not all partial Results for the row have
    // been received. We know that we have received all of the partial Results for the row when:
    // i) We notice a row change in the Results
    // ii) We see a Result for the partial row that is NOT marked as a partial Result
    //
    // 3. (partial == null && partialResults.isEmpty())
    // Business as usual. We are not accumulating partial results and there wasn't a partial result
    // in the RPC response. This means that all of the results we received from the server are
    // complete and can be added directly to the cache
    if (partial != null && partialResults.isEmpty()) {
      addToPartialResults(partial);

      // Exclude the last result, it's a partial
      addResultsToList(resultsToAddToCache, resultsFromServer, 0, resultsFromServer.length - 1);
    } else if (!partialResults.isEmpty()) {
      for (int i = 0; i < resultsFromServer.length; i++) {
        Result result = resultsFromServer[i];

        // This result is from the same row as the partial Results. Add it to the list of partials
        // and check if it was the last partial Result for that row
        if (Bytes.equals(partialResultsRow, result.getRow())) {
          addToPartialResults(result);

          // If the result is not a partial, it is a signal to us that it is the last Result we
          // need to form the complete Result client-side
          if (!result.isPartial()) {
            resultsToAddToCache.add(Result.createCompleteResult(partialResults));
            clearPartialResults();
          }
        } else {
          // The row of this result differs from the row of the partial results we have received so
          // far. If our list of partials isn't empty, this is a signal to form the complete Result
          // since the row has now changed
          if (!partialResults.isEmpty()) {
            resultsToAddToCache.add(Result.createCompleteResult(partialResults));
            clearPartialResults();
          }

          // It's possible that in one response from the server we receive the final partial for
          // one row and receive a partial for a different row. Thus, make sure that all Results
          // are added to the proper list
          if (result.isPartial()) {
            addToPartialResults(result);
          } else {
            resultsToAddToCache.add(result);
          }
        }
      }
    } else { // partial == null && partialResults.isEmpty() -- business as usual
      addResultsToList(resultsToAddToCache, resultsFromServer, 0, resultsFromServer.length);
    }

    return resultsToAddToCache;
  }

  /**
   * A convenience method for adding a Result to our list of partials. This method ensure that only
   * Results that belong to the same row as the other partials can be added to the list.
   * @param result The result that we want to add to our list of partial Results
   * @throws IOException
   */
  private void addToPartialResults(final Result result) throws IOException {
    final byte[] row = result.getRow();
    if (partialResultsRow != null && !Bytes.equals(row, partialResultsRow)) {
      throw new IOException("Partial result row does not match. All partial results must come "
          + "from the same row. partialResultsRow: " + Bytes.toString(partialResultsRow) + "row: "
          + Bytes.toString(row));
    }
    partialResultsRow = row;
    partialResults.add(result);
  }

  /**
   * Convenience method for clearing the list of partials and resetting the partialResultsRow.
   */
  private void clearPartialResults() {
    partialResults.clear();
    partialResultsRow = null;
  }

  /**
   * Create the closest row before the specified row
   * @param row
   * @return a new byte array which is the closest front row of the specified one
   */
  protected static byte[] createClosestRowBefore(byte[] row) {
    if (row == null) {
      throw new IllegalArgumentException("The passed row is empty");
    }
    if (Bytes.equals(row, HConstants.EMPTY_BYTE_ARRAY)) {
      return MAX_BYTE_ARRAY;
    }
    if (row[row.length - 1] == 0) {
      return Arrays.copyOf(row, row.length - 1);
    } else {
      byte[] closestFrontRow = Arrays.copyOf(row, row.length);
      closestFrontRow[row.length - 1] = (byte) ((closestFrontRow[row.length - 1] & 0xff) - 1);
      closestFrontRow = Bytes.add(closestFrontRow, MAX_BYTE_ARRAY);
      return closestFrontRow;
    }
  }

  /**
   * Helper method for adding results between the indices [start, end) to the outputList
   * @param outputList the list that results will be added to
   * @param inputArray the array that results are taken from
   * @param start beginning index (inclusive)
   * @param end ending index (exclusive)
   */
  private void addResultsToList(List<Result> outputList, Result[] inputArray, int start, int end) {
    if (inputArray == null || start < 0 || end > inputArray.length) return;

    for (int i = start; i < end; i++) {
      outputList.add(inputArray[i]);
    }
  }

  @Override
  public void close() {
    if (!scanMetricsPublished) writeScanMetrics();
    if (callable != null) {
      callable.setClose();
      try {
        call(callable, caller, scannerTimeout);
      } catch (UnknownScannerException e) {
        // We used to catch this error, interpret, and rethrow. However, we
        // have since decided that it's not nice for a scanner's close to
        // throw exceptions. Chances are it was just due to lease time out.
      } catch (IOException e) {
        /* An exception other than UnknownScanner is unexpected. */
        LOG.warn("scanner failed to close. Exception follows: " + e);
      }
      callable = null;
    }
    closed = true;
  }

  protected void updateLastCellLoadedToCache(Result result) {
    if (result.rawCells().length == 0) {
      return;
    }
    this.lastCellLoadedToCache = result.rawCells()[result.rawCells().length - 1];
  }

  private static MetaComparator metaComparator = new MetaComparator();

  private int compare(Cell a, Cell b) {
    boolean isMeta = currentRegion != null && currentRegion.isMetaRegion();
    int r =
        isMeta
            ? metaComparator.compareRows(a.getRowArray(), a.getRowOffset(), a.getRowLength(),
              b.getRowArray(), b.getRowOffset(), b.getRowLength())
            : CellComparator.compareRows(a, b);
    if (r != 0) {
      return this.scan.isReversed() ? -r : r;
    }
    return isMeta ? metaComparator.compare(a, b) : CellComparator.compareWithoutRow(a, b, true);
  }

  private Result filterLoadedCell(Result result) {
    // we only filter result when last result is partial
    // so lastCellLoadedToCache and result should have same row key.
    // However, if 1) read some cells; 1.1) delete this row at the same time 2) move region;
    // 3) read more cell. lastCellLoadedToCache and result will be not at same row.
    if (lastCellLoadedToCache == null || result.rawCells().length == 0) {
      return result;
    }
    if (compare(this.lastCellLoadedToCache, result.rawCells()[0]) < 0) {
      // The first cell of this result is larger than the last cell of loadcache.
      // If user do not allow partial result, it must be true.
      return result;
    }
    if (compare(this.lastCellLoadedToCache, result.rawCells()[result.rawCells().length - 1]) >= 0) {
      // The last cell of this result is smaller than the last cell of loadcache, skip all.
      return null;
    }

    int index = 0;
    while (index < result.rawCells().length) {
      if (compare(this.lastCellLoadedToCache, result.rawCells()[index]) < 0) {
        break;
      }
      index++;
    }
    List<Cell> list = new ArrayList<Cell>(result.rawCells().length - index);
    for (; index < result.rawCells().length; index++) {
      list.add(result.rawCells()[index]);
    }
    // We mark this partial to be a flag that part of cells dropped
    return Result.create(list, result.getExists(), result.isStale(), true);
  }

  /**
   * Forms a single result from the partial results in the partialResults list. This method is
   * useful for reconstructing partial results on the client side.
   * @param partialResults list of partial cells
   * @return The complete result that is formed by combining all of the partial results together
   * @throws IOException A complete result cannot be formed because the results in the partial list
   *           come from different rows
   */
  @VisibleForTesting
  public static Result createCompleteResult(List<Cell[]> partialResults, boolean stale, int count)
      throws IOException {
    if (partialResults.size() == 1) {
      // fast-forward if we need not merge Cell arrays
      return Result.create(partialResults.get(0), null, stale);
    }
    Cell[] array = new Cell[count];
    int index = 0;
    for (Cell[] cells : partialResults) {
      System.arraycopy(cells, 0, array, index, cells.length);
      index += cells.length;
    }
    return Result.create(array, null, stale);
  }

  /**
   * Forms a group of batched results. This method will change the list by LinkedList.poll(). And
   * may add the remaining cells to head if complete is false. If complete is false and the last
   * part is less than batch size, it'll addFirst to LinkedList with remaining cells.
   * @param complete true if they are last part of this row, false if there may be more
   */
  @VisibleForTesting
  public static List<Result> createBatchedResults(LinkedList<Cell[]> list, int batch, boolean stale,
      boolean complete) {
    int count = 0;
    Cell[] tmp = new Cell[batch];
    List<Result> results = new ArrayList<Result>();
    Cell[] cells;
    while ((cells = list.poll()) != null) {
      if (count == 0 && cells.length == batch) {
        // fast-forward if we need not merge Cell arrays
        results.add(Result.create(cells, null, stale));
      } else {
        if (count + cells.length <= batch) {
          System.arraycopy(cells, 0, tmp, count, cells.length);
          count += cells.length;
          if (count == batch) {
            results.add(Result.create(tmp, null, stale));
            count = 0;
            tmp = new Cell[batch];
          }
        } else {
          System.arraycopy(cells, 0, tmp, count, batch - count);
          results.add(Result.create(tmp, null, stale));
          tmp = new Cell[batch];
          int pos = batch - count;
          count = cells.length - pos;
          System.arraycopy(cells, pos, tmp, 0, count);
        }
      }
    }
    if (count > 0) {
      // count must less than batch here
      if (complete) {
        Cell[] tmp2 = Arrays.copyOf(tmp, count);
        results.add(Result.create(tmp2, null, stale));
      } else {
        Cell[] tmp2 = Arrays.copyOf(tmp, count);
        list.addFirst(tmp2);
      }
    }
    return results;
  }
}
