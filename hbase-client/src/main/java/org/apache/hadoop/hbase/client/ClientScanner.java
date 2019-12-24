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

import static org.apache.hadoop.hbase.client.ConnectionUtils.calcEstimatedSize;
import static org.apache.hadoop.hbase.client.ConnectionUtils.createScanResultCache;
import static org.apache.hadoop.hbase.client.ConnectionUtils.incRegionCountMetrics;

import com.google.common.annotations.VisibleForTesting;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.LinkedList;

import org.apache.commons.lang.mutable.MutableBoolean;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.UnknownScannerException;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.ScannerCallable.MoreResults;
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
public abstract class ClientScanner extends AbstractClientScanner {
  private static final Log LOG = LogFactory.getLog(ClientScanner.class);

  protected final Scan scan;
  protected boolean closed = false;
  // Current region scanner is against. Gets cleared if current region goes
  // wonky: e.g. if it splits on us.
  protected HRegionInfo currentRegion = null;

  protected ScannerCallable callable = null;

  protected final LinkedList<Result> cache = new LinkedList<Result>();

  private final ScanResultCache scanResultCache;
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
    this.conf = conf;
    this.scanResultCache = createScanResultCache(scan);
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

  @VisibleForTesting
  protected long getMaxResultSize() {
    return maxScannerResultSize;
  }

  private final void closeScanner() throws IOException {
    if (this.callable != null) {
      this.callable.setClose();
      call(callable, caller, scannerTimeout, false);
      this.callable = null;
    }
  }

  /**
   * Will be called in moveToNextRegion when currentRegion is null. Abstract because for normal
   * scan, we will start next scan from the endKey of the currentRegion, and for reversed scan, we
   * will start next scan from the startKey of the currentRegion.
   * @return {@code false} if we have reached the stop row. Otherwise {@code true}.
   */
  protected abstract boolean setNewStartKey();

  /**
   * Will be called in moveToNextRegion to create ScannerCallable. Abstract because for reversed
   * scan we need to create a ReversedScannerCallable.
   */
  protected abstract ScannerCallable createScannerCallable();

  /**
   * Close the previous scanner and create a new ScannerCallable for the next scanner.
   * <p>
   * Marked as protected only because TestClientScanner need to override this method.
   * @return false if we should terminate the scan. Otherwise
   */
  @VisibleForTesting
  protected boolean moveToNextRegion() {
    // Close the previous scanner if it's open
    try {
      closeScanner();
    } catch (IOException e) {
      // not a big deal continue
      if (LOG.isDebugEnabled()) {
        LOG.debug("close scanner for " + currentRegion + " failed", e);
      }
    }
    if (currentRegion != null) {
      if (!setNewStartKey()) {
        return false;
      }
      scan.resetMvccReadPoint();
      if (LOG.isTraceEnabled()) {
        LOG.trace("Finished " + this.currentRegion);
      }
    }
    if (LOG.isDebugEnabled() && this.currentRegion != null) {
      // Only worth logging if NOT first region in scan.
      LOG.debug(
        "Advancing internal scanner to startKey at '" + Bytes.toStringBinary(scan.getStartRow()) +
            "', " + (scan.includeStartRow() ? "inclusive" : "exclusive"));
    }
    // clear the current region, we will set a new value to it after the first call of the new
    // callable.
    this.currentRegion = null;
    this.callable = createScannerCallable();
    this.callable.setCaching(this.caching);
    incRegionCountMetrics(scanMetrics);
    return true;
  }

  private Result[] call(ScannerCallable callable, RpcRetryingCaller<Result[]> caller,
      int scannerTimeout, boolean updateCurrentRegion) throws IOException {
    if (Thread.interrupted()) {
      throw new InterruptedIOException();
    }
    // TODO: Ignore the scannerTimeout here to keep the old behavior
    Result[] rrs = caller.callWithRetries(callable);
    if (currentRegion == null && updateCurrentRegion) {
      currentRegion = callable.getHRegionInfo();
    }
    return rrs;
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

  private boolean scanExhausted(Result[] values) {
    return callable.moreResultsForScan() == MoreResults.NO;
  }

  private boolean regionExhausted(Result[] values) {
    // 1. Not a heartbeat message and we get nothing, this means the region is exhausted. And in the
    // old time we always return empty result for a open scanner operation so we add a check here to
    // keep compatible with the old logic. Should remove the isOpenScanner in the future.
    // 2. Server tells us that it has no more results for this region.
    return (values.length == 0 && !callable.isHeartbeatMessage()) ||
        callable.moreResultsInRegion() == MoreResults.NO;
  }

  private void closeScannerIfExhausted(boolean exhausted) throws IOException {
    if (exhausted) {
      closeScanner();
    }
  }

  private void handleScanError(DoNotRetryIOException e,
      MutableBoolean retryAfterOutOfOrderException) throws DoNotRetryIOException {
    // An exception was thrown which makes any partial results that we were collecting
    // invalid. The scanner will need to be reset to the beginning of a row.
    scanResultCache.clear();

    // Unfortunately, DNRIOE is used in two different semantics.
    // (1) The first is to close the client scanner and bubble up the exception all the way
    // to the application. This is preferred when the exception is really un-recoverable
    // (like CorruptHFileException, etc). Plain DoNotRetryIOException also falls into this
    // bucket usually.
    // (2) Second semantics is to close the current region scanner only, but continue the
    // client scanner by overriding the exception. This is usually UnknownScannerException,
    // OutOfOrderScannerNextException, etc where the region scanner has to be closed, but the
    // application-level ClientScanner has to continue without bubbling up the exception to
    // the client. See RSRpcServices to see how it throws DNRIOE's.
    // See also: HBASE-16604, HBASE-17187

    // If exception is any but the list below throw it back to the client; else setup
    // the scanner and retry.
    Throwable cause = e.getCause();
    if ((cause != null && cause instanceof NotServingRegionException) ||
        (cause != null && cause instanceof RegionServerStoppedException) ||
        e instanceof OutOfOrderScannerNextException || e instanceof UnknownScannerException) {
      // Pass. It is easier writing the if loop test as list of what is allowed rather than
      // as a list of what is not allowed... so if in here, it means we do not throw.
    } else {
      throw e;
    }

    // Else, its signal from depths of ScannerCallable that we need to reset the scanner.
    if (this.lastResult != null) {
      // The region has moved. We need to open a brand new scanner at the new location.
      // Reset the startRow to the row we've seen last so that the new scanner starts at
      // the correct row. Otherwise we may see previously returned rows again.
      // If the lastRow is not partial, then we should start from the next row. As now we can
      // exclude the start row, the logic here is the same for both normal scan and reversed scan.
      // If lastResult is partial then include it, otherwise exclude it.
      scan.withStartRow(lastResult.getRow(), lastResult.mayHaveMoreCellsInRow());
    }
    if (e instanceof OutOfOrderScannerNextException) {
      if (retryAfterOutOfOrderException.isTrue()) {
        retryAfterOutOfOrderException.setValue(false);
      } else {
        // TODO: Why wrap this in a DNRIOE when it already is a DNRIOE?
        throw new DoNotRetryIOException(
            "Failed after retry of OutOfOrderScannerNextException: was there a rpc timeout?", e);
      }
    }
    // Clear region.
    this.currentRegion = null;
    // Set this to zero so we don't try and do an rpc and close on remote server when
    // the exception we got was UnknownScanner or the Server is going down.
    callable = null;
  }

  /**
   * Contact the servers to load more {@link Result}s in the cache.
   */
  protected void loadCache() throws IOException {
    // check if scanner was closed during previous prefetch
    if (closed) {
      return;
    }
    long remainingResultSize = maxScannerResultSize;
    int countdown = this.caching;
    // This is possible if we just stopped at the boundary of a region in the previous call.
    if (callable == null) {
      if (!moveToNextRegion()) {
        return;
      }
    }
    // This flag is set when we want to skip the result returned. We do
    // this when we reset scanner because it split under us.
    MutableBoolean retryAfterOutOfOrderException = new MutableBoolean(true);
    for (;;) {
      Result[] values;
      try {
        // Server returns a null values if scanning is to stop. Else,
        // returns an empty array if scanning is to go on and we've just
        // exhausted current region.
        // now we will also fetch data when openScanner, so do not make a next call again if values
        // is already non-null.
        values = call(callable, caller, scannerTimeout, true);
        retryAfterOutOfOrderException.setValue(true);
      } catch (DoNotRetryIOException e) {
        handleScanError(e, retryAfterOutOfOrderException);
        // reopen the scanner
        if (!moveToNextRegion()) {
          break;
        }
        continue;
      }
      long currentTime = System.currentTimeMillis();
      if (this.scanMetrics != null) {
        this.scanMetrics.sumOfMillisSecBetweenNexts.addAndGet(currentTime - lastNext);
      }
      lastNext = currentTime;
      // Groom the array of Results that we received back from the server before adding that
      // Results to the scanner's cache. If partial results are not allowed to be seen by the
      // caller, all book keeping will be performed within this method.
      int numberOfCompleteRowsBefore = scanResultCache.numberOfCompleteRows();
      Result[] resultsToAddToCache =
          scanResultCache.addAndGet(values, callable.isHeartbeatMessage());
      int numberOfCompleteRows =
          scanResultCache.numberOfCompleteRows() - numberOfCompleteRowsBefore;
      if (resultsToAddToCache.length > 0) {
        for (Result rs : resultsToAddToCache) {
          cache.add(rs);
          remainingResultSize -= calcEstimatedSize(rs);
          countdown--;
          this.lastResult = rs;
        }
      }

      if (scan.getLimit() > 0) {
        int newLimit = scan.getLimit() - numberOfCompleteRows;
        assert newLimit >= 0;
        scan.setLimit(newLimit);
      }
      if (scanExhausted(values)) {
        closeScanner();
        closed = true;
        break;
      }
      boolean regionExhausted = regionExhausted(values);
      if (callable.isHeartbeatMessage()) {
        if (cache.size() > 0) {
          // Caller of this method just wants a Result. If we see a heartbeat message, it means
          // processing of the scan is taking a long time server side. Rather than continue to
          // loop until a limit (e.g. size or caching) is reached, break out early to avoid causing
          // unnecesary delays to the caller
          if (LOG.isTraceEnabled()) {
            LOG.trace("Heartbeat message received and cache contains Results." +
                " Breaking out of scan loop");
          }
          // we know that the region has not been exhausted yet so just break without calling
          // closeScannerIfExhausted
          break;
        }
      }
      if (cache.isEmpty() && !closed && scan.isNeedCursorResult()) {
        if (callable.isHeartbeatMessage() && callable.getCursor() != null) {
          // Use cursor row key from server
          cache.add(Result.createCursorResult(callable.getCursor()));
          break;
        }
        if (values.length > 0) {
          // It is size limit exceed and we need return the last Result's row.
          // When user setBatch and the scanner is reopened, the server may return Results that
          // user has seen and the last Result can not be seen because the number is not enough.
          // So the row keys of results may not be same, we must use the last one.
          cache.add(Result.createCursorResult(new Cursor(values[values.length - 1].getRow())));
          break;
        }
      }
      if (countdown <= 0) {
        // we have enough result.
        closeScannerIfExhausted(regionExhausted);
        break;
      }
      if (remainingResultSize <= 0) {
        if (!cache.isEmpty()) {
          closeScannerIfExhausted(regionExhausted);
          break;
        }
        continue;
      }
      // we are done with the current region
      if (regionExhausted) {
        if (!moveToNextRegion()) {
          break;
        }
      }
    }
  }

  @Override
  public void close() {
    if (!scanMetricsPublished) writeScanMetrics();
    if (callable != null) {
      callable.setClose();
      try {
        call(callable, caller, scannerTimeout, false);
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

  @Override
  public boolean renewLease() {
    if (callable != null) {
      // do not return any rows, do not advance the scanner
      callable.setCaching(0);
      try {
        this.caller.callWithoutRetries(callable, this.scannerTimeout);
      } catch (Exception e) {
        return false;
      } finally {
        callable.setCaching(this.caching);
      }
      return true;
    }
    return false;
  }
}
