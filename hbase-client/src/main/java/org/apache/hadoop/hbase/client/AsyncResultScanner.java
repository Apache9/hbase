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
import static org.apache.hadoop.hbase.client.ConnectionUtils.filterCells;

import com.google.common.base.Throwables;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.function.Function;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * The {@link ResultScanner} implementation for {@link AsyncTable}.
 * <p>
 * It will fetch data automatically in background and cache it in memory. If {@link #cacheSize} is
 * greater than {@link #maxCacheSize} we will stop the background fetch temporarily to avoid OOM.
 * And then if the {@link #cacheSize} is less than half of {@link #maxCacheSize}, we will resume the
 * background scan.
 * <p>
 * Typically the {@link #maxCacheSize} will be {@code 2 * scan.getMaxResultSize()}.
 */
@InterfaceAudience.Private
class AsyncResultScanner implements ResultScanner, ScanResultConsumer {

  private static final Log LOG = LogFactory.getLog(AsyncResultScanner.class);

  private final RawAsyncTable rawTable;

  private final Scan scan;

  private final long maxCacheSize;

  private final Queue<Result> queue = new ArrayDeque<>();

  private long cacheSize;

  private boolean closed = false;

  private Throwable error;

  private boolean prefetchStopped;

  private boolean ignoreOnComplete;

  // used to filter out cells that already returned when we restart a scan
  private Cell lastCell;

  private Function<byte[], byte[]> createClosestRow;

  public AsyncResultScanner(RawAsyncTable table, Scan scan, long maxCacheSize) {
    this.rawTable = table;
    this.scan = scan;
    this.maxCacheSize = maxCacheSize;
    this.createClosestRow = scan.isReversed() ? ConnectionUtils::createClosestRowBefore
        : ConnectionUtils::createClosestRowAfter;
    table.scan(scan, this);
  }

  private void addToCache(Result result) {
    queue.add(result);
    cacheSize += calcEstimatedSize(result);
  }

  private void stopPrefetch(Result lastResult) {
    prefetchStopped = true;
    if (lastResult.isPartial() || scan.getBatch() > 0) {
      scan.setStartRow(lastResult.getRow());
      lastCell = lastResult.rawCells()[lastResult.rawCells().length - 1];
    } else {
      scan.setStartRow(createClosestRow.apply(lastResult.getRow()));
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug(System.identityHashCode(this) + " stop prefetching when scanning "
          + rawTable.getName() + " as the cache size " + cacheSize
          + " is greater than the maxCacheSize + " + maxCacheSize + ", the next start row is "
          + Bytes.toStringBinary(scan.getStartRow()) + ", lastCell is " + lastCell);
    }
    // ignore the first onComplete call as the scan is stopped by us.
    ignoreOnComplete = true;
  }

  @Override
  public synchronized boolean onNext(Result[] results) {
    assert results.length > 0;
    if (closed) {
      return false;
    }
    Result firstResult = results[0];
    if (lastCell != null) {
      firstResult = filterCells(firstResult, lastCell);
      if (firstResult != null) {
        // do not set lastCell to null if the result after filtering is null as we may still need to
        lastCell = null;
        addToCache(firstResult);
      } else if (results.length == 1) {
        // the only one result is null
        return true;
      }
    } else {
      addToCache(firstResult);
    }
    for (int i = 1; i < results.length; i++) {
      addToCache(results[i]);
    }
    notifyAll();
    if (cacheSize < maxCacheSize) {
      return true;
    }
    stopPrefetch(results[results.length - 1]);
    return false;
  }

  @Override
  public synchronized boolean onHeartbeat() {
    return !closed;
  }

  @Override
  public synchronized void onError(Throwable error) {
    this.error = error;
  }

  @Override
  public synchronized void onComplete() {
    if (ignoreOnComplete) {
      // The scan is stopped by us due to cache size limit, do not set closed as we may resume the
      // scan later.
      ignoreOnComplete = false;
      return;
    }
    closed = true;
    notifyAll();
  }

  private void resumePrefetch() {
    if (LOG.isDebugEnabled()) {
      LOG.debug(System.identityHashCode(this) + " resume prefetching");
    }
    prefetchStopped = false;
    rawTable.scan(scan, this);
  }

  @Override
  public synchronized Result next() throws IOException {
    while (queue.isEmpty()) {
      if (closed) {
        return null;
      }
      if (error != null) {
        Throwables.propagateIfPossible(error, IOException.class);
        throw new IOException(error);
      }
      try {
        wait();
      } catch (InterruptedException e) {
        throw new InterruptedIOException();
      }
    }
    Result result = queue.poll();
    cacheSize -= calcEstimatedSize(result);
    if (prefetchStopped && cacheSize <= maxCacheSize / 2) {
      resumePrefetch();
    }
    return result;
  }

  @Override
  public synchronized void close() {
    closed = true;
    queue.clear();
    cacheSize = 0;
    notifyAll();
  }

  @Override
  public boolean renewLease() {
    return false;
  }
}
