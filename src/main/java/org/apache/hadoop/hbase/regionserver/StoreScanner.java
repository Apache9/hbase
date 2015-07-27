/**
 * Copyright 2010 The Apache Software Foundation
 *
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

package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableSet;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.KVComparator;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.regionserver.ScanQueryMatcher.MatchCode;
import org.apache.hadoop.hbase.regionserver.Store.ScanInfo;
import org.apache.hadoop.hbase.regionserver.metrics.RegionMetricsStorage;
import org.apache.hadoop.hbase.regionserver.metrics.SchemaMetrics;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

/**
 * Scanner scans both the memstore and the HStore. Coalesce KeyValue stream
 * into List<KeyValue> for a single row.
 */
public class StoreScanner extends NonReversedNonLazyKeyValueScanner
    implements KeyValueScanner, InternalScanner, ChangedReadersObserver {
  static final Log LOG = LogFactory.getLog(StoreScanner.class);
  protected Store store;
  private ScanQueryMatcher matcher;
  protected KeyValueHeap heap;
  private boolean cacheBlocks;

  private String metricNamePrefix;
  // Used to indicate that the scanner has closed (see HBASE-1107)
  // Doesnt need to be volatile because it's always accessed via synchronized methods
  private boolean closing = false;
  private final boolean isGet;
  private final boolean explicitColumnQuery;
  private final boolean useRowColBloom;
  /**
   * A flag that enables StoreFileScanner parallel-seeking
   */
  protected boolean isParallelSeekEnabled = false;
  protected ExecutorService seekExecutor;
  protected final Scan scan;
  private final NavigableSet<byte[]> columns;
  private final long oldestUnexpiredTS;
  private final int minVersions;
  private int hugeKvWarningSizeInByte;
  private long hugeRowWarningSizeInByte;
  private long currentInResultRowSizeInByte;

  /** We don't ever expect to change this, the constant is just for clarity. */
  static final boolean LAZY_SEEK_ENABLED_BY_DEFAULT = true;
  public static final String STORESCANNER_PARALLEL_SEEK_ENABLE =
      "hbase.storescanner.parallel.seek.enable";

  /** Used during unit testing to ensure that lazy seek does save seek ops */
  private static boolean lazySeekEnabledGlobally =
      LAZY_SEEK_ENABLED_BY_DEFAULT;

  // if heap == null and lastTop != null, you need to reseek given the key below
  private KeyValue lastTop = null;

  // A flag whether use pread for scan
  private boolean scanUsePread = false;

  /** An internal constructor. */
  private StoreScanner(Store store, boolean cacheBlocks, Scan scan,
      final NavigableSet<byte[]> columns, long ttl, int minVersions) {
    this.store = store;
    this.cacheBlocks = cacheBlocks;
    isGet = scan.isGetScan();
    int numCol = columns == null ? 0 : columns.size();
    explicitColumnQuery = numCol > 0;
    this.scan = scan;
    this.columns = columns;
    oldestUnexpiredTS = EnvironmentEdgeManager.currentTimeMillis() - ttl;
    this.minVersions = minVersions;

    // We look up row-column Bloom filters for multi-column queries as part of
    // the seek operation. However, we also look the row-column Bloom filter
    // for multi-row (non-"get") scans because this is not done in
    // StoreFile.passesBloomFilter(Scan, SortedSet<byte[]>).
    useRowColBloom = numCol > 1 || (!isGet && numCol == 1);
		this.scanUsePread = scan.isSmall();
		hugeKvWarningSizeInByte = HConstants.HUGE_KV_SIZE_IN_BYTE_WARN_VALUE; // for testing

    // The parallel-seeking is on :
    // 1) the config value is *true*
    // 2) have more than one store file
    if (store != null && store.getHRegion() != null) {
      RegionServerServices rsService = store.getHRegion().getRegionServerServices();
      if (rsService == null || rsService.getConfiguration() == null) return;
      hugeKvWarningSizeInByte = rsService.getConfiguration().getInt(
        HConstants.HUGE_KV_SIZE_IN_BYTE_WARN_NAME, HConstants.HUGE_KV_SIZE_IN_BYTE_WARN_VALUE);
      hugeRowWarningSizeInByte = rsService.getConfiguration().getLong(
        HConstants.HUGE_ROW_SIZE_IN_BYTE_WARN_NAME, HConstants.HUGE_ROW_SIZE_IN_BYTE_WARN_VALUE);
      long maxResultSize = rsService.getConfiguration().getLong(
        HConstants.HBASE_CLIENT_SCANNER_MAX_RESULT_SIZE_KEY,
        HConstants.DEFAULT_HBASE_CLIENT_SCANNER_MAX_RESULT_SIZE);
      hugeRowWarningSizeInByte = hugeRowWarningSizeInByte <= maxResultSize ? hugeRowWarningSizeInByte
          : maxResultSize;
      if (store.getStorefilesCount() > 1) {
        boolean parallelSeekConfFlag = rsService.getConfiguration().getBoolean(
          STORESCANNER_PARALLEL_SEEK_ENABLE, false);
        if (!parallelSeekConfFlag || !(rsService instanceof HRegionServer)) return;
        isParallelSeekEnabled = true;
        seekExecutor = ((HRegionServer) rsService).getParallelSFSeekExecutor();
      }
    }
  }

  /**
   * Opens a scanner across memstore, snapshot, and all StoreFiles. Assumes we
   * are not in a compaction.
   *
   * @param store who we scan
   * @param scan the spec
   * @param columns which columns we are scanning
   * @throws IOException
   */
  public StoreScanner(Store store, ScanInfo scanInfo, Scan scan, final NavigableSet<byte[]> columns)
                              throws IOException {
    this(store, scan.getCacheBlocks(), scan, columns, scanInfo.getTtl(),
        scanInfo.getMinVersions());
    initializeMetricNames();
    if (columns != null && columns.size() > 0 && scan.isRaw()) {
      throw new DoNotRetryIOException(
          "Cannot specify any column for a raw scan");
    }
    matcher = new ScanQueryMatcher(scan, scanInfo, columns,
        ScanType.USER_SCAN, Long.MAX_VALUE, HConstants.LATEST_TIMESTAMP,
        oldestUnexpiredTS);

    // Pass columns to try to filter out unnecessary StoreFiles.
    List<KeyValueScanner> scanners = getScannersNoCompaction();

    seekScanners(scanners, matcher.getStartKey(), explicitColumnQuery
        && lazySeekEnabledGlobally, isParallelSeekEnabled);

    // Combine all seeked scanners with a heap
    resetKVHeap(scanners, store.getComparator());

    this.store.addChangedReaderObserver(this);
  }

  /**
   * Used for major compactions.<p>
   *
   * Opens a scanner across specified StoreFiles.
   * @param store who we scan
   * @param scan the spec
   * @param scanners ancillary scanners
   * @param smallestReadPoint the readPoint that we should use for tracking
   *          versions
   */
  public StoreScanner(Store store, ScanInfo scanInfo, Scan scan,
      List<? extends KeyValueScanner> scanners, ScanType scanType,
      long smallestReadPoint, long earliestPutTs) throws IOException {
    this(store, false, scan, null, scanInfo.getTtl(),
        scanInfo.getMinVersions());
    initializeMetricNames();
    matcher = new ScanQueryMatcher(scan, scanInfo, null, scanType,
        smallestReadPoint, earliestPutTs, oldestUnexpiredTS);

    // Filter the list of scanners using Bloom filters, time range, TTL, etc.
    scanners = selectScannersFrom(scanners);

    // Seek all scanners to the initial key
    seekScanners(scanners, matcher.getStartKey(), false, isParallelSeekEnabled);

    // Combine all seeked scanners with a heap
    resetKVHeap(scanners, store.getComparator());
  }

  /** Constructor for testing. */
  StoreScanner(final Scan scan, Store.ScanInfo scanInfo,
      ScanType scanType, final NavigableSet<byte[]> columns,
      final List<KeyValueScanner> scanners) throws IOException {
    this(scan, scanInfo, scanType, columns, scanners,
        HConstants.LATEST_TIMESTAMP);
  }

  // Constructor for testing.
  StoreScanner(final Scan scan, Store.ScanInfo scanInfo,
      ScanType scanType, final NavigableSet<byte[]> columns,
      final List<KeyValueScanner> scanners, long earliestPutTs)
          throws IOException {
    this(null, scan.getCacheBlocks(), scan, columns, scanInfo.getTtl(),
        scanInfo.getMinVersions());
    this.initializeMetricNames();
    this.matcher = new ScanQueryMatcher(scan, scanInfo, columns, scanType,
        Long.MAX_VALUE, earliestPutTs, oldestUnexpiredTS);

    // Seek all scanners to the initial key
    seekScanners(scanners, matcher.getStartKey(), false, isParallelSeekEnabled);
    resetKVHeap(scanners, scanInfo.getComparator());
  }

  /**
   * Method used internally to initialize metric names throughout the
   * constructors.
   *
   * To be called after the store variable has been initialized!
   */
  private void initializeMetricNames() {
    String tableName = SchemaMetrics.UNKNOWN;
    String family = SchemaMetrics.UNKNOWN;
    if (store != null) {
      tableName = store.getTableName();
      family = Bytes.toString(store.getFamily().getName());
    }
    this.metricNamePrefix =
        SchemaMetrics.generateSchemaMetricsPrefix(tableName, family);
  }

  /**
   * Seek the specified scanners with the given key
   * @param scanners
   * @param seekKey
   * @param isLazy true if using lazy seek
   * @throws IOException
   */
  protected void seekScanners(List<? extends KeyValueScanner> scanners,
      KeyValue seekKey, boolean isLazy, boolean isParallelSeek)
      throws IOException {
    // Seek all scanners to the start of the Row (or if the exact matching row
    // key does not exist, then to the start of the next matching Row).
    // Always check bloom filter to optimize the top row seek for delete
    // family marker.
    if (isLazy) {
      for (KeyValueScanner scanner : scanners) {
        scanner.requestSeek(seekKey, false, true);
      }
    } else {
      if (!isParallelSeek) {
        for (KeyValueScanner scanner : scanners) {
          scanner.seek(seekKey);
        }
      } else {
        parallelSeek(scanners, seekKey);
      }
    }
  }

  protected void resetKVHeap(List<? extends KeyValueScanner> scanners,
      KVComparator comparator) throws IOException {
    // Combine all seeked scanners with a heap
    heap = new KeyValueHeap(scanners, comparator);
  }

  /**
   * Get a filtered list of scanners. Assumes we are not in a compaction.
   * @return list of scanners to seek
   */
  private List<KeyValueScanner> getScannersNoCompaction() throws IOException {
    final boolean isCompaction = false;
    boolean usePread = isGet || scanUsePread;
    return selectScannersFrom(store.getScanners(cacheBlocks, usePread,
        isCompaction, matcher));
  }

  /**
   * Filters the given list of scanners using Bloom filter, time range, and
   * TTL.
   */
  private List<KeyValueScanner> selectScannersFrom(
      final List<? extends KeyValueScanner> allScanners) {
    boolean memOnly;
    boolean filesOnly;
    if (scan instanceof InternalScan) {
      InternalScan iscan = (InternalScan)scan;
      memOnly = iscan.isCheckOnlyMemStore();
      filesOnly = iscan.isCheckOnlyStoreFiles();
    } else {
      memOnly = false;
      filesOnly = false;
    }

    List<KeyValueScanner> scanners =
        new ArrayList<KeyValueScanner>(allScanners.size());

    // We can only exclude store files based on TTL if minVersions is set to 0.
    // Otherwise, we might have to return KVs that have technically expired.
    long expiredTimestampCutoff = minVersions == 0 ? oldestUnexpiredTS :
        Long.MIN_VALUE;

    // include only those scan files which pass all filters
    for (KeyValueScanner kvs : allScanners) {
      boolean isFile = kvs.isFileScanner();
      if ((!isFile && filesOnly) || (isFile && memOnly)) {
        continue;
      }

      if (kvs.shouldUseScanner(scan, columns, expiredTimestampCutoff)) {
        scanners.add(kvs);
      }
    }
    return scanners;
  }

  @Override
  public synchronized KeyValue peek() {
    if (this.heap == null) {
      return this.lastTop;
    }
    return this.heap.peek();
  }

  @Override
  public KeyValue next() {
    // throw runtime exception perhaps?
    throw new RuntimeException("Never call StoreScanner.next()");
  }

  @Override
  public synchronized void close() {
    if (this.closing) return;
    this.closing = true;
    // under test, we dont have a this.store
    if (this.store != null)
      this.store.deleteChangedReaderObserver(this);
    if (this.heap != null)
      this.heap.close();
    this.heap = null; // CLOSED!
    this.lastTop = null; // If both are null, we are closed.
  }

  @Override
  public synchronized boolean seek(KeyValue key) throws IOException {
    if (this.heap == null) {

      List<KeyValueScanner> scanners = getScannersNoCompaction();

      heap = new KeyValueHeap(scanners, store.comparator);
    }

    return this.heap.seek(key);
  }

  /**
   * Get the next row of values from this Store.
   * @param outResult
   * @param limit
   * @param rawLimit
   * @return scanner status
   */
  @Override
  public synchronized ScannerStatus next(List<KeyValue> outResult, int limit, int rawLimit)
      throws IOException {
    return next(outResult, limit, rawLimit, null);
  }

  /**
   * Get the next row of values from this Store.
   * @param outResult
   * @param limit
   * @param rawLimit
   * @return scanner status
   */
  @Override
  public synchronized ScannerStatus next(List<KeyValue> outResult, int limit, int rawLimit,
      String metric) throws IOException {

    if (checkReseek()) {
      return ScannerStatus.CONTINUED_WITH_NO_STATS;
    }

    // if the heap was left null, then the scanners had previously run out anyways, close and
    // return.
    if (this.heap == null) {
      close();
      return ScannerStatus.DONE_WITH_NO_STATS;
    }

    KeyValue peeked = this.heap.peek();
    if (peeked == null) {
      close();
      return ScannerStatus.DONE_WITH_NO_STATS;
    }
    if (scan.isDebug()) {
      LOG.info("Debug scan: peeked kv: " + peeked);
    }
    // only call setRow if the row changes; avoids confusing the query matcher
    // if scanning intra-row
    byte[] row = peeked.getBuffer();
    int offset = peeked.getRowOffset();
    short length = peeked.getRowLength();
    if (limit < 0 || matcher.row == null || !Bytes.equals(row, offset, length, matcher.row, matcher.rowOffset, matcher.rowLength)) {
      matcher.setRow(row, offset, length);
      currentInResultRowSizeInByte = 0;
    }

    KeyValue kv;
    KeyValue prevKV = null;

    // Only do a sanity-check if store and comparator are available.
    KeyValue.KVComparator comparator =
        store != null ? store.getComparator() : null;

    long cumulativeMetric = 0;
    int count = 0;
    int rawCount = 0;
    try {
      LOOP: while((kv = this.heap.peek()) != null) {
        checkScanOrder(prevKV, kv, comparator);
        prevKV = kv;
        ScanQueryMatcher.MatchCode qcode = matcher.match(kv);
        qcode = optimize(qcode, kv);
        if (scan.isDebug()) {
          LOG.info("Debug scan: current kv: " + kv + " match code: " + qcode);
        }
        switch(qcode) {
          case INCLUDE:
          case INCLUDE_AND_SEEK_NEXT_ROW:
          case INCLUDE_AND_SEEK_NEXT_COL:

            Filter f = matcher.getFilter();
            KeyValue resultKv = f == null ? kv : f.transform(kv);
            checkScanOrder(outResult, resultKv, comparator);
            checkKvSize(resultKv);
            outResult.add(resultKv);
            count++;

            if (qcode == ScanQueryMatcher.MatchCode.INCLUDE_AND_SEEK_NEXT_ROW) {
              if (!matcher.moreRowsMayExistAfter(kv)) {
                return ScannerStatus.done(rawCount);
              }
              seekToNextRow(kv);
              if (rawLimit > 0 && rawCount >= rawLimit) {
                return ScannerStatus.continued(this.heap.peek(), rawCount);
              }
            } else if (qcode == ScanQueryMatcher.MatchCode.INCLUDE_AND_SEEK_NEXT_COL) {
              seekAsDirection(matcher.getKeyForNextColumn(kv));
            } else {
              this.heap.next();
              ++rawCount;
            }

            cumulativeMetric += kv.getLength();
            if (limit > 0 && (count == limit)) {
              break LOOP;
            }
            continue;

          case DONE:
            return ScannerStatus.continued(this.heap.peek(), rawCount);

          case DONE_SCAN:
            close();

            return ScannerStatus.done(rawCount);

          case SEEK_NEXT_ROW:
            ++rawCount;
            // This is just a relatively simple end of scan fix, to short-cut end
            // us if there is an endKey in the scan.
            if (!matcher.moreRowsMayExistAfter(kv)) {
              return ScannerStatus.done(rawCount);
            }

            seekToNextRow(kv);
            if (rawLimit > 0 && rawCount >= rawLimit) {
              return ScannerStatus.continued(this.heap.peek(), rawCount);
            }
            break;

          case SEEK_NEXT_COL:
            ++rawCount;
            seekAsDirection(matcher.getKeyForNextColumn(kv));
            break;

          case SKIP:
            this.heap.next();
            ++rawCount;
            break;

          case SEEK_NEXT_USING_HINT:
            KeyValue nextKV = matcher.getNextKeyHint(kv);
            if (nextKV != null) {
              seekAsDirection(nextKV);
            } else {
              heap.next();
              ++rawCount;
            }
            break;

          default:
            throw new RuntimeException("UNEXPECTED");
        }
      }
    } finally {
      if (cumulativeMetric > 0 && metric != null) {
        RegionMetricsStorage.incrNumericMetric(this.metricNamePrefix + metric,
            cumulativeMetric);
      }
    }

    if (count > 0) {
      return ScannerStatus.continued(this.heap.peek(), rawCount);
    }

    // No more keys
    close();
    return ScannerStatus.done(rawCount);
  }

  /*
   * See if we should actually SEEK or rather just SKIP to the next kv.
   * (See HBASE-13109)
   */
  private ScanQueryMatcher.MatchCode optimize(ScanQueryMatcher.MatchCode qcode, KeyValue kv) {
    byte[] nextIndexedKey = getNextIndexedKey();
    if (nextIndexedKey == null || nextIndexedKey == HConstants.NO_NEXT_INDEXED_KEY ||
        store == null) {
      return qcode;
    }
    switch(qcode) {
    case INCLUDE_AND_SEEK_NEXT_COL:
    case SEEK_NEXT_COL:
    {
      if (matcher.compareKeyForNextColumn(nextIndexedKey, kv) >= 0) {
        return qcode == MatchCode.SEEK_NEXT_COL ? MatchCode.SKIP : MatchCode.INCLUDE;
      }
      break;
    }
    case INCLUDE_AND_SEEK_NEXT_ROW:
    case SEEK_NEXT_ROW:
    {
      if (matcher.compareKeyForNextRow(nextIndexedKey, kv) >= 0) {
        return qcode == MatchCode.SEEK_NEXT_ROW ? MatchCode.SKIP : MatchCode.INCLUDE;
      }
      break;
    }
    default:
      break;
    }
    return qcode;
  }

  @Override
  public synchronized ScannerStatus next(List<KeyValue> outResult) throws IOException {
    return next(outResult, -1, -1, null);
  }

  @Override
  public synchronized ScannerStatus next(List<KeyValue> outResult, String metric)
      throws IOException {
    return next(outResult, -1, -1, metric);
  }

  // Implementation of ChangedReadersObserver
  @Override
  public synchronized void updateReaders() throws IOException {
    if (this.closing) return;

    // All public synchronized API calls will call 'checkReseek' which will cause
    // the scanner stack to reseek if this.heap==null && this.lastTop != null.
    // But if two calls to updateReaders() happen without a 'next' or 'peek' then we
    // will end up calling this.peek() which would cause a reseek in the middle of a updateReaders
    // which is NOT what we want, not to mention could cause an NPE. So we early out here.
    if (this.heap == null) return;

    // this could be null.
    this.lastTop = this.peek();

    //DebugPrint.println("SS updateReaders, topKey = " + lastTop);

    // close scanners to old obsolete Store files
    this.heap.close(); // bubble thru and close all scanners.
    this.heap = null; // the re-seeks could be slow (access HDFS) free up memory ASAP

    // Let the next() call handle re-creating and seeking
  }

  /**
   * @return true if top of heap has changed (and KeyValueHeap has to try the
   *         next KV)
   * @throws IOException
   */
  protected boolean checkReseek() throws IOException {
    if (this.heap == null && this.lastTop != null) {
      resetScannerStack(this.lastTop);
      if (this.heap.peek() == null
          || store.comparator.compareRows(this.lastTop, this.heap.peek()) != 0) {
        LOG.debug("Storescanner.peek() is changed where before = "
            + this.lastTop.toString() + ",and after = " + this.heap.peek());
        this.lastTop = null;
        return true;
      }
      this.lastTop = null; // gone!
    }
    // else dont need to reseek
    return false;
  }

  private void resetScannerStack(KeyValue lastTopKey) throws IOException {
    if (heap != null) {
      throw new RuntimeException("StoreScanner.reseek run on an existing heap!");
    }

    /* When we have the scan object, should we not pass it to getScanners()
     * to get a limited set of scanners? We did so in the constructor and we
     * could have done it now by storing the scan object from the constructor */
    List<KeyValueScanner> scanners = getScannersNoCompaction();

    seekScanners(scanners, lastTopKey, false, isParallelSeekEnabled);

    // Combine all seeked scanners with a heap
    resetKVHeap(scanners, store.getComparator());

    // Reset the state of the Query Matcher and set to top row.
    // Only reset and call setRow if the row changes; avoids confusing the
    // query matcher if scanning intra-row.
    KeyValue kv = heap.peek();
    if (kv == null) {
      kv = lastTopKey;
    }
    byte[] row = kv.getBuffer();
    int offset = kv.getRowOffset();
    short length = kv.getRowLength();
    if ((matcher.row == null) || !Bytes.equals(row, offset, length, matcher.row, matcher.rowOffset, matcher.rowLength)) {
      matcher.reset();
      matcher.setRow(row, offset, length);
      currentInResultRowSizeInByte = 0;
    }
  }

  /**
   * Check whether scan as expected order
   * @param prevKV
   * @param kv
   * @param comparator
   * @throws IOException
   */
  protected void checkScanOrder(KeyValue prevKV, KeyValue kv,
      KeyValue.KVComparator comparator) throws IOException {
    // Check that the heap gives us KVs in an increasing order.
    assert prevKV == null || comparator == null
        || comparator.compare(prevKV, kv) <= 0 : "Key " + prevKV
        + " followed by a " + "smaller key " + kv + " in cf " + store;
  }

  protected void checkScanOrder(List<KeyValue> resultList, KeyValue kv,
      KeyValue.KVComparator comparator) throws IOException {
    //nop inside StoreScanner class
  }

  // Only be used while adding the kv into result currently
  protected void checkKvSize(KeyValue kv) {
    long size = kv.heapSize();
    if (kv.heapSize() > hugeKvWarningSizeInByte) {
      LOG.warn("adding a HUGE KV into result list, kv size:" + size + ", key:"
          + Bytes.toStringBinary(kv.getKey()));
    }
    currentInResultRowSizeInByte += size;
    if (currentInResultRowSizeInByte > hugeRowWarningSizeInByte) {
      LOG.warn("adding a HUGE ROW's kv into result list, added row size:"
          + currentInResultRowSizeInByte + ", key:" + Bytes.toStringBinary(kv.getKey()));
    }
  }

  protected synchronized boolean seekToNextRow(KeyValue kv) throws IOException {
    return reseek(matcher.getKeyForNextRow(kv));
  }

  /**
   * Do a reseek in a normal StoreScanner(scan forward)
   * @param kv
   * @return true if scanner has values left, false if end of scanner
   * @throws IOException
   */
  protected synchronized boolean seekAsDirection(KeyValue kv)
      throws IOException {
    return reseek(kv);
  }

  @Override
  public synchronized boolean reseek(KeyValue kv) throws IOException {
    //Heap will not be null, if this is called from next() which.
    //If called from RegionScanner.reseek(...) make sure the scanner
    //stack is reset if needed.
    checkReseek();
    if (explicitColumnQuery && lazySeekEnabledGlobally) {
      return heap.requestSeek(kv, true, useRowColBloom);
    } else {
      return heap.reseek(kv);
    }
  }

  @Override
  public long getSequenceID() {
    return 0;
  }

  /**
   * Seek storefiles in parallel to optimize IO latency as much as possible
   * @param scanners the list {@link KeyValueScanner}s to be read from
   * @param kv the KeyValue on which the operation is being requested
   * @throws IOException
   */
  private void parallelSeek(final List<? extends KeyValueScanner>
      scanners, final KeyValue kv) throws IOException {
    if (scanners.isEmpty()) return;
    int storeFileScannerCount = scanners.size();
    List<Future<Void>> futures = new ArrayList<Future<Void>>(storeFileScannerCount);
    for (KeyValueScanner scanner : scanners) {
      if (scanner instanceof StoreFileScanner) {
        Callable<Void> task = new ScannerSeekWorker(scanner, kv,
          MultiVersionConsistencyControl.getThreadReadPoint());
        futures.add(seekExecutor.submit(task));
      } else {
        scanner.seek(kv);
      }
    }
    try {
      for (Future<Void> future : futures) {
        future.get();
      }
    } catch (InterruptedException ie) {
      throw new InterruptedIOException(ie.getMessage());
    } catch (ExecutionException e) {
      throw new IOException(e.getMessage());
    } catch (CancellationException ce) {
      throw new IOException(ce.getMessage());
    }
  }

  private static class ScannerSeekWorker implements Callable<Void> {
    private KeyValueScanner scanner;
    private KeyValue keyValue;
    private long readPoint;

    public ScannerSeekWorker(KeyValueScanner scanner, KeyValue keyValue,
        long readPoint) {
      this.scanner = scanner;
      this.keyValue = keyValue;
      this.readPoint = readPoint;
    }

    public Void call() throws IOException {
      MultiVersionConsistencyControl.setThreadReadPoint(readPoint);
      scanner.seek(keyValue);
      return null;
    }
  }

  /**
   * Used in testing.
   * @return all scanners in no particular order
   */
  List<KeyValueScanner> getAllScannersForTesting() {
    List<KeyValueScanner> allScanners = new ArrayList<KeyValueScanner>();
    KeyValueScanner current = heap.getCurrentForTesting();
    if (current != null)
      allScanners.add(current);
    for (KeyValueScanner scanner : heap.getHeap())
      allScanners.add(scanner);
    return allScanners;
  }

  static void enableLazySeekGlobally(boolean enable) {
    lazySeekEnabledGlobally = enable;
  }

  @Override
  public byte[] getNextIndexedKey() {
    return this.heap.getNextIndexedKey();
  }
}

