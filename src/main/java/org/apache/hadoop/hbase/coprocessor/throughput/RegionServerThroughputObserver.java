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
package org.apache.hadoop.hbase.coprocessor.throughput;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.WritableByteArrayComparable;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Pair;


public class RegionServerThroughputObserver extends BaseRegionObserver{

  public static final String REGION_SERVER_THROUGHPUT_WINDOW_SIZE_IN_MS = "hbase.regionserver.qps.window.size.in.ms";
  public static final String REGION_SERVER_MAX_READ_QPS = "hbase.regionserver.qps.read.max";
  public static final String REGION_SERVER_MAX_WRITE_QPS = "hbase.regionserver.qps.write.max";

  public static final long DEFAULT_REGION_SERVER_MAX_READ_QPS = 10000;
  public static final long DEFAULT_REGION_SERVER_MAX_WRITE_QPS = 30000;
  public static final long DEFAULT_REGION_SERVER_THROUGHPUT_WINDOW_SIZE_IN_MS = 3000;


  private static volatile RegionServerThroughputWindow window;
  private static final Object lock = new Object();

  @Override
  public void start(CoprocessorEnvironment e) throws IOException {
    if(! (e instanceof RegionCoprocessorEnvironment)) {
      throw new IOException("RegionServerThroughputObserver start failed : e not an instance of RegionCoprocessorEnvironment.");
    }

    if(window == null) {
      synchronized (lock) {
        if(window == null) {
          long windowSize = e.getConfiguration().getLong(REGION_SERVER_THROUGHPUT_WINDOW_SIZE_IN_MS,
              DEFAULT_REGION_SERVER_THROUGHPUT_WINDOW_SIZE_IN_MS);

          long maxReadQps = e.getConfiguration().getLong(REGION_SERVER_MAX_READ_QPS, DEFAULT_REGION_SERVER_MAX_READ_QPS);
          long maxWriteQps = e.getConfiguration().getLong(REGION_SERVER_MAX_WRITE_QPS, DEFAULT_REGION_SERVER_MAX_WRITE_QPS);

          if(windowSize <= 0 || maxReadQps <= 0 || maxWriteQps <= 0) {
            throw new IOException("REGION_SERVER_THROUGHPUT_WINDOW_SIZE_IN_MS or REGION_SERVER_MAX_READ_QPS or REGION_SERVER_MAX_WRITE_QPS not configured, abort RegionServerThroughputWindow");
          }

          window = new RegionServerThroughputWindow(windowSize, maxReadQps, maxWriteQps);
        }
      }
    }

  }

  //Only be called when unit test
  public static void resetThroughputWindow(){
    if(window != null ) {
      window.reset();
    }
  }

  @Override
  public void preGet(final ObserverContext<RegionCoprocessorEnvironment> e,
      final Get get, final List<KeyValue> results) throws IOException {
    window.check(e.getEnvironment().getRegion().getRegionInfo().getTableName(), RegionServerThroughputWindow.RequestType.BEFORE_READ);
  }

  @Override
  public void postGet(final ObserverContext<RegionCoprocessorEnvironment> e,
      final Get get, final List<KeyValue> results) throws IOException {
    int n = computeRecordNum(results);
    window.check(e.getEnvironment().getRegion().getRegionInfo().getTableName(),
        RegionServerThroughputWindow.RequestType.AFTER_READ, n);
  }

  @Override
  public boolean preScannerNext(final ObserverContext<RegionCoprocessorEnvironment> e,
      final InternalScanner s, final List<Result> results,
      final int limit, final boolean hasMore) throws IOException {
    window.check(e.getEnvironment().getRegion().getRegionInfo().getTableName(), RegionServerThroughputWindow.RequestType.BEFORE_READ);
    return hasMore;
  }

  @Override
  public boolean postScannerNext(final ObserverContext<RegionCoprocessorEnvironment> e,
      final InternalScanner s, final List<Result> results, final int limit,
      final boolean hasMore) throws IOException {
    int n = computeRecordNumByResultList(results);
    window.check(e.getEnvironment().getRegion().getRegionInfo().getTableName(), RegionServerThroughputWindow.RequestType.AFTER_READ, n);
    return hasMore;
  }

  @Override
  public void preDelete(final ObserverContext<RegionCoprocessorEnvironment> e,
      final Delete delete, final WALEdit edit, final boolean writeToWAL) throws IOException {
    window.check(e.getEnvironment().getRegion().getRegionInfo().getTableName(), RegionServerThroughputWindow.RequestType.WRITE);
  }


  @Override
  public void preBatchMutate(final ObserverContext<RegionCoprocessorEnvironment> c,
      final MiniBatchOperationInProgress<Pair<Mutation, Integer>> miniBatchOp) throws IOException {
    int n = computeRecordNumForBatchMutate(miniBatchOp);
    window.check(c.getEnvironment().getRegion().getRegionInfo().getTableName(), RegionServerThroughputWindow.RequestType.WRITE, n);
  }


  @Override
  public void preMutateRowsWithLocks(final ObserverContext<RegionCoprocessorEnvironment> c,
      final Collection<Mutation> mutations) throws IOException {
    int n = computeRecordNum(mutations);
    window.check(c.getEnvironment().getRegion().getRegionInfo().getTableName(), RegionServerThroughputWindow.RequestType.WRITE, n);
  }

  @Override
  public boolean preCheckAndPut(final ObserverContext<RegionCoprocessorEnvironment> e,
      final byte [] row, final byte [] family, final byte [] qualifier,
      final CompareFilter.CompareOp compareOp, final WritableByteArrayComparable comparator,
      final Put put, final boolean result) throws IOException {
    int n = computeRecordNum(put);
    window.check(e.getEnvironment().getRegion().getRegionInfo().getTableName(), RegionServerThroughputWindow.RequestType.WRITE, n);
    return result;
  }

  @Override
  public boolean preCheckAndDelete(final ObserverContext<RegionCoprocessorEnvironment> e,
      final byte [] row, final byte [] family, final byte [] qualifier,
      final CompareFilter.CompareOp compareOp, final WritableByteArrayComparable comparator,
      final Delete delete, final boolean result) throws IOException {
    window.check(e.getEnvironment().getRegion().getRegionInfo().getTableName(), RegionServerThroughputWindow.RequestType.WRITE);
    return result;
  }

  @Override
  public boolean preCheckAndMutate(final ObserverContext<RegionCoprocessorEnvironment> c,
      final Check check, final Mutation mutate, final boolean result) throws IOException {
    int n = computeRecordNum(mutate);
    window.check(c.getEnvironment().getRegion().getRegionInfo().getTableName(), RegionServerThroughputWindow.RequestType.WRITE, n);
    return result;
  }

  @Override
  public Result preAppend(final ObserverContext<RegionCoprocessorEnvironment> e,
      final Append append) throws IOException {
    int n = computeRecordNum(append);
    window.check(e.getEnvironment().getRegion().getRegionInfo().getTableName(), RegionServerThroughputWindow.RequestType.WRITE, n);
    return null;
  }

  @Override
  public long preIncrementColumnValue(final ObserverContext<RegionCoprocessorEnvironment> e,
      final byte [] row, final byte [] family, final byte [] qualifier,
      final long amount, final boolean writeToWAL) throws IOException {
    window.check(e.getEnvironment().getRegion().getRegionInfo().getTableName(), RegionServerThroughputWindow.RequestType.WRITE);
    return amount;
  }

  @Override
  public Result preIncrement(final ObserverContext<RegionCoprocessorEnvironment> e,
      final Increment increment) throws IOException {
    window.check(e.getEnvironment().getRegion().getRegionInfo().getTableName(), RegionServerThroughputWindow.RequestType.WRITE);
    return null;
  }

  private int divide1024(long n) {
    return (int) (n + 1023 >> 10);
  }

  private int computeRecordNum(Put put) {
    long n =  put.heapSize();
    return divide1024(n);
  }
  private int computeRecordNumByResultList(List<Result> results) {
    long n = 0;
    for(Result result : results) {
      n += result.getWritableSize();
    }
    return divide1024(n);
  }

  private int computeRecordNum(List<KeyValue> results) {
    long n = 0;
    for(KeyValue kv : results){
      n += kv.heapSize();
    }
    return divide1024(n);
  }

  private int computeRecordNum(Collection<Mutation> results) {
    long n = 0;
    for(Mutation mutation : results){
      for(Map.Entry<byte [], List<KeyValue>> entry:  mutation.getFamilyMap().entrySet()) {
        n += entry.getKey().length;
        for(KeyValue kv : entry.getValue()) {
          n += kv.heapSize();
        }
      }
    }
    return divide1024(n);
  }

  private int computeRecordNumForBatchMutate(MiniBatchOperationInProgress<Pair<Mutation, Integer>> miniBatchOp) {
    long n = 0;
    int length = miniBatchOp.size();
    for(int i=0; i<length; ++i) {

      Mutation mutation = miniBatchOp.getOperation(i).getFirst();
      if(mutation instanceof  Delete) {
        continue;
      }
      for(Map.Entry<byte[], List<KeyValue>> entry: mutation.getFamilyMap().entrySet()) {
        for(KeyValue kv : entry.getValue()) {
          n += kv.heapSize();
        }
      }
    }
    return divide1024(n);
  }

  private int computeRecordNum(Mutation mutation) {
    long n = 0;
    for(Map.Entry<byte [], List<KeyValue>> entry:  mutation.getFamilyMap().entrySet()) {
      for(KeyValue kv : entry.getValue()) {
        n += kv.heapSize();
      }
    }
    return divide1024(n);
  }

}
