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

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.OperationConfig.OperationConfigBuilder;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.ReflectionUtils;

/**
 * The implementation of AsyncTable. Based on {@link RawAsyncTable}.
 */
@InterfaceAudience.Private
class AsyncTableImpl implements AsyncTable {

  private final RawAsyncTable rawTable;

  private final ExecutorService pool;

  private final long defaultScannerMaxResultSize;

  public AsyncTableImpl(AsyncConnectionImpl conn, TableName tableName, ExecutorService pool) {
    this.rawTable = conn.getRawTable(tableName);
    this.pool = pool;
    this.defaultScannerMaxResultSize = conn.connConf.getScannerMaxResultSize();
  }

  @Override
  public TableName getName() {
    return rawTable.getName();
  }

  @Override
  public Configuration getConfiguration() {
    return rawTable.getConfiguration();
  }

  @Override
  public OperationConfigBuilder newOperationConfig() {
    return rawTable.newOperationConfig();
  }

  private <T> CompletableFuture<T> wrap(CompletableFuture<T> future) {
    CompletableFuture<T> asyncFuture = new CompletableFuture<>();
    future.whenCompleteAsync((r, e) -> {
      if (e != null) {
        asyncFuture.completeExceptionally(e);
      } else {
        asyncFuture.complete(r);
      }
    }, pool);
    return asyncFuture;
  }

  @Override
  public CompletableFuture<Result> get(Get get, OperationConfig retryConfig) {
    return wrap(rawTable.get(get, retryConfig));
  }

  @Override
  public CompletableFuture<Void> put(Put put, OperationConfig retryConfig) {
    return wrap(rawTable.put(put, retryConfig));
  }

  @Override
  public CompletableFuture<Void> delete(Delete delete, OperationConfig retryConfig) {
    return wrap(rawTable.delete(delete, retryConfig));
  }

  @Override
  public CompletableFuture<Result> append(Append append, OperationConfig retryConfig) {
    return wrap(rawTable.append(append, retryConfig));
  }

  @Override
  public CompletableFuture<Result> increment(Increment increment, OperationConfig retryConfig) {
    return wrap(rawTable.increment(increment, retryConfig));
  }

  @Override
  public CompletableFuture<Boolean> checkAndPut(byte[] row, byte[] family, byte[] qualifier,
      CompareOp compareOp, byte[] value, Put put, OperationConfig retryConfig) {
    return wrap(rawTable.checkAndPut(row, family, qualifier, compareOp, value, put, retryConfig));
  }

  @Override
  public CompletableFuture<Boolean> checkAndDelete(byte[] row, byte[] family, byte[] qualifier,
      CompareOp compareOp, byte[] value, Delete delete, OperationConfig retryConfig) {
    return wrap(
      rawTable.checkAndDelete(row, family, qualifier, compareOp, value, delete, retryConfig));
  }

  @Override
  public CompletableFuture<Void> mutateRow(RowMutations mutation, OperationConfig retryConfig) {
    return wrap(rawTable.mutateRow(mutation, retryConfig));
  }

  @Override
  public CompletableFuture<Boolean> checkAndMutate(byte[] row, byte[] family, byte[] qualifier,
      CompareOp compareOp, byte[] value, RowMutations mutation, OperationConfig retryConfig) {
    return wrap(
      rawTable.checkAndMutate(row, family, qualifier, compareOp, value, mutation, retryConfig));
  }

  @Override
  public CompletableFuture<List<Result>> smallScan(Scan scan, int limit,
      OperationConfig retryConfig) {
    return wrap(rawTable.smallScan(scan, limit, retryConfig));
  }

  private long resultSize2CacheSize(long maxResultSize) {
    // * 2 if possible
    return maxResultSize > Long.MAX_VALUE / 2 ? maxResultSize : maxResultSize * 2;
  }

  @Override
  public ResultScanner getScanner(Scan scan, OperationConfig retryConfig) {
    return new AsyncTableResultScanner(rawTable, ReflectionUtils.newInstance(scan.getClass(), scan),
        resultSize2CacheSize(
          scan.getMaxResultSize() > 0 ? scan.getMaxResultSize() : defaultScannerMaxResultSize),
        retryConfig);
  }

  private void scan0(Scan scan, ScanResultConsumer consumer, OperationConfig retryConfig) {
    try (ResultScanner scanner = getScanner(scan, retryConfig)) {
      for (Result result; (result = scanner.next()) != null;) {
        if (!consumer.onNext(result)) {
          break;
        }
      }
      consumer.onComplete();
    } catch (IOException e) {
      consumer.onError(e);
    }
  }

  @Override
  public void scan(Scan scan, ScanResultConsumer consumer, OperationConfig retryConfig) {
    pool.execute(() -> scan0(scan, consumer, retryConfig));
  }

  @Override
  public <T> List<CompletableFuture<T>> batch(List<? extends Row> actions,
      OperationConfig retryConfig) {
    return rawTable.<T> batch(actions, retryConfig).stream().map(this::wrap)
        .collect(Collectors.toList());
  }
}
