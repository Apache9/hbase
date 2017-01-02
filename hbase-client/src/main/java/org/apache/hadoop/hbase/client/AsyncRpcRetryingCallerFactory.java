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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import io.netty.util.HashedWheelTimer;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.ClientService;

/**
 * Factory to create an AsyncRpcRetryCaller.
 */
@InterfaceAudience.Private
class AsyncRpcRetryingCallerFactory {

  private final AsyncConnectionImpl conn;

  private final HashedWheelTimer retryTimer;

  public AsyncRpcRetryingCallerFactory(AsyncConnectionImpl conn, HashedWheelTimer retryTimer) {
    this.conn = conn;
    this.retryTimer = retryTimer;
  }

  public class SingleRequestCallerBuilder<T> {

    private TableName tableName;

    private byte[] row;

    private AsyncSingleRequestRpcRetryingCaller.Callable<T> callable;

    private OperationConfig operationConfig;

    private RegionLocateType locateType = RegionLocateType.CURRENT;

    public SingleRequestCallerBuilder<T> table(TableName tableName) {
      this.tableName = tableName;
      return this;
    }

    public SingleRequestCallerBuilder<T> row(byte[] row) {
      this.row = row;
      return this;
    }

    public SingleRequestCallerBuilder<T>
        action(AsyncSingleRequestRpcRetryingCaller.Callable<T> callable) {
      this.callable = callable;
      return this;
    }

    public SingleRequestCallerBuilder<T> operationConfig(OperationConfig operationConfig) {
      this.operationConfig = operationConfig;
      return this;
    }

    public SingleRequestCallerBuilder<T> locateType(RegionLocateType locateType) {
      this.locateType = locateType;
      return this;
    }

    public AsyncSingleRequestRpcRetryingCaller<T> build() {
      return new AsyncSingleRequestRpcRetryingCaller<>(retryTimer, conn,
          checkNotNull(tableName, "tableName is null"), checkNotNull(row, "row is null"),
          checkNotNull(locateType, "locateType is null"), checkNotNull(callable, "action is null"),
          checkNotNull(operationConfig, "operationConfig is null"));
    }

    /**
     * Shortcut for {@code build().call()}
     */
    public CompletableFuture<T> call() {
      return build().call();
    }
  }

  /**
   * Create retry caller for single action, such as get, put, delete, etc.
   */
  public <T> SingleRequestCallerBuilder<T> single() {
    return new SingleRequestCallerBuilder<>();
  }

  public class SmallScanCallerBuilder {

    private TableName tableName;

    private Scan scan;

    private int limit;

    private OperationConfig operationConfig;

    public SmallScanCallerBuilder table(TableName tableName) {
      this.tableName = tableName;
      return this;
    }

    public SmallScanCallerBuilder setScan(Scan scan) {
      this.scan = scan;
      return this;
    }

    public SmallScanCallerBuilder limit(int limit) {
      this.limit = limit;
      return this;
    }

    public SmallScanCallerBuilder operationConfig(OperationConfig operationConfig) {
      this.operationConfig = operationConfig;
      return this;
    }

    public AsyncSmallScanRpcRetryingCaller build() {
      checkArgument(limit > 0, "invalid limit %d", limit);
      return new AsyncSmallScanRpcRetryingCaller(conn, checkNotNull(tableName, "tableName is null"),
          checkNotNull(scan, "scan is null"), limit,
          checkNotNull(operationConfig, "operationConfig is null"));
    }

    /**
     * Shortcut for {@code build().call()}
     */
    public CompletableFuture<List<Result>> call() {
      return build().call();
    }
  }

  /**
   * Create retry caller for small scan.
   */
  public SmallScanCallerBuilder smallScan() {
    return new SmallScanCallerBuilder();
  }

  public class ScanSingleRegionCallerBuilder {

    private long scannerId = -1L;

    private Scan scan;

    private ScanResultCache resultCache;

    private RawScanResultConsumer consumer;

    private ClientService.Interface stub;

    private HRegionLocation loc;

    private OperationConfig operationConfig;

    public ScanSingleRegionCallerBuilder id(long scannerId) {
      this.scannerId = scannerId;
      return this;
    }

    public ScanSingleRegionCallerBuilder setScan(Scan scan) {
      this.scan = scan;
      return this;
    }

    public ScanSingleRegionCallerBuilder resultCache(ScanResultCache resultCache) {
      this.resultCache = resultCache;
      return this;
    }

    public ScanSingleRegionCallerBuilder consumer(RawScanResultConsumer consumer) {
      this.consumer = consumer;
      return this;
    }

    public ScanSingleRegionCallerBuilder stub(ClientService.Interface stub) {
      this.stub = stub;
      return this;
    }

    public ScanSingleRegionCallerBuilder location(HRegionLocation loc) {
      this.loc = loc;
      return this;
    }

    public ScanSingleRegionCallerBuilder operationConfig(OperationConfig operationConfig) {
      this.operationConfig = operationConfig;
      return this;
    }

    public AsyncScanSingleRegionRpcRetryingCaller build() {
      checkArgument(scannerId >= 0, "invalid scannerId %d", scannerId);
      return new AsyncScanSingleRegionRpcRetryingCaller(retryTimer, conn,
          checkNotNull(scan, "scan is null"), scannerId,
          checkNotNull(resultCache, "resultCache is null"),
          checkNotNull(consumer, "consumer is null"), checkNotNull(stub, "stub is null"),
          checkNotNull(loc, "location is null"),
          checkNotNull(operationConfig, "operationConfig is null"));
    }

    /**
     * Short cut for {@code build().start()}.
     */
    public CompletableFuture<Boolean> start() {
      return build().start();
    }
  }

  /**
   * Create retry caller for scanning a region.
   */
  public ScanSingleRegionCallerBuilder scanSingleRegion() {
    return new ScanSingleRegionCallerBuilder();
  }

  public class BatchCallerBuilder {

    private TableName tableName;

    private List<? extends Row> actions;

    private OperationConfig operationConfig;

    public BatchCallerBuilder table(TableName tableName) {
      this.tableName = tableName;
      return this;
    }

    public BatchCallerBuilder actions(List<? extends Row> actions) {
      this.actions = actions;
      return this;
    }

    public BatchCallerBuilder operationConfig(OperationConfig operationConfig) {
      this.operationConfig = operationConfig;
      return this;
    }

    public <T> AsyncBatchRpcRetryingCaller<T> build() {
      return new AsyncBatchRpcRetryingCaller<T>(retryTimer, conn,
          checkNotNull(tableName, "tableName is null"), checkNotNull(actions, "actions is null"),
          checkNotNull(operationConfig, "operationConfig is null"));
    }

    public <T> List<CompletableFuture<T>> call() {
      return this.<T> build().call();
    }
  }

  public BatchCallerBuilder batch() {
    return new BatchCallerBuilder();
  }
}
