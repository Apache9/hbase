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

import static org.apache.hadoop.hbase.HConstants.DEFAULT_HBASE_CLIENT_PAUSE;
import static org.apache.hadoop.hbase.HConstants.DEFAULT_HBASE_CLIENT_RETRIES_NUMBER;
import static org.apache.hadoop.hbase.HConstants.HBASE_CLIENT_PAUSE;
import static org.apache.hadoop.hbase.HConstants.HBASE_CLIENT_RETRIES_NUMBER;
import static org.apache.hadoop.hbase.client.AsyncProcess.DEFAULT_START_LOG_ERRORS_AFTER_COUNT;
import static org.apache.hadoop.hbase.client.AsyncProcess.START_LOG_ERRORS_AFTER_COUNT_KEY;

import com.google.common.base.Preconditions;

import io.netty.util.HashedWheelTimer;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;

/**
 * Factory to create an AsyncRpcRetryCaller.
 */
@InterfaceAudience.Private
class AsyncRpcRetryCallerFactory {

  private final AsyncConnectionImpl conn;

  private final HashedWheelTimer retryTimer;

  private final long pauseNs;

  private final int maxRetries;

  /** How many retries are allowed before we start to log */
  private final int startLogErrorsCnt;

  public AsyncRpcRetryCallerFactory(AsyncConnectionImpl conn, HashedWheelTimer retryTimer) {
    this.conn = conn;
    Configuration conf = conn.getConfiguration();
    this.pauseNs =
        TimeUnit.MILLISECONDS.toNanos(conf.getLong(HBASE_CLIENT_PAUSE, DEFAULT_HBASE_CLIENT_PAUSE));
    this.maxRetries = conf.getInt(HBASE_CLIENT_RETRIES_NUMBER, DEFAULT_HBASE_CLIENT_RETRIES_NUMBER);
    this.startLogErrorsCnt =
        conf.getInt(START_LOG_ERRORS_AFTER_COUNT_KEY, DEFAULT_START_LOG_ERRORS_AFTER_COUNT);
    this.retryTimer = retryTimer;
  }

  public class SingleActionCallerBuilder<T> {

    private TableName tableName;

    private byte[] row;

    private AsyncSingleActionRpcRetryCaller.Callable<T> callable;

    private long operationTimeoutNs = -1L;

    private long rpcTimeoutNs = -1L;

    public SingleActionCallerBuilder<T> table(TableName tableName) {
      this.tableName = tableName;
      return this;
    }

    public SingleActionCallerBuilder<T> row(byte[] row) {
      this.row = row;
      return this;
    }

    public SingleActionCallerBuilder<T>
        action(AsyncSingleActionRpcRetryCaller.Callable<T> callable) {
      this.callable = callable;
      return this;
    }

    public SingleActionCallerBuilder<T> operationTimeout(long operationTimeout, TimeUnit unit) {
      this.operationTimeoutNs = unit.toNanos(operationTimeout);
      return this;
    }

    public SingleActionCallerBuilder<T> rpcTimeout(long rpcTimeout, TimeUnit unit) {
      this.rpcTimeoutNs = unit.toNanos(rpcTimeout);
      return this;
    }

    public AsyncSingleActionRpcRetryCaller<T> build() {
      return new AsyncSingleActionRpcRetryCaller<>(retryTimer, conn,
          Preconditions.checkNotNull(tableName, "tableName is null"),
          Preconditions.checkNotNull(row, "row is null"),
          Preconditions.checkNotNull(callable, "action is null"), pauseNs, maxRetries,
          operationTimeoutNs, rpcTimeoutNs, startLogErrorsCnt);
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
  public <T> SingleActionCallerBuilder<T> single() {
    return new SingleActionCallerBuilder<>();
  }
}
