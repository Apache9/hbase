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

import static org.apache.hadoop.hbase.client.ConnectionUtils.retries2Attempts;

import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;

/**
 * Contains configurations for an operation.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class OperationConfig {

  private final long rpcTimeoutNs;

  private final long operationTimeoutNs;

  private final long pauseNs;

  private final int maxAttempts;

  private final int startLogErrorsCnt;

  private OperationConfig(long rpcTimeoutNs, long operationTimeoutNs, long pauseNs, int maxAttempts,
      int startLogErrorsCnt) {
    this.rpcTimeoutNs = rpcTimeoutNs;
    this.operationTimeoutNs = operationTimeoutNs;
    this.pauseNs = pauseNs;
    this.maxAttempts = maxAttempts;
    this.startLogErrorsCnt = startLogErrorsCnt;
  }

  public long getRpcTimeout(TimeUnit unit) {
    return unit.convert(rpcTimeoutNs, TimeUnit.NANOSECONDS);
  }

  public long getOperationTimeout(TimeUnit unit) {
    return unit.convert(operationTimeoutNs, TimeUnit.NANOSECONDS);
  }

  public long getRetryPause(TimeUnit unit) {
    return unit.convert(pauseNs, TimeUnit.NANOSECONDS);
  }

  public long getRpcTimeoutNs() {
    return rpcTimeoutNs;
  }

  public long getOperationTimeoutNs() {
    return operationTimeoutNs;
  }

  public long getRetryPauseNs() {
    return pauseNs;
  }

  public int getMaxAttempts() {
    return maxAttempts;
  }

  public int getStartLogErrorsCnt() {
    return startLogErrorsCnt;
  }

  @InterfaceAudience.Public
  @InterfaceStability.Unstable
  public static final class OperationConfigBuilder {

    private long rpcTimeoutNs;

    private long operationTimeoutNs;

    private long pauseNs;

    private int maxAttempts;

    private int startLogErrorsCnt;

    OperationConfigBuilder() {
    }

    OperationConfigBuilder(OperationConfig retryConfig) {
      this.rpcTimeoutNs = retryConfig.rpcTimeoutNs;
      this.operationTimeoutNs = retryConfig.operationTimeoutNs;
      this.pauseNs = retryConfig.pauseNs;
      this.maxAttempts = retryConfig.maxAttempts;
      this.startLogErrorsCnt = retryConfig.startLogErrorsCnt;
    }

    public OperationConfigBuilder setRpcTimeout(long timeout, TimeUnit unit) {
      this.rpcTimeoutNs = unit.toNanos(timeout);
      return this;
    }

    public OperationConfigBuilder setOperationTimeout(long timeout, TimeUnit unit) {
      this.operationTimeoutNs = unit.toNanos(timeout);
      return this;
    }

    public OperationConfigBuilder setRetryPause(long timeout, TimeUnit unit) {
      this.pauseNs = unit.toNanos(timeout);
      return this;
    }

    public OperationConfigBuilder setMaxAttempts(int maxAttempts) {
      this.maxAttempts = maxAttempts;
      return this;
    }

    public OperationConfigBuilder setMaxRetries(int maxRetries) {
      return setMaxAttempts(retries2Attempts(maxRetries));
    }

    public OperationConfigBuilder setStartLogErrorsCnt(int startLogErrorsCnt) {
      this.startLogErrorsCnt = startLogErrorsCnt;
      return this;
    }

    public OperationConfig build() {
      return new OperationConfig(rpcTimeoutNs, operationTimeoutNs, pauseNs, maxAttempts,
          startLogErrorsCnt);
    }
  }
}
