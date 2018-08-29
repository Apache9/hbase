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

import org.apache.hadoop.hbase.classification.InterfaceAudience;

import java.util.concurrent.TimeUnit;

@InterfaceAudience.Private
class BufferedMutatorBuilderImpl implements BufferedMutatorBuilder {

  private AsyncBufferedMutatorBuilder asyncBuilder;

  BufferedMutatorBuilderImpl(AsyncBufferedMutatorBuilder asyncBuilder) {
    this.asyncBuilder = asyncBuilder;
  }

  @Override
  public BufferedMutatorBuilder setOperationTimeout(long timeout, TimeUnit unit) {
    asyncBuilder.setOperationTimeout(timeout, unit);
    return this;
  }

  @Override
  public BufferedMutatorBuilder setRpcTimeout(long timeout, TimeUnit unit) {
    asyncBuilder.setRpcTimeout(timeout, unit);
    return this;
  }

  @Override
  public BufferedMutatorBuilder setRetryPause(long pause, TimeUnit unit) {
    asyncBuilder.setRetryPause(pause, unit);
    return this;
  }

  @Override
  public BufferedMutatorBuilder setMaxAttempts(int maxAttempts) {
    asyncBuilder.setMaxAttempts(maxAttempts);
    return this;
  }

  @Override
  public BufferedMutatorBuilder setStartLogErrorsCnt(int startLogErrorsCnt) {
    asyncBuilder.setStartLogErrorsCnt(startLogErrorsCnt);
    return this;
  }

  @Override
  public BufferedMutatorBuilder setWriteBufferSize(long writeBufferSize) {
    asyncBuilder.setWriteBufferSize(writeBufferSize);
    return this;
  }

  @Override
  public BufferedMutator build() {
    return new BufferedMutatorImpl(asyncBuilder.build());
  }
}
