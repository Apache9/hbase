/**
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
package org.apache.hadoop.hbase.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;

@InterfaceAudience.Private
class BufferedMutatorImpl implements BufferedMutator {

  private final AsyncBufferedMutator asyncImpl;

  private List<CompletableFuture<Void>> futures = new ArrayList<>();

  private final List<Throwable> errors;

  // If every mutation is 1KB and the default write buffer size is 2MB
  // So use 2048 as the default threshold.
  // But every HTable instance will have a buffered mutator, so reduce it to 1024.
  private final static int BUFFERED_FUTURES_THRESHOLD = 1024;

  BufferedMutatorImpl(AsyncBufferedMutator asyncImpl) {
    this.asyncImpl = asyncImpl;
    this.errors = new ArrayList<>();
  }

  @Override
  public TableName getName() {
    return asyncImpl.getName();
  }

  @Override
  public Configuration getConfiguration() {
    return asyncImpl.getConfiguration();
  }

  private synchronized void onError(Throwable error) {
    this.errors.add(error);
  }

  @Override
  public void mutate(Mutation mutation) throws IOException {
    mutate(Collections.singletonList(mutation));
  }

  @Override
  public void mutate(List<? extends Mutation> mutations) throws IOException {
    List<CompletableFuture<Void>> futures = new ArrayList<>();
    for (CompletableFuture<Void> future : asyncImpl.mutate(mutations)) {
      CompletableFuture<Void> toComplete = new CompletableFuture<>();
      future.whenComplete((result, error) -> {
        if (error != null) {
          onError(error);
          toComplete.completeExceptionally(error);
        } else {
          toComplete.complete(result);
        }
      });
      futures.add(toComplete);
    }
    synchronized (this) {
      this.futures.addAll(futures);
      if (!errors.isEmpty()) {
        errors.clear();
        flush();
      }
      if (this.futures.size() > BUFFERED_FUTURES_THRESHOLD) {
        tryCompleteFuture();
      }
    }
  }

  private void tryCompleteFuture() {
    futures = futures.stream().filter(f -> !f.isDone()).collect(Collectors.toList());
  }

  @Override
  public void close() throws IOException {
    flush();
    asyncImpl.close();
  }

  @Override
  public void flush() throws IOException {
    asyncImpl.flush();
    synchronized (this) {
      List<CompletableFuture<Void>> toComplete = this.futures;
      this.futures = new ArrayList<>();
      CompletableFuture.allOf(toComplete.toArray(new CompletableFuture<?>[toComplete.size()]))
          .join();
    }
  }

  @Override
  public long getWriteBufferSize() {
    return asyncImpl.getWriteBufferSize();
  }

  @VisibleForTesting
  public int getBufferedFutruesNumber() {
    return futures.size();
  }
}
