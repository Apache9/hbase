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
package org.apache.hadoop.hbase.util;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper class for processing futures.
 */
@InterfaceAudience.Private
public final class FutureUtils {

  private static final Logger LOG = LoggerFactory.getLogger(FutureUtils.class);

  private FutureUtils() {
  }

  /**
   * This is method is used when you do not care the result of an asynchronous operation. Ignoring
   * the return value of a Future is considered as a bad practice as it may suppress exceptions
   * thrown from the code that completes the future, so you can use method to log the exceptions
   * when the future is failed.
   * <p/>
   * And the error phone check will always report FutureReturnValueIgnored because every method in
   * the {@link CompletableFuture} class will return a new {@link CompletableFuture}, so you always
   * have one future that has not been checked. So we introduce this method and add a suppress
   * warnings annotation here.
   */
  @SuppressWarnings("FutureReturnValueIgnored")
  public static void ifFail(CompletableFuture<?> future, Consumer<Throwable> action) {
    future.whenComplete((resp, error) -> {
      if (error != null) {
        try {
          action.accept(error);
        } catch (Throwable e) {
          LOG.warn("Failed to process error", error);
        }
      }
    });
  }
}
