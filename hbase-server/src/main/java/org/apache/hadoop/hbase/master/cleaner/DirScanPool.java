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
package org.apache.hadoop.hbase.master.cleaner;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.DaemonThreadFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The thread pool used for scan directories
 */
@InterfaceAudience.Private
public class DirScanPool {
  private static final Logger LOG = LoggerFactory.getLogger(DirScanPool.class);
  private volatile int size;
  private final ThreadPoolExecutor pool;

  public DirScanPool(Configuration conf) {
    String poolSize = conf.get(CleanerChore.CHORE_POOL_SIZE, CleanerChore.DEFAULT_CHORE_POOL_SIZE);
    size = CleanerChore.calculatePoolSize(poolSize);
    // poolSize may be 0 or 0.0 from a careless configuration,
    // double check to make sure.
    size = size == 0 ? CleanerChore.calculatePoolSize(CleanerChore.DEFAULT_CHORE_POOL_SIZE) : size;
    pool = initializePool(size);
    LOG.info("Cleaner pool size is {}", size);
  }

  private static ThreadPoolExecutor initializePool(int size) {
    ThreadPoolExecutor executor = new ThreadPoolExecutor(size, size, 1, TimeUnit.MINUTES,
        new LinkedBlockingQueue<>(), new DaemonThreadFactory("dir-scan-pool"));
    executor.allowCoreThreadTimeOut(true);
    return executor;
  }

  synchronized void execute(Runnable runnable) {
    pool.execute(runnable);
  }

  public synchronized void shutdownNow() {
    if (pool == null || pool.isShutdown()) {
      return;
    }
    pool.shutdownNow();
  }

  public int getSize() {
    return size;
  }
}
