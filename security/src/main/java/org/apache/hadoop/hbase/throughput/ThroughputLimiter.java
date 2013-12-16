/*
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

package org.apache.hadoop.hbase.throughput;

import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Throughput rate limiter based on token bucket algorithm.
 */
@ThreadSafe
public class ThroughputLimiter {
  private double quotaPerSec;
  private double remains;
  private long lastRefillTimeMillis;
  // The max bucket capacity
  private double capacity;
  private long minRefillTimeMillis;
  private long overflowTimeMillis;

  public ThroughputLimiter(double quotaPerSec) {
    // buffer 1 sec quota by default
    this(quotaPerSec, quotaPerSec);
  }

  public ThroughputLimiter(double quotaPerSec, double capacity) {
    this.quotaPerSec = quotaPerSec;
    this.capacity = capacity;
    this.remains = 0;
    this.lastRefillTimeMillis = 0;

    // Refill in batch, to reduce calculation overhead
    final int MIN_REFILL_BATCH = 10;
    final int MAX_REFILL_INTERVAL = 1000;
    final int MIN_REFILL_INTERVAL = 100;
    if (quotaPerSec > 0) {
      this.minRefillTimeMillis = (long) Math.min(MAX_REFILL_INTERVAL,
        Math.max(MIN_REFILL_INTERVAL, MIN_REFILL_BATCH * 1000 / quotaPerSec));
      // The minimum refill interval which makes the bucket overflow
      this.overflowTimeMillis = (long) Math.ceil(1000 * this.capacity / quotaPerSec);
    } else {
      this.minRefillTimeMillis = Long.MAX_VALUE;
      this.overflowTimeMillis = 0;
    }
  }

  public double getQuotaPerSec() {
    return this.quotaPerSec;
  }

  public boolean tryAcquire(double tokens, long timeout, TimeUnit timeUnit) {
    if (timeout > 0) {
      int retry = 0;
      long timeoutMillis = System.currentTimeMillis() + timeUnit.toMillis(timeout);
      while (!(tryAcquire(tokens))) {
        if (System.currentTimeMillis() > timeoutMillis) {
          return false;
        }

        try {
          Thread.sleep(Math.min(1 << retry++, 100));
        } catch (InterruptedException e) {
        }
      }
      return true;
    } else {
      return tryAcquire(tokens);
    }
  }

  public synchronized boolean tryAcquire(double tokens) {
    if (acquire(tokens)) {
      return true;
    }

    if (refill()) {
      return acquire(tokens);
    }

    return false;
  }

  private boolean acquire(double tokens) {
    if (this.remains >= tokens) {
      this.remains -= tokens;
      return true;
    }
    return false;
  }

  private boolean refill() {
    long now = System.currentTimeMillis();
    long elapsed = now - this.lastRefillTimeMillis;

    if (elapsed >= this.minRefillTimeMillis) {
      if (elapsed >= this.overflowTimeMillis) {
        this.remains = this.capacity;
      } else {
        this.remains += elapsed * this.quotaPerSec / 1000.0;
        this.remains = Math.min(this.capacity, this.remains);
      }
      this.lastRefillTimeMillis = now;
      return true;
    }
    return false;
  }

  @Override
  public String toString() {
    return "ThroughputLimiter [quotaPerSec=" + quotaPerSec + ", remains=" + remains
        + ", lastRefillTimeMillis=" + lastRefillTimeMillis + ", capacity=" + capacity
        + ", minRefillTimeMillis=" + minRefillTimeMillis + ", overflowTimeMillis="
        + overflowTimeMillis + "]";
  }
}