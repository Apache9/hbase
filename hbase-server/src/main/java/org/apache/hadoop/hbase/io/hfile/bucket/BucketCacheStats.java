/**
 * Copyright The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hbase.io.hfile.bucket;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.io.hfile.CacheStats;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

/**
 * Class that implements cache metrics for bucket cache.
 */
@InterfaceAudience.Private
public class BucketCacheStats extends CacheStats {
  private final LongAdder ioHitCount = new LongAdder();
  private final LongAdder ioMissCount = new LongAdder();
  private final LongAdder ioHitTime = new LongAdder();
  private final static int nanoTime = 1000000;
  private long lastLogHitTime = EnvironmentEdgeManager.currentTimeMillis();
  private long lastLogMissTime = EnvironmentEdgeManager.currentTimeMillis();
  private long lastLogHitCount = 0;
  private long lastLogMissCount = 0;

  @Override
  public String toString() {
    return super.toString() + ", ioHitsPerSecond=" + getIOHitsPerSecond() +
      ", ioTimePerHit=" + getIOTimePerHit();
  }

  public void ioHit(long time) {
    ioHitCount.increment();
    ioHitTime.add(time);
  }

  public void ioMiss() {
    ioMissCount.increment();
  }

  public long getIOHitsPerSecond() {
    long now = EnvironmentEdgeManager.currentTimeMillis();
    long took = (now - lastLogHitTime) / 1000;
    lastLogHitTime = now;
    long delta = ioHitCount.sum() - lastLogHitCount;
    lastLogHitCount = ioHitCount.sum();
    return took == 0? 0: delta / took;
  }

  public long getIOMissPerSecond() {
    long now = EnvironmentEdgeManager.currentTimeMillis();
    long took = (now - lastLogMissTime) / 1000;
    lastLogMissTime = now;
    long delta = ioMissCount.sum() - lastLogMissCount;
    lastLogMissCount = ioMissCount.sum();
    return took == 0? 0: delta / took;
  }

  public double getIOTimePerHit() {
    long time = ioHitTime.sum() / nanoTime;
    long count = ioHitCount.sum();
    return ((float) time / (float) count);
  }

  public void reset() {
    ioHitCount.reset();
    ioHitTime.reset();
    ioMissCount.reset();
    lastLogHitCount = 0;
    lastLogMissCount = 0;
  }
}
