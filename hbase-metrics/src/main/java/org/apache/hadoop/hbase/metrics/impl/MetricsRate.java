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
package org.apache.hadoop.hbase.metrics.impl;

import org.apache.yetus.audience.InterfaceAudience;

/**
 * Publishes a rate based on a counter - you increment the counter each time an event occurs (eg: an
 * RPC call) and this publishes a rate.
 */
@InterfaceAudience.Private
public class MetricsRate {
  private long value = 0;
  private float prevRate;
  private long ts;

  public synchronized void inc(final long incr) {
    value += incr;
  }

  public synchronized void inc() {
    value++;
  }

  public synchronized void intervalHeartBeat() {
    long now = System.currentTimeMillis();
    long diff = (now - ts) / 1000;
    if (diff < 1) {
      // To make sure our averages aren't skewed by fast repeated calls,
      // we simply ignore fast repeated calls.
      return;
    }
    this.prevRate = (float) value / diff;
    this.value = 0;
    this.ts = now;
  }

  public synchronized float getPreviousIntervalValue() {
    return this.prevRate;
  }
}
