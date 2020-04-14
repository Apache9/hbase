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
package org.apache.hadoop.hbase.ipc;

import org.apache.yetus.audience.InterfaceAudience;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;

@InterfaceAudience.Private
public class QueueCounter {
  private final AtomicBoolean queueFull;
  private final LongAdder incomeRequestCount;
  private final LongAdder rejectedRequestCount;
  private final String name;

  public QueueCounter(String name) {
    this.name = name;
    this.queueFull = new AtomicBoolean(false);
    this.incomeRequestCount = new LongAdder();
    this.rejectedRequestCount = new LongAdder();
  }

  public void setQueueFull(boolean full) {
    queueFull.set(full);
  }

  public boolean getQueueFull() {
    return queueFull.get();
  }

  public void incIncomeRequestCount() {
    incomeRequestCount.add(1L);
  }

  public long getIncomeRequestCount() {
    return incomeRequestCount.sum();
  }

  public void incRejectedRequestCount() {
    rejectedRequestCount.add(1L);
  }

  public long getRejectedRequestCount() {
    return rejectedRequestCount.sum();
  }

  @Override
  public String toString() {
    return name + ".QueueCounter";
  }
}
