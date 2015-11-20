/**
 * Copyright 2010 The Apache Software Foundation Licensed to the Apache Software Foundation (ASF)
 * under one or more contributor license agreements. See the NOTICE file distributed with this work
 * for additional information regarding copyright ownership. The ASF licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law or agreed to in
 * writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */

package org.apache.hadoop.hbase.util;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class QueueCounter {
  private AtomicBoolean readQueueFull;
  private AtomicBoolean writeQueueFull;
  private AtomicLong incomeReadCount;
  private AtomicLong incomeWriteCount;
  private AtomicLong rejectedReadCount;
  private AtomicLong rejectedWriteCount;

  public QueueCounter() {
    readQueueFull = new AtomicBoolean(false);
    writeQueueFull = new AtomicBoolean(false);
    incomeReadCount = new AtomicLong(0);
    incomeWriteCount = new AtomicLong(0);
    rejectedReadCount = new AtomicLong(0);
    rejectedWriteCount = new AtomicLong(0);
  }

  public void setReadQueueFull(boolean full) {
    readQueueFull.set(full);
  }

  public boolean getReadQueueFull() {
    return readQueueFull.get();
  }

  public void setWriteQueueFull(boolean full) {
    writeQueueFull.set(full);
  }

  public boolean getWriteQueueFull() {
    return writeQueueFull.get();
  }

  public void incIncomeReadCount() {
    incomeReadCount.incrementAndGet();
  }

  public long getIncomeReadCount() {
    return incomeReadCount.get();
  }

  public void incIncomeWriteCount() {
    incomeWriteCount.incrementAndGet();
  }

  public long getIncomeWriteCount() {
    return incomeWriteCount.get();
  }

  public void incRejectedReadCount() {
    rejectedReadCount.incrementAndGet();
  }

  public long getRejectedReadCount() {
    return rejectedReadCount.get();
  }

  public void incRejectedWriteCount() {
    rejectedWriteCount.incrementAndGet();
  }

  public long getRejectedWriteCount() {
    return rejectedWriteCount.get();
  }
}
