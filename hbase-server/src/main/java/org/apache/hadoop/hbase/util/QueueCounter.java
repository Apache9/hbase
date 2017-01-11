package org.apache.hadoop.hbase.util;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class QueueCounter {
  private final AtomicBoolean queueFull;
  private final AtomicLong incomeRequestCount;
  private final AtomicLong rejectedRequestCount;

  public QueueCounter() {
    this.queueFull = new AtomicBoolean(false);
    this.incomeRequestCount = new AtomicLong(0);
    this.rejectedRequestCount = new AtomicLong(0);
  }

  public void setQueueFull(boolean full) {
    queueFull.set(full);
  }

  public boolean getQueueFull() {
    return queueFull.get();
  }

  public void incIncomeRequestCount() {
    incomeRequestCount.incrementAndGet();
  }

  public long getIncomeRequestCount() {
    return incomeRequestCount.get();
  }

  public void incRejectedRequestCount() {
    rejectedRequestCount.incrementAndGet();
  }

  public long getRejectedRequestCount() {
    return rejectedRequestCount.get();
  }
}
