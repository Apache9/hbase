package org.apache.hadoop.hbase.util;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;

public class QueueCounter {
  private final AtomicBoolean queueFull;
  private final LongAdder incomeRequestCount;
  private final LongAdder rejectedRequestCount;
  private String name;

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
