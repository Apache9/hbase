package com.xiaomi.infra.hbase.trace;

import org.apache.hadoop.hbase.util.Pair;

import java.sql.Timestamp;
import java.util.LinkedList;
import java.util.List;

public class MilliTracer implements Tracer {
  private String description;
  private long start;
  private long stop;
  private int maxEntriesLimit;
  private boolean reachedEntriesLimit;
  private List<Pair<Long, String>> records;

  public MilliTracer(final String description, final int maxEntriesLimit) {
    this.description = description;
    this.start = currentTimeMillis();
    this.stop = -1;
    this.maxEntriesLimit = maxEntriesLimit;
    this.records = new LinkedList<Pair<Long, String>>();
    this.records.add(new Pair<Long, String>(start, "Start trace: "
        + description));
  }

  @Override
  public void addAnnotation(String msg) {
    if (reachedEntriesLimit) {
      // discard the following msgs to avoid the potential OOM issue.
      return;
    }
    long time = currentTimeMillis();
    this.records.add(new Pair<Long, String>(time, msg));
    if (this.records.size() >= maxEntriesLimit) {
      reachedEntriesLimit = true;
    }
  }

  @Override
  public void stop() {
    if (start == 0) throw new IllegalStateException("Trace for " + description
        + " has not been started");
    stop = currentTimeMillis();
    this.records.add(new Pair<Long, String>(stop, "Stop trace: " + description
        + (reachedEntriesLimit ? ", DISCARDED MSG DUE TO REACH LIMIT:" + maxEntriesLimit : "")));
  }

  private long currentTimeMillis() {
    return System.currentTimeMillis();
  }

  @Override
  public long getAccumulatedMillis() {
    if (start == 0) return 0;
    if (stop > 0) return stop - start;
    return currentTimeMillis() - start;
  }

  @Override
  public String toString() {
    StringBuffer buf = new StringBuffer();
    buf.append("Slow Rpc Trace: total cost ").append(getAccumulatedMillis()).append(" ms\n");
    long startTime = start; 
    for (Pair<Long, String> record : records) {
      long cost = record.getFirst() - startTime;
      buf.append("---> ").append(new Timestamp(record.getFirst())).append(" : ")
          .append(record.getSecond()).append(" . Time cost from last record: ")
          .append(cost).append("\n");
      startTime = record.getFirst();
    }
    return buf.toString();
  }
}
