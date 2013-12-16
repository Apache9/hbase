package com.xiaomi.infra.hbase.trace;

public interface Tracer {
  void addAnnotation(final String msg);

  void stop();

  long getAccumulatedMillis();

  @Override
  String toString();
}
