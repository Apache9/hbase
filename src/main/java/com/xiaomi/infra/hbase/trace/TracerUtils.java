package com.xiaomi.infra.hbase.trace;

import org.apache.hadoop.hbase.ipc.RequestContext;

public class TracerUtils {
  public static void addAnnotation(final String msg) {
    Tracer tracer = RequestContext.getRequestTracer();
    if (tracer != null) {
      tracer.addAnnotation(msg);
    }
  }
}
