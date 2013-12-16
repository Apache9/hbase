package com.xiaomi.infra.hbase.trace;

import static org.junit.Assert.*;

import org.apache.hadoop.hbase.SmallTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestMilliTracer {
  @Test
  public void test() throws InterruptedException {
    Tracer tracer = new MilliTracer("A litte test");
    Thread.sleep(100);
    tracer.addAnnotation("step 1");
    Thread.sleep(100);
    tracer.addAnnotation("step 2");

    tracer.stop();
    assertTrue(tracer.getAccumulatedMillis() >= 200);
    String log = tracer.toString();
    assertTrue(log.startsWith("Slow Rpc Trace"));
  }
}
