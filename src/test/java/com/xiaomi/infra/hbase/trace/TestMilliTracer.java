package com.xiaomi.infra.hbase.trace;

import static org.junit.Assert.*;

import org.apache.hadoop.hbase.SmallTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestMilliTracer {
  @Test
  public void testBasic() throws InterruptedException {
    Tracer tracer = new MilliTracer("A litte test", Integer.MAX_VALUE);
    Thread.sleep(100);
    tracer.addAnnotation("step 1");
    Thread.sleep(100);
    tracer.addAnnotation("step 2");

    tracer.stop();
    assertTrue(tracer.getAccumulatedMillis() >= 200);
    String log = tracer.toString();
    assertTrue(log.startsWith("Slow Rpc Trace"));
  }

  @Test
  public void testReachLimit() {
    Tracer tracer = new MilliTracer("testReachLimit", 3);
    tracer.addAnnotation("step 1");
    tracer.stop();
    String log = tracer.toString();
    System.out.println(log);
    assertFalse(log.indexOf("DISCARDED") > 0);
    tracer = new MilliTracer("testReachLimit", 3);
    tracer.addAnnotation("step 1");
    tracer.addAnnotation("step 2");
    tracer.addAnnotation("step 3");
    tracer.addAnnotation("step 4");
    tracer.stop();
    log = tracer.toString();
    System.out.println(log);
    assertTrue(log.indexOf("DISCARDED") > 0);
    assertTrue(log.indexOf("step 1") > 0);
    assertTrue(log.indexOf("step 2") > 0);
    assertTrue(log.indexOf("step 3") < 0);
    assertTrue(log.indexOf("step 4") < 0);
  }
}
