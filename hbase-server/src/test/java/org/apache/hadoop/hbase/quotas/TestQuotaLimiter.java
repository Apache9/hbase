package org.apache.hadoop.hbase.quotas;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.QuotaProtos.Throttle;
import org.apache.hadoop.hbase.protobuf.generated.QuotaProtos.TimedQuota;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Verify the behaviour of the Quota Limiter.
 */
@Category({ SmallTests.class })
public class TestQuotaLimiter {

  private static final Log LOG = LogFactory.getLog(TimeBasedLimiter.class);
  
  private static QuotaLimiter quotaLimiter;
  
  @Test
  public void testGrabQuotaByRequestUnit() {
    Throttle throttle = buildThrottle(10, TimeUnit.MINUTES, 20, TimeUnit.MINUTES);
    quotaLimiter = QuotaLimiterFactory.fromThrottle(throttle);
    quotaLimiter.grabQuotaByRequestUnit(5, 5);
    assertEquals(5, quotaLimiter.getReadReqsAvailable());
    assertEquals(15, quotaLimiter.getWriteReqsAvailable());
  }
  
  
  @Test
  public void testCheckQuotaByRequestUnit() {
    Throttle throttle = buildThrottle(10, TimeUnit.MINUTES, 10, TimeUnit.MINUTES);
    quotaLimiter = QuotaLimiterFactory.fromThrottle(throttle);
    quotaLimiter.grabQuotaByRequestUnit(10, 10);
    runWithExpectedException(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        quotaLimiter.checkQuotaByRequestUnit(1, 0);
        return null;
      }
    }, ThrottlingException.class);
    runWithExpectedException(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        quotaLimiter.checkQuotaByRequestUnit(0, 1);
        return null;
      }
    }, ThrottlingException.class);
    
  }

  private Throttle buildThrottle(int readLimit, TimeUnit readUnit, int writeLimit,
      TimeUnit writeUnit) {
    Throttle.Builder throttle = Throttle.newBuilder();
    TimedQuota.Builder readNum = TimedQuota.newBuilder();
    readNum.setSoftLimit(readLimit).setTimeUnit(ProtobufUtil.toProtoTimeUnit(readUnit));
    throttle.setReadNum(readNum);
    TimedQuota.Builder writeNum = TimedQuota.newBuilder();
    writeNum.setSoftLimit(writeLimit).setTimeUnit(ProtobufUtil.toProtoTimeUnit(writeUnit));
    throttle.setWriteNum(writeNum);
    return throttle.build();
  }

  private static <V, E> void runWithExpectedException(Callable<V> callable, Class<E> exceptionClass) {
    try {
      callable.call();
    } catch (Exception ex) {
      Assert.assertEquals(exceptionClass, ex.getClass());
      return;
    }
    fail("Should have thrown exception " + exceptionClass);
  }
}
