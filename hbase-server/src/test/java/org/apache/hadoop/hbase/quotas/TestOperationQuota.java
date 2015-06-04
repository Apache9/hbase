package org.apache.hadoop.hbase.quotas;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.QuotaProtos.Throttle;
import org.apache.hadoop.hbase.protobuf.generated.QuotaProtos.TimedQuota;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;


/**
 * Verify the behaviour of the Operation Quota.
 */
@Category({ SmallTests.class })
public class TestOperationQuota {
  
  private static AllowExceedOperationQuota operationQuota;
  
  @Test
  public void testGrabQuota() {
    QuotaLimiter userLimiter = QuotaLimiterFactory.fromThrottle(buildThrottle(10, TimeUnit.MINUTES, 10, TimeUnit.MINUTES));
    QuotaLimiter rsLimiter = QuotaLimiterFactory.fromThrottle(buildThrottle(20, TimeUnit.MINUTES, 20, TimeUnit.MINUTES));
    operationQuota = new AllowExceedOperationQuota(userLimiter, rsLimiter);
    operationQuota.grabQuota(5, 5, 0);
    assertEquals(5, userLimiter.getReadReqsAvailable());
    assertEquals(5, userLimiter.getWriteReqsAvailable());
    assertEquals(15, rsLimiter.getReadReqsAvailable());
    assertEquals(15, rsLimiter.getReadReqsAvailable());
  }
  
  @Test
  public void testCheckQuota() {
    QuotaLimiter userLimiter = QuotaLimiterFactory.fromThrottle(buildThrottle(10, TimeUnit.MINUTES, 10, TimeUnit.MINUTES));
    QuotaLimiter rsLimiter = QuotaLimiterFactory.fromThrottle(buildThrottle(10, TimeUnit.MINUTES, 10, TimeUnit.MINUTES));
    operationQuota = new AllowExceedOperationQuota(userLimiter, rsLimiter);
    operationQuota.grabQuota(10, 10, 0);
    // user and rs both have no avail quota.
    runWithExpectedException(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        operationQuota.checkQuota(10, 10, 0);
        return null;
      }
    }, ThrottlingException.class);
  }
  
  @Test
  public void testCheckQuotaAllowExceed() {
    QuotaLimiter userLimiter = QuotaLimiterFactory.fromThrottle(buildThrottle(10, TimeUnit.MINUTES, 10, TimeUnit.MINUTES));
    QuotaLimiter rsLimiter = QuotaLimiterFactory.fromThrottle(buildThrottle(20, TimeUnit.MINUTES, 20, TimeUnit.MINUTES));
    operationQuota = new AllowExceedOperationQuota(userLimiter, rsLimiter);
    operationQuota.grabQuota(10, 10, 0);
    assertEquals(0, userLimiter.getReadReqsAvailable());
    assertEquals(0, userLimiter.getWriteReqsAvailable());
    assertEquals(10, rsLimiter.getReadReqsAvailable());
    assertEquals(10, rsLimiter.getReadReqsAvailable());
    // user have no avail quota, but rs have avail quota. Test if allow exceed
    try {
      operationQuota.checkQuota(10, 10, 0);
    } catch (ThrottlingException te) {
      fail("Allow Exceed, should not thrown exception");
    }
    // allow exceed not influence the user's quota
    assertEquals(0, userLimiter.getReadReqsAvailable());
    assertEquals(0, userLimiter.getWriteReqsAvailable());
    assertEquals(0, rsLimiter.getReadReqsAvailable());
    assertEquals(0, rsLimiter.getReadReqsAvailable());
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
