package org.apache.hadoop.hbase.quotas;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.QuotaProtos.Throttle;
import org.apache.hadoop.hbase.protobuf.generated.QuotaProtos.TimedQuota;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

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
    try {
      operationQuota.checkQuota(10, 10, 0);
    } catch (ThrottlingException te) {
      fail("quota avail is more than the need, should not thrown exception");
    }
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
    try {
      operationQuota.checkQuota(10, 10, 0);
    } catch (ThrottlingException te) {
      fail("quota avail is more than the need, should not thrown exception");
    }
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
  
  @Test
  public void testUserNoopQuotaLimiter() {
    QuotaLimiter userLimiter = QuotaLimiterFactory.fromThrottle(Throttle.newBuilder().build());
    QuotaLimiter rsLimiter = QuotaLimiterFactory.fromThrottle(buildThrottle(10, TimeUnit.MINUTES, 10, TimeUnit.MINUTES));
    operationQuota = new AllowExceedOperationQuota(userLimiter, rsLimiter);
    try {
      operationQuota.checkQuota(10, 10, 0);
    } catch (ThrottlingException te) {
      fail("quota avail is more than the need, should not thrown exception");
    }
    // rs have no avail quota, userLimiter is NoopQuotaLimiter
    runWithExpectedException(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        operationQuota.checkQuota(10, 10, 0);
        return null;
      }
    }, ThrottlingException.class);
  }

  @Test
  public void testGetAvailable() {
    QuotaLimiter userLimiter = QuotaLimiterFactory.fromThrottle(Throttle.newBuilder().build());
    QuotaLimiter rsLimiter = QuotaLimiterFactory.fromThrottle(buildThrottle(3000, TimeUnit.SECONDS,
      10000, TimeUnit.SECONDS));
    operationQuota = new AllowExceedOperationQuota(userLimiter, rsLimiter);
    assertEquals(3000 * QuotaUtil.READ_CAPACITY_UNIT, operationQuota.getReadAvailable());
    assertEquals(10000 * QuotaUtil.WRITE_CAPACITY_UNIT, operationQuota.getWriteAvailable());

    userLimiter = QuotaLimiterFactory.fromThrottle(buildThrottle(500, TimeUnit.SECONDS,
      1000, TimeUnit.SECONDS));
    operationQuota = new AllowExceedOperationQuota(userLimiter, rsLimiter);
    assertEquals(3000 * QuotaUtil.READ_CAPACITY_UNIT, operationQuota.getReadAvailable());
    assertEquals(10000 * QuotaUtil.WRITE_CAPACITY_UNIT, operationQuota.getWriteAvailable());
  }

  @Test
  public void testCanLogThrottlingException() {
    QuotaLimiter userLimiter = QuotaLimiterFactory.fromThrottle(Throttle.newBuilder().build());
    QuotaLimiter rsLimiter = QuotaLimiterFactory.fromThrottle(buildThrottle(10, TimeUnit.MINUTES, 10, TimeUnit.MINUTES));
    operationQuota = new AllowExceedOperationQuota(userLimiter, rsLimiter);
    // 5 is equals to DEFAULT_MAX_LOG_THROTTLING_COUNT
    for (int i = 0; i < 5; i++) {
      assertTrue(operationQuota.canLogThrottlingException());
    }
    assertFalse(operationQuota.canLogThrottlingException());
    QuotaLimiterFactory.update(rsLimiter, rsLimiter);
    for (int i = 0; i < 5; i++) {
      assertTrue(operationQuota.canLogThrottlingException());
    }
    assertFalse(operationQuota.canLogThrottlingException());
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
