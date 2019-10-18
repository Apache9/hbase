/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.quotas;

import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.quotas.OperationQuota.ReadOperationType;
import org.apache.hadoop.hbase.quotas.QuotaLimiter.QuotaLimiterType;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.Quotas;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.Throttle;

import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestTimeBasedLimiter {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestTimeBasedLimiter.class);

  @Test
  public void testAvgOperationSize() {
    Quotas quotas = Quotas.newBuilder()
        .setThrottle(Throttle.newBuilder()
            .setReqNum(ProtobufUtil.toTimedQuota(1, TimeUnit.MINUTES, QuotaScope.MACHINE)).build())
        .build();
    QuotaLimiter limiter = QuotaLimiterFactory.fromThrottle(quotas.getThrottle(), "user1-table2",
      QuotaLimiterType.USER_TABLE);
    Assert.assertEquals(0, limiter.getAverageOperationSize(ReadOperationType.GET));
    Assert.assertEquals(0, limiter.getAverageOperationSize(ReadOperationType.SCAN));

    limiter.addOperationCountAndSize(ReadOperationType.GET, 1, 200);
    limiter.addOperationCountAndSize(ReadOperationType.SCAN, 2, 500);
    Assert.assertEquals(200, limiter.getAverageOperationSize(ReadOperationType.GET));
    Assert.assertEquals(250, limiter.getAverageOperationSize(ReadOperationType.SCAN));

    limiter.addOperationCountAndSize(ReadOperationType.GET, 1, 400);
    limiter.addOperationCountAndSize(ReadOperationType.SCAN, 4, 700);
    Assert.assertEquals(300, limiter.getAverageOperationSize(ReadOperationType.GET));
    Assert.assertEquals(200, limiter.getAverageOperationSize(ReadOperationType.SCAN));
  }
}
