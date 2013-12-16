/*
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

package org.apache.hadoop.hbase.throughput;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.hbase.LargeTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(LargeTests.class)
public class TestThroughputQuota {
  private void assertRequestLimitsEquals(EnumMap<RequestType, Double> expected,
      EnumMap<RequestType, Double> actual) {
    if (expected == null || expected.isEmpty()) {
      assertTrue(actual == null || actual.isEmpty());
    } else {
      for (Entry<RequestType, Double> e : expected.entrySet()) {
        assertEquals(actual.get(e.getKey()), expected.get(e.getKey()), 1e-3);
      }
    }
  }

  private void assertThroughputQuotaEquals(ThroughputQuota expected,
      ThroughputQuota actual) {
    if (expected == null || actual == null) {
      assertEquals(null, actual);
    } else {
      assertEquals(expected.getTableName(), actual.getTableName());
      Set<String> users = expected.getLimits().keySet();
      users.addAll(actual.getLimits().keySet());
      for (String user : users) {
        assertRequestLimitsEquals(expected.getLimits().get(user),
          actual.getLimits().get(user));
      }
    }
  }

  @Test
  public void testEncodeDecodeThroughputQuota() throws Exception {
    ThroughputQuota expected = null;
    ThroughputQuota actual = null;

    actual = ThroughputQuota.fromBytes(ThroughputQuota.toBytes(actual));
    assertThroughputQuotaEquals(expected, actual);

    Map<String, EnumMap<RequestType, Double>> userQuota =
        new HashMap<String, EnumMap<RequestType, Double>>();
    userQuota.put("user1", new EnumMap<RequestType, Double>(RequestType.class));
    EnumMap<RequestType, Double> reqQuota = new EnumMap<RequestType, Double>(RequestType.class);
    reqQuota.put(RequestType.READ, 0.001);
    reqQuota.put(RequestType.WRITE, 100.0);
    userQuota.put("user2", reqQuota);
    expected = new ThroughputQuota("table1", userQuota);
    actual = ThroughputQuota.fromBytes(ThroughputQuota.toBytes(actual));
    assertThroughputQuotaEquals(expected, actual);
  }
}
