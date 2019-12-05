/**
 *
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
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.QuotaProtos.ThrottleRequest;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class RegionQuotaSettings {
  private final String regionName;
  private final ThrottleRequest throttle;

  public RegionQuotaSettings(final String regionName, final ThrottleRequest throttle) {
    this.regionName = regionName;
    this.throttle = throttle;
  }

  public String getRegionName() {
    return regionName;
  }

  public ThrottleRequest getThrottle() {
    return throttle;
  }

  public ThrottleType getThrottleType() {
    return ProtobufUtil.toThrottleType(throttle.getType());
  }

  public long getLimit() {
    return throttle.getTimedQuota().getSoftLimit();
  }

  public TimeUnit getTimeUnit() {
    return ProtobufUtil.toTimeUnit(throttle.getTimedQuota().getTimeUnit());
  }

  @Override
  public String toString() {
    return "RegionQuotaSettings{" + "regionName='" + regionName + '\'' + ", throttle=" + throttle
        + '}';
  }
}