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

package org.apache.hadoop.hbase.master;

import org.apache.hadoop.hbase.metrics.BaseSourceImpl;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class MetricsClusterSourceImpl extends BaseSourceImpl implements MetricsClusterSource {

  private final MetricsMasterWrapper masterWrapper;

  public MetricsClusterSourceImpl(MetricsMasterWrapper masterWrapper) {
    this(METRICS_NAME, METRICS_DESCRIPTION, METRICS_CONTEXT, METRICS_JMX_CONTEXT, masterWrapper);
  }

  public MetricsClusterSourceImpl(String metricsName, String metricsDescription,
      String metricsContext, String metricsJmxContext, MetricsMasterWrapper masterWrapper) {
    super(metricsName, metricsDescription, metricsContext, metricsJmxContext);
    this.masterWrapper = masterWrapper;
  }

  @Override
  public void getMetrics(MetricsCollector metricsCollector, boolean all) {
    MetricsRecordBuilder metricsRecordBuilder = metricsCollector.addRecord(metricsName);
    // masterWrapper can be null because this function is called inside of init.
    if (masterWrapper != null) {
      masterWrapper.addClusterMetrics(metricsRecordBuilder);
    }
    metricsRegistry.snapshot(metricsRecordBuilder, all);
  }
}
