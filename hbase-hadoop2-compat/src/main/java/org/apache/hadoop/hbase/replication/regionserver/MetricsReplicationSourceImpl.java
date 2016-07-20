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

package org.apache.hadoop.hbase.replication.regionserver;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.metrics.BaseSourceImpl;
import org.apache.hadoop.hbase.metrics.Interns;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;

/**
 * Hadoop2 implementation of MetricsReplicationSource. This provides access to metrics gauges and
 * counters.
 *
 * Implements BaseSource through BaseSourceImpl, following the pattern
 */
@InterfaceAudience.Private
public class MetricsReplicationSourceImpl extends BaseSourceImpl implements
    MetricsReplicationSource {
  private Map<String, String> peerIdToClusterKey = new ConcurrentHashMap<String, String>();

  public MetricsReplicationSourceImpl() {
    this(METRICS_NAME, METRICS_DESCRIPTION, METRICS_CONTEXT, METRICS_JMX_CONTEXT);
  }

  MetricsReplicationSourceImpl(String metricsName,
                               String metricsDescription,
                               String metricsContext,
                               String metricsJmxContext) {
    super(metricsName, metricsDescription, metricsContext, metricsJmxContext);
  }
  
  protected void addPeer(String peerId, String clusterKey) {
    peerIdToClusterKey.put(peerId, clusterKey);
  }
  
  protected void removePeer(String peerId) {
    peerIdToClusterKey.remove(peerId);
  }
  
  @Override
  public void getMetrics(MetricsCollector metricsCollector, boolean all) {
    MetricsRecordBuilder mrb = metricsCollector.addRecord(metricsName);
    for (Entry<String, String> entry : peerIdToClusterKey.entrySet()) {
      mrb.tag(Interns.info("source." + entry.getKey() + ".clusterKey", ""), entry.getValue());
    }
    metricsRegistry.snapshot(mrb, all);
  }
}
