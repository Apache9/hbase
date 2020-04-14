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

package com.xiaomi.infra.hbase;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.tool.Canary.Sink;
import org.apache.hadoop.hbase.util.Pair;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.xiaomi.infra.base.nameservice.ClusterInfo;
import com.xiaomi.infra.base.nameservice.ZkClusterInfo.ClusterType;
import com.xiaomi.infra.hbase.util.CanaryPerfCounterUtils;

public class FalconSink implements Sink, Configurable {
  private static final Log LOG = LogFactory.getLog(FalconSink.class);
  private static final String DEFAULT_COLLECTOR_URI = "http://10.105.5.111:8000/canary/push_metric/";
  private static final int DEFAULT_REPLICATION_LAG_UPPER_IN_SECONDS = 10800;
  private static final int DEFAULT_REPLICATION_LAG_LOWER_IN_SECONDS = 10;

  private static final String PUSH_REPLICATION_AVAIL_INTERVAL =
      "hbase.canary.push.replication.availability.interval";
  private static final int DEFAULT_PUSH_REPLICATION_AVAIL_INTERVAL = 600; // 10mins
  private int pushRepAvailInterval = DEFAULT_PUSH_REPLICATION_AVAIL_INTERVAL;
  private long lastPushedRepAvailTime = 0;

  private Configuration conf;
  private HttpClient client = new HttpClient();
  private AtomicLong failedReadCounter = new AtomicLong(0);
  private AtomicLong totalReadCounter = new AtomicLong(0);
  private AtomicLong failedWriteCounter = new AtomicLong(0);
  private AtomicLong totalWriteCounter = new AtomicLong(0);
  private long replicationSlavesCount = 0;
  private double replicationAvailSum = 0;
  private long oldWalsFilesCount = 0;
  private boolean ignoreFushToNet;
  private double readAvailability = 100.0;
  private double writeAvailability = 100.0;
  private double availability = 100.0;
  private int upperRplicationLagInSeconds;
  private int lowerRplicationLagInSeconds;
  private double masterAvailability = 100.0;
  private boolean enablePushTableMinAvailability;
  private boolean enablePushRSMinAvailability;
  private Pair<TableName, Double> tableMinAvailabilityPair = new Pair<>();
  private Pair<ServerName, Double> regionServerMinAvailabilityPair = new Pair<>();

  private String clusterName;
  private Map<String, Long> lastReplicationLags = new HashMap<>();

  private FalconSink() {
  }

  public void publishOldWalsFilesCount(long count){
    oldWalsFilesCount = count;
  }

  @Override
  public void publishReadFailure(HRegionInfo region, Throwable e) {
    failedReadCounter.incrementAndGet();
    totalReadCounter.incrementAndGet();
    CanaryPerfCounterUtils.addFailCounter(clusterName, "read", region.getTable());
    LOG.error(String.format("read from region %s failed", region.getRegionNameAsString()), e);
  }

  @Override
  public void publishReadFailure(HRegionInfo region, HColumnDescriptor column, Throwable e) {
    failedReadCounter.incrementAndGet();
    totalReadCounter.incrementAndGet();
    CanaryPerfCounterUtils.addFailCounter(clusterName, "read", region.getTable());
    LOG.error(String.format("read from region %s column family %s failed",
      region.getRegionNameAsString(), column.getNameAsString()), e);
  }

  @Override
  public void publishReadTiming(HRegionInfo region, HColumnDescriptor column, long msTime) {
    totalReadCounter.incrementAndGet();
    CanaryPerfCounterUtils.addCounter(clusterName, "read", region.getTable(), msTime);
    if (msTime > 500) {
      LOG.info(String.format("read from region %s column family %s in %dms",
        region.getRegionNameAsString(), column.getNameAsString(), msTime));
    }
  }

  @Override
  public void publishWriteFailure(HRegionInfo region, Throwable e) {
    failedWriteCounter.incrementAndGet();
    totalWriteCounter.incrementAndGet();
    CanaryPerfCounterUtils.addFailCounter(clusterName, "write", region.getTable());
    LOG.error(String.format("write to region %s failed", region.getRegionNameAsString()), e);
  }

  @Override
  public void publishWriteFailure(HRegionInfo region, HColumnDescriptor column,
      Throwable e) {
    failedWriteCounter.incrementAndGet();
    totalWriteCounter.incrementAndGet();
    CanaryPerfCounterUtils.addFailCounter(clusterName, "write", region.getTable());
    LOG.error(String.format("write to region %s column family %s failed",
      region.getRegionNameAsString(), column.getNameAsString()), e);
  }

  @Override
  public void publishWriteTiming(HRegionInfo region, HColumnDescriptor column,
      long msTime) {
    totalWriteCounter.incrementAndGet();
    CanaryPerfCounterUtils.addCounter(clusterName, "write", region.getTable(), msTime);
    if (msTime > 500) {
      LOG.info(String.format("write to region %s column family %s in %dms",
        region.getRegionNameAsString(), column.getNameAsString(), msTime));
    }
  }

  @Override
  public void publishReplicationLag(String peerId, long replicationLagInMilliseconds) {
    replicationSlavesCount++;
    double avail = this.calcReplicationPeerAvailability(replicationLagInMilliseconds);
    replicationAvailSum += avail;
    lastReplicationLags.put(peerId, replicationLagInMilliseconds);
    LOG.info("Peer id " + peerId + ": replication lag is " + replicationLagInMilliseconds
        + " ms; replication availability is " + avail + "%");
  }

  @Override
  public void publishMasterAvilability(double availability) {
    this.masterAvailability = availability;
    LOG.info("The availability of HMaster was set to " + availability + "%");
  }

  @Override
  public void publishTableMinAvilability(Pair<TableName, Double> tableNameDoublePair) {
      this.tableMinAvailabilityPair.setFirst(tableNameDoublePair.getFirst());
      this.tableMinAvailabilityPair.setSecond(tableNameDoublePair.getSecond());
      LOG.info("The min availability of table " + tableNameDoublePair.getFirst() + " was set to "
          + tableNameDoublePair.getSecond() + "%");
  }

  @Override
  public void publishRegionServerMinAvilability(Pair<ServerName, Double> serverNameDoublePair) {
      this.regionServerMinAvailabilityPair.setFirst(serverNameDoublePair.getFirst());
      this.regionServerMinAvailabilityPair.setSecond(serverNameDoublePair.getSecond());
      LOG.info("The min availability of regionServer " + serverNameDoublePair.getFirst() + " was set to "
          + serverNameDoublePair.getSecond() + "%");
  }


  @Override
  public double getReadAvailability() {
    return this.readAvailability;
  }

  @Override
  public double getWriteAvailability() {
    return this.writeAvailability;
  }

  @Override
  public double getAvailability() {
    return this.availability;
  }

  private double calc(AtomicLong failCounter, AtomicLong totalCounter) {
    if (totalCounter.get() == 0) return 100.0;
    double avail = 1.0 - 1.0 * failCounter.get() / totalCounter.get();
    failCounter.set(0);
    totalCounter.set(0);
    return avail * 100;
  }

  private double calcReplicationPeerAvailability(long replicationLagInMillionseconds) {
    double ret;
    if (replicationLagInMillionseconds < this.lowerRplicationLagInSeconds * 1000) {
      ret = 1.0;
    } else if (replicationLagInMillionseconds > this.upperRplicationLagInSeconds * 1000) {
      ret = 0.0;
    } else {
      ret = (this.upperRplicationLagInSeconds * 1000 - replicationLagInMillionseconds) / (double) (
          (this.upperRplicationLagInSeconds - this.lowerRplicationLagInSeconds) * 1000);
    }
    return ret * 100.0;
  }

  private void pushMetrics() {
    if (totalReadCounter.get() == 0L && totalWriteCounter.get() == 0L) {
      return;
    }
    String sniffCountStr = "failedReadCount=" + failedReadCounter.get() + ", totalReadCount="
            + totalReadCounter.get() + ", failedWriteCounter=" + failedWriteCounter.get()
            + ", totalWriterCount=" + totalWriteCounter.get();
    readAvailability = calc(failedReadCounter, totalReadCounter);
    writeAvailability = calc(failedWriteCounter, totalWriteCounter);
    availability = (readAvailability + writeAvailability) / 2;

    double replicationAvailability =
        0 == this.replicationSlavesCount ? 100.0 : this.replicationAvailSum / this.replicationSlavesCount;
    this.replicationSlavesCount = 0;
    this.replicationAvailSum = 0;

    LOG.info("Try to push metrics to falcon and collector. Cluster: " + clusterName
        + " availability is " + availability + ", read availability is " + readAvailability
        + ", write availability is " + writeAvailability
        + ", average replication availability is " + replicationAvailability + ", " + sniffCountStr);

    if (!ignoreFushToNet) {
      pushToCollector(clusterName, availability, readAvailability, writeAvailability,
          replicationAvailability);
      pushToFalcon(clusterName, availability, readAvailability, writeAvailability,
          replicationAvailability);
    }
    lastReplicationLags.clear();
  }

  private JSONObject buildCanaryMetric(String clusterName, String key, double value) throws JSONException {
    JSONObject metric = new JSONObject();
    metric.put("service", "hbase");
    metric.put("cluster", clusterName);
    metric.put("name", key);
    metric.put("timestamp", System.currentTimeMillis() / 1000);
    metric.put("value", value);
    metric.put("unit", "%");
    return metric;
  }

  public void pushToCollector(String clusterName, double avail, double readAvail, double writeAvail,
      double replicationAvail) {
    String uri = conf.get("hbase.canary.sink.collector.uri", DEFAULT_COLLECTOR_URI);
    PostMethod post = new PostMethod(uri);
    JSONArray data = new JSONArray();
    try {
      JSONObject metric = buildCanaryMetric(clusterName, "cluster-availability", avail);
      metric.put("replicationavail", replicationAvail);
      data.put(metric);
      data.put(buildCanaryMetric(clusterName, "cluster-read-availability", readAvail));
      data.put(buildCanaryMetric(clusterName, "cluster-write-availability", writeAvail));
    } catch (JSONException e) {
      LOG.error("Create json error.", e);
    }
    post.setRequestBody(data.toString());
    try {
      client.executeMethod(post);
    } catch (IOException e) {
      LOG.info("Push metrics to collector failed", e);
    }
  }

  private <V> JSONObject buildFalconMetric(String clusterName, String key, V value, Map<String, String> customTags) throws Exception {
    JSONObject metric = new JSONObject();
    metric.put("endpoint", "hbase-canary");
    metric.put("metric", key);
    metric.put("timestamp", System.currentTimeMillis() / 1000);
    metric.put("value", value);
    metric.put("step", 60);
    metric.put("counterType", "GAUGE");
    ClusterType type = new ClusterInfo(clusterName).getZkClusterInfo().getClusterType();
    StringBuilder customTagStr = new StringBuilder();
    customTags.forEach((tagk, tagv) -> {
      customTagStr.append("," + tagk + "=");
      customTagStr.append(tagv);
    });
    metric.put("tags",
        "srv=hbase,type=" + type.toString().toLowerCase() + ",cluster=" + clusterName + customTagStr.toString());
    return metric;
  }

  private JSONObject buildFalconMetric(String clusterName, String key, double value) throws Exception {
    return buildFalconMetric(clusterName, key, value, Collections.emptyMap());
  }

  private void pushToFalcon(String clusterName, double avail, double readAvail, double writeAvail,
      double replicationAvail) {
    String uri = conf.get("hbase.canary.sink.falcon.uri", HConstants.DEFAULT_FALCON_URI);
    PostMethod post = new PostMethod(uri);
    JSONArray data = new JSONArray();
    try {
      data.put(buildFalconMetric(clusterName, "cluster-master-availability",
          this.masterAvailability));
      data.put(buildFalconMetric(clusterName, "cluster-availability", avail));
      data.put(buildFalconMetric(clusterName, "cluster-read-availability", readAvail));
      data.put(buildFalconMetric(clusterName, "cluster-write-availability", writeAvail));
      data.put(buildFalconMetric(clusterName, "cluster-oldWals-files-count", oldWalsFilesCount));
      long now = System.currentTimeMillis();
      if ((now - lastPushedRepAvailTime) > (pushRepAvailInterval * 1000)) {
        data.put(
          buildFalconMetric(clusterName, "cluster-replication-availability", replicationAvail));
        lastPushedRepAvailTime = now;
      }
      for (Map.Entry<String, Long> e : lastReplicationLags.entrySet()) {
        data.put(buildFalconMetric(clusterName, "cluster-replication-lag", e.getValue(),
            Collections.singletonMap("peerId", e.getKey())));
      }
      if (tableMinAvailabilityPair.getFirst() != null && enablePushTableMinAvailability) {
        data.put(buildFalconMetric(clusterName, "cluster-table-min-availability",
            this.tableMinAvailabilityPair.getSecond()));
      }
      if (regionServerMinAvailabilityPair.getFirst() != null && enablePushRSMinAvailability) {
        data.put(buildFalconMetric(clusterName, "cluster-rs-min-availability",
            this.regionServerMinAvailabilityPair.getSecond()));
      }
    } catch (Exception e) {
      LOG.error("Create json error.", e);
    }
    post.setRequestBody(data.toString());
    try {
      client.executeMethod(post);
    } catch (IOException e) {
      LOG.warn("Push metrics to falcon failed", e);
    }
  }

  @Override
  public void reportSummary() {
    pushMetrics();
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    clusterName = conf.get("hbase.cluster.name", "unknown");
    this.ignoreFushToNet = conf.getBoolean("hbase.canary.sink.falcon.ignore.push.to.net", false);
    LOG.info("falcon sink ignore push to net : " + this.ignoreFushToNet);
    upperRplicationLagInSeconds = conf.getInt("hbase.canary.replication.lag.upper.bound.in.seconds",
        DEFAULT_REPLICATION_LAG_UPPER_IN_SECONDS);
    lowerRplicationLagInSeconds = conf.getInt("hbase.canary.replication.lag.lower.bound.in.seconds",
        DEFAULT_REPLICATION_LAG_LOWER_IN_SECONDS);
    pushRepAvailInterval =
        conf.getInt(PUSH_REPLICATION_AVAIL_INTERVAL, DEFAULT_PUSH_REPLICATION_AVAIL_INTERVAL);
    enablePushTableMinAvailability = conf.getBoolean(HConstants.CANARY_PUSH_TABLE_MIN_AVAIL_ENABLE,
      HConstants.CANARY_PUSH_TABLE_MIN_AVAIL_ENABLE_DEFAULT);
    enablePushRSMinAvailability = conf.getBoolean(HConstants.CANARY_PUSH_RS_MIN_AVAIL_ENABLE,
        HConstants.CANARY_PUSH_RS_MIN_AVAIL_ENABLE_DEFAULT);
  }

  @Override
  public Configuration getConf() {
    return conf;
  }
}
