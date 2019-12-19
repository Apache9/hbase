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

import com.xiaomi.infra.base.nameservice.ClusterInfo;
import com.xiaomi.infra.base.nameservice.ZkClusterInfo.ClusterType;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicLong;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.tool.Canary.Sink;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.HConstants;
import org.apache.http.HttpStatus;
import org.apache.http.client.entity.EntityBuilder;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xiaomi.infra.thirdparty.com.google.gson.Gson;
import com.xiaomi.infra.thirdparty.com.google.gson.JsonArray;
import com.xiaomi.infra.thirdparty.com.google.gson.JsonObject;

@InterfaceAudience.Private
public class FalconSink implements Sink, Configurable {
  private static final Logger LOG = LoggerFactory.getLogger(FalconSink.class);
  private static final String DEFAULT_FALCON_URI = "http://127.0.0.1:1988/v1/push";
  private static final String DEFAULT_COLLECTOR_URI =
      "http://canary.d.xiaomi.net/canary/push_metric/";
  private static final Gson GSON = new Gson();

  private Configuration conf;
  private final CloseableHttpClient client = HttpClients.createDefault();
  private final AtomicLong failedReadCounter = new AtomicLong(0);
  private final AtomicLong totalReadCounter = new AtomicLong(0);
  private final AtomicLong failedWriteCounter = new AtomicLong(0);
  private final AtomicLong totalWriteCounter = new AtomicLong(0);
  private static final int DEFAULT_REPLICATION_LAG_UPPER_IN_SECONDS = 10800;
  private static final int DEFAULT_REPLICATION_LAG_LOWER_IN_SECONDS = 10;
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

  private FalconSink() {
  }

  public void publishOldWalsFilesCount(long count) {
    oldWalsFilesCount = count;
  }

  @Override
  public void publishReadFailure(RegionInfo region, Throwable e) {
    failedReadCounter.incrementAndGet();
    totalReadCounter.incrementAndGet();
    LOG.error(String.format("read from region %s failed", region.getRegionNameAsString()), e);
  }

  @Override
  public void publishReadFailure(RegionInfo region, ColumnFamilyDescriptor column, Throwable e) {
    failedReadCounter.incrementAndGet();
    totalReadCounter.incrementAndGet();
    LOG.error(String.format("read from region %s column family %s failed",
      region.getRegionNameAsString(), column.getNameAsString()), e);
  }

  @Override
  public void publishReadTiming(RegionInfo region, ColumnFamilyDescriptor column, long msTime) {
    totalReadCounter.incrementAndGet();
    if (msTime > 500) {
      LOG.info(String.format("read from region %s column family %s in %dms",
        region.getRegionNameAsString(), column.getNameAsString(), msTime));
    }
  }

  @Override
  public void publishWriteFailure(RegionInfo region, Throwable e) {
    failedWriteCounter.incrementAndGet();
    totalWriteCounter.incrementAndGet();
    LOG.error(String.format("write to region %s failed", region.getRegionNameAsString()), e);
  }

  @Override
  public void publishWriteFailure(RegionInfo region, ColumnFamilyDescriptor column, Throwable e) {
    failedWriteCounter.incrementAndGet();
    totalWriteCounter.incrementAndGet();
    LOG.error(String.format("write to region %s column family %s failed",
      region.getRegionNameAsString(), column.getNameAsString()), e);
  }

  @Override
  public void publishWriteTiming(RegionInfo region, ColumnFamilyDescriptor column, long msTime) {
    totalWriteCounter.incrementAndGet();
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
    LOG.info(
        "Peer id " + peerId + ":replication lag is " + replicationLagInMilliseconds + "ms;replication availability is "
            + avail + "%");


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
    String clusterName = conf.get("hbase.cluster.name", "unknown");
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
  }

  private JsonObject buildCanaryMetric(String clusterName, String key, double value) {
    JsonObject metric = new JsonObject();
    metric.addProperty("service", "hbase");
    metric.addProperty("cluster", clusterName);
    metric.addProperty("name", key);
    metric.addProperty("timestamp", System.currentTimeMillis() / 1000);
    metric.addProperty("value", value);
    metric.addProperty("unit", "%");
    return metric;
  }

  public void pushToCollector(String clusterName, double avail, double readAvail,
      double writeAvail, double replicationAvail) {
    JsonArray data = new JsonArray();
    JsonObject clusterMetric = buildCanaryMetric(clusterName, "cluster-availability", avail);
    clusterMetric.addProperty("replicationavail", replicationAvail);
      data.add(clusterMetric);
      data.add(buildCanaryMetric(clusterName, "cluster-read-availability", readAvail));
      data.add(buildCanaryMetric(clusterName, "cluster-write-availability", writeAvail));

    String uri = conf.get("hbase.canary.sink.collector.uri", DEFAULT_COLLECTOR_URI);
    HttpPost post = new HttpPost(uri);
    post.setEntity(EntityBuilder.create().setContentType(ContentType.APPLICATION_JSON)
      .setContentEncoding(StandardCharsets.UTF_8.name()).setText(GSON.toJson(data)).build());
    try (CloseableHttpResponse resp = client.execute(post)) {
      int code = resp.getStatusLine().getStatusCode();
      if (code != HttpStatus.SC_OK) {
        LOG.warn("Push metrics to collector failed, status code={}, content={}", code,
          EntityUtils.toString(resp.getEntity()));
      }
    } catch (IOException e) {
      LOG.warn("Push metrics to collector failed", e);
    }
  }


  private JsonObject buildFalconMetric(String clusterName, String key, double value, Map<String, String> customTags)
      throws IOException {
    JsonObject metric = new JsonObject();
    metric.addProperty("endpoint", "hbase-canary");
    metric.addProperty("metric", key);
    metric.addProperty("timestamp", System.currentTimeMillis() / 1000);
    metric.addProperty("value", value);
    metric.addProperty("step", 60);
    metric.addProperty("counterType", "GAUGE");
    ClusterType type = new ClusterInfo(clusterName).getZkClusterInfo().getClusterType();
    StringBuilder customTagStr = new StringBuilder();
    customTags.forEach((tagk, tagv) -> {
      customTagStr.append("," + tagk + "=");
      customTagStr.append(tagv);
    });
    metric.addProperty("tags",
      "srv=hbase,type=" + type.toString().toLowerCase() + ",cluster=" + clusterName+customTagStr.toString());
    return metric;
  }

  private JsonObject buildFalconMetric(String clusterName, String key, double value) throws IOException {
    return buildFalconMetric(clusterName, key, value, new HashMap<>());
  }


  private void pushToFalcon(String clusterName, double avail, double readAvail, double writeAvail,
      double replicationAvail) {
    JsonArray data = new JsonArray();
    try {
      data.add(buildFalconMetric(clusterName, "cluster-master-availability", this.masterAvailability));
      data.add(buildFalconMetric(clusterName, "cluster-replication-availability", replicationAvail));
      data.add(buildFalconMetric(clusterName, "cluster-availability", avail));
      data.add(buildFalconMetric(clusterName, "cluster-read-availability", readAvail));
      data.add(buildFalconMetric(clusterName, "cluster-write-availability", writeAvail));
      data.add(buildFalconMetric(clusterName, "cluster-oldWals-files-count", oldWalsFilesCount));
      if (tableMinAvailabilityPair.getFirst() != null && enablePushTableMinAvailability) {
        data.add(buildFalconMetric(clusterName, "cluster-table-min-availability",
          this.tableMinAvailabilityPair.getSecond()));
      }
      if (regionServerMinAvailabilityPair.getFirst() != null && enablePushRSMinAvailability) {
        data.add(buildFalconMetric(clusterName, "cluster-rs-min-availability",
          this.regionServerMinAvailabilityPair.getSecond()));
      }
    } catch (IOException e) {
      LOG.error("Create json error", e);
      return;
    }
    String uri = conf.get("hbase.canary.sink.falcon.uri", DEFAULT_FALCON_URI);
    HttpPost post = new HttpPost(uri);
    post.setEntity(EntityBuilder.create().setContentType(ContentType.APPLICATION_JSON)
      .setContentEncoding(StandardCharsets.UTF_8.name()).setText(GSON.toJson(data)).build());
    try (CloseableHttpResponse resp = client.execute(post)) {
      int code = resp.getStatusLine().getStatusCode();
      if (code != HttpStatus.SC_OK) {
        LOG.warn("Push metrics to falcon failed, status code={}, content={}", code,
          EntityUtils.toString(resp.getEntity()));
      }
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
    this.ignoreFushToNet = conf.getBoolean("hbase.canary.sink.falcon.ignore.push.to.net", false);
    LOG.info("falcon sink ignore push to net : " + this.ignoreFushToNet);
    upperRplicationLagInSeconds = conf.getInt("hbase.canary.replication.lag.upper.bound.in.seconds",
        DEFAULT_REPLICATION_LAG_UPPER_IN_SECONDS);
    lowerRplicationLagInSeconds = conf.getInt("hbase.canary.replication.lag.lower.bound.in.seconds",
        DEFAULT_REPLICATION_LAG_LOWER_IN_SECONDS);
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
