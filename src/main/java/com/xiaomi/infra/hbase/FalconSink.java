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
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.tool.Canary.Sink;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.xiaomi.infra.base.nameservice.ClusterInfo;
import com.xiaomi.infra.base.nameservice.ZkClusterInfo.ClusterType;

public class FalconSink implements Sink, Configurable {
  private static final Log LOG = LogFactory.getLog(FalconSink.class);
  private static final String DEFAULT_FALCON_URI = "http://127.0.0.1:1988/v1/push";
  private static final String DEFAULT_COLLECTOR_URI = "http://10.105.5.111:8000/canary/push_metric/";

  private Configuration conf;
  private HttpClient client = new HttpClient();
  private AtomicLong failedReadCounter = new AtomicLong(0);
  private AtomicLong totalReadCounter = new AtomicLong(0);
  private AtomicLong failedWriteCounter = new AtomicLong(0);
  private AtomicLong totalWriteCounter = new AtomicLong(0);

  private FalconSink() {
  }

  @Override
  public void publishReadFailure(HRegionInfo region, Exception e) {
    failedReadCounter.incrementAndGet();
    totalReadCounter.incrementAndGet();
    LOG.error(String.format("read from region %s failed", region.getRegionNameAsString()), e);
  }

  @Override
  public void publishReadFailure(HRegionInfo region, HColumnDescriptor column, Exception e) {
    failedReadCounter.incrementAndGet();
    totalReadCounter.incrementAndGet();
    LOG.error(String.format("read from region %s column family %s failed",
      region.getRegionNameAsString(), column.getNameAsString()), e);
  }

  @Override
  public void publishReadTiming(HRegionInfo region, HColumnDescriptor column, long msTime) {
    totalReadCounter.incrementAndGet();
    if (msTime > 500) {
      LOG.info(String.format("read from region %s column family %s in %dms",
        region.getRegionNameAsString(), column.getNameAsString(), msTime));
    }
  }

  @Override
  public void publishWriteFailure(HRegionInfo region, Exception e) {
    failedWriteCounter.incrementAndGet();
    totalWriteCounter.incrementAndGet();
    LOG.error(String.format("write to region %s failed", region.getRegionNameAsString()), e);
  }

  @Override
  public void publishWriteFailure(HRegionInfo region, HColumnDescriptor column,
      Exception e) {
    failedWriteCounter.incrementAndGet();
    totalWriteCounter.incrementAndGet();
    LOG.error(String.format("write to region %s column family %s failed",
      region.getRegionNameAsString(), column.getNameAsString()), e);
  }

  @Override
  public void publishWriteTiming(HRegionInfo region, HColumnDescriptor column,
      long msTime) {
    totalWriteCounter.incrementAndGet();
    if (msTime > 500) {
      LOG.info(String.format("write to region %s column family %s in %dms",
        region.getRegionNameAsString(), column.getNameAsString(), msTime));
    }
  }

  private double calc(AtomicLong failCounter, AtomicLong totalCounter) {
    if (totalCounter.get() == 0) return 100.0;
    double avail = 1.0 - 1.0 * failCounter.get() / totalCounter.get();
    failCounter.set(0);
    totalCounter.set(0);
    return avail * 100;
  }

  private void pushMetrics() {
    if (totalReadCounter.get() == 0L && totalWriteCounter.get() == 0L) {
      return;
    }
    String clusterName = conf.get("hbase.cluster.name", "unknown");
    double readAvail = calc(failedReadCounter, totalReadCounter);
    double writeAvail = calc(failedWriteCounter, totalWriteCounter);
    double avail = (readAvail + writeAvail) /2;
    LOG.info("Try to push metrics to falcon and collector. Cluster: "
        + clusterName + " availability is " + avail + ", read availability is "
        + readAvail + ", write availability is " + writeAvail);
    pushToCollector(clusterName, avail, readAvail, writeAvail);
    pushToFalcon(clusterName, avail, readAvail, writeAvail);
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

  public void pushToCollector(String clusterName, double avail, double readAvail, double writeAvail) {
    String uri = conf.get("hbase.canary.sink.collector.uri", DEFAULT_COLLECTOR_URI);
    PostMethod post = new PostMethod(uri);
    JSONArray data = new JSONArray();
    try {
      data.put(buildCanaryMetric(clusterName, "cluster-availability", avail));
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

  private JSONObject buildFalconMetric(String clusterName, String key, double value) throws Exception {
    JSONObject metric = new JSONObject();
    metric.put("endpoint", "hbase-canary");
    metric.put("metric", key);
    metric.put("timestamp", System.currentTimeMillis() / 1000);
    metric.put("value", value);
    metric.put("step", 60);
    metric.put("counterType", "GAUGE");
    ClusterType type = new ClusterInfo(clusterName).getZkClusterInfo().getClusterType();
    metric.put("tags", "srv=hbase,type=" + type.toString().toLowerCase() + ",cluster=" + clusterName);
    return metric;
  }

  private void pushToFalcon(String clusterName, double avail, double readAvail, double writeAvail) {
    String uri = conf.get("hbase.canary.sink.falcon.uri", DEFAULT_FALCON_URI);
    PostMethod post = new PostMethod(uri);
    JSONArray data = new JSONArray();
    try {
      data.put(buildFalconMetric(clusterName, "cluster-availability", avail));
      data.put(buildFalconMetric(clusterName, "cluster-read-availability", readAvail));
      data.put(buildFalconMetric(clusterName, "cluster-write-availability", writeAvail));
    } catch (Exception e) {
      LOG.error("Create json error.", e);
    }
    post.setRequestBody(data.toString());
    try {
      client.executeMethod(post);
    } catch (IOException e) {
      LOG.info("Push metrics to falcon failed", e);
    }
  }

  @Override
  public void reportSummary() {
    pushMetrics();
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }
}
