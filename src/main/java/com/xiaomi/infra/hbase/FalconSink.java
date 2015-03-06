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
import java.util.Timer;
import java.util.TimerTask;
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

public class FalconSink implements Sink, Configurable {
  private static final Log LOG = LogFactory.getLog(FalconSink.class);
  private static final String URI = "http://127.0.0.1:1988/v1/push";
  private static final int PERIOD = 60; // s

  private Configuration conf;
  private HttpClient client = new HttpClient();
  private AtomicLong failCounter = new AtomicLong(0);
  private AtomicLong totalCounter = new AtomicLong(0);

  private FalconSink() {
    new Timer(true).schedule(new TimerTask() {
      @Override
      public void run() {
        pushMetrics();
      }
    }, PERIOD * 1000, PERIOD * 1000);
  }

  @Override
  public void publishReadFailure(HRegionInfo region) {
    failCounter.incrementAndGet();
    totalCounter.incrementAndGet();
    LOG.error(String.format("read from region %s failed", region.getRegionNameAsString()));
  }

  @Override
  public void publishReadFailure(HRegionInfo region, HColumnDescriptor column) {
    failCounter.incrementAndGet();
    totalCounter.incrementAndGet();
    LOG.error(String.format("read from region %s column family %s failed",
      region.getRegionNameAsString(), column.getNameAsString()));
  }

  @Override
  public void publishReadTiming(HRegionInfo region, HColumnDescriptor column, long msTime) {
    totalCounter.incrementAndGet();
    LOG.info(String.format("read from region %s column family %s in %dms",
      region.getRegionNameAsString(), column.getNameAsString(), msTime));
  }

  private double calc() {
    if (totalCounter.get() == 0) return 100.0;
    double avail = 1.0 - 1.0 * failCounter.get() / totalCounter.get();
    failCounter.set(0);
    totalCounter.set(0);
    return avail * 100;
  }

  private void pushMetrics() {
    String clusterName = conf.get("hbase.cluster.name", "unknown");
    double avail = calc();
    LOG.info("Try to push metrics to falcon. Cluster: " + clusterName + " availability is " + avail);
    PostMethod post = new PostMethod(URI);
    JSONArray data = new JSONArray();
    try {
      JSONObject metric = new JSONObject();
      metric.put("endpoint", "hbase-canary");
      metric.put("metric", "cluster-availability");
      metric.put("timestamp", System.currentTimeMillis() / 1000);
      metric.put("value", avail);
      metric.put("step", PERIOD);
      metric.put("counterType", "GAUGE");
      metric.put("tags", "srv=hbase-" + clusterName);
      data.put(metric);
    } catch (JSONException e) {
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
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }
}
