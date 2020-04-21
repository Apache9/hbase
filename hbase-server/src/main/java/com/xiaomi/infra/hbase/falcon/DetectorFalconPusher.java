/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.xiaomi.infra.hbase.falcon;

import java.util.Collection;
import java.util.Collections;
import java.util.stream.Collectors;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DetectorFalconPusher {

  private static final Logger LOG = LoggerFactory.getLogger(DetectorFalconPusher.class);

  private static final String HBASE_ONCALL_ROBOT_PUSH_FALCON_ENABLE =
      "hbase.oncall.robot.push.falcon.enable";
  private static final boolean DEFAULT_HBASE_ONCALL_ROBOT_PUSH_FALCON_ENABLE = false;

  private static final String FALCON_ENDPOINT = "hbase-oncall-robot";

  private static final String FALCON_METRICS = "failure-process";

  private static HttpClient client = new HttpClient();

  private String clusterName;

  private String falconUri;

  private String type;

  private String commonTags;

  public static DetectorFalconPusher getInstance(Configuration conf, String metric) {
    boolean enabled = conf.getBoolean(HBASE_ONCALL_ROBOT_PUSH_FALCON_ENABLE, DEFAULT_HBASE_ONCALL_ROBOT_PUSH_FALCON_ENABLE);
    if (enabled) {
      DetectorFalconPusher detectorFalconPusher = new DetectorFalconPusher();
      detectorFalconPusher.init(conf, metric);
      return detectorFalconPusher;
    }
    return new DummyDetectorFalconPusher();
  }

  private DetectorFalconPusher() {
  }

  private void init(Configuration conf, String type) {
    this.clusterName = conf.get(HConstants.CLUSTER_NAME, "unknown");
    this.falconUri = conf.get("hbase.canary.sink.falcon.uri", HConstants.DEFAULT_FALCON_URI);
    this.type = type;
    this.commonTags = "srv=hbase,cluster=" + clusterName + ",type=" + type + ",";
  }

  public void trigger(String host) {
    trigger(Collections.singletonList(host));
  }

  public void trigger(Collection<String> hosts) {
    pushToFalcon(hosts, 1);
  }

  public void resume(String host) {
    resume(Collections.singletonList(host));
  }

  public void resume(Collection<String> hosts) {
    pushToFalcon(hosts, 0);
  }

  private void pushToFalcon(Collection<String> hosts, int onOrOff) {
    if (hosts == null || hosts.isEmpty()) {
      return;
    }
    PostMethod post = new PostMethod(falconUri);
    JSONArray data = new JSONArray();
    try {
      for (String host : hosts) {
        data.put(buildFalconMetric(host, onOrOff));
      }
    } catch (Exception e) {
      LOG.warn("Failed to create falcon json, type {}, hosts {}, onOrOff {}", type,
          hosts.stream().collect(Collectors.joining(",", "[", "]")), onOrOff, e);
    }
    post.setRequestBody(data.toString());
    try {
      client.executeMethod(post);
    } catch (Exception e) {
      LOG.warn("Failed to push metrics to falcon, type {}, data {}", type, data, e);
    }
  }

  private JSONObject buildFalconMetric(String host, int onOrOff) throws Exception {
    JSONObject json = new JSONObject();
    json.put("endpoint", FALCON_ENDPOINT);
    json.put("metric", FALCON_METRICS);
    json.put("timestamp", System.currentTimeMillis() / 1000);
    json.put("value", onOrOff);
    json.put("step", 60);
    json.put("counterType", "GAUGE");
    json.put("tags", commonTags + "host=" + host);
    return json;
  }

  /**
   * do nothing
   */
  public static class DummyDetectorFalconPusher extends DetectorFalconPusher {

    private DummyDetectorFalconPusher() {
    }

    @Override
    public void trigger(Collection<String> hosts) {
    }

    @Override
    public void resume(Collection<String> hosts) {
    }
  }
}
