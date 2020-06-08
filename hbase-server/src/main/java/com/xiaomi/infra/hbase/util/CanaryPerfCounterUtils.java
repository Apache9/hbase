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

package com.xiaomi.infra.hbase.util;

import static com.xiaomi.common.perfcounter.PerfCounter.count;
import static com.xiaomi.miliao.counter.MultiCounter.FAIL_SUFFIX;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.xiaomi.infra.thirdparty.com.google.common.annotations.VisibleForTesting;

@InterfaceAudience.Private
public final class CanaryPerfCounterUtils {

  private static final Logger LOG = LoggerFactory.getLogger(CanaryPerfCounterUtils.class);

  public final static String HBASE_URI_PREFIX = "hbase://";

  public static final String HBASE_CANARY_PREFIX = "hbase-canary-";

  public static final String PERFCOUNT_NAME_TAG_DELIMITER = ",";

  private static String HOSTNAME;

  static {
    try {
      HOSTNAME = InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      LOG.error("Failed to get hostname.", e);
      HOSTNAME = "localhost";
    }
  }

  public static void addCounter(String cluster, String method, TableName tableName, long time) {
    count(constructClusterPerfcountName(cluster, method).getName(), 1, time);
    count(constructTablePerfcountName(cluster, method, tableName).getName(), 1, time);
  }

  public static void addFailCounter(String cluster, String method, TableName tableName) {
    count(constructClusterPerfcountName(cluster, method).failed().getName(), 1);
    count(constructTablePerfcountName(cluster, method, tableName).failed().getName(), 1);
  }

  private static PerfCounterNameJoiner constructClusterPerfcountName(String cluster, String method) {
    return new PerfCounterNameJoiner().method(method).cluster(cluster).host();
  }

  private static PerfCounterNameJoiner constructTablePerfcountName(String cluster, String method,
      TableName tableName) {
    return new PerfCounterNameJoiner().method(method).cluster(cluster).table(tableName).host();
  }

  private static String parseClusterName(String cluster) {
    cluster = cluster.trim();
    if (cluster.startsWith(HBASE_URI_PREFIX)) {
      cluster = cluster.substring(HBASE_URI_PREFIX.length());
    }
    if (cluster.endsWith("/")) {
      cluster = cluster.substring(0, cluster.length() - 1);
    }
    return cluster;
  }

  static class PerfCounterNameJoiner {

    private String method;
    private List<Pair<String, String>> tags = new ArrayList<>();
    private boolean failed;

    public PerfCounterNameJoiner method(String method) {
      this.method = method;
      return this;
    }

    public PerfCounterNameJoiner cluster(String cluster) {
      return append("cluster", parseClusterName(cluster));
    }

    public PerfCounterNameJoiner table(TableName tableName) {
      return append("table", tableName.getNameAsString());
    }

    public PerfCounterNameJoiner host() {
      return append("host", HOSTNAME);
    }

    public PerfCounterNameJoiner append(String key, String val) {
      tags.add(Pair.newPair(key, val));
      return this;
    }

    public PerfCounterNameJoiner failed() {
      this.failed = true;
      return this;
    }

    public String getName() {
      String prefix = HBASE_CANARY_PREFIX + (failed ? method + FAIL_SUFFIX : method)
          + PERFCOUNT_NAME_TAG_DELIMITER;
      StringJoiner joiner = new StringJoiner(PERFCOUNT_NAME_TAG_DELIMITER, prefix, "");
      tags.forEach(kv -> joiner.add(kv.getFirst() + "=" + kv.getSecond()));
      return joiner.toString();
    }
  }

  @VisibleForTesting
  public static String getHostname() {
    return HOSTNAME;
  }
}
