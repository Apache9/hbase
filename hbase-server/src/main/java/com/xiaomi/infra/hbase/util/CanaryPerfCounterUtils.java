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
import static com.xiaomi.miliao.counter.MultiCounter.PATH_SEPARATOR_STRING;

import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class CanaryPerfCounterUtils {

  private static final Logger LOG = LoggerFactory.getLogger(CanaryPerfCounterUtils.class);

  public final static String HBASE_URI_PREFIX = "hbase://";

  public static final String HBASE_CANARY_PREFIX = "hbase-canary-";

  public static final String PERFCOUNT_NAME_TAG_DELIMITER = ",";

  public static void addCounter(String cluster, String method, TableName tableName, long time) {
    count(constructClusterPerfcountName(cluster, method).getName(), 1, time);
    count(constructTablePerfcountName(cluster, method, tableName).getName(), 1, time);
  }

  public static void addFailCounter(String cluster, String method, TableName tableName) {
    count(constructClusterPerfcountName(cluster, method).failed().getName(), 1);
    count(constructTablePerfcountName(cluster, method, tableName).failed().getName(), 1);
  }

  private static PerfCounterNameJoiner constructClusterPerfcountName(String cluster, String method) {
    return new PerfCounterNameJoiner(HBASE_CANARY_PREFIX + method).appendCluster(cluster);
  }

  private static PerfCounterNameJoiner constructTablePerfcountName(String cluster, String method,
      TableName tableName) {
    return new PerfCounterNameJoiner(HBASE_CANARY_PREFIX + method)
        .appendCluster(cluster).appendTable(tableName);
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

    private String prefix;
    private List<Pair<String, String>> tags;
    private boolean failed;

    PerfCounterNameJoiner(String prefix) {
      this.prefix = prefix;
      tags = new ArrayList<>();
    }

    public PerfCounterNameJoiner appendCluster(String cluster) {
      return append("cluster", parseClusterName(cluster));
    }

    public PerfCounterNameJoiner appendTable(TableName tableName) {
      return append("table", tableName.getNameAsString());
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
      prefix = (failed ? prefix + FAIL_SUFFIX : prefix) + PATH_SEPARATOR_STRING;
      StringJoiner joiner = new StringJoiner(PERFCOUNT_NAME_TAG_DELIMITER, prefix, "");
      tags.forEach(kv -> joiner.add(kv.getFirst() + "=" + kv.getSecond()));
      return joiner.toString();
    }
  }
}
