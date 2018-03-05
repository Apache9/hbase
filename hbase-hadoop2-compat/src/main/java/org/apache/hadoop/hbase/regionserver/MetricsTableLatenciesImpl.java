/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.regionserver;

import java.util.HashMap;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.metrics.BaseSourceImpl;
import org.apache.hadoop.metrics2.MetricHistogram;
import org.apache.hadoop.metrics2.lib.DynamicMetricsRegistry;

/**
 * Implementation of {@link MetricsTableLatencies} to track latencies for one table in a
 * RegionServer.
 */
@InterfaceAudience.Private
public class MetricsTableLatenciesImpl extends BaseSourceImpl implements MetricsTableLatencies {

  private final HashMap<TableName,TableHistograms> histogramsByTable = new HashMap<>();

  public HashMap<TableName,TableHistograms> getHistogramsByTable() {
    return histogramsByTable;
  }

  public static class TableHistograms {
    final MetricHistogram getTimeHisto;
    final MetricHistogram putTimeHisto;
    final MetricHistogram scanTimeHisto;
    final MetricHistogram batchTimeHisto;
    final MetricHistogram appendTimeHisto;
    final MetricHistogram deleteTimeHisto;
    final MetricHistogram incrementTimeHisto;

    TableHistograms(DynamicMetricsRegistry registry, TableName tn) {
      getTimeHisto = registry.newTimeHistogram(qualifyMetricsName(tn, GET_TIME));
      putTimeHisto = registry.newTimeHistogram(qualifyMetricsName(tn, PUT_TIME));
      scanTimeHisto = registry.newTimeHistogram(qualifyMetricsName(tn, SCAN_TIME));
      batchTimeHisto = registry.newTimeHistogram(qualifyMetricsName(tn, BATCH_TIME));
      deleteTimeHisto = registry.newTimeHistogram(qualifyMetricsName(tn, DELETE_TIME));
      appendTimeHisto = registry.newTimeHistogram(qualifyMetricsName(tn, APPEND_TIME));
      incrementTimeHisto = registry.newTimeHistogram(qualifyMetricsName(tn, INCREMENT_TIME));
    }

    public void updatePut(long time) {
      putTimeHisto.add(time);
    }

    public void updateGet(long t) {
      getTimeHisto.add(t);
    }

    public void updateScan(long t) {
      scanTimeHisto.add(t);
    }

    public void updateBatch(long time) {
      batchTimeHisto.add(time);
    }

    public void updateDelete(long t) {
      deleteTimeHisto.add(t);
    }

    public void updateAppend(long t) {
      appendTimeHisto.add(t);
    }

    public void updateIncrement(long t) {
      incrementTimeHisto.add(t);
    }
  }

  public static String qualifyMetricsName(TableName tableName, String metric) {
    StringBuilder sb = new StringBuilder();
    sb.append("Namespace_").append(tableName.getNamespaceAsString());
    sb.append("_table_").append(tableName.getQualifierAsString());
    sb.append("_metric_").append(metric);
    return sb.toString();
  }

  public TableHistograms getOrCreateTableHistogram(String tableName) {
    final TableName tn = TableName.valueOf(tableName);
    return histogramsByTable.computeIfAbsent(tn, t -> new TableHistograms(getMetricsRegistry(), tn));
  }

  public MetricsTableLatenciesImpl() {
    this(METRICS_NAME, METRICS_DESCRIPTION, METRICS_CONTEXT, METRICS_JMX_CONTEXT);
  }

  public MetricsTableLatenciesImpl(String metricsName, String metricsDescription,
                                   String metricsContext, String metricsJmxContext) {
    super(metricsName, metricsDescription, metricsContext, metricsJmxContext);
  }

  @Override
  public void updatePut(String tableName, long t) {
    getOrCreateTableHistogram(tableName).updatePut(t);
  }

  @Override
  public void updateGet(String tableName, long t) {
    getOrCreateTableHistogram(tableName).updateGet(t);
  }

  @Override
  public void updateScan(String tableName, long t) {
    getOrCreateTableHistogram(tableName).updateScan(t);
  }

  @Override
  public void updateBatch(String tableName, long t) {
    getOrCreateTableHistogram(tableName).updateBatch(t);
  }

  @Override
  public void updateDelete(String tableName, long t) {
    getOrCreateTableHistogram(tableName).updateDelete(t);
  }

  @Override
  public void updateAppend(String tableName, long t) {
    getOrCreateTableHistogram(tableName).updateAppend(t);
  }

  @Override
  public void updateIncrement(String tableName, long t) {
    getOrCreateTableHistogram(tableName).updateIncrement(t);
  }
}
