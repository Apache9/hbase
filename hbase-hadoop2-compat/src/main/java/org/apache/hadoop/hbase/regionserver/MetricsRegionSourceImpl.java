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

package org.apache.hadoop.hbase.regionserver;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.metrics.Interns;
import org.apache.hadoop.metrics2.MetricHistogram;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.impl.JmxCacheBuster;
import org.apache.hadoop.metrics2.lib.DynamicMetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableFastCounter;

@InterfaceAudience.Private
public class MetricsRegionSourceImpl implements MetricsRegionSource {

  private final MetricsRegionWrapper regionWrapper;

  private volatile boolean closed = false;
  private MetricsRegionAggregateSourceImpl agg;
  private DynamicMetricsRegistry registry;
  private static final Log LOG = LogFactory.getLog(MetricsRegionSourceImpl.class);

  private String regionNamePrefix;
  private String regionPutKey;
  private String regionDeleteKey;
  private String regionGetKey;
  private String regionIncrementKey;
  private String regionAppendKey;
  private String regionScanNextKey;
  private String regionReadKey;
  private String regionWriteKey;
  private String regionThrottledReadKey;
  private String regionThrottledWriteKey;
  private MutableFastCounter regionPut;
  private MutableFastCounter regionDelete;

  private MutableFastCounter regionIncrement;
  private MutableFastCounter regionAppend;

  private MetricHistogram regionGet;
  private MetricHistogram regionScanNext;

  private MetricHistogram regionRead;
  private MetricHistogram regionWrite;

  private MutableFastCounter regionThrottledRead;
  private MutableFastCounter regionThrottledWrite;

  private boolean metricsStringInited;
  private String STORE_COUNT;
  private String STOREFILE_COUNT;
  private String MEMSTORE_SIZE;
  private String STOREFILE_SIZE;
  private String COMPACTIONS_COMPLETED_COUNT;
  private String NUM_BYTES_COMPACTED_COUNT;
  private String NUM_FILES_COMPACTED_COUNT;


  public MetricsRegionSourceImpl(MetricsRegionWrapper regionWrapper,
                                 MetricsRegionAggregateSourceImpl aggregate) {
    this.regionWrapper = regionWrapper;
    agg = aggregate;
    agg.register(regionWrapper.getRegionName(), this);

    LOG.info("Creating new MetricsRegionSourceImpl for table " +
        regionWrapper.getTableName() + " " + regionWrapper.getRegionName());

    registry = agg.getMetricsRegistry();

    regionNamePrefix = "namespace_" + regionWrapper.getNamespace() +
        "_table_" + regionWrapper.getTableName() +
        "_region_" + regionWrapper.getRegionName()  +
        "_metric_";

    String suffix = "Count";

    regionPutKey = regionNamePrefix + MetricsRegionServerSource.MUTATE_KEY + suffix;
    regionPut = registry.getCounter(regionPutKey, 0l);

    regionDeleteKey = regionNamePrefix + MetricsRegionServerSource.DELETE_KEY + suffix;
    regionDelete = registry.getCounter(regionDeleteKey, 0l);

    regionIncrementKey = regionNamePrefix + MetricsRegionServerSource.INCREMENT_KEY + suffix;
    regionIncrement = registry.getCounter(regionIncrementKey, 0l);

    regionAppendKey = regionNamePrefix + MetricsRegionServerSource.APPEND_KEY + suffix;
    regionAppend = registry.getCounter(regionAppendKey, 0l);

    regionGetKey = regionNamePrefix + MetricsRegionServerSource.GET_KEY;
    regionGet = registry.newSizeHistogram(regionGetKey);

    regionScanNextKey = regionNamePrefix + MetricsRegionServerSource.SCAN_NEXT_KEY;
    regionScanNext = registry.newSizeHistogram(regionScanNextKey);

    regionReadKey = regionNamePrefix + MetricsRegionServerSource.READ_KEY;
    regionRead = registry.newSizeHistogram(regionReadKey);

    regionWriteKey = regionNamePrefix + MetricsRegionServerSource.WRITE_KEY;
    regionWrite = registry.newSizeHistogram(regionWriteKey);

    regionThrottledReadKey = regionNamePrefix + MetricsRegionServerSource.THROTTLE_READ_KEY;
    regionThrottledRead = registry.getCounter(regionThrottledReadKey, 0l);

    regionThrottledWriteKey = regionNamePrefix + MetricsRegionServerSource.THROTTLE_WRITE_KEY;
    regionThrottledWrite = registry.getCounter(regionThrottledWriteKey, 0l);
  }

  @Override
  public void close() {
    if (closed) {
      return;
    }

    synchronized (this) {
      // Before removing the metrics remove this region from the aggregate region bean.
      // This should mean that it's unlikely that snapshot and close happen at the same time.
      agg.deregister(regionWrapper.getRegionName());
      closed = true;
    }

    LOG.info("Removing region Metrics: " + regionWrapper.getRegionName());
    registry.removeMetric(regionPutKey);
    registry.removeMetric(regionDeleteKey);
    registry.removeMetric(regionIncrementKey);
    registry.removeMetric(regionAppendKey);
    registry.removeMetric(regionGetKey);
    registry.removeMetric(regionScanNextKey);
    registry.removeMetric(regionReadKey);
    registry.removeMetric(regionWriteKey);
    registry.removeMetric(regionThrottledReadKey);
    registry.removeMetric(regionThrottledWriteKey);
    JmxCacheBuster.clearJmxCache();
  }

  @Override
  public void updatePut() {
    regionPut.incr();
  }

  @Override
  public void updateDelete() {
    regionDelete.incr();
  }

  @Override
  public void updateGet(long getSize) {
    regionGet.add(getSize);
  }

  @Override
  public void updateScan(long scanSize) {
    regionScanNext.add(scanSize);
  }

  @Override
  public void updateIncrement() {
    regionIncrement.incr();
  }

  @Override
  public void updateAppend() {
    regionAppend.incr();
  }

  @Override
  public void updateRead(long readCapacityUnitCount) {
    regionRead.add(readCapacityUnitCount);
  }

  @Override
  public void updateWrite(long writeCapacityUnitCount) {
    regionWrite.add(writeCapacityUnitCount);
  }

  @Override
  public void updateThrottledRead(long readNum) {
    regionThrottledRead.incr(readNum);
  }

  @Override
  public void updateThrottledWrite(long writeNum) {
    regionThrottledWrite.incr(writeNum);
  }

  @Override
  public long getThrottledRead() {
    return regionThrottledRead.value();
  }

  @Override
  public long getThrottledWrite() {
    return regionThrottledWrite.value();
  }

  @Override
  public MetricsRegionAggregateSource getAggregateSource() {
    return agg;
  }

  @Override
  public int compareTo(MetricsRegionSource source) {

    if (!(source instanceof MetricsRegionSourceImpl))
      return -1;

    MetricsRegionSourceImpl impl = (MetricsRegionSourceImpl) source;
    return this.regionWrapper.getRegionName()
        .compareTo(impl.regionWrapper.getRegionName());
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) return true;
    if (!(obj instanceof MetricsRegionSourceImpl)) return false;
    return compareTo((MetricsRegionSourceImpl)obj) == 0;
  }

  void snapshot(MetricsRecordBuilder mrb, boolean ignored) {
    // This ensures that removes of the metrics
    // can't happen while we are putting them back in.
    synchronized (this) {
      if (closed) {
        return;
      }

      if (!metricsStringInited) {
        STORE_COUNT = regionNamePrefix + MetricsRegionServerSource.STORE_COUNT;
        STOREFILE_COUNT = regionNamePrefix + MetricsRegionServerSource.STOREFILE_COUNT;
        MEMSTORE_SIZE = regionNamePrefix + MetricsRegionServerSource.MEMSTORE_SIZE;
        STOREFILE_SIZE = regionNamePrefix + MetricsRegionServerSource.STOREFILE_SIZE;
        COMPACTIONS_COMPLETED_COUNT =
            regionNamePrefix + MetricsRegionSource.COMPACTIONS_COMPLETED_COUNT;
        NUM_BYTES_COMPACTED_COUNT =
            regionNamePrefix + MetricsRegionSource.NUM_BYTES_COMPACTED_COUNT;
        NUM_FILES_COMPACTED_COUNT =
            regionNamePrefix + MetricsRegionSource.NUM_FILES_COMPACTED_COUNT;
        metricsStringInited = true;
      }

      mrb.addGauge(Interns.info(STORE_COUNT, MetricsRegionServerSource.STORE_COUNT_DESC),
          this.regionWrapper.getNumStores());
      mrb.addGauge(Interns.info(STOREFILE_COUNT, MetricsRegionServerSource.STOREFILE_COUNT_DESC),
          this.regionWrapper.getNumStoreFiles());
      mrb.addGauge(Interns.info(MEMSTORE_SIZE, MetricsRegionServerSource.MEMSTORE_SIZE_DESC),
          this.regionWrapper.getMemstoreSize());
      mrb.addGauge(Interns.info(STOREFILE_SIZE, MetricsRegionServerSource.STOREFILE_SIZE_DESC),
          this.regionWrapper.getStoreFileSize());
      mrb.addCounter(
          Interns.info(COMPACTIONS_COMPLETED_COUNT, MetricsRegionSource.COMPACTIONS_COMPLETED_DESC),
          this.regionWrapper.getNumCompactionsCompleted());
      mrb.addCounter(
          Interns.info(NUM_BYTES_COMPACTED_COUNT, MetricsRegionSource.NUM_BYTES_COMPACTED_DESC),
          this.regionWrapper.getNumBytesCompacted());
      mrb.addCounter(
          Interns.info(NUM_FILES_COMPACTED_COUNT, MetricsRegionSource.NUM_FILES_COMPACTED_DESC),
          this.regionWrapper.getNumFilesCompacted());
      for (Map.Entry<String, DescriptiveStatistics> entry : this.regionWrapper
          .getCoprocessorExecutionStatistics().entrySet()) {
        DescriptiveStatistics ds = entry.getValue();
        mrb.addGauge(Interns.info(regionNamePrefix + " " + entry.getKey() + " " +
                MetricsRegionSource.COPROCESSOR_EXECUTION_STATISTICS,
            MetricsRegionSource.COPROCESSOR_EXECUTION_STATISTICS_DESC + "Min: "),
            ds.getMin() / 1000);
        mrb.addGauge(Interns.info(regionNamePrefix + " " + entry.getKey() + " " +
                MetricsRegionSource.COPROCESSOR_EXECUTION_STATISTICS,
            MetricsRegionSource.COPROCESSOR_EXECUTION_STATISTICS_DESC + "Mean: "),
            ds.getMean() / 1000);
        mrb.addGauge(Interns.info(regionNamePrefix + " " + entry.getKey() + " " +
                MetricsRegionSource.COPROCESSOR_EXECUTION_STATISTICS,
            MetricsRegionSource.COPROCESSOR_EXECUTION_STATISTICS_DESC + "Max: "),
            ds.getMax() / 1000);
        mrb.addGauge(Interns.info(regionNamePrefix + " " + entry.getKey() + " " +
                MetricsRegionSource.COPROCESSOR_EXECUTION_STATISTICS,
            MetricsRegionSource.COPROCESSOR_EXECUTION_STATISTICS_DESC + "90th percentile: "),
            ds.getPercentile(90d) / 1000);
        mrb.addGauge(Interns.info(regionNamePrefix + " " + entry.getKey() + " " +
                MetricsRegionSource.COPROCESSOR_EXECUTION_STATISTICS,
            MetricsRegionSource.COPROCESSOR_EXECUTION_STATISTICS_DESC + "95th percentile: "),
            ds.getPercentile(95d) / 1000);
        mrb.addGauge(Interns.info(regionNamePrefix + " " + entry.getKey() + " " +
                MetricsRegionSource.COPROCESSOR_EXECUTION_STATISTICS,
            MetricsRegionSource.COPROCESSOR_EXECUTION_STATISTICS_DESC + "99th percentile: "),
            ds.getPercentile(99d) / 1000);
      }
    }
  }
}