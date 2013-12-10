package org.apache.hadoop.hbase.metrics.histogram;

import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.util.MetricsFloatValue;
import org.apache.hadoop.metrics.util.MetricsLongValue;
import org.apache.hadoop.metrics.util.MetricsRegistry;

import com.yammer.metrics.stats.Snapshot;

/**
 * The MetricsTimeVaryingHistogram class is for a MetricsHistogram that naturally
 * varies over time (e.g. latency of operation). The MetricsHistogram is published
 * at the end of each interval and then clear. Hence the counter has the histogram
 * in the current interval. 
 */
public class MetricsTimeVaryingHistogram extends MetricsHistogram {
  public static final String HISTOGRAM = "histogram";
  
  public MetricsTimeVaryingHistogram(final String name, final MetricsRegistry registry, 
      final String description, boolean forwardBiased) {
    super(name, registry, description, forwardBiased);
  }
  
  public MetricsTimeVaryingHistogram(final String name, MetricsRegistry registry, 
      final String description) {
    super(name, registry, description);
  }

  public MetricsTimeVaryingHistogram(final String name, MetricsRegistry registry) {
    super(name, registry);
  }
  
  @Override
  public void pushMetric(MetricsRecord mr) {
    super.pushMetric(mr);
    clear();
  }
  
  // The methodName will end with '_' in OperationMetrics, however, don't end
  // with '_' in HBaseRpcMetrics. This method will adapt the two cases.
  public static String getHistogramMetricName(String methodName, String suffix) {
    if (methodName.endsWith("_")) {
      return methodName + HISTOGRAM + suffix;
    }
    return methodName + "_" + HISTOGRAM + suffix;
  }
  
  // we can't register histogram into register directly, because MetricsDynamicMBeanBase
  // won't handle hbase metric type. Therefore, we use some hadoop metric types to
  // represent histogram and register these metrics into MetricsDynamicMBeanBase
  public static void registerTimeVaryingHistogramMetric(String methodName,
      MetricsTimeVaryingHistogram histogram, MetricsRegistry registry) {
    String countName = getHistogramMetricName(methodName, MetricsHistogram.NUM_OPS_METRIC_NAME);
    MetricsLongValue metricCount = (MetricsLongValue)registry.get(countName);
    if (metricCount == null) {
      metricCount = new MetricsLongValue(countName, registry);
    }
    metricCount.set(histogram.getCount());
    
    String minName = getHistogramMetricName(methodName, MetricsHistogram.MIN_METRIC_NAME);
    MetricsLongValue metricMin = (MetricsLongValue)registry.get(minName);
    if (metricMin == null) {
      metricMin = new MetricsLongValue(minName, registry);
    }
    metricMin.set(histogram.getMin());
    
    String maxName = getHistogramMetricName(methodName, MetricsHistogram.MAX_METRIC_NAME);
    MetricsLongValue metricMax = (MetricsLongValue)registry.get(maxName);
    if (metricMax == null) {
      metricMax = new MetricsLongValue(maxName, registry);
    }
    metricMax.set(histogram.getMax());
    
    String meanName = getHistogramMetricName(methodName, MetricsHistogram.MEAN_METRIC_NAME);
    MetricsFloatValue metricMean = (MetricsFloatValue)registry.get(meanName);
    if (metricMean == null) {
      metricMean = new MetricsFloatValue(meanName, registry);
    }
    metricMean.set((float)histogram.getMean());
    
    String stdDevName = getHistogramMetricName(methodName, MetricsHistogram.STD_DEV_METRIC_NAME);
    MetricsFloatValue metricStdDev = (MetricsFloatValue)registry.get(stdDevName);
    if (metricStdDev == null) {
      metricStdDev = new MetricsFloatValue(stdDevName, registry);
    }
    metricStdDev.set((float)histogram.getStdDev());
    
    Snapshot snapshot = histogram.getSnapshot();
    
    String medianName = getHistogramMetricName(methodName, MetricsHistogram.MEDIAN_METRIC_NAME);
    MetricsFloatValue metricMedian = (MetricsFloatValue)registry.get(medianName);
    if (metricMedian == null) {
      metricMedian = new MetricsFloatValue(medianName, registry);
    }
    metricMedian.set((float)snapshot.getMedian());
    
    String _75thPercentileName = getHistogramMetricName(methodName,
      MetricsHistogram.SEVENTY_FIFTH_PERCENTILE_METRIC_NAME);
    MetricsFloatValue metric75thPercentile = (MetricsFloatValue)registry.get(_75thPercentileName);
    if (metric75thPercentile == null) {
      metric75thPercentile = new MetricsFloatValue(_75thPercentileName, registry);
    }
    metric75thPercentile.set((float)snapshot.get75thPercentile());
    
    String _95thPercentileName = getHistogramMetricName(methodName,
      MetricsHistogram.NINETY_FIFTH_PERCENTILE_METRIC_NAME);
    MetricsFloatValue metric95thPercentile = (MetricsFloatValue)registry.get(_95thPercentileName);
    if (metric95thPercentile == null) {
      metric95thPercentile = new MetricsFloatValue(_95thPercentileName, registry);
    }
    metric95thPercentile.set((float)snapshot.get95thPercentile());
    
    String _99thPercentileName = getHistogramMetricName(methodName,
      MetricsHistogram.NINETY_NINETH_PERCENTILE_METRIC_NAME);
    MetricsFloatValue metric99thPercentile = (MetricsFloatValue)registry.get(_99thPercentileName);
    if (metric99thPercentile == null) {
      metric99thPercentile = new MetricsFloatValue(_99thPercentileName, registry);
    }
    metric99thPercentile.set((float)snapshot.get99thPercentile());
    
    String _999thPercentileName = getHistogramMetricName(methodName,
      MetricsHistogram.NINETY_NINE_POINT_NINETH_PERCENTILE_METRIC_NAME);
    MetricsFloatValue metric999thPercentile = (MetricsFloatValue)registry.get(_999thPercentileName);
    if (metric999thPercentile == null) {
      metric999thPercentile = new MetricsFloatValue(_999thPercentileName, registry);
    }
    metric999thPercentile.set((float)snapshot.get999thPercentile());
  }
}
