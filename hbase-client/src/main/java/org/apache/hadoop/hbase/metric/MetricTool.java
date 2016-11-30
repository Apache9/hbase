package org.apache.hadoop.hbase.metric;

import com.xiaomi.owl.metric.Dimension;
import com.xiaomi.owl.metric.Metric;
import com.xiaomi.owl.metric.MetricCounter;
import com.xiaomi.owl.metric.MetricUnit;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MetricTool {
  public static final String HBASE_CLIENT_OWL_METRIC_PUSH_ENABLE = "hbase.client.push.owl.metric.enable";

  // metric namespace
  private static final String METRIC_NAMESPACE = "owl-metric-hbase-client";

  // metric dimension keys
  private static final String METRIC_DIMENSION_REGION_SERVER = "region-server";
  private static final String METRIC_DIMENSION_DURATION_RANGE = "duration-range";

  // metric name prefix, for example, rpc.counts.put
  private static final String METRIC_NAME_PREFIX_RPC_COUNTS = "rpc.counts.";
  private static final String METRIC_NAME_PREFIX_RPC_FAILED = "rpc.failed.";
  private static final String METRIC_NAME_PREFIX_RPC_DURATION = "rpc.duration.";

  // aggregation metric name
  private static final String METRIC_NAME_RPC_DURATION = "rpc.duration";
  private static final String METRIC_NAME_RPC_FAILED_SUM = "rpc.failed.sum";

  private static final long[] TIME_SPLITS = new long[] { 10, 25, 50, 100, 200, 500, 1000, 3000 };
  private static final String[] TIME_RANGES = new String[] {
    "0-10", "10-25", "25-50", "50-100", "100-200", "200-500", "500-1000", "1000-3000", "3000+"
  };

  /**
   * InetSocketAddress.toString() return string like that
   *    "c3-hadoop-tst-st25.bj/10.108.81.31:28600"
   * so we should strip prefix "c3-hadoop-tst-st25.bj/"
   * and return "10.108.81.31:28600"
   */
  public static String addrNormalize(InetSocketAddress addr){
    return addr.getAddress().getHostAddress() + addr.getPort();
  }

  public static void update(String methodName, long count, long callTime, String remoteAddr){
    updateMethodCount(methodName, count,remoteAddr);
    updateMethodDuration(methodName, count, callTime, remoteAddr);
    updateRegionServerDuration(count, callTime, remoteAddr);
  }

  public static void updateFailedCall(String methodName, long count, String remoteAddr){
    updateMethodFailed(methodName, count, remoteAddr);
    updateRegionServerMetricFailedSum(count, remoteAddr);
  }

  private static void updateMethodCount(String method, long count, String remoteAddr) {
    List<Dimension> dimensions = new ArrayList<Dimension>();
    dimensions.add(new Dimension(METRIC_DIMENSION_REGION_SERVER, remoteAddr));
    String metricName = METRIC_NAME_PREFIX_RPC_COUNTS + method;
    Metric metric = new Metric(METRIC_NAMESPACE, metricName, MetricUnit.Count, dimensions);
    MetricCounter.count(metric, count);
  }

  private static void updateMethodDuration(String method, long count, long duration,
      String remoteAddr) {
    List<Dimension> dimensions = new ArrayList<Dimension>();
    dimensions.add(new Dimension(METRIC_DIMENSION_REGION_SERVER, remoteAddr));
    dimensions.add(new Dimension(METRIC_DIMENSION_DURATION_RANGE, locateTimeRange(duration)));
    String metricName = METRIC_NAME_PREFIX_RPC_DURATION + method;
    Metric metric = new Metric(METRIC_NAMESPACE, metricName, MetricUnit.Count, dimensions);
    MetricCounter.count(metric, count);
  }

  private static void updateRegionServerDuration(long count, long duration, String remoteAddr){
    List<Dimension> dimensions = new ArrayList<Dimension>();
    dimensions.add(new Dimension(METRIC_DIMENSION_REGION_SERVER, remoteAddr));
    dimensions.add(new Dimension(METRIC_DIMENSION_DURATION_RANGE, locateTimeRange(duration)));
    String metricName = METRIC_NAME_RPC_DURATION;
    Metric metric = new Metric(METRIC_NAMESPACE, metricName, MetricUnit.Count, dimensions);
    MetricCounter.count(metric, count);
  }

  private static void updateMethodFailed(String method, long count, String remoteAddr){
    List<Dimension> dimensions = new ArrayList<Dimension>();
    dimensions.add(new Dimension(METRIC_DIMENSION_REGION_SERVER, remoteAddr));
    String metricName = METRIC_NAME_PREFIX_RPC_FAILED + method;
    Metric metric = new Metric(METRIC_NAMESPACE, metricName, MetricUnit.Count, dimensions);
    MetricCounter.count(metric, count);
  }

  private static void updateRegionServerMetricFailedSum(long count, String remoteAddr){
    List<Dimension> dimensions = new ArrayList<Dimension>();
    dimensions.add(new Dimension(METRIC_DIMENSION_REGION_SERVER, remoteAddr));
    String metricName = METRIC_NAME_RPC_FAILED_SUM;
    Metric metric = new Metric(METRIC_NAMESPACE, metricName, MetricUnit.Count, dimensions);
    MetricCounter.count(metric, count);
  }

  private static String locateTimeRange(long duration) {
    int i = Arrays.binarySearch(TIME_SPLITS, duration);
    int loc = i < 0 ? -(i + 1) : i;
    return TIME_RANGES[loc];
  }
}
