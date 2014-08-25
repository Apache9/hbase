package com.xiaomi.infra.hbase.throughput;

import org.apache.hadoop.hbase.metrics.MetricsMBeanBase;
import org.apache.hadoop.metrics.util.MBeanUtil;
import org.apache.hadoop.metrics.util.MetricsRegistry;

import javax.management.ObjectName;

public class RegionServerThroughputWindowStatistics extends MetricsMBeanBase {
  private final ObjectName mbeanName;

  public RegionServerThroughputWindowStatistics(MetricsRegistry registry, String rsName) {
    super(registry, "RegionServerThroughputWindowStatistics");
    mbeanName = MBeanUtil.registerMBean("RegionServerThroughputWindow",
        "RegionServerThroughputWindowMetrics", this);
  }
}
