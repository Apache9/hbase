package com.xiaomi.infra.hbase.throughput;


import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.Updater;
import org.apache.hadoop.metrics.util.MetricsLongValue;
import org.apache.hadoop.metrics.util.MetricsRegistry;


public class RegionServerThroughputWindowMetrics implements Updater{

  private MetricsRegistry registry = new MetricsRegistry();
  private RegionServerThroughputWindowStatistics statistics;
  public final MetricsLongValue readRecordsCount = new MetricsLongValue("readRecordsCount", registry);
  public final MetricsLongValue writeRecordsCount = new MetricsLongValue("writeRecordsCount", registry);

  private final MetricsRecord metricsRecord;

  public RegionServerThroughputWindowMetrics(){
    MetricsContext context = MetricsUtil.getContext("hbase");
    metricsRecord = context.createRecord("regionserverthroughputwindow");
    String name = Thread.currentThread().getName();
    metricsRecord.setTag("RegionServerThroughputWindow", name);
    context.registerUpdater(this);
    statistics = new RegionServerThroughputWindowStatistics(this.registry, name);
  }

  @Override
  public void doUpdates(org.apache.hadoop.metrics.MetricsContext metricsContext) {
      this.readRecordsCount.pushMetric(this.metricsRecord);
      this.writeRecordsCount.pushMetric(this.metricsRecord);
      this.metricsRecord.update();
  }

}
