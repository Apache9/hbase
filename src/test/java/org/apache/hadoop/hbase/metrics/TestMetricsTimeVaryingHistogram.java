package org.apache.hadoop.hbase.metrics;

import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.metrics.histogram.MetricsTimeVaryingHistogram;
import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.util.MetricsFloatValue;
import org.apache.hadoop.metrics.util.MetricsLongValue;
import org.apache.hadoop.metrics.util.MetricsRegistry;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestMetricsTimeVaryingHistogram {
  @Test
  public void testTimeVaring() {
    MetricsContext context = MetricsUtil.getContext("test");
    MetricsRecord metricsRecord = MetricsUtil.createRecord(context, "test");
    
    MetricsTimeVaryingHistogram h = new MetricsTimeVaryingHistogram("testHistogram", null);

    for (int i = 1; i <= 100; i++) {
      h.update(i);
    }

    Assert.assertEquals(100, h.getCount());
    Assert.assertEquals(1, h.getMin());
    Assert.assertEquals(100, h.getMax());
    
    // push metric and clear counters for last interval
    h.pushMetric(metricsRecord);    
    for (int i = 1; i <= 1000; i++) {
      h.update(i);
    }
    Assert.assertEquals(1000, h.getCount());
    Assert.assertEquals(1, h.getMin());
    Assert.assertEquals(1000, h.getMax());
  }
  
  @Test
  public void testGetHistogramMetricName() {
    String method = "methodA";
    String suffix = "_suffix";
    Assert.assertEquals("methodA_histogram_suffix",
      MetricsTimeVaryingHistogram.getHistogramMetricName(method, suffix));
    
    method = "methodA_";
    Assert.assertEquals("methodA_histogram_suffix",
      MetricsTimeVaryingHistogram.getHistogramMetricName(method, suffix));
  }
  
  @Test
  public void testRegisterTimeVaryingHistogramMetric() {
    MetricsTimeVaryingHistogram histogram = new MetricsTimeVaryingHistogram("TestHistogram", null);
    for (int i = 1; i <= 100; ++i) {
      histogram.update(i);
    }
    String methodName = "testMethod";
    MetricsRegistry registry = new MetricsRegistry();
    MetricsTimeVaryingHistogram.registerTimeVaryingHistogramMetric(methodName, histogram, registry);
    Assert.assertNotNull(registry.get("testMethod_histogram_num_ops"));
    Assert.assertEquals(MetricsLongValue.class, registry.get("testMethod_histogram_num_ops").getClass());
    Assert.assertEquals(100, ((MetricsLongValue)registry.get("testMethod_histogram_num_ops")).get());
    
    Assert.assertNotNull(registry.get("testMethod_histogram_min"));
    Assert.assertEquals(MetricsLongValue.class, registry.get("testMethod_histogram_min").getClass());
    Assert.assertEquals(1, ((MetricsLongValue)registry.get("testMethod_histogram_min")).get());
    
    Assert.assertNotNull(registry.get("testMethod_histogram_max"));
    Assert.assertEquals(MetricsLongValue.class, registry.get("testMethod_histogram_max").getClass());
    Assert.assertEquals(100, ((MetricsLongValue)registry.get("testMethod_histogram_max")).get());
    
    Assert.assertNotNull(registry.get("testMethod_histogram_mean"));
    Assert.assertEquals(MetricsFloatValue.class, registry.get("testMethod_histogram_mean").getClass());
    Assert.assertEquals(50.5f, ((MetricsFloatValue)registry.get("testMethod_histogram_mean")).get(), 0.0001f);
    
    Assert.assertNotNull(registry.get("testMethod_histogram_std_dev"));
    Assert.assertEquals(MetricsFloatValue.class, registry.get("testMethod_histogram_std_dev").getClass());    
    Assert.assertNotNull(registry.get("testMethod_histogram_median"));
    Assert.assertEquals(MetricsFloatValue.class, registry.get("testMethod_histogram_median").getClass());
    Assert.assertEquals(50.5f, ((MetricsFloatValue)registry.get("testMethod_histogram_median")).get(), 0.0001f);
    
    Assert.assertNotNull(registry.get("testMethod_histogram_75th_percentile"));
    Assert.assertEquals(MetricsFloatValue.class, registry.get("testMethod_histogram_75th_percentile").getClass());
    Assert.assertEquals(75.75f, ((MetricsFloatValue)registry.get("testMethod_histogram_75th_percentile")).get(), 0.0001f);
    
    Assert.assertNotNull(registry.get("testMethod_histogram_95th_percentile"));
    Assert.assertEquals(MetricsFloatValue.class, registry.get("testMethod_histogram_95th_percentile").getClass());
    Assert.assertEquals(95.95f, ((MetricsFloatValue)registry.get("testMethod_histogram_95th_percentile")).get(), 0.0001f);
    
    Assert.assertNotNull(registry.get("testMethod_histogram_99th_percentile"));
    Assert.assertEquals(MetricsFloatValue.class, registry.get("testMethod_histogram_99th_percentile").getClass());
    Assert.assertEquals(99.99f, ((MetricsFloatValue)registry.get("testMethod_histogram_99th_percentile")).get(), 0.0001f);
    
    Assert.assertNotNull(registry.get("testMethod_histogram_999th_percentile"));
    Assert.assertEquals(MetricsFloatValue.class, registry.get("testMethod_histogram_999th_percentile").getClass());
    Assert.assertEquals(100f, ((MetricsFloatValue)registry.get("testMethod_histogram_999th_percentile")).get(), 0.0001f);
  }
}
