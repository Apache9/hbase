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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.test.MetricsAssertHelper;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.metrics2.MetricHistogram;
import org.apache.hadoop.metrics2.lib.MutableTimeHistogram;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runners.MethodSorters;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
@Category(MediumTests.class)
public class TestRegionServerMetrics {
  private static final Log LOG = LogFactory.getLog(TestRegionServerMetrics.class);
  private static MetricsAssertHelper metricsHelper;

  static {
    Logger.getLogger("org.apache.hadoop.hbase").setLevel(Level.DEBUG);
  }

  private static MiniHBaseCluster cluster;
  private static HRegionServer rs;
  private static Configuration conf;
  private static HBaseTestingUtility TEST_UTIL;
  private static MetricsRegionServer metricsRegionServer;
  private static MetricsRegionServerSource serverSource;

  @BeforeClass
  public static void startCluster() throws Exception {
    metricsHelper = CompatibilityFactory.getInstance(MetricsAssertHelper.class);
    TEST_UTIL = new HBaseTestingUtility();
    conf = TEST_UTIL.getConfiguration();
    conf.getLong("hbase.splitlog.max.resubmit", 0);
    // Make the failure test faster
    conf.setInt("zookeeper.recovery.retry", 0);
    // make sure that RS will not reset the table latency metrics
    conf.setInt("hbase.regionserver.msginterval", Integer.MAX_VALUE);
    conf.setInt(HConstants.REGIONSERVER_INFO_PORT, -1);

    TEST_UTIL.startMiniCluster(1, 1);
    cluster = TEST_UTIL.getHBaseCluster();

    cluster.waitForActiveAndReadyMaster();

    while (cluster.getLiveRegionServerThreads().size() < 1) {
      Threads.sleep(100);
    }

    rs = cluster.getRegionServer(0);
    metricsRegionServer = rs.getMetrics();
    serverSource = metricsRegionServer.getMetricsSource();
  }

  @AfterClass
  public static void after() throws Exception {
    if (TEST_UTIL != null) {
      TEST_UTIL.shutdownMiniCluster();
    }
  }

  @Test(timeout = 300000)
  public void testRegionCount() throws Exception {
    String regionMetricsKey = "regionCount";
    long regions = metricsHelper.getGaugeLong(regionMetricsKey, serverSource);
    // Creating a table should add one region
    TEST_UTIL.createTable(Bytes.toBytes("table"), Bytes.toBytes("cf"));
    metricsHelper.assertGaugeGt(regionMetricsKey, regions, serverSource);
  }

  @Test
  public void testLocalFiles() throws Exception {
    metricsHelper.assertGauge("percentFilesLocal", 0, serverSource);
  }

  @Test
  public void testRequestCount() throws Exception {
    String tableNameString = "testRequestCount";
    byte[] tName = Bytes.toBytes(tableNameString);
    byte[] cfName = Bytes.toBytes("d");
    byte[] row = Bytes.toBytes("rk");
    byte[] qualifier = Bytes.toBytes("qual");
    byte[] initValue = Bytes.toBytes("Value");
    byte[] nextValue = Bytes.toBytes("NEXT VAL");

    TEST_UTIL.createTable(tName, cfName);

    new HTable(conf, tName).close(); //wait for the table to come up.

    // Do a first put to be sure that the connection is established, meta is there and so on.
    HTable table = new HTable(conf, tName);
    Put p = new Put(row);
    p.add(cfName, qualifier, initValue);
    table.put(p);

    metricsRegionServer.getRegionServerWrapper().forceRecompute();
    long requests = metricsHelper.getCounter("totalRequestCount", serverSource);
    long readRequests = metricsHelper.getCounter("readRequestCount", serverSource);
    long writeRequests = metricsHelper.getCounter("writeRequestCount", serverSource);

    for (int i=0; i< 30; i++) {
      table.put(p);
    }

    metricsRegionServer.getRegionServerWrapper().forceRecompute();
    metricsHelper.assertCounter("totalRequestCount", requests + 30, serverSource);
    metricsHelper.assertCounter("readRequestCount", readRequests, serverSource);
    metricsHelper.assertCounter("writeRequestCount", writeRequests + 30, serverSource);

    Get g = new Get(row);
    for (int i=0; i< 10; i++) {
      table.get(g);
    }

    metricsRegionServer.getRegionServerWrapper().forceRecompute();
    metricsHelper.assertCounter("totalRequestCount", requests + 40, serverSource);
    metricsHelper.assertCounter("readRequestCount", readRequests + 10, serverSource);
    metricsHelper.assertCounter("writeRequestCount", writeRequests + 30, serverSource);

    for ( HRegionInfo i:table.getRegionLocations().keySet()) {
      MetricsRegionAggregateSource agg = rs.getRegion(i.getRegionName())
          .getMetrics()
          .getSource()
          .getAggregateSource();
      String prefix = "namespace_"+NamespaceDescriptor.DEFAULT_NAMESPACE_NAME_STR+
          "_table_"+tableNameString +
          "_region_" + i.getEncodedName()+
          "_metric";
      metricsHelper.assertCounter(prefix + "_getNumOps", 10, agg);
      metricsHelper.assertCounter(prefix + "_mutateCount", 31, agg);
    }

    metricsRegionServer.getRegionServerWrapper().forceRecompute();
    metricsHelper.assertCounter("totalRequestCount", requests + 40 + 1, serverSource);
    metricsHelper.assertCounter("readRequestCount", readRequests + 10 + 1, serverSource);
    // end of 0.98 specific

    List<Get> gets = new ArrayList<Get>();
    for (int i=0; i< 10; i++) {
      gets.add(new Get(row));
    }
    table.get(gets);

    metricsRegionServer.getRegionServerWrapper().forceRecompute();
    metricsHelper.assertCounter("totalRequestCount", requests + 50 + 1, serverSource);
    metricsHelper.assertCounter("readRequestCount", readRequests + 20 + 1, serverSource);
    metricsHelper.assertCounter("writeRequestCount", writeRequests + 30, serverSource);

    table.setAutoFlushTo(false);
    for (int i=0; i< 30; i++) {
      table.put(p);
    }
    table.flushCommits();

    metricsRegionServer.getRegionServerWrapper().forceRecompute();
    metricsHelper.assertCounter("totalRequestCount", requests + 80 + 1, serverSource);
    metricsHelper.assertCounter("readRequestCount", readRequests + 20 + 1, serverSource);
    metricsHelper.assertCounter("writeRequestCount", writeRequests + 60, serverSource);

    table.close();
  }

  @Test
  public void testMutationsWithoutWal() throws Exception {
    byte[] tableName = Bytes.toBytes("testMutationsWithoutWal");
    byte[] cf = Bytes.toBytes("d");
    byte[] row = Bytes.toBytes("rk");
    byte[] qualifier = Bytes.toBytes("qual");
    byte[] val = Bytes.toBytes("Value");

    metricsRegionServer.getRegionServerWrapper().forceRecompute();

    TEST_UTIL.createTable(tableName, cf);

    HTable t = new HTable(conf, tableName);

    Put p = new Put(row);
    p.add(cf, qualifier, val);
    p.setDurability(Durability.SKIP_WAL);

    t.put(p);
    t.flushCommits();

    metricsRegionServer.getRegionServerWrapper().forceRecompute();
    metricsHelper.assertGauge("mutationsWithoutWALCount", 1, serverSource);
    long minLength = row.length + cf.length + qualifier.length + val.length;
    metricsHelper.assertGaugeGt("mutationsWithoutWALSize", minLength, serverSource);

    t.close();
  }

  @Test
  public void testStoreCount() throws Exception {
    byte[] tableName = Bytes.toBytes("testStoreCount");
    byte[] cf = Bytes.toBytes("d");
    byte[] row = Bytes.toBytes("rk");
    byte[] qualifier = Bytes.toBytes("qual");
    byte[] val = Bytes.toBytes("Value");

    metricsRegionServer.getRegionServerWrapper().forceRecompute();
    long stores = metricsHelper.getGaugeLong("storeCount", serverSource);
    long storeFiles = metricsHelper.getGaugeLong("storeFileCount", serverSource);

    TEST_UTIL.createTable(tableName, cf);

    //Force a hfile.
    HTable t = new HTable(conf, tableName);
    Put p = new Put(row);
    p.add(cf, qualifier, val);
    t.put(p);
    t.flushCommits();
    TEST_UTIL.getHBaseAdmin().flush(tableName);

    metricsRegionServer.getRegionServerWrapper().forceRecompute();
    metricsHelper.assertGauge("storeCount", stores +1, serverSource);
    metricsHelper.assertGauge("storeFileCount", storeFiles + 1, serverSource);

    t.close();
  }

  @Test
  public void testCheckAndPutCount() throws Exception {
    String tableNameString = "testCheckAndPutCount";
    byte[] tableName = Bytes.toBytes(tableNameString);
    byte[] cf = Bytes.toBytes("d");
    byte[] row = Bytes.toBytes("rk");
    byte[] qualifier = Bytes.toBytes("qual");
    byte[] valOne = Bytes.toBytes("Value");
    byte[] valTwo = Bytes.toBytes("ValueTwo");
    byte[] valThree = Bytes.toBytes("ValueThree");

    TEST_UTIL.createTable(tableName, cf);
    HTable t = new HTable(conf, tableName);
    Put p = new Put(row);
    p.add(cf, qualifier, valOne);
    t.put(p);
    t.flushCommits();

    Put pTwo = new Put(row);
    pTwo.add(cf, qualifier, valTwo);
    t.checkAndPut(row, cf, qualifier, valOne, pTwo);
    t.flushCommits();

    Put pThree = new Put(row);
    pThree.add(cf, qualifier, valThree);
    t.checkAndPut(row, cf, qualifier, valOne, pThree);
    t.flushCommits();


    metricsRegionServer.getRegionServerWrapper().forceRecompute();
    metricsHelper.assertCounter("checkMutateFailedCount", 1, serverSource);
    metricsHelper.assertCounter("checkMutatePassedCount", 1, serverSource);

    t.close();
  }

  @Test
  public void testIncrement() throws Exception {
    String tableNameString = "testIncrement";
    byte[] tableName = Bytes.toBytes(tableNameString);
    byte[] cf = Bytes.toBytes("d");
    byte[] row = Bytes.toBytes("rk");
    byte[] qualifier = Bytes.toBytes("qual");
    byte[] val = Bytes.toBytes(0l);


    TEST_UTIL.createTable(tableName, cf);
    HTable t = new HTable(conf, tableName);

    Put p = new Put(row);
    p.add(cf, qualifier, val);
    t.put(p);
    t.flushCommits();

    for(int count = 0; count< 13; count++) {
      Increment inc = new Increment(row);
      inc.addColumn(cf, qualifier, 100);
      t.increment(inc);
    }

    t.flushCommits();

    metricsRegionServer.getRegionServerWrapper().forceRecompute();
    metricsHelper.assertCounter("incrementNumOps", 13, serverSource);

    t.close();
  }

  @Test
  public void testAppend() throws Exception {
    String tableNameString = "testAppend";
    byte[] tableName = Bytes.toBytes(tableNameString);
    byte[] cf = Bytes.toBytes("d");
    byte[] row = Bytes.toBytes("rk");
    byte[] qualifier = Bytes.toBytes("qual");
    byte[] val = Bytes.toBytes("One");


    TEST_UTIL.createTable(tableName, cf);
    HTable t = new HTable(conf, tableName);

    Put p = new Put(row);
    p.add(cf, qualifier, val);
    t.put(p);
    t.flushCommits();

    for(int count = 0; count< 73; count++) {
      Append append = new Append(row);
      append.add(cf, qualifier, Bytes.toBytes(",Test"));
      t.append(append);
    }

    t.flushCommits();

    metricsRegionServer.getRegionServerWrapper().forceRecompute();
    metricsHelper.assertCounter("appendNumOps", 73, serverSource);

    t.close();
  }

  @Test
  public void testScanNext() throws IOException {
    String tableNameString = "testScanNext";
    byte[] tableName = Bytes.toBytes(tableNameString);
    byte[] cf = Bytes.toBytes("d");
    byte[] qualifier = Bytes.toBytes("qual");
    byte[] val = Bytes.toBytes("One");


    TEST_UTIL.createTable(tableName, cf);
    HTable t = new HTable(conf, tableName);
    t.setAutoFlush(false, true);
    for (int insertCount =0; insertCount < 100; insertCount++) {
      Put p = new Put(Bytes.toBytes("" + insertCount + "row"));
      p.add(cf, qualifier, val);
      t.put(p);
    }
    t.flushCommits();

    Scan s = new Scan();
    s.setBatch(1);
    s.setCaching(1);
    ResultScanner resultScanners = t.getScanner(s);

    for (int nextCount = 0; nextCount < 30; nextCount++) {
      Result result = resultScanners.next();
      assertNotNull(result);
      assertEquals(1, result.size());
    }
    for ( HRegionInfo i:t.getRegionLocations().keySet()) {
      MetricsRegionAggregateSource agg = rs.getRegion(i.getRegionName())
          .getMetrics()
          .getSource()
          .getAggregateSource();
      String prefix = "namespace_"+NamespaceDescriptor.DEFAULT_NAMESPACE_NAME_STR+
          "_table_"+tableNameString +
          "_region_" + i.getEncodedName()+
          "_metric";
      metricsHelper.assertCounter(prefix + "_scanNextNumOps", 30, agg);
    }
  }

  @Test
  public void testTableLatency() throws Exception {
    String tableNameString = "testTableLatency";
    byte[] tableName = Bytes.toBytes(tableNameString);
    byte[][] splits = new byte[][] { Bytes.toBytes("1"), Bytes.toBytes("2"), Bytes.toBytes("3") };
    byte[] family = Bytes.toBytes("f");
    byte[] row = Bytes.toBytes(30);
    byte[] qualifier = Bytes.toBytes("q");
    byte[] qualifier2 = Bytes.toBytes("q2");
    byte[] value = Bytes.toBytes(30);

    TEST_UTIL.createTable(tableName, family, splits);
    new HTable(conf, tableName).close();
    HTable table = new HTable(conf, tableName);

    // put
    Put put = new Put(row);
    put.add(family, qualifier, value);
    table.put(put);
    table.flushCommits();
    MetricsTableLatenciesImpl.TableHistograms histograms = getTableHistograms(tableNameString);
    Assert.assertEquals(1, histograms.putTimeHisto.getOperationCountAndMeanAnd99PercentileTime()[0]);

    // put 30
    for (int i = 0; i < 30; i++) {
      Put putTmp = new Put(Bytes.toBytes(i));
      putTmp.add(family, qualifier, Bytes.toBytes(i));
      table.put(putTmp);
    }
    table.flushCommits();
    Assert.assertEquals(30, histograms.putTimeHisto.getOperationCountAndMeanAnd99PercentileTime()[0]);

    // get 10
    Get get = new Get(row);
    for (int i = 0; i < 10; i++) {
      table.get(get);
    }
    table.flushCommits();
    Assert.assertEquals(10, histograms.getTimeHisto.getOperationCountAndMeanAnd99PercentileTime()[0]);

    // scan
    Scan scan = new Scan();
    scan.setStartRow(Bytes.toBytes(0));
    scan.setStopRow(Bytes.toBytes(30));
    Iterator<Result> iterable = table.getScanner(scan).iterator();
    while (iterable.hasNext()) {
      iterable.next();
    }
    table.flushCommits();
    Assert.assertEquals(1, histograms.scanTimeHisto.getOperationCountAndMeanAnd99PercentileTime()[0]);

    // append
    Append append = new Append(row);
    append.add(family, qualifier2, Bytes.toBytes(1));
    table.append(append);
    table.flushCommits();
    Assert.assertEquals(1, histograms.appendTimeHisto.getOperationCountAndMeanAnd99PercentileTime()[0]);

    // delete
    Delete delete = new Delete(row);
    table.delete(delete);
    table.flushCommits();
    Assert.assertEquals(1, histograms.deleteTimeHisto.getOperationCountAndMeanAnd99PercentileTime()[0]);

    // increment
    Increment increment = new Increment(row);
    increment.addColumn(family, qualifier2, 10);
    table.increment(increment);
    table.flushCommits();
    Assert.assertEquals(1, histograms.incrementTimeHisto.getOperationCountAndMeanAnd99PercentileTime()[0]);

    // batch
    List<Get> gets = new ArrayList<Get>();
    for (int i = 0; i < 10; i++) {
      gets.add(new Get(row));
    }
    table.get(gets);
    table.flushCommits();
    Assert.assertEquals(1, histograms.batchTimeHisto.getOperationCountAndMeanAnd99PercentileTime()[0]);

    // batch
    RowMutations mutations = new RowMutations(row);
    mutations.add(new Delete(row));
    mutations.add(put);
    table.mutateRow(mutations);
    table.flushCommits();
    Assert.assertEquals(1, histograms.batchTimeHisto.getOperationCountAndMeanAnd99PercentileTime()[0]);

    // batch
    table.setAutoFlushTo(false);
    for (int i = 0; i < 30; i++) {
      table.put(put);
    }
    table.flushCommits();
    Assert.assertEquals(1, histograms.batchTimeHisto.getOperationCountAndMeanAnd99PercentileTime()[0]);

    table.close();
  }

  private MetricsTableLatenciesImpl.TableHistograms getTableHistograms(String tableName) {
    if (metricsRegionServer.getTableMetrics() != null
      && metricsRegionServer.getTableMetrics().getMetricsTableLatency() != null) {
      MetricsTableLatencies tableLatency =
        metricsRegionServer.getTableMetrics().getMetricsTableLatency();
      if (tableLatency instanceof MetricsTableLatenciesImpl) {
        MetricsTableLatenciesImpl tableLatencies = (MetricsTableLatenciesImpl) tableLatency;
        return tableLatencies.getHistogramsByTable().get(TableName.valueOf(tableName));
      }
    }
    return null;
  }
}
