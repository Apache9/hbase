package org.apache.hadoop.hbase.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.MultiRowMutationEndpoint;
import org.apache.hadoop.hbase.coprocessor.MultiRowMutationProtocol;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.WritableByteArrayComparable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestNonIdempotentOperation {
  final Log LOG = LogFactory.getLog(getClass());
  protected static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static byte [] ROW = Bytes.toBytes("testRow");
  private static byte [] FAMILY = Bytes.toBytes("testFamily");
  private static byte [] QUALIFIER = Bytes.toBytes("testQualifier");
  private static byte [] VALUE = Bytes.toBytes("testValue");
  protected static int SLAVES = 3;
  
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Configuration conf = TEST_UTIL.getConfiguration();
    conf.setStrings(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
      MultiRowMutationEndpoint.class.getName(), TimeoutCp.class.getName());
    conf.setBoolean(HConstants.HBASE_NON_IDEMPOTENT_OPERATION_RETRY_KEY, false);
    // We need more than one region server in this test
    TEST_UTIL.startMiniCluster(SLAVES);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }
  
  public static class TimeoutCp extends BaseRegionObserver {
    @Override
    public boolean preCheckAndPut(final ObserverContext<RegionCoprocessorEnvironment> e,
        final byte [] row, final byte [] family, final byte [] qualifier,
        final CompareOp compareOp, final WritableByteArrayComparable comparator,
        final Put put, final boolean result) throws IOException {
      Threads.sleep(1000);
      return super.preCheckAndPut(e, row, family, qualifier, compareOp, comparator, put, result);
    }
    
    @Override
    public void preMutateRowsWithLocks(final ObserverContext<RegionCoprocessorEnvironment> c,
        final Collection<Mutation> mutations) throws IOException {
      Threads.sleep(1000);
    }
  }
  
  protected void setTimeoutAndRetry(Configuration conf) {
    conf.setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, 1500);
    conf.setInt(HConstants.HBASE_RPC_TIMEOUT_KEY, 500);
    conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 2);
  }
  
  @Test
  public void testNonIdempotentOperation() throws IOException {
    final byte [] TABLENAME = Bytes.toBytes("testIncrement");
    TEST_UTIL.createTable(TABLENAME, FAMILY);
    Configuration conf = new Configuration(TEST_UTIL.getConfiguration());
    setTimeoutAndRetry(conf);
    
    HTable table = new HTable(conf, TABLENAME);
    Put put = new Put(ROW).add(FAMILY, QUALIFIER, VALUE);
    try {
      table.checkAndPut(ROW, FAMILY, QUALIFIER, VALUE, put);
    } catch (IOException e) {
      Assert.assertTrue(e instanceof RetriesExhaustedException);
      Assert.assertTrue(e.getMessage().contains(
        "not retry non-idmpotent operation when SocketTimeoutException"));
    }
    
    MultiRowMutationProtocol p =
        table.coprocessorProxy(MultiRowMutationProtocol.class, ROW);
    Condition condition = new Condition(ROW, FAMILY, QUALIFIER, CompareOp.NOT_EQUAL, VALUE);
    List<Condition> conditions = new ArrayList<Condition>();
    conditions.add(condition);
    List<Mutation> mutations = new ArrayList<Mutation>();
    mutations.add(put);
    try {
      p.mutateRowsWithConditions(mutations, conditions);
    } catch (IOException e) {
      Assert.assertTrue(e instanceof RetriesExhaustedException);
      Assert.assertTrue(e.getMessage().contains(
        "not retry non-idmpotent operation when SocketTimeoutException"));
    }
    try {
      p.batchMutatesWithConditions(mutations, conditions);
    } catch (IOException e) {
      Assert.assertTrue(e instanceof RetriesExhaustedException);
      Assert.assertTrue(e.getMessage().contains(
        "not retry non-idmpotent operation when SocketTimeoutException"));
    }
    table.close();
  }
}
