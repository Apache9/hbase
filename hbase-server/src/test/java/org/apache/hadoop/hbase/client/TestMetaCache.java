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
package org.apache.hadoop.hbase.client;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.CallDroppedException;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.MultiActionResultTooLarge;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.RegionTooBusyException;
import org.apache.hadoop.hbase.RetryImmediatelyException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TooManyRegionScannersException;
import org.apache.hadoop.hbase.exceptions.ClientExceptionsUtil;
import org.apache.hadoop.hbase.exceptions.RegionOpeningException;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.GetResponse;
import org.apache.hadoop.hbase.quotas.ThrottlingException;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.JVMClusterUtil;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@Category({ MediumTests.class })
public class TestMetaCache {
  private static final Log LOG = LogFactory.getLog(TestMetaCache.class);
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  private static final TableName TABLE_NAME = TableName.valueOf("test_table");
  private static final byte[] FAMILY = Bytes.toBytes("fam1");
  private static final byte[] QUALIFIER = Bytes.toBytes("qual");
  private HConnectionImplementation conn;
  private HRegionServer badRS;
  private static final int TIMEOUT = 30;

  /**
   * @throws java.lang.Exception
   */
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    TEST_UTIL.startMiniCluster(1);
    TEST_UTIL.getHBaseCluster().waitForActiveAndReadyMaster();
    TEST_UTIL.waitUntilAllRegionsAssigned(TABLE_NAME.META_TABLE_NAME);
  }

  /**
   * @throws java.lang.Exception
   */
  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setup() throws Exception {
    MiniHBaseCluster cluster = TEST_UTIL.getHBaseCluster();
    cluster.getConfiguration()
        .setStrings(HConstants.REGION_SERVER_IMPL, RegionServerWithFakeRpcServices.class.getName());
    JVMClusterUtil.RegionServerThread rsThread = cluster.startRegionServer();
    rsThread.waitForServerOnline();
    badRS = rsThread.getRegionServer();
    assertTrue(badRS instanceof RegionServerWithFakeRpcServices);
    cluster.getConfiguration()
        .setStrings(HConstants.REGION_SERVER_IMPL, HRegionServer.class.getName());
    assertEquals(2, cluster.getRegionServerThreads().size());

    Configuration conf = TEST_UTIL.getConfiguration();
    HTableDescriptor table = new HTableDescriptor(TABLE_NAME);
    HColumnDescriptor fam = new HColumnDescriptor(FAMILY);
    fam.setMaxVersions(2);
    table.addFamily(fam);
    try (HBaseAdmin admin = new HBaseAdmin(conf)) {
      admin.createTable(table, HBaseTestingUtility.KEYS_FOR_HBA_CREATE_TABLE);
    }
    TEST_UTIL.waitUntilAllRegionsAssigned(TABLE_NAME);

    conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 1);
    conf.setInt(HConstants.HBASE_RPC_TIMEOUT_KEY, TIMEOUT);
    conf.setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, TIMEOUT);
    conn = (HConnectionImplementation) HConnectionManager.createConnection(conf);
  }

  /**
   * @throws java.lang.Exception
   */
  @After
  public void tearDown() throws Exception {
    conn.close();
  }

  @Test
  public void testPreserveMetaCacheOnException() throws Exception {
    HTableInterface table = conn.getTable(TABLE_NAME);
    byte[] row = badRS.getOnlineRegions(TABLE_NAME).get(0).getRegionInfo().getStartKey();

    Put put = new Put(row);
    put.add(FAMILY, QUALIFIER, Bytes.toBytes(10));
    Get get = new Get(row);

    Exception exp;
    for (int i = 0; i < 50; i++) {
      exp = null;
      try {
        // put will success and cache the region location
        table.put(put);
        // get may failed and throw exception and clear the region location
        table.get(get);
      } catch (IOException ex) {
        exp = ex;
      }

      if (exp != null) {
        if (ClientExceptionsUtil.isMetaClearingException(exp)) {
          assertNull(conn.getCachedLocation(TABLE_NAME, row));
        } else {
          assertNotNull(conn.getCachedLocation(TABLE_NAME, row));
        }
      } else {
        assertNotNull(conn.getCachedLocation(TABLE_NAME, row));
      }
    }
  }

  public static List<Throwable> metaCachePreservingExceptions() {
    return new ArrayList<Throwable>() {{
      add(new RegionOpeningException(" "));
      add(new RegionTooBusyException());
      add(new ThrottlingException(" "));
      add(new MultiActionResultTooLarge(" "));
      add(new RetryImmediatelyException(" "));
      add(new CallDroppedException(" "));
      add(new TooManyRegionScannersException(" "));
    }};
  }

  protected static class RegionServerWithFakeRpcServices extends HRegionServer {

    public RegionServerWithFakeRpcServices(Configuration conf)
        throws IOException, InterruptedException {
      super(conf);
    }

    private int numReqs = -1;
    private int expCount = -1;
    private List<Throwable> metaCachePreservingExceptions = metaCachePreservingExceptions();

    @Override
    public GetResponse get(final RpcController controller, final ClientProtos.GetRequest request)
        throws ServiceException {
      throwSomeExceptions();
      return super.get(controller, request);
    }

    /**
     * Throw some exceptions. Mostly throw exceptions which do not clear meta cache.
     * Periodically throw NotSevingRegionException which clears the meta cache.
     *
     * @throws ServiceException
     */
    private void throwSomeExceptions() throws ServiceException {
      numReqs++;
      // Succeed every 5 request, throw cache clearing exceptions twice every 5 requests and throw
      // meta cache preserving exceptions otherwise.
      if (numReqs % 5 == 0) {
        return;
      } else if (numReqs % 5 == 1) {
        throw new ServiceException(new NotServingRegionException());
      } else if (numReqs % 5 == 2) {
        // Client will throw CallTimeoutException
        try {
          Thread.sleep(2 * TIMEOUT);
        } catch (InterruptedException e) {
        }
      }
      // Round robin between different special exceptions.
      // This is not ideal since exception types are not tied to the operation performed here,
      // But, we don't really care here if we throw MultiActionTooLargeException while doing
      // single Gets.
      expCount++;
      Throwable t =
          metaCachePreservingExceptions.get(expCount % metaCachePreservingExceptions.size());
      throw new ServiceException(t);
    }
  }
}