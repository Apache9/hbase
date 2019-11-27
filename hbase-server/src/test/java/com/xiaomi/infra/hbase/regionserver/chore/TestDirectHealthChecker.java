package com.xiaomi.infra.hbase.regionserver.chore;

import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdge;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Category(MediumTests.class)
public class TestDirectHealthChecker {
	private static final Logger LOG = LoggerFactory.getLogger(DirectHealthChecker.class.getName());

	private static Configuration conf;
	private static HBaseTestingUtility UTIL;
	private HRegionServer server;
	private DirectHealthChecker directHealthChecker;

	@BeforeClass
	public static void setupCluster() throws Exception {
		conf = HBaseConfiguration.create();
		conf.set(HConstants.HEALTH_SCRIPT_LOC, "");

		UTIL = new HBaseTestingUtility(conf);
		UTIL.startMiniCluster(1);
	}

	@Before
	public void setup() throws Exception {
		server = UTIL.getHBaseCluster().getRegionServer(0);
		directHealthChecker = new DirectHealthChecker(server);
	}

	@Test
	public void testRead() throws Exception {
		HRegion normalRegion = Mockito.mock(HRegion.class);
		when(normalRegion.get(Mockito.any(Get.class))).thenAnswer(new Answer<Result>() {
			@Override
			public Result answer(InvocationOnMock invocation) {
				return new Result();
			}
		});
    mockRegionRowKey(normalRegion);
		Assert.assertTrue(directHealthChecker.checkGet(normalRegion));

		HRegion mockTimeoutRegion = Mockito.mock(HRegion.class);
		long readTimeout = conf.getLong(HConstants.CANARY_RPC_TIMEOUT,
				HConstants.DEFAULT_CANARY_RPC_TIMEOUT);
		when(mockTimeoutRegion.get(Mockito.any(Get.class))).thenAnswer(new Answer<Result>() {
			@Override
			public Result answer(InvocationOnMock invocation) {

				try {
					Thread.sleep(readTimeout + 1000);
				} catch (InterruptedException e) {
					LOG.warn("Failed to sleep", e);
				}
				return null;
			}
		});
		mockRegionRowKey(mockTimeoutRegion);
		long start = EnvironmentEdgeManager.currentTimeMillis();
		directHealthChecker.checkGet(mockTimeoutRegion);
		long end = EnvironmentEdgeManager.currentTimeMillis();
		Assert.assertTrue(end - start > readTimeout);

		HRegion mockOccasionalExceptionRegion = Mockito.mock(HRegion.class);
		when(mockOccasionalExceptionRegion.get(Mockito.any(Get.class)))
				.thenThrow(TableNotEnabledException.class);
		mockRegionRowKey(mockOccasionalExceptionRegion);
		Assert.assertTrue(directHealthChecker.checkGet(mockOccasionalExceptionRegion));
	}

	@Test
	public void testWrite() throws Exception {
		HRegion normalRegion = Mockito.mock(HRegion.class);
		doAnswer(new Answer() {
			@Override
			public Void answer(InvocationOnMock invocationOnMock) throws Throwable {
				return null;
			}
		}).when(normalRegion).put(Mockito.any(Put.class));
		mockRegionRowKey(normalRegion);
		Assert.assertTrue(directHealthChecker.checkWrite(normalRegion));

		long writeTimeout = conf.getLong(HConstants.CANARY_RPC_TIMEOUT,
				HConstants.DEFAULT_CANARY_RPC_TIMEOUT);
		HRegion mockTimeoutRegion = Mockito.mock(HRegion.class);
		doAnswer(new Answer() {
			@Override
			public Void answer(InvocationOnMock invocationOnMock) throws Throwable {
				Thread.sleep(writeTimeout + 1000);
				return null;
			}
		}).when(mockTimeoutRegion).put(Mockito.any(Put.class));
		mockRegionRowKey(mockTimeoutRegion);
		long start = EnvironmentEdgeManager.currentTimeMillis();
		directHealthChecker.checkWrite(mockTimeoutRegion);
		long end = EnvironmentEdgeManager.currentTimeMillis();
		Assert.assertTrue(end - start > writeTimeout);
	}

	private void mockRegionRowKey(HRegion mock) {
		when(mock.getStartKey()).thenAnswer(new Answer<byte[]>() {
			@Override
			public byte[] answer(InvocationOnMock invocation) {
				return Bytes.toBytes(0);
			}
		});
		when(mock.getEndKey()).thenAnswer(new Answer<byte[]>() {
			@Override
			public byte[] answer(InvocationOnMock invocation) {
				return Bytes.toBytes(0);
			}
		});
	}

	@After
	public void shutdown() throws Exception {

	}
	@AfterClass
	public static void shutdownCluster() throws Exception {
		UTIL.shutdownMiniCluster();
	}
}
