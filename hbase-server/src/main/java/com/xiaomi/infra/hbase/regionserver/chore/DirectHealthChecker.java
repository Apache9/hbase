package com.xiaomi.infra.hbase.regionserver.chore;

import static org.apache.hadoop.hbase.HConstants.EMPTY_BYTE_ARRAY;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HealthChecker;
import org.apache.hadoop.hbase.HealthCheckerExitStatus;
import org.apache.hadoop.hbase.HealthReport;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.RegionTooBusyException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.NoSuchColumnFamilyException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

public final class DirectHealthChecker implements HealthChecker {
	private static final Logger logger = LoggerFactory.getLogger(DirectHealthChecker.class);
	private static final int MAX_REGION_CHECKED = 100;

	private final HRegionServer regionServer;
	private final int actionTimeout;
	private final double successRate;
	private final ExecutorService executorService;

	public DirectHealthChecker(HRegionServer server) {
		this.regionServer = server;
		Configuration conf = server.getConfiguration();
		actionTimeout = conf.getInt(HConstants.CANARY_RPC_TIMEOUT,
				HConstants.DEFAULT_CANARY_RPC_TIMEOUT);
		successRate = conf.getDouble(HConstants.DIRECT_HEALTH_CHECKER_SUCCESS_RATE_THRESHOLD,
				HConstants.DEFAULT_DIRECT_HEALTH_CHECKER_SUCCESS_RATE_THRESHOLD);
		int executorPoolSize = conf.getInt(HConstants.DIRECT_HEALTH_CHECKER_EXECUTOR_THREAD_POOL_SIZE,
				HConstants.DEFAULT_DIRECT_HEALTH_CHECKER_EXECUTOR_THREAD_POOL_SIZE);
		executorService = Executors.newFixedThreadPool(executorPoolSize,
				Threads.newDaemonThreadFactory("DirectHealthChecker-executorService-"));
	}

	@Override
	public HealthReport checkHealth() {
		HealthReport readHealthReport = checkReadHealth();
		HealthReport writeHealthReport = checkWriteHealth();
		if ((readHealthReport.getStatus() == HealthCheckerExitStatus.SUCCESS) && (
				writeHealthReport.getStatus() == HealthCheckerExitStatus.SUCCESS)) {
			logger.info("DirectHealthChecker success");
			return new HealthReport(HealthCheckerExitStatus.SUCCESS, "DirectHealthChecker success");
		} else {
			logger.warn(readHealthReport.getReport());
			logger.warn(writeHealthReport.getReport());
			return new HealthReport(HealthCheckerExitStatus.FAILED, "DirectHealthChecker failure");
		}
	}

	private HealthReport checkReadHealth() {
		List<HRegion> readRegions = selectReadRegions();
		int successCount = 0;

		for (HRegion region : readRegions) {
			CompletableFuture<Boolean> successFuture =
					CompletableFuture.supplyAsync(() -> checkGet(region), executorService);
			if (completeWithin(successFuture, actionTimeout)) {
				++successCount;
			}
		}
		return newHealthReport("read", successCount, readRegions.size() * successRate);
	}

	private HealthReport checkWriteHealth() {
		List<HRegion> writeRegions = selectWriteRegions();
		int successCount = 0;
		for (HRegion region : writeRegions) {
			CompletableFuture<Boolean> successFuture =
					CompletableFuture.supplyAsync(() -> checkWrite(region), executorService);
			if (completeWithin(successFuture, actionTimeout)) {
				++successCount;
			}
		}

		return newHealthReport("write", successCount, writeRegions.size() * successRate);
	}

	@VisibleForTesting
	boolean checkGet(HRegion region) {
		byte[] rowKey = randomRowKey(region);
		Get get = new Get(rowKey).setFilter(new FirstKeyOnlyFilter()).setCheckExistenceOnly(true);
		get.setCacheBlocks(false);
		try {
			region.get(get);
			return true;
		} catch (IOException ioe) {
			String regionName = region.getRegionNameAsString();
			// These exceptions are none of my business, and will recover in next round,
			// so seen as success.
			if (isOccasionalException(ioe)) {
				logger
						.debug("Failed to get but unrelated to health checker for region {}", regionName, ioe);
				return true;
			} else {
				logger.warn("Failed to check row exists for region {}", regionName, ioe);
				return false;
			}
		}
	}

	@VisibleForTesting
	boolean checkWrite(HRegion region) {
    byte[] rowKey = randomRowKey(region);
		Put put = new Put(rowKey)
				.add(Bytes.toBytes(HConstants.CANARY_TABLE_FAMILY_NAME), EMPTY_BYTE_ARRAY, EMPTY_BYTE_ARRAY);
		try {
			region.put(put);
			return true;
		} catch (IOException ioe) {
			String regionName = region.getRegionNameAsString();
			// These exceptions are none of my business, and will recover in next round,
			// so seen as success. The same as checkGet.
			if (isOccasionalException(ioe)) {
				logger.debug("Failed to get but unrelated to health checker for region {}", regionName,
						ioe);
				return true;
			} else {
				logger.warn("Failed to put canary for region {} ", regionName);
				return false;
			}
		}
	}

	private byte[] randomRowKey(HRegion region) {
		byte[] rowKey = region.getStartKey();
		try {
			rowKey = Bytes.randomKey(region.getStartKey(), region.getEndKey());
		} catch (IOException e) {
			logger.debug("Failed to random key between {} and {} Exception {}",
					Bytes.toString(region.getStartKey()), Bytes.toString(region.getEndKey()), e);
		}
		return rowKey;
	}

	private List<HRegion> selectReadRegions() {
		List<HRegion> allRegions = regionServer.getOnlineRegions();
		return normalizeRegionSize(allRegions);
	}

	private List<HRegion> selectWriteRegions() {
		List<HRegion> canaryRegions =
				regionServer.getOnlineRegions(TableName.valueOf(HConstants.CANARY_TABLE_NAME));
		return normalizeRegionSize(canaryRegions);
	}

	private List<HRegion> normalizeRegionSize(List<HRegion> source) {
		if (source.isEmpty()) {
			return Collections.emptyList();
		}
		Collections.shuffle(source);
		List<HRegion> normalizedRegions = new ArrayList<>(MAX_REGION_CHECKED);
		int size = source.size();
		for (int i = 0; i < MAX_REGION_CHECKED; ++i) {
			normalizedRegions.add(source.get(i % size));
		}
		return normalizedRegions;
	}

	private boolean completeWithin(CompletableFuture<Boolean> future, long timeout) {
		try {
			return future.get(timeout, TimeUnit.MILLISECONDS);
		} catch (InterruptedException | ExecutionException | TimeoutException e) {
			logger.warn("Failed to get Future within " + timeout + " ms, exception ", e);
			return false;
		}
	}

	private boolean isOccasionalException(IOException e) {
		return isTableDdlException(e) || isRegionInNonOperationState(e);
	}

	private boolean isTableDdlException(IOException e) {
		return e instanceof TableNotFoundException || e instanceof TableNotEnabledException
				|| e instanceof NoSuchColumnFamilyException || e.getMessage()
				.contains("TableNotFoundException") || e.getMessage().contains("is disabled");
	}

	private boolean isRegionInNonOperationState(IOException e) {
		return e.getMessage().contains("region is read only") || e instanceof RegionTooBusyException
				|| e instanceof NotServingRegionException;
	}

	private HealthReport newHealthReport(String checkType, int successCount,
			double successThreshold) {
		HealthCheckerExitStatus status = getStatus(successCount, successThreshold);
		String report = createReport(checkType, successCount, successThreshold);
		return new HealthReport(status, report);
	}

	private String createReport(String checkType, int successCount, double successThreshold) {
		return "DirectHealthChecker " + checkType + " successCount " + successCount
				+ ", successThreshold " + successThreshold;
	}

	private HealthCheckerExitStatus getStatus(int successCount, double successThreshold) {
		return successCount >= successThreshold ?
				HealthCheckerExitStatus.SUCCESS :
				HealthCheckerExitStatus.FAILED;
	}
}
