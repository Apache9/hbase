/**
 *
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

package com.xiaomi.infra.hbase.master.chore;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

/**
 * This class is to log the result of the BadRSDetector chore execution.
 */
public final class BadRsDetectorStats {
	private static final String SEPARATOR = " : ";
	private static final String END_LINE = "\n";
	private static final DateTimeFormatter DATE_TIME_FORMATTER =
			DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
	private String clusterName;
	private String details;
	private long startTime;
	private long endTime;
	private boolean isSuccess;
	private boolean isMetaMoved;
	private String regionServer;
	private double load;
	private double loadPerCoreThreshold;
	private List<String> regionNames;

	private BadRsDetectorStats(Builder builder) {
		details = builder.details;
		clusterName = builder.clusterName;
		startTime = builder.startTime;
		endTime = builder.endTime;
		isSuccess = builder.isSuccess;
		isMetaMoved = builder.isMetaMoved;
		regionServer = builder.regionServer;
		load = builder.load;
		loadPerCoreThreshold = builder.loadPerCoreThreshold;
		regionNames = builder.regionNames;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		return sb.append(toLine("details", details)).append(toLine("clusterName", clusterName))
				.append(toLine("RegionServer", regionServer)).append(toLine("load", load))
				.append(toLine("loadPerCoreThreshold", loadPerCoreThreshold))
				.append(toLine("detector startTime", timeToString(startTime)))
				.append(toLine("detector endTime", timeToString(endTime)))
				.append(toLine("isSuccess", isSuccess))
				.append(toLine("isMetaMoved", isMetaMoved))
				.append(toLine("regionNames", regionNames.stream().collect(Collectors.joining(","))))
				.toString();
	}

	public String getClusterName() {
		return clusterName;
	}

	public String getDetails() {
		return details;
	}

	public boolean isSuccess() {
		return isSuccess;
	}

	private <T> String toLine(String key, T value) {
		return key + SEPARATOR + value + END_LINE;
	}

	private String timeToString(long unixtimestamp) {
		return Instant.ofEpochMilli(unixtimestamp).atZone(ZoneId.systemDefault())
				.format(DATE_TIME_FORMATTER);
	}

	public static class Builder {
		private String clusterName = "Unknown";
		private String details = "";
		private long startTime = 0L;
		private long endTime = 0L;
		private boolean isSuccess = false;
		private boolean isMetaMoved = false;
		private String regionServer = "";
		private double load = 0.0;
		private double loadPerCoreThreshold =
				HConstants.DEFAULT_BAD_REGIONSERVER_LOAD_PER_CORE_THRESHOLD;
		private List<String> regionNames = new ArrayList<>();

		public Builder setClusterName(String clusterName) {
			this.clusterName = clusterName;
			return this;
		}

		public Builder setDetails(String details) {
			this.details = details;
			return this;
		}

		public Builder setStartTime(long startTime) {
			this.startTime = startTime;
			return this;
		}

		public Builder setEndTime(long endTime) {
			this.endTime = endTime;
			return this;
		}

		public Builder setIsSuccess(boolean isSuccess) {
			this.isSuccess = isSuccess;
			return this;
		}

		public Builder setIsMetaMoved(boolean isMetaMoved) {
			this.isMetaMoved = isMetaMoved;
			return this;
		}

		public Builder setRegionServer(String regionServer) {
			this.regionServer = regionServer;
			return this;
		}

		public Builder setLoad(double load) {
			this.load = load;
			return this;
		}

		public Builder setLoadPerCoreThreshold(double loadPerCoreThreshold) {
			this.loadPerCoreThreshold = loadPerCoreThreshold;
			return this;
		}

		public Builder setRegionNames(List<String> regionNames) {
			this.regionNames = regionNames;
			return this;
		}

		public BadRsDetectorStats build() {
			endTime = EnvironmentEdgeManager.currentTimeMillis();
			return new BadRsDetectorStats(this);
		}

	}

}
