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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hbase.Chore;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.xiaomi.infra.base.nameservice.ClusterInfo;
import com.xiaomi.infra.base.nameservice.ZkClusterInfo;
import com.xiaomi.infra.hbase.falcon.FalconConstant;
import com.xiaomi.infra.hbase.falcon.FalconDataPoint;
import com.xiaomi.infra.hbase.falcon.FalconLatestRequest;
import com.xiaomi.infra.hbase.falcon.FalconLatestResponse;
import com.xiaomi.infra.hbase.util.HttpClientUtils;
import com.xiaomi.infra.hbase.util.MailUtils;

/**
 * This class is to detect the highest load regionserver and restart it if its load is above the
 * threshold periodically.
 */
public class BadRSDetector extends Chore {
	private static final Logger LOG = LoggerFactory.getLogger(BadRSDetector.class.getName());
	private static final Gson GSON = new Gson();
	private final long maxExecutionTime;

	private HMaster master;

	private String clusterName;
	private double badRsLoadPerCoreThreshold;
	private int continuousCount = 0;

	/**
	 * This chore is to detect very high load regionservers and restart them, which are seen as one
	 * of many causes of availability problems.
	 *
	 * @param period execution period
	 */
  public BadRSDetector(HMaster master, Stoppable stoppable, int period) {
    super("BadRSDetector", period, stoppable);
    this.master = master;
    this.maxExecutionTime = period;

	  clusterName = master.getConfiguration().get(HConstants.CLUSTER_NAME, "");
	  badRsLoadPerCoreThreshold = master.getConfiguration()
        .getDouble(HConstants.BAD_REGIONSERVER_LOAD_PER_CORE_THRESHOLD,
            HConstants.DEFAULT_BAD_REGIONSERVER_LOAD_PER_CORE_THRESHOLD);
  }

	@Override
	protected void chore() {
		long startTime = EnvironmentEdgeManager.currentTimeMillis();
		BadRsDetectorStats.Builder statsBuilder =
				new BadRsDetectorStats.Builder().setStartTime(startTime).setClusterName(clusterName);
		if (!master.isInitialized()) {
			LOG.warn("Not running BadRSDetector because master is initializing");
			return;
		} else if (this.master.getAssignmentManager().getRegionStates().isRegionsInTransition()) {
			LOG.info("Not running BadRSDetector because there is at least one region in RIT");
			return;
		}
		boolean originalBalanceSwitch = master.isBalancerOn();
		process(statsBuilder);
		if (originalBalanceSwitch) {
			try {
				master.balanceSwitch(originalBalanceSwitch);
			} catch (IOException e) {
				LOG.warn("Failed to set balance switch " + originalBalanceSwitch, e);
			}
		}
		postFalcon();
	}

	private void process(BadRsDetectorStats.Builder statsBuilder) {
		Map<HRegionInfo, ServerName> regionAssignments = getRegionAssignments();
		Set<ServerName> serverNames = new HashSet<>(regionAssignments.values());
		Pair<String, Double> maxLoadHost = getMaxLoadServer(serverNames);
		statsBuilder.setRegionServer(maxLoadHost.getKey()).setLoad(maxLoadHost.getValue())
				.setLoadPerCoreThreshold(badRsLoadPerCoreThreshold);
		LOG.info("The max load per core is " + maxLoadHost.getValue() + " by regionserver "
				+ maxLoadHost.getKey() + ", load per core threshold is " + badRsLoadPerCoreThreshold);
		if (maxLoadHost.getValue() < badRsLoadPerCoreThreshold) {
			continuousCount = 0;
			LOG.info("Not running BadRSDetector because the max load is less than threshold");
			return;
		}

		List<ServerName> vacatedDestinations =
				master.getServerManager().getOnlineServersList().stream().filter(
						serverName -> !serverName.getHostAndPort().contains(maxLoadHost.getKey()))
				.collect(Collectors.toList());

		Map<ServerName, Set<HRegionInfo>> vacatedServerMap = filterRegion(regionAssignments,
				e -> e.getValue().getHostAndPort().contains(maxLoadHost.getKey()));

		Set<String> vacatedRegionEncodedNames =
				vacatedServerMap.values().stream().flatMap(regionInfo -> regionInfo.stream())
						.map(HRegionInfo::getEncodedName)
				.collect(Collectors.toSet());
		boolean isMetaMoved =
				vacatedServerMap.values().stream().flatMap(regionInfo -> regionInfo.stream())
						.anyMatch(HRegionInfo::isMetaRegion);
		statsBuilder.setIsMetaMoved(isMetaMoved);
		try {
			master.balanceSwitch(false);
		} catch (IOException e) {
			LOG.warn("Not running BadRSDetector because failed to turn off the "
					+ "balance switch before vacate RS!", e);
			return;
		}

		Set<HRegionInfo> failVacatedRegions =
				vacateRegionServers(vacatedServerMap, vacatedDestinations);
    if (!failVacatedRegions.isEmpty()) {
	    String errorMessage = "Failed to vacate region(s) " + toString(failVacatedRegions);
      logAndSendMail(statsBuilder, false, errorMessage);
      return;
    }

    Set<String> hostAndPortsToRestart =
        vacatedServerMap.keySet().stream().map(ServerName::getHostAndPort)
            .collect(Collectors.toSet());
    if (restartRegionServers(hostAndPortsToRestart)) {
	    ++continuousCount;
	    List<ServerName> recoverDestinations =
			    master.getServerManager().getOnlineServersList().stream()
          .filter(serverName -> hostAndPortsToRestart.contains(serverName.getHostAndPort()))
          .collect(Collectors.toList());
	    regionAssignments = getRegionAssignments();

	    Map<ServerName, Set<HRegionInfo>> recoverSourceMap = filterRegion(regionAssignments,
			    e -> vacatedRegionEncodedNames.contains(e.getKey().getEncodedName()));

      Set<HRegionInfo> failRecoveredRegions =
		      recoverRegionServers(recoverSourceMap, recoverDestinations);
      if (failRecoveredRegions.isEmpty()) {
	      String message = "Success to recover regionservers " + toString(recoverDestinations);
        logAndSendMail(statsBuilder, true, message);
      } else {
	      String errorMessage = "Failed to recover regions " + toString(failRecoveredRegions);
        logAndSendMail(statsBuilder, false, errorMessage);
      }
		}
	}

	private Map<ServerName, Set<HRegionInfo>> filterRegion(
			Map<HRegionInfo, ServerName> regionAssignments,
			Predicate<Map.Entry<HRegionInfo, ServerName>> p) {
		return regionAssignments.entrySet().stream().filter(p).collect(Collectors
				.toMap(Map.Entry::getValue, e -> Sets.newHashSet(e.getKey()),
						(Set<HRegionInfo> oldSet, Set<HRegionInfo> newSet) -> {
							oldSet.addAll(newSet);
							return oldSet;
						}));
	}

  /**
   * First of all, we should vacate the region servers before restart them.
   *
   * @param vacatedServerMap ServerNames and its RegionInfos that are vacated.
   * @param destinations         ServerNames which the region is vacated to.
   * @return the failed regions.
   */
  @VisibleForTesting
  Set<HRegionInfo> vacateRegionServers(Map<ServerName, Set<HRegionInfo>> vacatedServerMap,
      List<ServerName> destinations) {
	  return moveRegion(vacatedServerMap, destinations, true);
  }

  private boolean restartRegionServers(Set<String> hostAndPorts) {
	  long startTime = EnvironmentEdgeManager.currentTimeMillis();
    for (String hostAndPort : hostAndPorts) {
      LOG.info("Try to stop regionserver " + hostAndPort);
	    stopRegionServer(hostAndPort);
    }

	  while (EnvironmentEdgeManager.currentTimeMillis() - startTime < maxExecutionTime) {
		  try {
			  LOG.info("Wait 10s to restart the stopped regionservers {}", toString(hostAndPorts));
			  Thread.sleep(TimeUnit.SECONDS.toMillis(10));
		  } catch (InterruptedException e) {
			  LOG.error("Interrupted when waiting for the regionserver to restart.", e);
			  Thread.currentThread().interrupt();
		  }

		  Collection<ServerName> allServerNames = master.getServerManager().getOnlineServersList();
		  Set<ServerName> restartedServerNames = allServerNames.stream()
				  .filter(serverName -> hostAndPorts.contains(serverName.getHostAndPort()))
				  .filter(serverName -> serverName.getStartcode() > startTime).collect(Collectors.toSet());
		  if (restartedServerNames.size() == hostAndPorts.size()) {
			  LOG.info("Success to restart the stopped regionservers {}", toString(hostAndPorts));
			  return true;
		  }
	  }
	  LOG.error("Timeout when waiting for the regionserver(s) {} to restart",
			  toString(hostAndPorts));
	  return false;
  }

  @VisibleForTesting
	void stopRegionServer(String hostAndPort) {
		try (HBaseAdmin admin = new HBaseAdmin(
					HConnectionManager.getConnection(master.getConfiguration()))) {
				admin.stopRegionServer(hostAndPort);
		} catch (Exception e) {
			LOG.warn("Got exception when stopping regionserver {}", hostAndPort, e);
		}
	}

  /**
   * There is no need to move original servers because after the restart, all the vacated
   * regionsrevers are the same.
   */
  @VisibleForTesting
  Set<HRegionInfo> recoverRegionServers(Map<ServerName, Set<HRegionInfo>> vacatedServerMap,
      List<ServerName> targetServerNames) {
	  return moveRegion(vacatedServerMap, targetServerNames, false);
  }

	private Pair<String, Double> getMaxLoadServer(Set<ServerName> serverNames) {
		Pair<String, Double> maxLoad = Pair.of("", 0.0);
		for (ServerName serverName : serverNames) {
			FalconLatestRequest request =
					new FalconLatestRequest.Builder(serverName.getHostname(), FalconConstant.METRIC_LOAD_1MIN)
							.build();
			String result = HttpClientUtils
					.post("http://" + serverName.getHostname() + ":1988/page/system/loadavg", request);
			if (result == null || StringUtils.isBlank(result)) {
				LOG.debug("Failed to get [{}] 's load from Falcon.", serverName);
				continue;
			}
			FalconLatestResponse response = GSON.fromJson(result, FalconLatestResponse.class);
			// The data format is data:[[1.55, 6], [2.19, 9], [3.64, 15]], the left is the load,
			// and the right is the load / (cpu cores * 100 ).
			// Obviously the right one makes more sense, and we choose the right one as the compactor to
			// sort.
			List<List<Double>> loads = response.getLoad();
			if (loads == null || loads.size() != 3) {
				continue;
			}
			List<Double> lastOneMinuteLoad = loads.get(0);
			if ((lastOneMinuteLoad.size() == 2) && (lastOneMinuteLoad.get(1) > maxLoad.getRight())) {
				maxLoad = Pair.of(serverName.getHostname(), lastOneMinuteLoad.get(1));
			}
		}
		// The load per core from falcon is the percent, which may be confusing in some cases,
		// so here use load per core instead of percent.
		maxLoad = Pair.of(maxLoad.getLeft(), maxLoad.getValue() / 100.0);
		return maxLoad;
	}

	private Set<HRegionInfo> moveRegion(Map<ServerName, Set<HRegionInfo>> sourceMap,
			List<ServerName> destinations, boolean isVacated) {
	  if (destinations.isEmpty()) {
		  return sourceMap.values().stream()
				  .flatMap(regionInfos -> regionInfos.stream()).collect(Collectors.toSet());
    }

		int i = 0;
		int destSize = destinations.size();
		List<RegionPlan> movePlans = new ArrayList<>();
		RegionPlan metaRegionPlan = null;
		for (Map.Entry<ServerName, Set<HRegionInfo>> entry : sourceMap.entrySet()) {
			ServerName source = entry.getKey();
			Set<HRegionInfo> regionInfos = entry.getValue();
			for (HRegionInfo regionInfo : regionInfos) {
				ServerName destination = destinations.get((i++) % destSize);
				RegionPlan regionPlan = new RegionPlan(regionInfo, source, destination);
				if (regionInfo.isMetaRegion()) {
					metaRegionPlan = regionPlan;
				} else {
					movePlans.add(regionPlan);
				}
			}
		}

		if (isVacated) {
			if (metaRegionPlan != null) {
				LOG.info("Moving meta region {} from {} to {} ",
						metaRegionPlan.getRegionInfo().getRegionNameAsString(), metaRegionPlan.getSource(),
						metaRegionPlan.getDestination());
				master.getAssignmentManager().balance(metaRegionPlan);
			}
		}
		for (RegionPlan regionPlan : movePlans) {
			LOG.info("Moving region {} from {} to {} ",
					regionPlan.getRegionInfo().getRegionNameAsString(), regionPlan.getSource(),
					regionPlan.getDestination());
			master.getAssignmentManager().balance(regionPlan);
		}

		if (!isVacated) {
			return Collections.emptySet();
		}

		long statTime = EnvironmentEdgeManager.currentTimeMillis();
		Set<ServerName> sourceServerNames = sourceMap.keySet();
		Set<HRegionInfo> mayFailedRegionInfos = new HashSet<>();
		while (EnvironmentEdgeManager.currentTimeMillis() - statTime < maxExecutionTime) {
			try {
				Thread.sleep(TimeUnit.SECONDS.toMillis(10));
			} catch (InterruptedException e) {
				LOG.warn("Interrupted when waiting for moving the region to succeed by ", e);
				Thread.currentThread().interrupt();
			}
			mayFailedRegionInfos = sourceServerNames.stream().flatMap(
					s -> master.getAssignmentManager().getRegionStates().getServerRegions(s).stream())
					.collect(Collectors.toSet());
			if (mayFailedRegionInfos.isEmpty()) {
				return mayFailedRegionInfos;
			}
		}
		LOG.warn("Timeout when waiting the regions {} to offline from {}",
				toString(mayFailedRegionInfos), toString(new ArrayList<>(sourceServerNames)));
		return mayFailedRegionInfos;
	}

  private void logAndSendMail(BadRsDetectorStats.Builder statsBuilder, boolean success,
      String message) {
    if (success) {
      LOG.info(message);
    } else {
      LOG.warn(message);
    }
    BadRsDetectorStats stats = statsBuilder.setDetails(message).setIsSuccess(success).build();
	  MailUtils.sendMail(master.getConfiguration(), HConstants.MAIL_TO,
			  "BadRSDetector for cluster " + stats.getClusterName(), stats.toString());
  }

	@VisibleForTesting
	String toString(Collection<HRegionInfo> regionInfoList) {
    return regionInfoList.stream().map(HRegionInfo::getEncodedName)
        .collect(Collectors.joining(", "));
  }

	@VisibleForTesting
	String toString(Set<String> hostAndPorts) {
    return hostAndPorts.stream().collect(Collectors.joining(", "));
  }

	@VisibleForTesting
	String toString(List<ServerName> serverNames) {
    return serverNames.stream().map(ServerName::toString).collect(Collectors.joining(", "));
  }

	@VisibleForTesting
	Map<HRegionInfo, ServerName> getRegionAssignments() {
		return master.getAssignmentManager().getRegionStates().getRegionAssignments();
	}

	private void postFalcon() {
  	if (StringUtils.isBlank(clusterName)) {
  		return;
	  }
		String type = "unknown";
		try {
			ZkClusterInfo.ClusterType clusterType =
					new ClusterInfo(clusterName).getZkClusterInfo().getClusterType();
			type = clusterType.toString().toLowerCase();
		} catch (IOException e) {
			LOG.warn("Failed to new ClusterInfo for {}", clusterName, e);
		}

		String tags = "type=" + type + ",cluster=" + clusterName;
		FalconDataPoint dataPoint =
				new FalconDataPoint.Builder().setEndpoint(HConstants.MASTER_ENDPOINT)
				.setMetric(HConstants.FALCON_METRIC_CONTINUOUS_RESTART_COUNT)
						.setTimestamp(EnvironmentEdgeManager.currentTimeMillis()).setStep(getPeriod() / 1000)
						.setTags(tags).setValue(continuousCount).build();
		HttpClientUtils.post(HConstants.DEFAULT_FALCON_URI, dataPoint);
	}
}