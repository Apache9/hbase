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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.RegionMetrics;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.ServerMetrics;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.PairOfSameType;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xiaomi.infra.thirdparty.com.google.common.annotations.VisibleForTesting;

/**
 * This class is to detect the most busy region and splits it if its R/W QPS is above the
 * threshold periodically.
 */
@InterfaceAudience.Private
public class BusyRegionDetector extends ScheduledChore {

  private static final Logger LOG = LoggerFactory.getLogger(BusyRegionDetector.class);

  public static final String BUSY_REGION_DETECTOR_ENABLE = "hbase.busy.region.detector.enable";
  public static final boolean BUSY_REGION_DETECTOR_ENABLE_DEFAULT = false;
  public static final String BUSY_REGION_DETECTOR_PERIOD = "hbase.busy.region.detector.period";
  public static final int BUSY_REGION_DETECTOR_PERIOD_DEFAULT = (int) TimeUnit.MINUTES.toMillis(2);
  public static final String BUSY_REGION_DETECTOR_READ_QPS_THRESHOLD = "hbase.busy.region.detector.read.qps.threshold";
  public static final Long BUSY_REGION_DETECTOR_READ_QPS_THRESHOLD_DEFAULT = 50000L;
  public static final String BUSY_REGION_DETECTOR_WRITE_QPS_THRESHOLD = "hbase.busy.region.detector.write.qps.threshold";
  public static final Long BUSY_REGION_DETECTOR_WRITE_QPS_THRESHOLD_DEFAULT = 100000L;
  public static final String BUSY_REGION_DETECTOR_CONSECURIVE_BUSY_TIMES = "hbase.busy.region.detector.consecutive.busy.times";
  public static final Integer BUSY_REGION_DETECTOR_CONSECURIVE_BUSY_TIMES_DEFAULT = 5;
  public static final String BUSY_REGION_DETECTOR_MAX_SPLIT_REGION_NUMBER = "hbase.busy.region.detector.max.split.region.number";
  public static final Integer BUSY_REGION_DETECTOR_MAX_SPLIT_REGION_NUMBER_DEFAULT = 10;

  private HMaster master;

  private long busyRegionDetectorPeriod;

  private long readQPSThreshold;

  private long writeQPSThreshold;

  /**
   * Region is really busy if it's checked busy in these consecutive times
   */
  private int maxConsecutiveBusyTimes;

  private int maxNumPerChore;

  /**
   * Count the number of consecutive busy times of region
   */
  private Map<byte[], RegionBusyInfo> consecutiveBusyRegions = new TreeMap<>(Bytes.BYTES_COMPARATOR);

  public BusyRegionDetector(HMaster master, Stoppable stopper, int period) throws IOException {
    super("BusyRegionDetector", stopper, period);

    Configuration conf = master.getConfiguration();
    busyRegionDetectorPeriod = conf.getLong(BUSY_REGION_DETECTOR_PERIOD,
      BUSY_REGION_DETECTOR_PERIOD_DEFAULT);
    readQPSThreshold = conf.getLong(BUSY_REGION_DETECTOR_READ_QPS_THRESHOLD,
      BUSY_REGION_DETECTOR_READ_QPS_THRESHOLD_DEFAULT);
    writeQPSThreshold = conf.getLong(BUSY_REGION_DETECTOR_WRITE_QPS_THRESHOLD,
      BUSY_REGION_DETECTOR_WRITE_QPS_THRESHOLD_DEFAULT);
    maxConsecutiveBusyTimes = conf.getInt(BUSY_REGION_DETECTOR_CONSECURIVE_BUSY_TIMES,
      BUSY_REGION_DETECTOR_CONSECURIVE_BUSY_TIMES_DEFAULT);
    maxNumPerChore = conf.getInt(BUSY_REGION_DETECTOR_MAX_SPLIT_REGION_NUMBER,
      BUSY_REGION_DETECTOR_MAX_SPLIT_REGION_NUMBER_DEFAULT);
    this.master = master;
  }

  @Override
  protected void chore() {
    if (!master.isInitialized()) {
      LOG.debug("Don't run BusyRegionDetector, because master is initializing");
      return;
    }
    List<RegionBusyInfo> busyRegions = findBusyRegions();
    if (busyRegions.isEmpty()) {
      return;
    }
    List<RegionBusyInfo> pickedRegions = pickBusiestRegions(busyRegions);
    List<String> regionNames = pickedRegions.stream()
      .map(r -> Bytes.toStringBinary(r.regionName))
      .collect(Collectors.toList());
    LOG.info("The number of regions in busy is {}, of which {} will be processed: {}",
      busyRegions.size(), pickedRegions.size(), regionNames);
    processBusyRegions(pickedRegions);
  }

  private List<RegionBusyInfo> findBusyRegions() {
    List<RegionBusyInfo> busyRegions = new ArrayList<>();
    Set<byte[]> currentBusyRegions = new TreeSet<>(Bytes.BYTES_COMPARATOR);
    master.getServerManager().getOnlineServers().forEach((serverName, serverMetrics) -> {
      if (!maybeHasBusyRegion(serverMetrics)) {
        return;
      }
      serverMetrics.getRegionMetrics().forEach((regionName, regionMetrics) -> {
        if (!isBusyRegion(regionMetrics) || !canSplit(regionName)) {
          return;
        }
        currentBusyRegions.add(regionName);
        RegionBusyInfo regionBusyInfo =
          consecutiveBusyRegions.computeIfAbsent(regionName, RegionBusyInfo::new);
        regionBusyInfo.updateAndIncreaseTimes(regionMetrics);
        if (regionBusyInfo.consecutiveBusyTimes >= maxConsecutiveBusyTimes) {
          busyRegions.add(regionBusyInfo);
        }
      });
    });

    // not found(e.g. CLOSED regions) or not busy regions will be removed from consecutive busy counter
    consecutiveBusyRegions.entrySet().removeIf(e -> !currentBusyRegions.contains(e.getKey()));
    return busyRegions;
  }

  /**
   * Pick the busiest maxNumPerChore regions
   * @param busyRegions
   * @return
   */
  private List<RegionBusyInfo> pickBusiestRegions(List<RegionBusyInfo> busyRegions) {
    Collections.sort(busyRegions, RegionBusyInfo.BUSY_COMPARATOR.reversed());
    return busyRegions.subList(0, Math.min(maxNumPerChore, busyRegions.size()));
  }

  private void processBusyRegions(List<RegionBusyInfo> busyRegions) {
    // split all passed regions
    List<RegionInfo> splitRegions = splitRegions(busyRegions);
    // get daughters of split region
    Map<RegionInfo, PairOfSameType<RegionInfo>> splitRegionAndDaughters =
      getDaughters(splitRegions);

    List<RegionInfo> regionsInTransition = new ArrayList<>();
    splitRegions.forEach(regionInfo -> {
      PairOfSameType<RegionInfo> daughters = splitRegionAndDaughters.get(regionInfo);
      if (daughters == null) {
        LOG.warn("Split region {} failed or wait timeout, try to major compact it",
          regionInfo.getRegionNameAsString());
        majorCompact(regionInfo.getRegionName());
        return;
      }
      //split completed, and record split table
      consecutiveBusyRegions.remove(regionInfo.getRegionName());
      // move one of daughter regions
      moveRegion(daughters.getFirst());
      regionsInTransition.add(daughters.getFirst());
      regionsInTransition.add(daughters.getSecond());
    });
    // major compact all daughters when they're open
    waitForRegionsOpenOrTimeout(regionsInTransition).forEach(r -> majorCompact(r.getRegionName()));
  }

  @VisibleForTesting
  List<RegionInfo> splitRegions(List<RegionBusyInfo> busyRegions) {
    List<Pair<RegionInfo, Future>> processingRegions = new ArrayList<>();
    try (Admin admin = master.getConnection().getAdmin()) {
      for (RegionBusyInfo busyInfo : busyRegions) {
        RegionInfo regionInfo = getRegionInfo(busyInfo.regionName);
        if (null != regionInfo) {
          LOG.info("Try to split region: {}", Bytes.toStringBinary(regionInfo.getRegionName()));
          Future<Void> future = admin.splitRegionAsync(regionInfo.getRegionName());
          processingRegions.add(Pair.newPair(regionInfo, future));
        }
      }
    } catch (IOException e) {
      LOG.warn("Failed to init HBaseAdmin", e);
    }
    List<RegionInfo> successSplitRegions = new ArrayList<>();
    processingRegions.forEach(p -> {
      RegionInfo regionInfo = p.getFirst();
      try {
        p.getSecond().get(busyRegionDetectorPeriod / 2, TimeUnit.MILLISECONDS);
        successSplitRegions.add(regionInfo);
      } catch (Exception e) {
        LOG.warn("Failed to split busy region: {}", regionInfo.getRegionNameAsString(), e);
      }
    });
    return successSplitRegions;
  }

  /**
   * Move region, randomly choose destination server
   * @param region
   * @return
   */
  @VisibleForTesting
  boolean moveRegion(RegionInfo region) {
    LOG.info("Try to move region: {}", region.getRegionNameAsString());
    try (Admin admin = master.getConnection().getAdmin()) {
      admin.move(region.getEncodedNameAsBytes());
    } catch (IOException e) {
      LOG.warn("Failed to move the region {}", region.getRegionNameAsString(), e);
      return false;
    }
    return true;
  }

  private void majorCompact(byte[] regionName) {
    LOG.info("Try to major compact region: {}", Bytes.toStringBinary(regionName));
    try (Admin admin = master.getConnection().getAdmin()) {
      admin.majorCompactRegion(regionName);
    } catch (IOException e) {
      LOG.warn("Failed to major compact region {}", Bytes.toStringBinary(regionName), e);
    }
  }

  private Map<RegionInfo, PairOfSameType<RegionInfo>> getDaughters(List<RegionInfo> splitRegions) {
    Map<RegionInfo, PairOfSameType<RegionInfo>> splitRegionAndDaughters = new HashMap<>();
    splitRegions.forEach(regionInfo -> {
      PairOfSameType<RegionInfo> daughters = tryToGetDaughters(regionInfo.getRegionName());
      if (null != daughters) {
        splitRegionAndDaughters.put(regionInfo, daughters);
      }
    });
    return splitRegionAndDaughters;
  }

  /**
   * Try to get the daughters of split region
   * @param regionName
   * @return
   */
  @VisibleForTesting
  PairOfSameType<RegionInfo> tryToGetDaughters(byte[] regionName) {
    try {
      Result result = MetaTableAccessor.getRegionResult(master.getConnection(), regionName);
      PairOfSameType<RegionInfo> daughters = MetaTableAccessor.getDaughterRegions(result);
      if (daughters.getFirst() != null && daughters.getSecond() != null) {
        return daughters;
      }
    } catch (IOException e) {
      LOG.warn("Failed to get daughters of the split region: {}",
        Bytes.toStringBinary(regionName), e);
    }
    return null;
  }

  private Set<RegionInfo> waitForRegionsOpenOrTimeout(List<RegionInfo> regionsInTransition) {
    Set<RegionInfo> movedRegions = new HashSet<>();
    long start = System.currentTimeMillis();
    while (System.currentTimeMillis() - start < busyRegionDetectorPeriod / 2) {
      try {
        regionsInTransition.stream()
          .filter(r -> !movedRegions.contains(r))
          .filter(master.getAssignmentManager().getRegionStates()::isRegionOnline)
          .filter(r -> master.getAssignmentManager().getRegionStates().getRegionState(r).isOpened())
          .forEach(movedRegions::add);
        if (movedRegions.size() == regionsInTransition.size()) {
          break;
        }
        Thread.sleep(TimeUnit.SECONDS.toMillis(5));
      } catch (InterruptedException e) {
        LOG.warn("Waiting for region moved interrupted", e);
        break;
      }
    }
    return movedRegions;
  }

  /**
   * Check whether the region can be split
   * @param regionName
   * @return
   */
  @VisibleForTesting
  boolean canSplit(byte[] regionName) {
    RegionInfo region = getRegionInfo(regionName);
    if (region == null || region.isMetaRegion()) {
      return false;
    }
    return true;
  }

  private RegionInfo getRegionInfo(byte[] regionName) {
    try {
      Result result = MetaTableAccessor.getRegionResult(master.getConnection(), regionName);
      return MetaTableAccessor.getRegionInfo(result);
    } catch (IOException e) {
      LOG.warn("Failed to get RegionInfo named {}", Bytes.toStringBinary(regionName));
    }
    return null;
  }

  @VisibleForTesting
  boolean maybeHasBusyRegion(ServerMetrics serverMetrics) {
    // read QPS
    if (serverMetrics.getRequestCountPerSecond() >= readQPSThreshold) {
      return true;
    }
    // write QPS
    if (serverMetrics.getRequestCountPerSecond() >= writeQPSThreshold) {
      return true;
    }
    return false;
  }

  @VisibleForTesting
  boolean isBusyRegion(RegionMetrics regionMetrics) {
    // read QPS
    if (regionMetrics.getReadRequestsPerSecond() >= readQPSThreshold) {
      return true;
    }
    // write QPS
    if (regionMetrics.getWriteRequestsPerSecond() >= writeQPSThreshold) {
      return true;
    }
    return false;
  }

  static class RegionBusyInfo {

    static Comparator<RegionBusyInfo> BUSY_COMPARATOR = (o1, o2) -> {
      int[] results = new int[] {
          Integer.compare(o1.consecutiveBusyTimes, o2.consecutiveBusyTimes),
          Long.compare(o1.regionMetrics.getReadRequestsPerSecond(), o2.regionMetrics.getReadRequestsPerSecond()),
          Long.compare(o1.regionMetrics.getWriteRequestsPerSecond(), o2.regionMetrics.getWriteRequestsPerSecond())
      };
      for (int r : results) {
        if (r != 0) {
          return r;
        }
      }
      return 0;
    };

    private byte[] regionName;
    private int consecutiveBusyTimes;
    private RegionMetrics regionMetrics;

    public RegionBusyInfo(byte[] regionName) {
      this.regionName = regionName;
    }

    void updateAndIncreaseTimes(RegionMetrics regionMetrics) {
      this.regionMetrics = regionMetrics;
      ++ consecutiveBusyTimes;
    }
  }
}
