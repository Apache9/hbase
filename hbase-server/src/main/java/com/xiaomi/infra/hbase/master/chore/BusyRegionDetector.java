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

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Chore;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.RegionLoad;
import org.apache.hadoop.hbase.ServerLoad;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.catalog.MetaReader;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.PairOfSameType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * This class is to detect the most busy region and splits it if its R/W QPS is above the
 * threshold periodically.
 */
public class BusyRegionDetector extends Chore {

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

  private HConnection connection;

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
    super("BusyRegionDetector", period, stopper);

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
    connection = HConnectionManager.getConnection(conf);
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
    master.getServerManager().getOnlineServers().forEach((serverName, serverLoad) -> {
      if (!maybeHasBusyRegion(serverLoad)) {
        return;
      }
      serverLoad.getRegionsLoad().forEach((regionName, regionLoad) -> {
        if (!isBusyRegion(regionLoad) || !canSplit(regionName)) {
          return;
        }
        currentBusyRegions.add(regionName);
        RegionBusyInfo regionBusyInfo =
            consecutiveBusyRegions.computeIfAbsent(regionName, RegionBusyInfo::new);
        regionBusyInfo.updateAndIncreaseTimes(regionLoad);
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
    Collections.sort(busyRegions);
    return busyRegions.subList(0, Math.min(maxNumPerChore, busyRegions.size()));
  }

  private void processBusyRegions(List<RegionBusyInfo> busyRegions) {
    // split all passed regions
    List<HRegionInfo> processingRegions = splitRegions(busyRegions);
    // wait for meta info of all split regions updates to complete
    Map<HRegionInfo, PairOfSameType<HRegionInfo>> splitRegionAndDaughters =
        waitForDaughtersOrTimeout(processingRegions);

    List<HRegionInfo> regionsInTransition = new ArrayList<>();
    processingRegions.forEach(regionInfo -> {
      PairOfSameType<HRegionInfo> daughters = splitRegionAndDaughters.get(regionInfo);
      if (daughters == null) {
        LOG.warn("Split region {} failed or wait timeout, try to major compact it",
            regionInfo.getRegionNameAsString());
        majorCompact(regionInfo);
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
    waitForRegionsOpenOrTimeout(regionsInTransition).forEach(this::majorCompact);
  }

  @VisibleForTesting
  List<HRegionInfo> splitRegions(List<RegionBusyInfo> busyRegions) {
    List<HRegionInfo> processingRegions = new ArrayList<>();
    try (HBaseAdmin admin = new HBaseAdmin(connection)) {
      busyRegions.forEach(busyInfo -> {
        HRegionInfo regionInfo = getHRegionInfo(busyInfo.regionName);
        if (regionInfo != null) {
          try {
            LOG.info("Try to split region: {}", regionInfo.getRegionNameAsString());
            admin.split(regionInfo.getRegionName());
            processingRegions.add(regionInfo);
          } catch (Exception e) {
            LOG.warn("Failed to split busy region: {}", regionInfo.getRegionNameAsString(), e);
          }
        }
      });
    } catch (IOException e) {
      LOG.warn("Failed to init HBaseAdmin", e);
    }
    return processingRegions;
  }

  /**
   * Move region, randomly choose destination server
   * @param region
   * @return
   */
  @VisibleForTesting
  boolean moveRegion(HRegionInfo region) {
    LOG.info("Try to move region: {}", region.getRegionNameAsString());
    try (HBaseAdmin admin = new HBaseAdmin(connection)) {
      admin.move(region.getEncodedNameAsBytes(), null);
    } catch (IOException e) {
      LOG.warn("Failed to move the region {}", region.getRegionNameAsString(), e);
      return false;
    }
    return true;
  }

  private void majorCompact(HRegionInfo region) {
    LOG.info("Try to major compact region: {}", region.getRegionNameAsString());
    try (HBaseAdmin admin = new HBaseAdmin(connection)) {
      admin.majorCompact(region.getRegionName());
    } catch (IOException | InterruptedException e) {
      LOG.warn("Failed to major compact region {}", region.getRegionNameAsString(), e);
    }
  }

  private Map<HRegionInfo, PairOfSameType<HRegionInfo>> waitForDaughtersOrTimeout(
      List<HRegionInfo> processingRegions) {
    Map<HRegionInfo, PairOfSameType<HRegionInfo>> splitRegionAndDaughters = new HashMap<>();
    long start = System.currentTimeMillis();
    while (System.currentTimeMillis() - start < busyRegionDetectorPeriod / 2) {
      try {
        processingRegions.stream()
            .filter(r -> !splitRegionAndDaughters.containsKey(r))
            .forEach(regionInfo -> {
              PairOfSameType<HRegionInfo> daughters = tryToGetDaughters(regionInfo.getRegionName());
              if (null != daughters) {
                splitRegionAndDaughters.put(regionInfo, daughters);
              }
            });
        if (splitRegionAndDaughters.size() == processingRegions.size()) {
          break;
        }
        Thread.sleep(TimeUnit.SECONDS.toMillis(5));
      } catch (InterruptedException e) {
        LOG.warn("processBusyRegions interrupted", e);
        break;
      }
    }
    return splitRegionAndDaughters;
  }

  /**
   * Try to get the daughters of split region
   * @param regionName
   * @return
   * @throws Exception
   */
  @VisibleForTesting
  PairOfSameType<HRegionInfo> tryToGetDaughters(byte[] regionName) {
    try {
      Result result = MetaReader.getRegionResult(connection, regionName);
      if (result != null) {
        HRegionInfo region = HRegionInfo.getHRegionInfo(result);
        if (region.isSplitParent()) {
          LOG.debug("Found parent region: {}", region);
          return HRegionInfo.getDaughterRegions(result);
        }
      }
    } catch (IOException e) {
      LOG.warn("Failed to get daughters of the split region: {}", Bytes.toStringBinary(regionName), e);
    }
    return null;
  }

  private Set<HRegionInfo> waitForRegionsOpenOrTimeout(List<HRegionInfo> regionsInTransition) {
    Set<HRegionInfo> movedRegions = new HashSet<>();
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
    HRegionInfo region = getHRegionInfo(regionName);
    if (region == null || region.isMetaRegion()) {
      return false;
    }
    return true;
  }

  private HRegionInfo getHRegionInfo(byte[] regionName) {
    return master.getAssignmentManager().getRegionStates().getRegionInfo(regionName);
  }

  @VisibleForTesting
  boolean maybeHasBusyRegion(ServerLoad serverLoad) {
    // read QPS
    if (serverLoad.getReadRequestsPerSecond() >= readQPSThreshold) {
      return true;
    }
    // write QPS
    if (serverLoad.getWriteRequestsPerSecond() >= writeQPSThreshold) {
      return true;
    }
    return false;
  }

  @VisibleForTesting
  boolean isBusyRegion(RegionLoad regionLoad) {
    // read QPS
    if (regionLoad.getReadRequestsPerSecond() >= readQPSThreshold) {
      return true;
    }
    // write QPS
    if (regionLoad.getWriteRequestsPerSecond() >= writeQPSThreshold) {
      return true;
    }
    return false;
  }

  static class RegionBusyInfo implements Comparable<RegionBusyInfo> {
    private byte[] regionName;
    private int consecutiveBusyTimes;
    private RegionLoad regionLoad;

    public RegionBusyInfo(byte[] regionName) {
      this.regionName = regionName;
    }

    void updateAndIncreaseTimes(RegionLoad regionLoad) {
      this.regionLoad = regionLoad;
      ++ consecutiveBusyTimes;
    }

    @Override
    public int compareTo(RegionBusyInfo o) {
      int[] results = new int[] {
          - Integer.compare(consecutiveBusyTimes, o.consecutiveBusyTimes),
          - Long.compare(regionLoad.getReadRequestsPerSecond(), o.regionLoad.getReadRequestsPerSecond()),
          - Long.compare(regionLoad.getWriteRequestsPerSecond(), o.regionLoad.getWriteRequestsPerSecond())
      };
      for (int r : results) {
        if (r != 0) {
          return r;
        }
      }
      return 0;
    }
  }
}
