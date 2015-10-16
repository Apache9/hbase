/**
 * Copyright 2010 The Apache Software Foundation
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
package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Chore;
import org.apache.hadoop.hbase.HServerLoad.RegionLoad;
import org.apache.hadoop.hbase.regionserver.compactions.CurrentHourProvider;
import org.apache.hadoop.hbase.regionserver.compactions.OffPeakHours;
import org.apache.hadoop.hbase.regionserver.wal.HLogCompactor;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

import com.google.common.annotations.VisibleForTesting;

public class RegionCompactor extends Chore {
  private final static Log LOG = LogFactory.getLog(RegionCompactor.class);

  public final static String REGION_ATUO_COMPACT_ENABLE = "hbase.region.auto.compact.enable";

  /* min interval between twice compact by locality, default value is a week */
  public final static String REGION_ATUO_COMPACT_INTERVAL = "hbase.region.auto.compact.interval";
  public final static long DEFAULT_REGION_ATUO_COMPACT_INTERVAL = 7 * 24 * 3600 * 1000;

  /* check period, default value is a hour */
  public final static String REGION_ATUO_COMPACT_PERIOD = "hbase.region.auto.compact.period";
  public final static int DEFAULT_REGION_ATUO_COMPACT_PERIOD = 3600 * 1000;

  /* decide if to start major compact request of a region by locality */
  public final static String REGION_COMPACT_LOCALITY_THRESHOLD = "hbase.region.compact.locality.threshold";
  public final static float DEFAULT_REGION_COMPACT_LOCALITY_THRESHOLD = 0.2f;

  public final boolean major = true;

  private HRegionServer server;
  private Configuration conf;
  private float localityThreshold;
  private OffPeakHours offPeak;
  private long lastAutoCompactTime;
  private long compactIntervalMs;
  private String serverHostName;
  private int lastTotalCompactRequestCount = 0;

  public RegionCompactor(final HRegionServer server, int period) {
    super("RegionCompactor", period, server);
    this.server = server;
    this.serverHostName = server.getServerName().getHostname();
    this.conf = server.getConfiguration();
    this.localityThreshold = this.conf.getFloat(REGION_COMPACT_LOCALITY_THRESHOLD,
      DEFAULT_REGION_COMPACT_LOCALITY_THRESHOLD);
    this.offPeak = OffPeakHours.getInstance(conf);
    this.lastAutoCompactTime = EnvironmentEdgeManager.currentTimeMillis();
    this.compactIntervalMs = this.conf.getLong(REGION_ATUO_COMPACT_INTERVAL,
      DEFAULT_REGION_ATUO_COMPACT_INTERVAL);
    LOG.info("New RegionCompactor : period=" + period + ", compactIntervalMs=" + compactIntervalMs
        + ", localityThreshold=" + localityThreshold);
  }

  @Override
  protected void chore() {
    if (server.isStopped()) {
      return;
    }
    if (server.getCompactSplitThread() == null) {
      return;
    }
    if (!conf.getBoolean(REGION_ATUO_COMPACT_ENABLE, false)) {
      LOG.debug("Can't start region compact by locality, beacuse hbase.region.auto.compact.enable is false");
      return;
    }

    long intervalMs = EnvironmentEdgeManager.currentTimeMillis() - lastAutoCompactTime;
    if (intervalMs < compactIntervalMs) {
      LOG.debug("Can't start region compact by locality, the interval from last compact is "
          + intervalMs + ", which is smaller than " + compactIntervalMs);
      return;
    }
    if (!offPeak.isOffPeakHour()) {
      LOG.debug("Can't start region compact by locality, now hour "
          + CurrentHourProvider.getCurrentHour() + " is not in offpeak");
      return;
    }

    LOG.info("Start region compact by locality, lastAutoCompactTime is " + lastAutoCompactTime);
    int totalCompactRequestCount = 0;
    String why = "locality-triggered " + (major ? "major " : "") + "compaction";

    for (HRegion region : server.onlineRegions.values()) {
      if (region == null || region.getRegionInfo().isMetaTable()) {
        continue;
      }
      for (Store store : region.getStores().values()) {
        float storeLocality = store.getHDFSBlocksDistribution().getBlockLocalityIndex(
          serverHostName);
        if (storeLocality <= localityThreshold) {
          if (major) {
            store.triggerMajorCompaction();
          }
          try {
            server.getCompactSplitThread().requestCompaction(region, store, why, Store.NO_PRIORITY,
              null);
            totalCompactRequestCount += 1;
            LOG.info("Start a major compact request for " + store.getFamily().getNameAsString() + " in "
                + region + ", which locality is " + storeLocality);
            Thread.sleep(1000);
          } catch (IOException ioe) {
            LOG.warn("Fail to request major compact for " + store.getFamily().getNameAsString() + " in "
                + region, ioe);
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
          }
        }
      }
    }

    lastAutoCompactTime = EnvironmentEdgeManager.currentTimeMillis();
    lastTotalCompactRequestCount = totalCompactRequestCount;
    LOG.info("After region compact by locality, total major compact requests count is "
        + totalCompactRequestCount);
  }

  @VisibleForTesting
  protected long getLastAutoCompactTime() {
    return this.lastAutoCompactTime;
  }

  @VisibleForTesting
  protected void setServerHostName(String host) {
    this.serverHostName = host;
  }

  @VisibleForTesting
  protected int getLastTotalCompactRequestCount() {
    return this.lastTotalCompactRequestCount;
  }
}
