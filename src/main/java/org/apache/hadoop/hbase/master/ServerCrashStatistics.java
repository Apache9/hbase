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
package org.apache.hadoop.hbase.master;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Data structure that holds server crash and recover statistics.
 */
public class ServerCrashStatistics {
  private String serverName;
  private long crashTs;
  private long minRegionRecoverTime = -1;
  private long maxRegionRecoverTime = -1;
  private double avgRegionRecoverTime = -1;
  private long totalRegionRecoverTime = 0;
  private Set<String> pendingRegions;
  private Map<String, Long> regionRecoverTime = new TreeMap<String, Long>();

  public ServerCrashStatistics(String serverName, long crashTs,
      Set<String> pendingRegions) {
    this.serverName = serverName;
    this.crashTs = crashTs;
    this.pendingRegions = pendingRegions;
  }

  public ServerCrashStatistics(ServerCrashStatistics another) {
    this.serverName = another.serverName;
    this.crashTs = another.crashTs;
    this.minRegionRecoverTime = another.minRegionRecoverTime;
    this.maxRegionRecoverTime = another.maxRegionRecoverTime;
    this.avgRegionRecoverTime = another.avgRegionRecoverTime;
    this.totalRegionRecoverTime = another.totalRegionRecoverTime;
    this.pendingRegions = new TreeSet<String>(another.getPendingRegions());
    this.regionRecoverTime = new TreeMap<String, Long>();
    for (Map.Entry<String, Long> e : another.getRegionRecoverTime().entrySet()) {
      this.regionRecoverTime.put(e.getKey(), e.getValue());
    }
  }

  public void markRegionAsRecovered(String region) {
    boolean deleted = pendingRegions.remove(region);
    assert deleted;
    long recoverTime = System.currentTimeMillis() - crashTs;
    regionRecoverTime.put(region, recoverTime);
    totalRegionRecoverTime += recoverTime;
    if (minRegionRecoverTime < 0) minRegionRecoverTime = recoverTime;
    if (pendingRegions.isEmpty() && regionRecoverTime.size() != 0) {
      maxRegionRecoverTime = recoverTime;
      avgRegionRecoverTime = totalRegionRecoverTime / (double) regionRecoverTime.size();
    }
  }

  public String getServerName() {
    return serverName;
  }

  public long getCrashTs() {
    return crashTs;
  }

  public long getMinRegionRecoverTime() {
    return minRegionRecoverTime;
  }

  public long getMaxRegionRecoverTime() {
    return maxRegionRecoverTime;
  }

  public double getAvgRegionRecoverTime() {
    return avgRegionRecoverTime;
  }

  public Set<String> getPendingRegions() {
    return pendingRegions;
  }

  public Map<String, Long> getRegionRecoverTime() {
    return regionRecoverTime;
  }
}
