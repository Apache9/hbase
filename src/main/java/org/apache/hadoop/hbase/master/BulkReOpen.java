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
package org.apache.hadoop.hbase.master;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;

/**
 * Performs bulk reopen of the list of regions provided to it.
 */
public class BulkReOpen extends BulkAssigner {
  private final Map<ServerName, List<HRegionInfo>> rsToRegions;
  private final AssignmentManager assignmentManager;
  private static final Log LOG = LogFactory.getLog(BulkReOpen.class);
  // max estimated time to reopen a region
  private static final long MAX_REGION_REOPEN_TIME = 20000;

  public BulkReOpen(final Server server,
      final Map<ServerName, List<HRegionInfo>> serverToRegions,
    final AssignmentManager am) {
    super(server);
    this.assignmentManager = am;
    this.rsToRegions = serverToRegions;
  }

  /**
   * Unassign all regions, so that they go through the regular region
   * assignment flow (in assignment manager) and are re-opened.
   */
  @Override
  protected void populatePool(ExecutorService pool) {
    LOG.debug("Creating threads for each region server ");
    for (Map.Entry<ServerName, List<HRegionInfo>> e : rsToRegions
        .entrySet()) {
      final List<HRegionInfo> hris = e.getValue();
      // add plans for the regions that need to be reopened
      Map<String, RegionPlan> plans = new HashMap<String, RegionPlan>();
      for (HRegionInfo hri : hris) {
        RegionPlan reOpenPlan = new RegionPlan(hri, null,
            assignmentManager.getRegionServerOfRegion(hri));
        plans.put(hri.getEncodedName(), reOpenPlan);
      }
      assignmentManager.addPlans(plans);
      pool.execute(new Runnable() {
        public void run() {
          assignmentManager.unassign(hris);
        }
      });
    }
  }

 /**
  * Wait until all regions are opened.
  * @return true
  */
  @Override
  protected boolean waitUntilDone(long timeout) {
    long startTime = System.currentTimeMillis();
    for (Map.Entry<ServerName, List<HRegionInfo>> e : rsToRegions.entrySet()) {
      final List<HRegionInfo> hris = e.getValue();
      for (HRegionInfo hri : hris) {
        long remaining = timeout - (System.currentTimeMillis() - startTime);
        if (!waitUntilRegionOnline(hri, remaining)) {
          return false;
        }
      }
    }
    return true;
  }

  /**
   * Wait the region to be opened
   * @param hri the region
   * @param timeout the timeout
   * @return true if region is opened, and false if timeout
   * @throws InterruptedException
   */
  private boolean waitUntilRegionOnline(final HRegionInfo hri, long timeout) {
    long startTime = System.currentTimeMillis();
    long remaining = timeout;
    while (!server.isStopped() && remaining > 0) {
      if (assignmentManager.isRegionInTransition(hri) == null) {
        LOG.info("region is not in transition " + hri);
        return true;
      }
      remaining = timeout - (System.currentTimeMillis() - startTime);
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
      }
    }
    return false;
  }

  /**
   * Configuration knobs "hbase.bulk.reopen.threadpool.size" number of regions
   * that can be reopened concurrently. The maximum number of threads the master
   * creates is never more than the number of region servers.
   * If configuration is not defined it defaults to 20
   */
  protected int getThreadCount() {
    int defaultThreadCount = super.getThreadCount();
    return this.server.getConfiguration().getInt(
        "hbase.bulk.reopen.threadpool.size", defaultThreadCount);
  }

  public boolean bulkReOpen() throws InterruptedException, IOException {
    return bulkAssign();
  }
  
  protected long getTimeoutOnRIT() {
    int regionCount = 0;
    for (List<HRegionInfo> regions : rsToRegions.values()) {
      regionCount += regions.size();
    }
    long timeoutByRegionCount = (regionCount * MAX_REGION_REOPEN_TIME) / getThreadCount();
    long configuredTimeout = super.getTimeoutOnRIT();
    LOG.info("BulkReOpen timeoutOnRIT, regionCount=" + regionCount + ", timeoutByRegionCount="
        + timeoutByRegionCount + ", configuredTimeout=" + configuredTimeout);
    return Math.max(timeoutByRegionCount, configuredTimeout);
  }
}
