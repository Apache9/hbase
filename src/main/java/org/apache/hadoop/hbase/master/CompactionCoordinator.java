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
package org.apache.hadoop.hbase.master;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.regionserver.CompactionQuota;

/**
 * To coordinate all the compaction in the cluster
 */
public class CompactionCoordinator implements Configurable {
  private static final Log LOG = LogFactory.getLog(CompactionCoordinator.class);

  public static final String HBASE_CLUSTER_COMPACTION_RATIO = "hbase.cluster.compaction.ratio";

  private Configuration conf;
  private ServerManager manager;

  private float compactionRatio;

  private Map<ServerName, CompactionQuota> quotas;
  
  private int usedQuota;
  private int totalQuota;

  public CompactionCoordinator(final Configuration conf,
      final ServerManager manager) {
    this.conf = conf;
    this.manager = manager;
    this.compactionRatio = conf.getFloat(HBASE_CLUSTER_COMPACTION_RATIO, 2.0f);
    this.quotas = new ConcurrentHashMap<ServerName, CompactionQuota>();
    this.totalQuota = (int)(this.manager.countOfRegionServers() * compactionRatio);
    this.usedQuota = 0;
  }

  public synchronized CompactionQuota requestCompactionQuota(
      final ServerName serverName, final CompactionQuota request) {
    CompactionQuota last = quotas.remove(serverName);
    if (last == null && request.getUsingQuota() == 0 && request.getRequestQuota() == 0) {
      return request;
    }
    updateUsedQuota(last, request);

    // no quota requested
    if (request.getRequestQuota() == 0) {
      return request;
    }

    CompactionQuota response = grantQuota(request);
    updateUsedQuota(request, response);
    
    if (response.getUsingQuota() > 0 || response.getGrantQuota() > 0) {
      quotas.put(serverName, response);
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Regionserver: " + serverName + " get compaction quota: " + response
          + ". Cluster total compaction quota: (" + totalQuota + "), + . Used quota: (" + usedQuota);
    }
    return response;
  }

  /**
   * Update used quota when regionserver expired
   * @param serverName
   */
  public void expireServer(final ServerName serverName) {
    CompactionQuota last = quotas.remove(serverName);
    updateUsedQuota(last, null);
  }

  public int getRunningCompactionNum() {
    return usedQuota;
  }

  public int getCompactionNumLimit() {
    return totalQuota;
  }

  /**
   * simple policy, just grant quota if there is left
   * @param request
   */
  private CompactionQuota grantQuota(final CompactionQuota request) {
    CompactionQuota response = new CompactionQuota(request);
    totalQuota = (int)(this.manager.countOfRegionServers() * compactionRatio);
    int left = Math.max(0, totalQuota - usedQuota);
    response.setGrantQuota(Math.min(left, request.getRequestQuota()));
    return response;
  }

  /**
   * update the used quota
   */
  private synchronized void updateUsedQuota(CompactionQuota last, CompactionQuota current) {
    if (last != null) {
      usedQuota -= last.getUsingQuota() + last.getGrantQuota();
    }
    if (current != null) {
      usedQuota += current.getUsingQuota() + current.getGrantQuota();
    }
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }
  
  public int getTotalQuota() {
    return totalQuota;
  }
  
  public int getUsedQuota() {
    return usedQuota;
  }
}
