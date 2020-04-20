/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hbase.master.balancer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.master.LoadBalancer;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.hbase.master.balancer.GalaxyGroupInfoManager.GroupInfo;

import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hbase.HConstants.HBASE_GALAXY_GROUP_INTERNAL_LOADBALANCER_CLASS;

/**
 * copy from HBase 2.x , RSGroupBasedLoadBalancer
 */

public class GalaxyGroupLoadBalancer implements LoadBalancer {
  private static final Logger LOG = LoggerFactory.getLogger(GalaxyGroupLoadBalancer.class);

  private Configuration config;
  private ClusterStatus clusterStatus;
  private MasterServices masterServices;
  private GalaxyGroupInfoManager galaxyGroupInfoManager;
  private LoadBalancer internalBalancer;
  private final static Random rand = new Random();

  GalaxyGroupInfoManager getGalaxyGroupInfoManager() {
    return galaxyGroupInfoManager;
  }

  @Override
  public void stop(String why) {
    internalBalancer.stop(why);
  }

  @Override
  public boolean isStopped() {
    return internalBalancer.isStopped();
  }

  @Override
  public void initialize() throws HBaseIOException {
    this.galaxyGroupInfoManager.setMasterServices(masterServices);
    galaxyGroupInfoManager.refreshGroupInfo();
    // Create the balancer
    Class<? extends LoadBalancer> balancerKlass =
        config.getClass(HBASE_GALAXY_GROUP_INTERNAL_LOADBALANCER_CLASS,
          StochasticLoadBalancer.class, LoadBalancer.class);
    internalBalancer = ReflectionUtils.newInstance(balancerKlass, config);
    internalBalancer.setMasterServices(masterServices);
    if (clusterStatus != null) {
      internalBalancer.setClusterStatus(clusterStatus);
    }
    internalBalancer.setConf(config);
    internalBalancer.initialize();
  }

  @Override
  public void setClusterLoad(Map<TableName, Map<ServerName, List<HRegionInfo>>> clusterLoad) {
    if (clusterLoad != null) {
      internalBalancer.setClusterLoad(clusterLoad);
    }
  }

  @Override
  public void setClusterStatus(ClusterStatus st) {
    this.clusterStatus = st;
    if (internalBalancer != null) {
      internalBalancer.setClusterStatus(st);
    }
  }

  @Override
  public void setMasterServices(MasterServices masterServices) {
    this.masterServices = masterServices;
    if (galaxyGroupInfoManager != null) {
      galaxyGroupInfoManager.setMasterServices(masterServices);
    }
  }

  @Override
  public void regionOffline(HRegionInfo regionInfo) {
    internalBalancer.regionOffline(regionInfo);
  }

  @Override
  public void regionOnline(HRegionInfo regionInfo, ServerName sn) {
    internalBalancer.regionOnline(regionInfo, sn);
  }

  @Override
  public Map<HRegionInfo, ServerName> immediateAssignment(List<HRegionInfo> regions,
      List<ServerName> servers) throws HBaseIOException {
    return internalBalancer.immediateAssignment(regions, servers);
  }

  @Override
  public void setIsolateMeta(boolean isolateMeta) {
    this.galaxyGroupInfoManager.setIsolateMeta(isolateMeta);
  }

  @Override
  public void setConf(Configuration conf) {
    this.config = conf;
    this.galaxyGroupInfoManager = new GalaxyGroupInfoManager(config);
  }

  @Override
  public Configuration getConf() {
    return config;
  }

  @Override
  public Map<ServerName, List<HRegionInfo>> roundRobinAssignment(List<HRegionInfo> regions,
      List<ServerName> servers) throws HBaseIOException {
    return internalBalancer.roundRobinAssignment(regions, servers);
  }

  @Override
  public Map<ServerName, List<HRegionInfo>> retainAssignment(Map<HRegionInfo, ServerName> regions,
      List<ServerName> servers) throws HBaseIOException {
    return internalBalancer.retainAssignment(regions, servers);
  }

  @Override
  public List<RegionPlan> balanceCluster(TableName tableName,
      Map<ServerName, List<HRegionInfo>> clusterState) throws HBaseIOException {
    LOG.debug("Start Generate Balance plan for table " + tableName);
    return balanceCluster(clusterState);

  }

  @Override
  public ServerName randomAssignment(HRegionInfo region, List<ServerName> servers)
      throws HBaseIOException {
    return internalBalancer.randomAssignment(region, servers);
  }


  private ServerName selectDestServer(HRegionInfo regionInfo) {
    GroupInfo groupInfo = galaxyGroupInfoManager.getGroupInfoForTable(regionInfo.getTable());
    List<ServerName> candidateList = new ArrayList<>(groupInfo.getServers());
    if (candidateList.size() < 1) {
      candidateList = new ArrayList<>(galaxyGroupInfoManager.getDefaultGroupInfo().getServers());
    }
    return candidateList.get(rand.nextInt(candidateList.size()));
  }

  private Map<HRegionInfo, ServerName>
      findMisplacedRegions(Map<ServerName, List<HRegionInfo>> clusterState) {
    Map<HRegionInfo, ServerName> result = new HashMap<>();
    for (Map.Entry<ServerName, List<HRegionInfo>> entry : clusterState.entrySet()) {
      ServerName serverName = entry.getKey();
      List<HRegionInfo> regions = entry.getValue();
      for (HRegionInfo region : regions) {
        GroupInfo targetRSGInfo = null;
        try {
          targetRSGInfo = galaxyGroupInfoManager.getGroupInfoForTable(region.getTable());
        } catch (Exception exp) {
          LOG.debug("Galaxy Group information null for region of table " + region.getTable(), exp);
        }
        if ((targetRSGInfo == null) || (!targetRSGInfo.getServers().contains(serverName))) {
          result.put(region, serverName);
        }
      }
    }
    return result;
  }


  @Override
  public List<RegionPlan> balanceCluster(Map<ServerName, List<HRegionInfo>> clusterState)
      throws HBaseIOException {
    galaxyGroupInfoManager.refreshGroupInfo();
    List<RegionPlan> regionPlans = new ArrayList<>();
    // find out mistaken assgined region, and select correct server as dest
    Map<HRegionInfo, ServerName> misplacedRegions = findMisplacedRegions(clusterState);
    misplacedRegions.forEach((region, server) -> {
      regionPlans.add(new RegionPlan(region, server, selectDestServer(region)));
    });

    for (GroupInfo groupInfo : galaxyGroupInfoManager.getAllGroupInfo()) {
      // form ClusterLoad and ClusterState
      // TableName set to "hbase:ensemble" is enough for balance
      Map<ServerName, List<HRegionInfo>> groupClusterState = new HashMap<>();
      Map<TableName, Map<ServerName, List<HRegionInfo>>> groupClusterLoad = new HashMap<>();
      for (Map.Entry<ServerName, List<HRegionInfo>> entry : clusterState.entrySet()) {
        ServerName server = entry.getKey();
        List<HRegionInfo> regions = entry.getValue();
        if (groupInfo.getServers().contains(server)) {
          List<HRegionInfo> regionInfos =
              groupClusterState.computeIfAbsent(server, k -> new ArrayList<>());
          for (HRegionInfo region : regions) {
            if (!misplacedRegions.containsKey(region)) {
              regionInfos.add(region);
            }
          }
        }
      }
      groupClusterLoad.put(TableName.valueOf(HConstants.ENSEMBLE_TABLE_NAME), groupClusterState);
      this.internalBalancer.setClusterLoad(groupClusterLoad);
      List<RegionPlan> groupPlans = this.internalBalancer.balanceCluster(groupClusterState);
      if (groupPlans != null) {
        regionPlans.addAll(groupPlans);
      }
    }
    return regionPlans;
  }

}
