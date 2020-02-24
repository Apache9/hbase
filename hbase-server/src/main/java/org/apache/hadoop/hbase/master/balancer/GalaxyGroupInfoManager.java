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

import com.xiaomi.miliao.common.ConcurrentHashSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.master.MasterServices;

import java.util.List;
import java.util.Set;
import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hbase.HConstants.HBASE_GALAXY_GROUP_CLUSTER;

public class GalaxyGroupInfoManager {
  private static final Logger LOG = LoggerFactory.getLogger(GalaxyGroupInfoManager.class);
  private final GroupInfo defaultGroupInfo; // for one default group
  private final List<GalaxyGroupInfo> galaxyGroupInfoList; // for galaxy group, fds/emq/sds/talos
  private MasterServices masterServices;
  private static final String SEPARATOR = ",";

  public void setMasterServices(MasterServices masterServices) {
    this.masterServices = masterServices;
  }

  List<GalaxyGroupInfo> getGalaxyGroupInfo() {
    return galaxyGroupInfoList;
  }

  GroupInfo getDefaultGroupInfo() {
    return defaultGroupInfo;
  }

  GroupInfo getGroupInfoForTable(TableName tableName) {
    for (GalaxyGroupInfo galaxyGroupInfo : galaxyGroupInfoList) {
      if (galaxyGroupInfo.containsTable(tableName)) {
        return galaxyGroupInfo;
      }
    }
    return defaultGroupInfo;
  }

  List<GroupInfo> getAllGroupInfo() {
    List<GroupInfo> result = new ArrayList<>();
    result.add(defaultGroupInfo);
    result.addAll(galaxyGroupInfoList);
    return result;
  }

  class GroupInfo {
    private final String name;
    private final ConcurrentHashSet<ServerName> servers;

    GroupInfo(String name) {
      this(name, new ConcurrentHashSet<>());
    }

    GroupInfo(String name, Set<ServerName> servers) {
      this.name = name;
      this.servers =
          (servers == null) ? new ConcurrentHashSet<>() : new ConcurrentHashSet<>(servers);
    }

    @Override
    public String toString() {
      return "GroupInfo{" + "name='" + name + '\'' + ", servers=" + servers + '}';
    }

    Set<ServerName> getServers() {
      return servers;
    }

    void addServer(ServerName serverName) {
      this.servers.add(serverName);
    }

    void clearServer() {
      this.servers.clear();
    }

    String getName() {
      return name;
    }
  }

  class GalaxyGroupInfo extends GroupInfo {
    private final String tablePrefix;
    private final String configServerStr;

    GalaxyGroupInfo(Configuration conf, String name) {
      super(name);
      tablePrefix = conf.get(HBASE_GALAXY_GROUP_CLUSTER + "." + name + ".table.prefix");
      configServerStr = conf.get(HBASE_GALAXY_GROUP_CLUSTER + "." + name + ".servers");
    }

    boolean containsTable(TableName tableName) {
      return tableName.getNameAsString().startsWith(tablePrefix);
    }
  }

  GalaxyGroupInfoManager(Configuration conf) {
    galaxyGroupInfoList = new ArrayList<>();
    String galaxyClusterStr = conf.get(HBASE_GALAXY_GROUP_CLUSTER, "");
    if (!"".equals(galaxyClusterStr)) {
      for (String groupName : galaxyClusterStr.split(SEPARATOR)) {
        GalaxyGroupInfo galaxyGroupInfo = new GalaxyGroupInfo(conf, groupName);
        galaxyGroupInfoList.add(galaxyGroupInfo);
      }
    }
    defaultGroupInfo = new GroupInfo("default");
  }

  /**
   * update Server for each group
   */
  void refreshGroupInfo() {
    LOG.info("now start refresh groupInfo");
    List<ServerName> onlineServers = masterServices.getServerManager().getOnlineServersList();
    getAllGroupInfo().forEach(groupInfo -> {
      groupInfo.clearServer();
    });
    for (ServerName serverName : onlineServers) {
      boolean isGalaxyServer = false;
      for (GalaxyGroupInfo galaxyGroupInfo : getGalaxyGroupInfo()) {
        if (galaxyGroupInfo.configServerStr.contains(serverName.getHostAndPort())) {
          galaxyGroupInfo.addServer(serverName);
          isGalaxyServer = true;
          break;
        }
      }
      if (!isGalaxyServer) {
        getDefaultGroupInfo().addServer(serverName);
      }
    }
    LOG.info("finish refresh groupInfo");
    getAllGroupInfo().forEach(groupInfo->LOG.info("Finished refresh: "+ groupInfo));
  }
}
