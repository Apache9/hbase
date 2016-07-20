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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.hbase.ClusterLoad;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerLoad;
import org.apache.hadoop.hbase.RegionLoad;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableLoad;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.namespace.NamespaceLoad;
import org.apache.hadoop.hbase.replication.ReplicationLoadSource;

/**
 * Impl for exposing HMaster Information through JMX
 */
public class MXBeanImpl implements MXBean {

  private final HMaster master;

  private static MXBeanImpl instance = null;
  public synchronized static MXBeanImpl init(final HMaster master) {
    if (instance == null) {
      instance = new MXBeanImpl(master);
    }
    return instance;
  }

  protected MXBeanImpl(final HMaster master) {
    this.master = master;
  }

  @Override
  public double getAverageLoad() {
    return master.getAverageLoad();
  }

  @Override
  public String getClusterId() {
    return master.getClusterId();
  }

  @Override
  public String getZookeeperQuorum() {
    return master.getZooKeeperWatcher().getQuorum();
  }

  @Override
  public String[] getCoprocessors() {
    return master.getCoprocessors();
  }

  @Override
  public long getMasterStartTime() {
    return master.getMasterStartTime();
  }

  @Override
  public long getMasterActiveTime() {
    return master.getMasterActiveTime();
  }

  @Override
  public Map<String, ServerLoad> getRegionServers() {
    Map<String, ServerLoad> data = new HashMap<String, ServerLoad>();
    for (final Entry<ServerName, ServerLoad> entry :
      master.getServerManager().getOnlineServers().entrySet()) {
      data.put(entry.getKey().getServerName(),
          entry.getValue());
    }
    return data;
  }

  @Override
  public String[] getDeadRegionServers() {
    List<String> deadServers = new ArrayList<String>();
    for (ServerName name : master.getServerManager().getDeadServers().copyServerNames()) {
      deadServers.add(name.getHostAndPort());
    }
    return deadServers.toArray(new String[0]);
  }

  @Override
  public RegionsInTransitionInfo[] getRegionsInTransition() {
    List<RegionsInTransitionInfo> info =
        new ArrayList<RegionsInTransitionInfo>();
    for (final Entry<String, RegionState> entry :
      master.getAssignmentManager().getRegionStates().getRegionsInTransition().entrySet()) {
      RegionsInTransitionInfo innerinfo = new RegionsInTransitionInfo() {

        @Override
        public String getRegionState() {
          return entry.getValue().getState().toString();
        }

        @Override
        public String getRegionName() {
          return entry.getKey();
        }

        @Override
        public long getLastUpdateTime() {
          return entry.getValue().getStamp();
        }

        @Override
        public String getRegionServerName() {
          ServerName serverName = entry.getValue().getServerName();
          if (serverName != null) {
            return serverName.getServerName();
          }
          else {
            return "";
          }
        }
      };
      info.add(innerinfo);
    }
    RegionsInTransitionInfo[] data =
        new RegionsInTransitionInfo[info.size()];
    info.toArray(data);
    return data;
  }

  @Override
  public String getServerName() {
    return master.getServerName().getServerName();
  }

  @Override
  public boolean getIsActiveMaster() {
    return master.isActiveMaster();
  }

  @Override
  public ClusterLoad getClusterLoad() {
    Set<String> tableSet = new HashSet<String>();
    int regionServerNum = 0;
    int regionNum = 0;
    long readRequestPerSecond = 0;
    long writeRequestPerSecond = 0;
    long readRequestsByCapacityUnitPerSecond = 0;
    long writeRequestsByCapacityUnitPerSecond = 0;
    long readCellCountPerSecond = 0;
    long readRawCellCountPerSecond = 0;
    for (final Entry<ServerName, ServerLoad> entry : master.getServerManager().getOnlineServers()
        .entrySet()) {
      regionServerNum++;
      readRequestPerSecond += entry.getValue().getReadRequestsPerSecond();
      writeRequestPerSecond += entry.getValue().getWriteRequestsPerSecond();
      readRequestsByCapacityUnitPerSecond += entry.getValue()
          .getReadRequestsByCapacityUnitPerSecond();
      writeRequestsByCapacityUnitPerSecond += entry.getValue()
          .getWriteRequestsByCapacityUnitPerSecond();
      readCellCountPerSecond += entry.getValue().getReadCellCountPerSecond();
      readRawCellCountPerSecond += entry.getValue().getReadRawCellCountPerSecond();
      for (final Entry<byte[], RegionLoad> regionEntry : entry.getValue().getRegionsLoad()
          .entrySet()) {
        String table = new String(HRegionInfo.getTableName(regionEntry.getKey()));
        regionNum++;
        tableSet.add(table);
      }
    }
    return new ClusterLoad(tableSet.size(), regionServerNum, regionNum, readRequestPerSecond,
        writeRequestPerSecond, readRequestsByCapacityUnitPerSecond,
        writeRequestsByCapacityUnitPerSecond, readCellCountPerSecond, readRawCellCountPerSecond);
  }

  @Override
  public List<TableLoad> getTableLoads() {
    Map<String, TableLoad> data = new HashMap<String, TableLoad>();
    for (final Entry<ServerName, ServerLoad> entry : master.getServerManager()
        .getOnlineServers().entrySet()) {
      for (final Entry<byte[], RegionLoad> regionEntry : entry.getValue()
          .getRegionsLoad().entrySet()) {
        String table =
            new String(HRegionInfo.getTableName(regionEntry.getKey()));
        TableLoad load = data.get(table);
        if (load == null) {
          load = new TableLoad(table);
          data.put(table, load);
        }
        load.updateTableLoad(regionEntry.getValue());
      }
    }
    return new LinkedList<TableLoad>(data.values());
  }
  
  @Override
  public List<NamespaceLoad> getNamespaceLoads() {
    Map<String, NamespaceLoad> data = new HashMap<String, NamespaceLoad>();
    for (final Entry<TableName, TableLoad> entry : master.getTableLoads().entrySet()) {
      String namespace = entry.getKey().getNamespaceAsString();
      NamespaceLoad load = data.get(namespace);
      if (load == null) {
        load = new NamespaceLoad(namespace);
        data.put(namespace, load);
      }
      load.updateNamespaceLoad(entry.getValue());
    }
    return new LinkedList<NamespaceLoad>(data.values());
  }

  @Override
  public List<ReplicationLoadSource> getReplicationLoads() {
    Map<String, ReplicationLoadSource> replications = new HashMap<String, ReplicationLoadSource>();
    for (final Entry<ServerName, ServerLoad> entry : master.getServerManager().getOnlineServers()
        .entrySet()) {
      for (ReplicationLoadSource load : entry.getValue().getReplicationLoadSourceList()) {
        ReplicationLoadSource tmp = replications.get(load.getPeerID());
        if (tmp == null) {
          replications.put(load.getPeerID(), load);
        } else {
          tmp.setSizeOfLogQueue(tmp.getSizeOfLogQueue() + load.getSizeOfLogQueue());
        }
      }
    }
    return new LinkedList<ReplicationLoadSource>(replications.values());
  }
}
