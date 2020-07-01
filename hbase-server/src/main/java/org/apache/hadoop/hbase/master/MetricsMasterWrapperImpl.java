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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.RegionMetrics;
import org.apache.hadoop.hbase.ServerMetrics;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.metrics.Interns;
import org.apache.hadoop.hbase.quotas.QuotaObserverChore;
import org.apache.hadoop.hbase.quotas.SpaceQuotaSnapshot;
import org.apache.hadoop.hbase.replication.ReplicationLoadSource;
import org.apache.hadoop.hbase.rsgroup.RSGroupInfo;
import org.apache.hadoop.hbase.rsgroup.RSGroupUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.VersionInfo;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Impl for exposing HMaster Information through JMX
 */
@InterfaceAudience.Private
public class MetricsMasterWrapperImpl implements MetricsMasterWrapper {

  private final HMaster master;
  private static final Logger LOG = LoggerFactory.getLogger(MetricsMasterWrapperImpl.class);

  public MetricsMasterWrapperImpl(final HMaster master) {
    this.master = master;
  }

  @Override
  public double getAverageLoad() {
    return master.getAverageLoad();
  }

  @Override
  public long getSplitPlanCount() {
    return master.getSplitPlanCount();
  }

  @Override
  public long getMergePlanCount() {
    return master.getMergePlanCount();
  }

  @Override
  public long getMasterInitializationTime() {
    return master.getMasterFinishedInitializationTime();
  }

  @Override
  public String getClusterId() {
    return master.getClusterId();
  }

  @Override
  public String getZookeeperQuorum() {
    ZKWatcher zk = master.getZooKeeper();
    if (zk == null) {
      return "";
    }
    return zk.getQuorum();
  }

  @Override
  public String getVersion() {
    return VersionInfo.getVersion();
  }

  @Override
  public String getRevision() {
    return VersionInfo.getRevision();
  }

  @Override
  public String[] getCoprocessors() {
    return master.getMasterCoprocessors();
  }

  @Override
  public long getStartTime() {
    return master.getMasterStartTime();
  }

  @Override
  public long getActiveTime() {
    return master.getMasterActiveTime();
  }

  @Override
  public String getRegionServers() {
    ServerManager serverManager = this.master.getServerManager();
    if (serverManager == null) {
      return "";
    }
    return StringUtils.join(serverManager.getOnlineServers().keySet(), ";");
  }

  @Override
  public int getNumRegionServers() {
    ServerManager serverManager = this.master.getServerManager();
    if (serverManager == null) {
      return 0;
    }
    return serverManager.getOnlineServers().size();
  }

  @Override
  public String getDeadRegionServers() {
    ServerManager serverManager = this.master.getServerManager();
    if (serverManager == null) {
      return "";
    }
    return StringUtils.join(serverManager.getDeadServers().copyServerNames(), ";");
  }


  @Override
  public int getNumDeadRegionServers() {
    ServerManager serverManager = this.master.getServerManager();
    if (serverManager == null) {
      return 0;
    }
    return serverManager.getDeadServers().size();
  }

  @Override
  public String getServerName() {
    ServerName serverName = master.getServerName();
    if (serverName == null) {
      return "";
    }
    return serverName.getServerName();
  }

  @Override
  public boolean getIsActiveMaster() {
    return master.isActiveMaster();
  }

  @Override
  public long getNumWALFiles() {
    return master.getNumWALFiles();
  }

  @Override
  public Map<String,Entry<Long,Long>> getTableSpaceUtilization() {
    QuotaObserverChore quotaChore = master.getQuotaObserverChore();
    if (quotaChore == null) {
      return Collections.emptyMap();
    }
    Map<TableName,SpaceQuotaSnapshot> tableSnapshots = quotaChore.getTableQuotaSnapshots();
    Map<String,Entry<Long,Long>> convertedData = new HashMap<>();
    for (Entry<TableName,SpaceQuotaSnapshot> entry : tableSnapshots.entrySet()) {
      convertedData.put(entry.getKey().toString(), convertSnapshot(entry.getValue()));
    }
    return convertedData;
  }

  @Override
  public Map<String,Entry<Long,Long>> getNamespaceSpaceUtilization() {
    QuotaObserverChore quotaChore = master.getQuotaObserverChore();
    if (quotaChore == null) {
      return Collections.emptyMap();
    }
    Map<String,SpaceQuotaSnapshot> namespaceSnapshots = quotaChore.getNamespaceQuotaSnapshots();
    Map<String,Entry<Long,Long>> convertedData = new HashMap<>();
    for (Entry<String,SpaceQuotaSnapshot> entry : namespaceSnapshots.entrySet()) {
      convertedData.put(entry.getKey(), convertSnapshot(entry.getValue()));
    }
    return convertedData;
  }

  Entry<Long,Long> convertSnapshot(SpaceQuotaSnapshot snapshot) {
    return new SimpleImmutableEntry<>(snapshot.getUsage(), snapshot.getLimit());
  }

  public Map<ServerName, ServerMetrics> getRegionServerMetrics() {
    ServerManager serverManager = this.master.getServerManager();
    if (serverManager == null) {
      return Collections.emptyMap();
    } else {
      return Collections.unmodifiableMap(serverManager.getOnlineServers());
    }
  }

  static RegionInfo parseRegionInfo(byte[] region) {
    try {
      return MetaTableAccessor.parseRegionInfoFromRegionName(region);
    } catch (IOException e) {
      LOG.warn("Failed to parse the RegionInfo from region: {}", Bytes.toStringBinary(region));
      return null;
    }
  }

  private static <K> void accumulateMap(Map<K, Long> map, K key, Long value) {
    map.compute(key, (k, oldValue) -> oldValue == null ? value : oldValue + value);
  }

  private static MetricsRecordBuilder appendMetrics(MetricsRecordBuilder builder, String prefix,
      Metrics metrics) {
    metrics = (metrics == null) ? new Metrics() : metrics;
    return builder
        .addCounter(Interns.info(prefix + MetricsClusterSource.READ_REQUEST_PER_SECOND,
          MetricsClusterSource.READ_REQUEST_PER_SECOND_DESC), metrics.readRequestPerSecond)
        .addCounter(Interns.info(prefix + MetricsClusterSource.WRITE_REQUEST_PER_SECOND,
          MetricsClusterSource.WRITE_REQUEST_PER_SECOND_DESC), metrics.writeRequestPerSecond)
        .addCounter(Interns.info(prefix + MetricsClusterSource.GET_REQEUST_PER_SECOND,
          MetricsClusterSource.GET_REQUEST_PER_SECOND_DESC), metrics.getRequestPerSecond)
        .addCounter(Interns.info(prefix + MetricsClusterSource.SCAN_REQUEST_PER_SECOND,
          MetricsClusterSource.SCAN_REQUEST_PER_SECOND_DESC), metrics.scanRequestPerSecond)
        .addCounter(Interns.info(prefix + MetricsClusterSource.SCAN_ROWS_COUNT_PER_SECOND,
          MetricsClusterSource.SCAN_ROWS_COUNT_PER_SECOND_DESC), metrics.scanRowsCountPerSecond)
        .addCounter(
          Interns.info(prefix + MetricsClusterSource.READ_REQUEST_BY_CAPACITY_UNIT_PER_SECOND,
            MetricsClusterSource.READ_REQUEST_BY_CAPACITY_UNIT_PER_SECOND_DESC),
            metrics.readRequestByCapacityUnit)
        .addCounter(
          Interns.info(prefix + MetricsClusterSource.WRITE_REQUEST_BY_CAPACITY_UNIT_PER_SECOND,
            MetricsClusterSource.WRITE_REQUEST_BY_CAPACITY_UNIT_PER_SECOND_DESC),
            metrics.writeRequestByCapacityUnit)
        .addCounter(Interns.info(prefix + MetricsClusterSource.READ_CELLS_PER_SECOND,
          MetricsClusterSource.READ_CELLS_PER_SECOND_DESC), metrics.readCellCountPerSecond)
        .addCounter(Interns.info(prefix + MetricsClusterSource.READ_RAW_CELLS_PER_SECOND,
          MetricsClusterSource.READ_RAW_CELLS_PER_SECOND_DESC), metrics.readRawCellCountPerSecond)
        .addCounter(Interns.info(prefix + MetricsClusterSource.MEMSTORE_SIZE_MB,
            MetricsClusterSource.MEMSTORE_SIZE_MB_DESC), metrics.memStoreSizeMB)
        .addCounter(Interns.info(prefix + MetricsClusterSource.STOREFILE_SIZE_MB,
            MetricsClusterSource.STOREFILE_SIZE_MB_DESC), metrics.storeFileSizeMB)
        .addCounter(Interns.info(prefix + MetricsClusterSource.REGION_COUNT,
            MetricsClusterSource.REGION_COUNT_DESC), metrics.regionCount)
        .addCounter(Interns.info(prefix + MetricsClusterSource.APPROXIMATE_ROW_COUNT,
          MetricsClusterSource.APPROXIMATE_ROW_COUNT_DESC), metrics.approximateRowCount)
        .addCounter(Interns.info(prefix + MetricsClusterSource.USER_READ_REQUEST_PER_SECOND,
          MetricsClusterSource.USER_READ_REQUEST_PER_SECOND_DESC), metrics.userReadRequestPerSecond)
        .addCounter(Interns.info(prefix + MetricsClusterSource.USER_WRITE_REQUEST_PER_SECOND,
          MetricsClusterSource.USER_WRITE_REQUEST_PER_SECOND_DESC), metrics.userWriteRequestPerSecond)
        .addCounter(
          Interns.info(prefix + MetricsClusterSource.USER_READ_REQUEST_BY_CAPACITY_UNIT_PER_SECOND,
            MetricsClusterSource.USER_READ_REQUEST_BY_CAPACITY_UNIT_PER_SECOND_DESC),
            metrics.userReadRequestByCapacityUnit)
        .addCounter(
          Interns.info(prefix + MetricsClusterSource.USER_WRITE_REQUEST_BY_CAPACITY_UNIT_PER_SECOND,
            MetricsClusterSource.USER_WRITE_REQUEST_BY_CAPACITY_UNIT_PER_SECOND_DESC),
            metrics.userWriteRequestByCapacityUnit);
  }

  /**
   * Xiaomi aggregation metrics
   * @param builder
   */
  @Override
  public void addClusterMetrics(MetricsRecordBuilder builder) {
    if (!master.isActiveMaster()) {
      return;
    }

    Map<ServerName, ServerMetrics> serverMetricsMap = this.getRegionServerMetrics();

    Metrics globalMetrics = new Metrics();

    // Table metrics
    Map<TableName, Metrics> tableMetrics = new HashMap<>();

    // Namespace metrics
    Map<String, Metrics> nsMetrics = new HashMap<>();

    // RSGroup metrics
    Map<String, Metrics> rsgroupMetrics = new HashMap<>();
    Map<String, Integer> rsgroupRsCount = new HashMap<>();
    RSGroupInfo defaultRSGroup = null;
    try {
      defaultRSGroup = master.getRSGroupInfoManager().getRSGroup(RSGroupInfo.DEFAULT_GROUP);
    } catch (IOException e) {
      LOG.warn("Failed to get default rsgroup");
    }

    // Replication metrics
    Map<String, Long> peerSizeOfLogQueue = new HashMap<>();
    Map<String, Long> peerReplicationLag = new HashMap<>();

    for (Entry<ServerName, ServerMetrics> serverEntry : serverMetricsMap.entrySet()) {
      ServerMetrics serverMetrics = serverEntry.getValue();
      for (Entry<byte[], RegionMetrics> regionEntry : serverMetrics.getRegionMetrics().entrySet()) {
        RegionInfo r = parseRegionInfo(regionEntry.getKey());
        if (r == null) {
          continue;
        }
        RegionMetrics regionMetrics = regionEntry.getValue();
        TableName table = r.getTable();
        // Update table metrics
        accumulateMetrics(tableMetrics.computeIfAbsent(table, t -> new Metrics()), convertRegionMetrics(regionMetrics));
      }

      // Append server replication metric
      String serverPrefix = "Server_" + serverMetrics.getServerName();
      for (ReplicationLoadSource source : serverMetrics.getReplicationLoadSourceList()) {
        String peerId = source.getPeerID();
        accumulateMap(peerSizeOfLogQueue, peerId, source.getSizeOfLogQueue());
        accumulateMap(peerReplicationLag, peerId, source.getReplicationLag());
        String peerPrefix = serverPrefix + "_peer_" + peerId + "_metric_";
        builder.addCounter(Interns.info(peerPrefix + MetricsClusterSource.SIZE_OF_LOG_QUEUE,
            MetricsClusterSource.SIZE_OF_LOG_QUEUE_DESC), source.getSizeOfLogQueue()).addCounter(
            Interns.info(peerPrefix + MetricsClusterSource.REPLICATION_LAG,
                MetricsClusterSource.REPLICATION_LAG_DESC), source.getReplicationLag());
      }
    }

    // Append cluster replication metric
    for (Map.Entry<String, Long> e : peerSizeOfLogQueue.entrySet()) {
      String peerId = e.getKey();
      Long peerSize = e.getValue();
      String clusterPeerPrefix = "Cluster_peer_" + peerId + "_metric_";
      builder.addCounter(Interns.info(clusterPeerPrefix + MetricsClusterSource.SIZE_OF_LOG_QUEUE,
          MetricsClusterSource.SIZE_OF_LOG_QUEUE_DESC), peerSize).addCounter(
          Interns.info(clusterPeerPrefix + MetricsClusterSource.REPLICATION_LAG,
              MetricsClusterSource.REPLICATION_LAG_DESC), peerReplicationLag.get(peerId));
    }

    for (TableName table : tableMetrics.keySet()) {
      Metrics metrics = tableMetrics.get(table);
      metrics.tableCount = 1;
      // accumulate ns metrics
      String ns = table.getNamespaceAsString();
      accumulateMetrics(nsMetrics.computeIfAbsent(ns, n -> new Metrics()), metrics);
      // accumulate rsgroup metrics
      try {
        RSGroupInfo rsGroupInfo = RSGroupUtil.getRSGroupInfo(master, master.getRSGroupInfoManager(), table)
            .orElse(defaultRSGroup);
        if (rsGroupInfo != null) {
          accumulateMetrics(rsgroupMetrics.computeIfAbsent(rsGroupInfo.getName(), g -> new Metrics()), metrics);
          rsgroupRsCount.put(rsGroupInfo.getName(), rsGroupInfo.getServers().size());
        }
      } catch (IOException e) {
        LOG.warn("Failed to Get RSGroup of table {}", table, e);
      }
    }

    for (Metrics ns : nsMetrics.values()) {
      accumulateMetrics(globalMetrics, ns);
    }
    long globalNamespaceCount = nsMetrics.size();

    // Append cluster metrics
    String clusterPrefix = "Cluster_metric_";
    appendMetrics(builder, clusterPrefix, globalMetrics);
    builder.addCounter(Interns.info(clusterPrefix + MetricsClusterSource.TABLE_COUNT,
        MetricsClusterSource.TABLE_COUNT_DESC), globalMetrics.tableCount);
    builder.addCounter(Interns.info(clusterPrefix + MetricsClusterSource.NAMESPACE_COUNT,
        MetricsClusterSource.NAMESPACE_COUNT_DESC), globalNamespaceCount);

    // Append table metrics
    for (TableName table : tableMetrics.keySet()) {
      String tablePrefix = "Namespace_" + table.getNamespaceAsString() + "_table_"
          + table.getQualifierAsString() + "_metric_";
      appendMetrics(builder, tablePrefix, tableMetrics.get(table));
    }

    // Append namespace metrics
    for (String ns : nsMetrics.keySet()) {
      String namespacePrefix = "Namespace_" + ns + "_metric_";
      appendMetrics(builder, namespacePrefix, nsMetrics.get(ns));
      builder.addCounter(Interns.info(namespacePrefix + MetricsClusterSource.TABLE_COUNT,
          MetricsClusterSource.TABLE_COUNT_DESC), nsMetrics.get(ns).tableCount);
    }

    // Append rsgroup metrics
    for (String rsgroup : rsgroupMetrics.keySet()) {
      String rsgroupPrefix = "RSGroup_" + rsgroup + "_metric_";
      appendMetrics(builder, rsgroupPrefix, rsgroupMetrics.get(rsgroup));
      builder.addCounter(Interns.info(rsgroupPrefix + MetricsClusterSource.TABLE_COUNT,
          MetricsClusterSource.TABLE_COUNT_DESC), rsgroupMetrics.get(rsgroup).tableCount);
      builder.addCounter(Interns.info(rsgroupPrefix + MetricsClusterSource.REGION_SERVER_COUNT,
          MetricsClusterSource.REGION_SERVER_COUNT_DESC), rsgroupRsCount.get(rsgroup));
    }
  }

  private static Metrics convertRegionMetrics(RegionMetrics regionMetrics) {
    Metrics metrics = new Metrics();
    metrics.readRequestPerSecond = regionMetrics.getReadRequestsPerSecond();
    metrics.writeRequestPerSecond = regionMetrics.getWriteRequestsPerSecond();
    metrics.getRequestPerSecond = regionMetrics.getGetRequestsPerSecond();
    metrics.scanRequestPerSecond = regionMetrics.getScanRequestsCountPerSecond();
    metrics.scanRowsCountPerSecond = regionMetrics.getScanRowsCountPerSecond();
    metrics.readRequestByCapacityUnit = regionMetrics.getReadRequestsByCapacityUnitPerSecond();
    metrics.writeRequestByCapacityUnit = regionMetrics.getWriteRequestsByCapacityUnitPerSecond();
    metrics.readCellCountPerSecond = regionMetrics.getReadCellCountPerSecond();
    metrics.readRawCellCountPerSecond = regionMetrics.getReadRawCellCountPerSecond();
    metrics.memStoreSizeMB = regionMetrics.getMemStoreSize().getLongValue();
    metrics.storeFileSizeMB = regionMetrics.getStoreFileSize().getLongValue();
    metrics.regionCount = 1L;
    metrics.approximateRowCount = regionMetrics.getApproximateRowCount();
    metrics.userReadRequestPerSecond = regionMetrics.getUserReadRequestsPerSecond();
    metrics.userWriteRequestPerSecond = regionMetrics.getUserWriteRequestsPerSecond();
    metrics.userReadRequestByCapacityUnit = regionMetrics.getUserReadRequestsByCapacityUnitPerSecond();
    metrics.userWriteRequestByCapacityUnit = regionMetrics.getUserWriteRequestsByCapacityUnitPerSecond();
    return metrics;
  }

  private void accumulateMetrics(Metrics father, Metrics son) {
    father.readRequestPerSecond += son.readRequestPerSecond;
    father.writeRequestPerSecond += son.writeRequestPerSecond;
    father.getRequestPerSecond += son.getRequestPerSecond;
    father.scanRequestPerSecond += son.scanRequestPerSecond;
    father.scanRowsCountPerSecond += son.scanRowsCountPerSecond;
    father.readRequestByCapacityUnit += son.readRequestByCapacityUnit;
    father.writeRequestByCapacityUnit += son.writeRequestByCapacityUnit;
    father.readCellCountPerSecond += son.readCellCountPerSecond;
    father.readRawCellCountPerSecond += son.readRawCellCountPerSecond;
    father.memStoreSizeMB += son.memStoreSizeMB;
    father.storeFileSizeMB += son.storeFileSizeMB;
    father.regionCount += son.regionCount;
    father.tableCount += son.tableCount;
    father.approximateRowCount += son.approximateRowCount;
    father.userReadRequestPerSecond += son.userReadRequestPerSecond;
    father.userWriteRequestPerSecond += son.userWriteRequestPerSecond;
    father.userReadRequestByCapacityUnit += son.userReadRequestByCapacityUnit;
    father.userWriteRequestByCapacityUnit += son.userWriteRequestByCapacityUnit;
  }

  private static class Metrics {
    long readRequestPerSecond = 0;
    long writeRequestPerSecond = 0;
    long getRequestPerSecond = 0;
    long scanRequestPerSecond = 0;
    long scanRowsCountPerSecond = 0;
    long readRequestByCapacityUnit = 0;
    long writeRequestByCapacityUnit = 0;
    long readCellCountPerSecond = 0;
    long readRawCellCountPerSecond = 0;
    long memStoreSizeMB = 0;
    long storeFileSizeMB = 0;
    long regionCount = 0;
    long tableCount = 0;
    long approximateRowCount = 0;
    long userReadRequestPerSecond = 0;
    long userWriteRequestPerSecond = 0;
    long userReadRequestByCapacityUnit = 0;
    long userWriteRequestByCapacityUnit = 0;
  }
}
