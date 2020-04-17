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

  private static <K> long sumUpMap(Map<K, Long> map) {
    return map.values().stream().mapToLong(l -> l.longValue()).sum();
  }

  private static MetricsRecordBuilder appendMetrics(MetricsRecordBuilder builder, String prefix,
      long readRequestPerSecond, long writeRequestPerSecond, long getRequestPerSecond,
      long scanRequestPerSecond, long scanRowsCountPerSecond, long readRequestByCapacityUnit,
      long writeRequestByCapacityUnit, long readCellCountPerSecond, long readRawCellCountPerSecond,
      long memStoreSizeMB, long storeFileSizeMB, long regionCount, long approximateRowCount,
      long userReadRequestPerSecond, long userWriteRequestPerSecond,
      long userReadRequestByCapacityUnit, long userWriteRequestByCapacityUnit) {
    return builder
        .addCounter(Interns.info(prefix + MetricsClusterSource.READ_REQUEST_PER_SECOND,
          MetricsClusterSource.READ_REQUEST_PER_SECOND_DESC), readRequestPerSecond)
        .addCounter(Interns.info(prefix + MetricsClusterSource.WRITE_REQUEST_PER_SECOND,
          MetricsClusterSource.WRITE_REQUEST_PER_SECOND_DESC), writeRequestPerSecond)
        .addCounter(Interns.info(prefix + MetricsClusterSource.GET_REQEUST_PER_SECOND,
          MetricsClusterSource.GET_REQUEST_PER_SECOND_DESC), getRequestPerSecond)
        .addCounter(Interns.info(prefix + MetricsClusterSource.SCAN_REQUEST_PER_SECOND,
          MetricsClusterSource.SCAN_REQUEST_PER_SECOND_DESC), scanRequestPerSecond)
        .addCounter(Interns.info(prefix + MetricsClusterSource.SCAN_ROWS_COUNT_PER_SECOND,
          MetricsClusterSource.SCAN_ROWS_COUNT_PER_SECOND_DESC), scanRowsCountPerSecond)
        .addCounter(
          Interns.info(prefix + MetricsClusterSource.READ_REQUEST_BY_CAPACITY_UNIT_PER_SECOND,
            MetricsClusterSource.READ_REQUEST_BY_CAPACITY_UNIT_PER_SECOND_DESC),
          readRequestByCapacityUnit)
        .addCounter(
          Interns.info(prefix + MetricsClusterSource.WRITE_REQUEST_BY_CAPACITY_UNIT_PER_SECOND,
            MetricsClusterSource.WRITE_REQUEST_BY_CAPACITY_UNIT_PER_SECOND_DESC),
          writeRequestByCapacityUnit)
        .addCounter(Interns.info(prefix + MetricsClusterSource.READ_CELLS_PER_SECOND,
          MetricsClusterSource.READ_CELLS_PER_SECOND_DESC), readCellCountPerSecond)
        .addCounter(Interns.info(prefix + MetricsClusterSource.READ_RAW_CELLS_PER_SECOND,
          MetricsClusterSource.READ_RAW_CELLS_PER_SECOND_DESC), readRawCellCountPerSecond)
        .addCounter(Interns.info(prefix + MetricsClusterSource.MEMSTORE_SIZE_MB,
            MetricsClusterSource.MEMSTORE_SIZE_MB_DESC), memStoreSizeMB)
        .addCounter(Interns.info(prefix + MetricsClusterSource.STOREFILE_SIZE_MB,
            MetricsClusterSource.STOREFILE_SIZE_MB_DESC), storeFileSizeMB)
        .addCounter(Interns.info(prefix + MetricsClusterSource.REGION_COUNT,
            MetricsClusterSource.REGION_COUNT_DESC), regionCount)
        .addCounter(Interns.info(prefix + MetricsClusterSource.APPROXIMATE_ROW_COUNT,
          MetricsClusterSource.APPROXIMATE_ROW_COUNT_DESC), approximateRowCount)
        .addCounter(Interns.info(prefix + MetricsClusterSource.USER_READ_REQUEST_PER_SECOND,
          MetricsClusterSource.USER_READ_REQUEST_PER_SECOND_DESC), userReadRequestPerSecond)
        .addCounter(Interns.info(prefix + MetricsClusterSource.USER_WRITE_REQUEST_PER_SECOND,
          MetricsClusterSource.USER_WRITE_REQUEST_PER_SECOND_DESC), userWriteRequestPerSecond)
        .addCounter(
          Interns.info(prefix + MetricsClusterSource.USER_READ_REQUEST_BY_CAPACITY_UNIT_PER_SECOND,
            MetricsClusterSource.USER_READ_REQUEST_BY_CAPACITY_UNIT_PER_SECOND_DESC),
          userReadRequestByCapacityUnit)
        .addCounter(
          Interns.info(prefix + MetricsClusterSource.USER_WRITE_REQUEST_BY_CAPACITY_UNIT_PER_SECOND,
            MetricsClusterSource.USER_WRITE_REQUEST_BY_CAPACITY_UNIT_PER_SECOND_DESC),
          userWriteRequestByCapacityUnit);
  }

  /**
   * Xiaomi aggregation metrics
   * @param builder
   */
  @Override
  public void addClusterMetrics(MetricsRecordBuilder builder) {
    Map<ServerName, ServerMetrics> serverMetricsMap = this.getRegionServerMetrics();

    long globalReadRequestPerSecond = 0;
    long globalWriteRequestPerSecond = 0;
    long globalGetRequestPerSecond = 0;
    long globalScanRequestPerSecond = 0;
    long globalScanRowsCountPerSecond = 0;
    long globalReadRequestByCapacityUnit = 0;
    long globalWriteRequestByCapacityUnit = 0;
    long globalReadCellCountPerSecond = 0;
    long globalReadRawCellCountPerSecond = 0;
    long globalMemStoreSizeMB = 0;
    long globalStoreFileSizeMB = 0;
    long globalRegionCount = 0;
    long globalTableCount = 0;
    long globalApproximateRowCount = 0;
    long globalUserReadRequestPerSecond = 0;
    long globalUserWriteRequestPerSecond = 0;
    long globalUserReadRequestByCapacityUnit = 0;
    long globalUserWriteRequestByCapacityUnit = 0;

    // Table metrics
    Map<TableName, Long> tableReadRequestPerSecond = new HashMap<>();
    Map<TableName, Long> tableWriteRequestPerSecond = new HashMap<>();
    Map<TableName, Long> tableGetRequestPerSecond = new HashMap<>();
    Map<TableName, Long> tableScanRequestPerSecond = new HashMap<>();
    Map<TableName, Long> tableScanRowsCountPerSecond = new HashMap<>();
    Map<TableName, Long> tableReadRequestByCapacityUnit = new HashMap<>();
    Map<TableName, Long> tableWriteRequestByCapacityUnit = new HashMap<>();
    Map<TableName, Long> tableReadCellCountPerSecond = new HashMap<>();
    Map<TableName, Long> tableReadRawCellCountPerSecond = new HashMap<>();
    Map<TableName, Long> tableMemStoreSizeMB = new HashMap<>();
    Map<TableName, Long> tableStoreFileSizeMB = new HashMap<>();
    Map<TableName, Long> tableRegionCount = new HashMap<>();
    Map<TableName, Long> tableApproximateRowCount = new HashMap<>();
    Map<TableName, Long> tableUserReadRequestPerSecond = new HashMap<>();
    Map<TableName, Long> tableUserWriteRequestPerSecond = new HashMap<>();
    Map<TableName, Long> tableUserReadRequestByCapacityUnit = new HashMap<>();
    Map<TableName, Long> tableUserWriteRequestByCapacityUnit = new HashMap<>();

    // Namespace metrics
    Map<String, Long> nsReadRequestPerSecond = new HashMap<>();
    Map<String, Long> nsWriteRequestPerSecond = new HashMap<>();
    Map<String, Long> nsGetRequestPerSecond = new HashMap<>();
    Map<String, Long> nsScanRequestPerSecond = new HashMap<>();
    Map<String, Long> nsScanRowsCountPerSecond = new HashMap<>();
    Map<String, Long> nsReadRequestByCapacityUnit = new HashMap<>();
    Map<String, Long> nsWriteRequestByCapacityUnit = new HashMap<>();
    Map<String, Long> nsReadCellCountPerSecond = new HashMap<>();
    Map<String, Long> nsReadRawCellCountPerSecond = new HashMap<>();
    Map<String, Long> nsMemStoreSizeMB = new HashMap<>();
    Map<String, Long> nsStoreFileSizeMB = new HashMap<>();
    Map<String, Long> nsTableCount = new HashMap<>();
    Map<String, Long> nsRegionCount = new HashMap<>();
    Map<String, Long> nsApproximateRowCount = new HashMap<>();
    Map<String, Long> nsUserReadRequestPerSecond = new HashMap<>();
    Map<String, Long> nsUserWriteRequestPerSecond = new HashMap<>();
    Map<String, Long> nsUserReadRequestByCapacityUnit = new HashMap<>();
    Map<String, Long> nsUserWriteRequestByCapacityUnit = new HashMap<>();

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
        accumulateMap(tableReadRequestPerSecond, table, regionMetrics.getReadRequestsPerSecond());
        accumulateMap(tableWriteRequestPerSecond, table, regionMetrics.getWriteRequestsPerSecond());
        accumulateMap(tableGetRequestPerSecond, table, regionMetrics.getGetRequestsPerSecond());
        accumulateMap(tableScanRequestPerSecond, table,
            regionMetrics.getScanRequestsCountPerSecond());
        accumulateMap(tableScanRowsCountPerSecond, table,
            regionMetrics.getScanRowsCountPerSecond());
        accumulateMap(tableReadRequestByCapacityUnit, table,
            regionMetrics.getReadRequestsByCapacityUnitPerSecond());
        accumulateMap(tableWriteRequestByCapacityUnit, table,
            regionMetrics.getWriteRequestsByCapacityUnitPerSecond());
        accumulateMap(tableReadCellCountPerSecond, table,
            regionMetrics.getReadCellCountPerSecond());
        accumulateMap(tableReadRawCellCountPerSecond, table,
            regionMetrics.getReadRawCellCountPerSecond());
        accumulateMap(tableMemStoreSizeMB, table, regionMetrics.getMemStoreSize().getLongValue());
        accumulateMap(tableStoreFileSizeMB, table, regionMetrics.getStoreFileSize().getLongValue());
        accumulateMap(tableRegionCount, table, 1L);
        accumulateMap(tableApproximateRowCount, table, regionMetrics.getApproximateRowCount());
        accumulateMap(tableUserReadRequestPerSecond, table, regionMetrics.getUserReadRequestsPerSecond());
        accumulateMap(tableUserWriteRequestPerSecond, table, regionMetrics.getUserWriteRequestsPerSecond());
        accumulateMap(tableUserReadRequestByCapacityUnit, table, regionMetrics.getUserReadRequestsByCapacityUnitPerSecond());
        accumulateMap(tableUserWriteRequestByCapacityUnit, table, regionMetrics.getUserWriteRequestsByCapacityUnitPerSecond());
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

    for (Map.Entry<TableName, Long> e : tableReadRequestPerSecond.entrySet()) {
      TableName table = e.getKey();
      Long requestPerSecond = e.getValue();
      String ns = table.getNamespaceAsString();
      accumulateMap(nsReadRequestPerSecond, ns, requestPerSecond);
      accumulateMap(nsWriteRequestPerSecond, ns, tableWriteRequestPerSecond.get(table));
      accumulateMap(nsGetRequestPerSecond, ns, tableGetRequestPerSecond.get(table));
      accumulateMap(nsScanRequestPerSecond, ns, tableScanRequestPerSecond.get(table));
      accumulateMap(nsScanRowsCountPerSecond, ns, tableScanRowsCountPerSecond.get(table));
      accumulateMap(nsReadRequestByCapacityUnit, ns, tableReadRequestByCapacityUnit.get(table));
      accumulateMap(nsWriteRequestByCapacityUnit, ns, tableWriteRequestByCapacityUnit.get(table));
      accumulateMap(nsReadCellCountPerSecond, ns, tableReadCellCountPerSecond.get(table));
      accumulateMap(nsReadRawCellCountPerSecond, ns, tableReadRawCellCountPerSecond.get(table));
      accumulateMap(nsMemStoreSizeMB, ns, tableMemStoreSizeMB.get(table));
      accumulateMap(nsStoreFileSizeMB, ns, tableStoreFileSizeMB.get(table));
      accumulateMap(nsRegionCount, ns, tableRegionCount.get(table));
      accumulateMap(nsTableCount, ns, 1L);
      accumulateMap(nsApproximateRowCount, ns, tableApproximateRowCount.get(table));
      accumulateMap(nsUserReadRequestPerSecond, ns, tableUserReadRequestPerSecond.get(table));
      accumulateMap(nsUserWriteRequestPerSecond, ns, tableUserWriteRequestPerSecond.get(table));
      accumulateMap(nsUserReadRequestByCapacityUnit, ns, tableUserReadRequestByCapacityUnit.get(table));
      accumulateMap(nsUserWriteRequestByCapacityUnit, ns, tableUserWriteRequestByCapacityUnit.get(table));
    }

    globalReadRequestPerSecond += sumUpMap(nsReadRequestPerSecond);
    globalWriteRequestPerSecond += sumUpMap(nsWriteRequestPerSecond);
    globalGetRequestPerSecond += sumUpMap(nsGetRequestPerSecond);
    globalScanRequestPerSecond += sumUpMap(nsScanRequestPerSecond);
    globalScanRowsCountPerSecond += sumUpMap(nsScanRowsCountPerSecond);
    globalReadRequestByCapacityUnit += sumUpMap(nsReadRequestByCapacityUnit);
    globalWriteRequestByCapacityUnit += sumUpMap(nsWriteRequestByCapacityUnit);
    globalReadCellCountPerSecond += sumUpMap(nsReadCellCountPerSecond);
    globalReadRawCellCountPerSecond += sumUpMap(nsReadRawCellCountPerSecond);
    globalMemStoreSizeMB += sumUpMap(nsMemStoreSizeMB);
    globalStoreFileSizeMB += sumUpMap(nsStoreFileSizeMB);
    globalRegionCount += sumUpMap(nsRegionCount);
    globalTableCount += sumUpMap(nsTableCount);
    globalApproximateRowCount += sumUpMap(nsApproximateRowCount);
    globalUserReadRequestPerSecond += sumUpMap(nsUserReadRequestPerSecond);
    globalUserWriteRequestPerSecond += sumUpMap(nsUserWriteRequestPerSecond);
    globalUserReadRequestByCapacityUnit += sumUpMap(nsUserReadRequestByCapacityUnit);
    globalUserWriteRequestByCapacityUnit += sumUpMap(nsUserWriteRequestByCapacityUnit);
    long globalNamespaceCount = nsTableCount.size();

    // Append cluster metrics
    String clusterPrefix = "Cluster_metric_";
    appendMetrics(builder,
            clusterPrefix,
            globalReadRequestPerSecond,
            globalWriteRequestPerSecond,
            globalGetRequestPerSecond,
            globalScanRequestPerSecond,
            globalScanRowsCountPerSecond,
            globalReadRequestByCapacityUnit,
            globalWriteRequestByCapacityUnit,
            globalReadCellCountPerSecond,
            globalReadRawCellCountPerSecond,
            globalMemStoreSizeMB,
            globalStoreFileSizeMB,
            globalRegionCount,
            globalApproximateRowCount,
            globalUserReadRequestPerSecond,
            globalUserWriteRequestPerSecond,
            globalUserReadRequestByCapacityUnit,
            globalUserWriteRequestByCapacityUnit);
    builder.addCounter(Interns.info(clusterPrefix + MetricsClusterSource.TABLE_COUNT,
        MetricsClusterSource.TABLE_COUNT_DESC), globalTableCount);
    builder.addCounter(Interns.info(clusterPrefix + MetricsClusterSource.NAMESPACE_COUNT,
        MetricsClusterSource.NAMESPACE_COUNT_DESC), globalNamespaceCount);

    // Append table metrics
    for (TableName table : tableReadRequestPerSecond.keySet()) {
      String tablePrefix = "Namespace_" + table.getNamespaceAsString() + "_table_"
          + table.getQualifierAsString() + "_metric_";
      appendMetrics(builder,
              tablePrefix,
              tableReadRequestPerSecond.getOrDefault(table, 0L),
              tableWriteRequestPerSecond.getOrDefault(table, 0L),
              tableGetRequestPerSecond.getOrDefault(table, 0L),
              tableScanRequestPerSecond.getOrDefault(table, 0L),
              tableScanRowsCountPerSecond.getOrDefault(table, 0L),
              tableReadRequestByCapacityUnit.getOrDefault(table, 0L),
              tableWriteRequestByCapacityUnit.getOrDefault(table, 0L),
              tableReadCellCountPerSecond.getOrDefault(table, 0L),
              tableReadRawCellCountPerSecond.getOrDefault(table, 0L),
              tableMemStoreSizeMB.getOrDefault(table, 0L),
              tableStoreFileSizeMB.getOrDefault(table, 0L),
              tableRegionCount.getOrDefault(table, 0L),
              tableApproximateRowCount.getOrDefault(table, 0L),
              tableUserReadRequestPerSecond.getOrDefault(table, 0L),
              tableUserWriteRequestPerSecond.getOrDefault(table, 0L),
              tableUserReadRequestByCapacityUnit.getOrDefault(table, 0L),
              tableUserWriteRequestByCapacityUnit.getOrDefault(table, 0L));
    }

    // Append namespace metrics
    for(String ns : nsReadRequestPerSecond.keySet()){
      String namespacePrefix = "Namespace_" + ns + "_metric_";
      appendMetrics(builder,
              namespacePrefix,
              nsReadRequestPerSecond.getOrDefault(ns,0L),
              nsWriteRequestPerSecond.getOrDefault(ns,0L),
              nsGetRequestPerSecond.getOrDefault(ns, 0L),
              nsScanRequestPerSecond.getOrDefault(ns, 0L),
              nsScanRowsCountPerSecond.getOrDefault(ns, 0L),
              nsReadRequestByCapacityUnit.getOrDefault(ns, 0L),
              nsWriteRequestByCapacityUnit.getOrDefault(ns, 0L),
              nsReadCellCountPerSecond.getOrDefault(ns, 0L),
              nsReadRawCellCountPerSecond.getOrDefault(ns, 0L),
              nsMemStoreSizeMB.getOrDefault(ns, 0L),
              nsStoreFileSizeMB.getOrDefault(ns, 0L),
              nsRegionCount.getOrDefault(ns, 0L),
              nsApproximateRowCount.getOrDefault(ns, 0L),
              nsUserReadRequestPerSecond.getOrDefault(ns, 0L),
              nsUserWriteRequestPerSecond.getOrDefault(ns, 0L),
              nsUserReadRequestByCapacityUnit.getOrDefault(ns, 0L),
              nsUserWriteRequestByCapacityUnit.getOrDefault(ns, 0L));
      builder.addCounter(Interns.info(namespacePrefix + MetricsClusterSource.TABLE_COUNT,
          MetricsClusterSource.TABLE_COUNT_DESC), nsTableCount.getOrDefault(ns, 0L));
    }
  }
}
