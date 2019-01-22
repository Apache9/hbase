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

import java.io.IOException;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.RegionMetrics;
import org.apache.hadoop.hbase.ServerMetrics;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.metrics.Interns;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.VersionInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.quotas.QuotaObserverChore;
import org.apache.hadoop.hbase.quotas.SpaceQuotaSnapshot;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  public static <K> void accumulateMap(Map<K, Long> map, K key, Long value) {
    map.compute(key, (k, oldValue) -> oldValue == null ? value : oldValue + value);
  }

  public static <K> long sumUpMap(Map<K, Long> map) {
    return map.values().stream().mapToLong(l -> l.longValue()).sum();
  }

  public static MetricsRecordBuilder appendMetrics(MetricsRecordBuilder builder, String prefix,
      long readRequestPerSecond, long writeRequestPerSecond, long getRequestPerSecond,
      long scanRequestPerSecond, long scanRowsCountPerSecond, long readRequestByCapacityUnit,
      long writeRequestByCapacityUnit, long readCellsPerSecond, long readRawCellsPerSecond) {
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
          MetricsClusterSource.READ_CELLS_PER_SECOND_DESC), readCellsPerSecond)
        .addCounter(Interns.info(prefix + MetricsClusterSource.READ_RAW_CELLS_PER_SECOND,
          MetricsClusterSource.READ_RAW_CELLS_PER_SECOND_DESC), readRawCellsPerSecond);
  }

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
    long globalReadCellsPerSecond = 0;
    long globalReadRawCellsPerSecond = 0;

    // Table metrics
    Map<TableName, Long> tableReadRequestPerSecond = new HashMap<>();
    Map<TableName, Long> tableWriteRequestPerSecond = new HashMap<>();
    Map<TableName, Long> tableGetRequestPerSecond = new HashMap<>();
    Map<TableName, Long> tableScanRequestPerSecond = new HashMap<>();
    Map<TableName, Long> tableScanRowsCountPerSecond = new HashMap<>();
    Map<TableName, Long> tableReadRequestByCapacityUnit = new HashMap<>();
    Map<TableName, Long> tableWriteRequestByCapacityUnit = new HashMap<>();
    Map<TableName, Long> tableReadCellsPerSecond = new HashMap<>();
    Map<TableName, Long> tableReadRawCellsPerSecond = new HashMap<>();

    // Namespace metrics
    Map<byte[], Long> nsReadRequestPerSecond = new HashMap<>();
    Map<byte[], Long> nsWriteRequestPerSecond = new HashMap<>();
    Map<byte[], Long> nsGetRequestPerSecond = new HashMap<>();
    Map<byte[], Long> nsScanRequestPerSecond = new HashMap<>();
    Map<byte[], Long> nsScanRowsCountPerSecond = new HashMap<>();
    Map<byte[], Long> nsReadRequestByCapacityUnit = new HashMap<>();
    Map<byte[], Long> nsWriteRequestByCapacityUnit = new HashMap<>();
    Map<byte[], Long> nsReadCellsPerSecond = new HashMap<>();
    Map<byte[], Long> nsReadRawCellsPerSecond = new HashMap<>();

    for (Entry<ServerName, ServerMetrics> serverEntry : serverMetricsMap.entrySet()) {
      ServerName serverName = serverEntry.getKey();

      for (Entry<byte[], RegionMetrics> regionEntry : serverEntry.getValue().getRegionMetrics()
              .entrySet()) {
        RegionInfo r = parseRegionInfo(regionEntry.getKey());
        if (r == null) {
          continue;
        }
        RegionMetrics regionMetrics = regionEntry.getValue();

        TableName table = r.getTable();
        // Update table metrics
        accumulateMap(tableReadRequestPerSecond, table, regionMetrics.getReadRequestsCountPerSecond());
        accumulateMap(tableWriteRequestPerSecond, table, regionMetrics.getWriteRequestsCountPerSecond());
        accumulateMap(tableGetRequestPerSecond, table, regionMetrics.getGetRequestsCountPerSecond());
        accumulateMap(tableScanRequestPerSecond, table, regionMetrics.getScanRequestsCountPerSecond());
        accumulateMap(tableScanRowsCountPerSecond, table, regionMetrics.getScanRowsCountPerSecond());
        accumulateMap(tableReadRequestByCapacityUnit, table, regionMetrics.getReadRequestsByCapacityUnitPerSecond());
        accumulateMap(tableWriteRequestByCapacityUnit, table, regionMetrics.getWriteRequestsByCapacityUnitPerSecond());
        accumulateMap(tableReadCellsPerSecond, table, regionMetrics.getReadCellsPerSecond());
        accumulateMap(tableReadRawCellsPerSecond, table, regionMetrics.getReadRawCellsPerSecond());

        // Update Namespace metrics
        byte[] ns = table.getNamespace();
        accumulateMap(nsReadRequestPerSecond, ns, regionMetrics.getReadRequestsCountPerSecond());
        accumulateMap(nsWriteRequestPerSecond, ns, regionMetrics.getWriteRequestsCountPerSecond());
        accumulateMap(nsGetRequestPerSecond, ns, regionMetrics.getGetRequestsCountPerSecond());
        accumulateMap(nsScanRequestPerSecond, ns, regionMetrics.getScanRequestsCountPerSecond());
        accumulateMap(nsScanRowsCountPerSecond, ns, regionMetrics.getScanRowsCountPerSecond());
        accumulateMap(nsReadRequestByCapacityUnit, ns, regionMetrics.getReadRequestsByCapacityUnitPerSecond());
        accumulateMap(nsWriteRequestByCapacityUnit, ns, regionMetrics.getWriteRequestsByCapacityUnitPerSecond());
        accumulateMap(nsReadCellsPerSecond, ns, regionMetrics.getReadCellsPerSecond());
        accumulateMap(nsReadRawCellsPerSecond, ns, regionMetrics.getReadRawCellsPerSecond());
      }
    }
    globalReadRequestPerSecond += sumUpMap(tableReadRequestPerSecond);
    globalWriteRequestPerSecond += sumUpMap(tableWriteRequestPerSecond);
    globalGetRequestPerSecond += sumUpMap(tableGetRequestPerSecond);
    globalScanRequestPerSecond += sumUpMap(tableScanRequestPerSecond);
    globalScanRowsCountPerSecond += sumUpMap(tableScanRowsCountPerSecond);
    globalReadRequestByCapacityUnit += sumUpMap(tableReadRequestByCapacityUnit);
    globalWriteRequestByCapacityUnit += sumUpMap(tableWriteRequestByCapacityUnit);
    globalReadCellsPerSecond += sumUpMap(tableReadCellsPerSecond);
    globalReadRawCellsPerSecond += sumUpMap(tableReadRawCellsPerSecond);


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
            globalReadCellsPerSecond,
            globalReadRawCellsPerSecond);

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
              tableReadCellsPerSecond.getOrDefault(table, 0L),
              tableReadRawCellsPerSecond.getOrDefault(table, 0L));
    }

    // Append namespace metrics
    for(byte[] ns: nsReadRequestPerSecond.keySet()){
      String namespacePrefix = "Namespace_" + Bytes.toStringBinary(ns) + "_metric_";
      appendMetrics(builder,
              namespacePrefix,
              nsReadRequestPerSecond.getOrDefault(ns,0L),
              nsWriteRequestPerSecond.getOrDefault(ns,0L),
              nsGetRequestPerSecond.getOrDefault(ns, 0L),
              nsScanRequestPerSecond.getOrDefault(ns, 0L),
              nsScanRowsCountPerSecond.getOrDefault(ns, 0L),
              nsReadRequestByCapacityUnit.getOrDefault(ns, 0L),
              nsWriteRequestByCapacityUnit.getOrDefault(ns, 0L),
              nsReadCellsPerSecond.getOrDefault(ns, 0L),
              nsReadRawCellsPerSecond.getOrDefault(ns, 0L));
    }
  }
}
