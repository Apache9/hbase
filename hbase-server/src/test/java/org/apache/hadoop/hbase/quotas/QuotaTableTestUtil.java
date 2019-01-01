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
package org.apache.hadoop.hbase.quotas;

import static org.apache.hadoop.hbase.quotas.QuotaTableUtil.QUOTA_TABLE_NAME;
import static org.apache.hadoop.hbase.quotas.QuotaTableUtil.createScanForSpaceSnapshotSizes;
import static org.apache.hadoop.hbase.quotas.QuotaTableUtil.extractSnapshotNameFromSizeCell;
import static org.apache.hadoop.hbase.quotas.QuotaTableUtil.parseSnapshotSize;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.QuotaStatusCalls;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Table;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.GetQuotaStatesResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.GetSpaceQuotaRegionSizesResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.GetSpaceQuotaRegionSizesResponse.RegionSizes;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.GetSpaceQuotaSnapshotsResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.GetSpaceQuotaSnapshotsResponse.TableQuotaSnapshot;

/**
 *
 */
final class QuotaTableTestUtil {

  private QuotaTableTestUtil() {
  }


  /**
   * Fetches any persisted HBase snapshot sizes stored in the quota table. The sizes here are
   * computed relative to the table which the snapshot was created from. A snapshot's size will
   * not include the size of files which the table still refers. These sizes, in bytes, are what
   * is used internally to compute quota violation for tables and namespaces.
   *
   * @return A map of snapshot name to size in bytes per space quota computations
   */
  public static Map<String,Long> getObservedSnapshotSizes(Connection conn) throws IOException {
    try (Table quotaTable = conn.getTable(QUOTA_TABLE_NAME);
        ResultScanner rs = quotaTable.getScanner(createScanForSpaceSnapshotSizes())) {
      final Map<String,Long> snapshotSizes = new HashMap<>();
      for (Result r : rs) {
        CellScanner cs = r.cellScanner();
        while (cs.advance()) {
          Cell c = cs.current();
          final String snapshot = extractSnapshotNameFromSizeCell(c);
          final long size = parseSnapshotSize(c);
          snapshotSizes.put(snapshot, size);
        }
      }
      return snapshotSizes;
    }
  }

  /* =========================================================================
   *  Space quota status RPC helpers
   */
  /**
   * Fetches the table sizes on the filesystem as tracked by the HBase Master.
   */
  public static Map<TableName,Long> getMasterReportedTableSizes(
      Connection conn) throws IOException {
    if (!(conn instanceof ClusterConnection)) {
      throw new IllegalArgumentException("Expected a ClusterConnection");
    }
    ClusterConnection clusterConn = (ClusterConnection) conn;
    GetSpaceQuotaRegionSizesResponse response = QuotaStatusCalls.getMasterRegionSizes(
        clusterConn, 0);
    Map<TableName,Long> tableSizes = new HashMap<>();
    for (RegionSizes sizes : response.getSizesList()) {
      TableName tn = ProtobufUtil.toTableName(sizes.getTableName());
      tableSizes.put(tn, sizes.getSize());
    }
    return tableSizes;
  }

  /**
   * Fetches the observed {@link SpaceQuotaSnapshot}s observed by a RegionServer.
   */
  public static Map<TableName,SpaceQuotaSnapshot> getRegionServerQuotaSnapshots(
      Connection conn, ServerName regionServer) throws IOException {
    if (!(conn instanceof ClusterConnection)) {
      throw new IllegalArgumentException("Expected a ClusterConnection");
    }
    ClusterConnection clusterConn = (ClusterConnection) conn;
    GetSpaceQuotaSnapshotsResponse response = QuotaStatusCalls.getRegionServerQuotaSnapshot(
        clusterConn, 0, regionServer);
    Map<TableName,SpaceQuotaSnapshot> snapshots = new HashMap<>();
    for (TableQuotaSnapshot snapshot : response.getSnapshotsList()) {
      snapshots.put(
          ProtobufUtil.toTableName(snapshot.getTableName()),
          SpaceQuotaSnapshot.toSpaceQuotaSnapshot(snapshot.getSnapshot()));
    }
    return snapshots;
  }

  /**
   * Returns the Master's view of a quota on the given {@code tableName} or null if the
   * Master has no quota information on that table.
   */
  public static SpaceQuotaSnapshot getCurrentSnapshot(
      Connection conn, TableName tn) throws IOException {
    if (!(conn instanceof ClusterConnection)) {
      throw new IllegalArgumentException("Expected a ClusterConnection");
    }
    ClusterConnection clusterConn = (ClusterConnection) conn;
    GetQuotaStatesResponse resp = QuotaStatusCalls.getMasterQuotaStates(clusterConn, 0);
    HBaseProtos.TableName protoTableName = ProtobufUtil.toProtoTableName(tn);
    for (GetQuotaStatesResponse.TableQuotaSnapshot tableSnapshot : resp.getTableSnapshotsList()) {
      if (protoTableName.equals(tableSnapshot.getTableName())) {
        return SpaceQuotaSnapshot.toSpaceQuotaSnapshot(tableSnapshot.getSnapshot());
      }
    }
    return null;
  }

  /**
   * Returns the Master's view of a quota on the given {@code namespace} or null if the
   * Master has no quota information on that namespace.
   */
  public static SpaceQuotaSnapshot getCurrentSnapshot(
      Connection conn, String namespace) throws IOException {
    if (!(conn instanceof ClusterConnection)) {
      throw new IllegalArgumentException("Expected a ClusterConnection");
    }
    ClusterConnection clusterConn = (ClusterConnection) conn;
    GetQuotaStatesResponse resp = QuotaStatusCalls.getMasterQuotaStates(clusterConn, 0);
    for (GetQuotaStatesResponse.NamespaceQuotaSnapshot nsSnapshot : resp.getNsSnapshotsList()) {
      if (namespace.equals(nsSnapshot.getNamespace())) {
        return SpaceQuotaSnapshot.toSpaceQuotaSnapshot(nsSnapshot.getSnapshot());
      }
    }
    return null;
  }
}
