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
package org.apache.hadoop.hbase.client;

import static org.apache.hadoop.hbase.TableName.META_TABLE_NAME;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.Message;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcChannel;

import org.apache.hadoop.hbase.shaded.io.netty.util.Timeout;
import org.apache.hadoop.hbase.shaded.io.netty.util.TimerTask;

import java.util.stream.Stream;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.RegionLoad;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotDisabledException;
import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.UnknownRegionException;
import org.apache.hadoop.hbase.catalog.AsyncMetaTableAccessor;
import org.apache.hadoop.hbase.catalog.MetaReader;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.AsyncRpcRetryingCallerFactory.AdminRequestCallerBuilder;
import org.apache.hadoop.hbase.client.AsyncRpcRetryingCallerFactory.MasterRequestCallerBuilder;
import org.apache.hadoop.hbase.client.AsyncRpcRetryingCallerFactory.ServerRequestCallerBuilder;
import org.apache.hadoop.hbase.client.RawAsyncTable.CoprocessorCallable;
import org.apache.hadoop.hbase.client.Scan.ReadType;
import org.apache.hadoop.hbase.client.replication.ReplicationSerDeHelper;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.RequestConverter;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.AdminService;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.CloseRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.CloseRegionResponse;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.CompactRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.CompactRegionResponse;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.FlushRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.FlushRegionResponse;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetOnlineRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetOnlineRegionResponse;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetRegionInfoRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetRegionInfoResponse;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetRegionInfoResponse.CompactionState;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.MergeRegionsRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.MergeRegionsResponse;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.RollWALWriterRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.RollWALWriterResponse;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.SplitRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.SplitRegionResponse;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.StopServerRequest;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.StopServerResponse;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.TableSchema;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.AddColumnRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.AddColumnResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.AssignRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.AssignRegionResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.BalanceRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.BalanceResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.CreateNamespaceRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.CreateNamespaceResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.CreateTableRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.CreateTableResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.DeleteColumnRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.DeleteColumnResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.DeleteNamespaceRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.DeleteNamespaceResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.DeleteSnapshotRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.DeleteSnapshotResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.DeleteTableRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.DeleteTableResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.DisableTableRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.DisableTableResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.DispatchMergingRegionsRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.DispatchMergingRegionsResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.EnableCatalogJanitorRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.EnableCatalogJanitorResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.EnableTableRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.EnableTableResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.GetClusterStatusRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.GetClusterStatusResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.GetCompletedSnapshotsRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.GetCompletedSnapshotsResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.GetNamespaceDescriptorRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.GetNamespaceDescriptorResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.GetSchemaAlterStatusRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.GetSchemaAlterStatusResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.GetTableDescriptorsRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.GetTableDescriptorsResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.GetTableNamesRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.GetTableNamesResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.IsCatalogJanitorEnabledRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.IsCatalogJanitorEnabledResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.IsSnapshotDoneRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.IsSnapshotDoneResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.ListNamespaceDescriptorsRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.ListNamespaceDescriptorsResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.MasterService;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.ModifyColumnRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.ModifyColumnResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.ModifyNamespaceRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.ModifyNamespaceResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.MoveRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.MoveRegionResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.OfflineRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.OfflineRegionResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.RestoreSnapshotRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.RestoreSnapshotResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.RunCatalogScanRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.RunCatalogScanResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.SetBalancerRunningRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.SetBalancerRunningResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.SetQuotaRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.SetQuotaResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.ShutdownRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.ShutdownResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.SnapshotRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.SnapshotResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.StopMasterRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.StopMasterResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.TruncateTableRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.TruncateTableResponse;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.UnassignRegionRequest;
import org.apache.hadoop.hbase.protobuf.generated.MasterProtos.UnassignRegionResponse;
import org.apache.hadoop.hbase.protobuf.generated.SnapshotProtos;
import org.apache.hadoop.hbase.quotas.QuotaFilter;
import org.apache.hadoop.hbase.quotas.QuotaSettings;
import org.apache.hadoop.hbase.quotas.QuotaTableUtil;
import org.apache.hadoop.hbase.snapshot.ClientSnapshotDescriptionUtils;
import org.apache.hadoop.hbase.snapshot.RestoreSnapshotException;
import org.apache.hadoop.hbase.snapshot.SnapshotCreationException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Pair;

/**
 * The implementation of AsyncAdmin.
 */
@InterfaceAudience.Private
public class RawAsyncHBaseAdmin implements AsyncAdmin {
  public static final String FLUSH_TABLE_PROCEDURE_SIGNATURE = "flush-table-proc";

  private static final Log LOG = LogFactory.getLog(AsyncHBaseAdmin.class);

  private final AsyncConnectionImpl connection;

  private final RawAsyncTable metaTable;

  private final long rpcTimeoutNs;

  private final long operationTimeoutNs;

  private final long pauseNs;

  private final int maxAttempts;

  private final int startLogErrorsCnt;

  private final NonceGenerator ng;

  private final AsyncRegistry registry;

  RawAsyncHBaseAdmin(AsyncConnectionImpl connection, AsyncAdminBuilderBase<?> builder) {
    this.connection = connection;
    this.metaTable = connection.getRawTable(META_TABLE_NAME);
    this.rpcTimeoutNs = builder.rpcTimeoutNs;
    this.operationTimeoutNs = builder.operationTimeoutNs;
    this.pauseNs = builder.pauseNs;
    this.maxAttempts = builder.maxAttempts;
    this.startLogErrorsCnt = builder.startLogErrorsCnt;
    this.ng = connection.getNonceGenerator();
    this.registry = AsyncRegistryFactory.getRegistry(connection.getConfiguration());
  }

  private <T> MasterRequestCallerBuilder<T> newMasterCaller() {
    return this.connection.callerFactory.<T> masterRequest()
        .rpcTimeout(rpcTimeoutNs, TimeUnit.NANOSECONDS)
        .operationTimeout(operationTimeoutNs, TimeUnit.NANOSECONDS)
        .pause(pauseNs, TimeUnit.NANOSECONDS).maxAttempts(maxAttempts)
        .startLogErrorsCnt(startLogErrorsCnt);
  }

  private <T> AdminRequestCallerBuilder<T> newAdminCaller() {
    return this.connection.callerFactory.<T> adminRequest()
        .rpcTimeout(rpcTimeoutNs, TimeUnit.NANOSECONDS)
        .operationTimeout(operationTimeoutNs, TimeUnit.NANOSECONDS)
        .pause(pauseNs, TimeUnit.NANOSECONDS).maxAttempts(maxAttempts)
        .startLogErrorsCnt(startLogErrorsCnt);
  }

  @FunctionalInterface
  private interface MasterRpcCall<RESP, REQ> {
    void call(MasterService.Interface stub, HBaseRpcController controller, REQ req,
        RpcCallback<RESP> done);
  }

  @FunctionalInterface
  private interface AdminRpcCall<RESP, REQ> {
    void call(AdminService.Interface stub, HBaseRpcController controller, REQ req,
        RpcCallback<RESP> done);
  }

  @FunctionalInterface
  private interface Converter<D, S> {
    D convert(S src) throws IOException;
  }

  private <PREQ, PRESP, RESP> CompletableFuture<RESP> call(HBaseRpcController controller,
      MasterService.Interface stub, PREQ preq, MasterRpcCall<PRESP, PREQ> rpcCall,
      Converter<RESP, PRESP> respConverter) {
    CompletableFuture<RESP> future = new CompletableFuture<>();
    rpcCall.call(stub, controller, preq, new RpcCallback<PRESP>() {

      @Override
      public void run(PRESP resp) {
        if (controller.failed()) {
          future.completeExceptionally(controller.getFailed());
        } else {
          try {
            future.complete(respConverter.convert(resp));
          } catch (IOException e) {
            future.completeExceptionally(e);
          }
        }
      }
    });
    return future;
  }

  private <PREQ, PRESP, RESP> CompletableFuture<RESP> adminCall(HBaseRpcController controller,
      AdminService.Interface stub, PREQ preq, AdminRpcCall<PRESP, PREQ> rpcCall,
      Converter<RESP, PRESP> respConverter) {

    CompletableFuture<RESP> future = new CompletableFuture<>();
    rpcCall.call(stub, controller, preq, new RpcCallback<PRESP>() {

      @Override
      public void run(PRESP resp) {
        if (controller.failed()) {
          future.completeExceptionally(new IOException(controller.errorText()));
        } else {
          try {
            future.complete(respConverter.convert(resp));
          } catch (IOException e) {
            future.completeExceptionally(e);
          }
        }
      }
    });
    return future;
  }

  @FunctionalInterface
  private interface TableOperator {
    CompletableFuture<Void> operate(TableName table);
  }

  private CompletableFuture<List<HTableDescriptor>> batchTableOperations(Pattern pattern,
      TableOperator operator, String operationType) {
    CompletableFuture<List<HTableDescriptor>> future = new CompletableFuture<>();
    List<HTableDescriptor> failed = new LinkedList<>();
    listTables(Optional.ofNullable(pattern)).whenComplete(
      (tables, error) -> {
        if (error != null) {
          future.completeExceptionally(error);
          return;
        }
        CompletableFuture[] futures = tables.stream()
            .map((table) -> operator.operate(table.getTableName()).whenComplete((v, ex) -> {
              if (ex != null) {
                LOG.info("Failed to " + operationType + " table " + table.getTableName(), ex);
                failed.add(table);
              }
            })).<CompletableFuture> toArray(size -> new CompletableFuture[size]);
        CompletableFuture.allOf(futures).thenAccept((v) -> {
          future.complete(failed);
        });
      });
    return future;
  }

  @Override
  public CompletableFuture<Boolean> tableExists(TableName tableName) {
    return AsyncMetaTableAccessor.tableExists(metaTable, tableName);
  }

  @Override
  public CompletableFuture<List<HTableDescriptor>> listTables(Optional<Pattern> pattern) {
    return this
        .<List<HTableDescriptor>> newMasterCaller()
        .action(
          (controller, stub) -> this
              .<GetTableDescriptorsRequest, GetTableDescriptorsResponse, List<HTableDescriptor>> call(
                controller, stub, RequestConverter.buildGetTableDescriptorsRequest(pattern), (s, c,
                    req, done) -> s.getTableDescriptors(c, req, done),
                ProtobufUtil::toTableDescriptorList)).call();
  }

  @Override
  public CompletableFuture<List<TableName>> listTableNames() {
    return this.<List<TableName>> newMasterCaller()
        .action((controller, stub) -> this
            .<GetTableNamesRequest, GetTableNamesResponse, List<TableName>> call(controller, stub,
              GetTableNamesRequest.newBuilder().build(),
              (s, c, req, done) -> s.getTableNames(c, req, done),
              (resp) -> ProtobufUtil.toTableNameList(resp.getTableNamesList())))
        .call();
  }

  @Override
  public CompletableFuture<HTableDescriptor> getTableDescriptor(TableName tableName) {
    CompletableFuture<HTableDescriptor> future = new CompletableFuture<>();
    this.<List<TableSchema>> newMasterCaller()
        .action(
          (controller, stub) -> this
              .<GetTableDescriptorsRequest, GetTableDescriptorsResponse, List<TableSchema>> call(
                controller, stub, RequestConverter.buildGetTableDescriptorsRequest(tableName), (s,
                    c, req, done) -> s.getTableDescriptors(c, req, done), (resp) -> resp
                    .getTableSchemaList())).call().whenComplete((tableSchemas, error) -> {
          if (error != null) {
            future.completeExceptionally(error);
            return;
          }
          if (!tableSchemas.isEmpty()) {
            future.complete(HTableDescriptor.convert(tableSchemas.get(0)));
          } else {
            future.completeExceptionally(new TableNotFoundException(tableName.getNameAsString()));
          }
        });
    return future;
  }

  @Override
  public CompletableFuture<Void> createTable(HTableDescriptor desc, byte[] startKey, byte[] endKey,
      int numRegions) {
    try {
      return createTable(desc, Optional.of(getSplitKeys(startKey, endKey, numRegions)));
    } catch (IllegalArgumentException e) {
      return failedFuture(e);
    }
  }

  @Override
  public CompletableFuture<Void> createTable(HTableDescriptor desc, Optional<byte[][]> splitKeys) {
    if (desc.getTableName() == null) {
      return failedFuture(new IllegalArgumentException("TableName cannot be null"));
    }
    try {
      splitKeys.ifPresent(keys -> verifySplitKeys(keys));
      return this
          .<Void> newMasterCaller()
          .action(
            (controller, stub) -> this.<CreateTableRequest, CreateTableResponse, Void> call(
              controller, stub, RequestConverter.buildCreateTableRequest(desc, splitKeys), (s, c,
                  req, done) -> s.createTable(c, req, done), (resp) -> null)).call();
    } catch (IllegalArgumentException e) {
      return failedFuture(e);
    }
  }

  @Override
  public CompletableFuture<Void> deleteTable(TableName tableName) {
    return this
        .<Void> newMasterCaller()
        .action(
          (controller, stub) -> this.<DeleteTableRequest, DeleteTableResponse, Void> call(
            controller, stub, RequestConverter.buildDeleteTableRequest(tableName),
            (s, c, req, done) -> s.deleteTable(c, req, done), (resp) -> null)).call();
  }

  @Override
  public CompletableFuture<List<HTableDescriptor>> deleteTables(Pattern pattern) {
    return batchTableOperations(pattern, (table) -> deleteTable(table), "DELETE");
  }

  @Override
  public CompletableFuture<Void> truncateTable(TableName tableName, boolean preserveSplits) {
    return this
        .<Void> newMasterCaller()
        .action(
          (controller, stub) -> this.<TruncateTableRequest, TruncateTableResponse, Void> call(
            controller, stub, RequestConverter.buildTruncateTableRequest(tableName, preserveSplits),
            (s, c, req, done) -> s.truncateTable(c, req, done), (resp) -> null)).call();
  }

  @Override
  public CompletableFuture<Void> enableTable(TableName tableName) {
    return this
        .<Void> newMasterCaller()
        .action(
          (controller, stub) -> this.<EnableTableRequest, EnableTableResponse, Void> call(
            controller, stub, RequestConverter.buildEnableTableRequest(tableName),
            (s, c, req, done) -> s.enableTable(c, req, done), (resp) -> null)).call();
  }

  @Override
  public CompletableFuture<List<HTableDescriptor>> enableTables(Pattern pattern) {
    return batchTableOperations(pattern, (table) -> enableTable(table), "ENABLE");
  }

  @Override
  public CompletableFuture<Void> disableTable(TableName tableName) {
    return this
        .<Void> newMasterCaller()
        .action(
          (controller, stub) -> this.<DisableTableRequest, DisableTableResponse, Void> call(
            controller, stub, RequestConverter.buildDisableTableRequest(tableName),
            (s, c, req, done) -> s.disableTable(c, req, done), (resp) -> null)).call();
  }

  @Override
  public CompletableFuture<List<HTableDescriptor>> disableTables(Pattern pattern) {
    return batchTableOperations(pattern, (table) -> disableTable(table), "DISABLE");
  }

  @Override
  public CompletableFuture<Boolean> isTableEnabled(TableName tableName) {
    return registry.isTableEnabled(tableName);
  }

  @Override
  public CompletableFuture<Boolean> isTableDisabled(TableName tableName) {
    return registry.isTableDisabled(tableName);
  }

  @Override
  public CompletableFuture<Boolean> isTableAvailable(TableName tableName) {
    return isTableAvailable(tableName, null);
  }

  @Override
  public CompletableFuture<Boolean> isTableAvailable(TableName tableName, byte[][] splitKeys) {
    CompletableFuture<Boolean> future = new CompletableFuture<>();
    isTableEnabled(tableName).whenComplete(
      (enabled, error) -> {
        if (error != null) {
          future.completeExceptionally(error);
          return;
        }
        if (!enabled) {
          future.complete(false);
        } else {
          AsyncMetaTableAccessor.getTableHRegionLocations(metaTable, Optional.of(tableName))
              .whenComplete(
                (locations, error1) -> {
                  if (error1 != null) {
                    future.completeExceptionally(error1);
                    return;
                  }
                  int notDeployed = 0;
                  int regionCount = 0;
                  for (HRegionLocation location : locations) {
                    HRegionInfo info = location.getRegionInfo();
                    if (location.getServerName() == null) {
                      if (LOG.isDebugEnabled()) {
                        LOG.debug("Table " + tableName + " has not deployed region "
                            + info.getEncodedName());
                      }
                      notDeployed++;
                    } else if (splitKeys != null
                        && !Bytes.equals(info.getStartKey(), HConstants.EMPTY_BYTE_ARRAY)) {
                      for (byte[] splitKey : splitKeys) {
                        // Just check if the splitkey is available
                        if (Bytes.equals(info.getStartKey(), splitKey)) {
                          regionCount++;
                          break;
                        }
                      }
                    } else {
                      // Always empty start row should be counted
                      regionCount++;
                    }
                  }
                  if (notDeployed > 0) {
                    if (LOG.isDebugEnabled()) {
                      LOG.debug("Table " + tableName + " has " + notDeployed + " regions");
                    }
                    future.complete(false);
                  } else if (splitKeys != null && regionCount != splitKeys.length + 1) {
                    if (LOG.isDebugEnabled()) {
                      LOG.debug("Table " + tableName + " expected to have "
                          + (splitKeys.length + 1) + " regions, but only " + regionCount
                          + " available");
                    }
                    future.complete(false);
                  } else {
                    if (LOG.isDebugEnabled()) {
                      LOG.debug("Table " + tableName + " should be available");
                    }
                    future.complete(true);
                  }
                });
        }
      });
    return future;
  }

  @Override
  public CompletableFuture<Pair<Integer, Integer>> getAlterStatus(TableName tableName) {
    return this
        .<Pair<Integer, Integer>>newMasterCaller()
        .action(
          (controller, stub) -> this
              .<GetSchemaAlterStatusRequest, GetSchemaAlterStatusResponse, Pair<Integer, Integer>> call(
                controller, stub, RequestConverter.buildGetSchemaAlterStatusRequest(tableName), (s,
                    c, req, done) -> s.getSchemaAlterStatus(c, req, done), (resp) -> new Pair<>(
                    resp.getYetToUpdateRegions(), resp.getTotalRegions()))).call();
  }

  @Override
  public CompletableFuture<Void> addColumnFamily(TableName tableName, HColumnDescriptor columnFamily) {
    return this
        .<Void> newMasterCaller()
        .action(
          (controller, stub) -> this.<AddColumnRequest, AddColumnResponse, Void> call(controller,
            stub, RequestConverter.buildAddColumnRequest(tableName, columnFamily),
            (s, c, req, done) -> s.addColumn(c, req, done), (resp) -> null)).call();
  }

  @Override
  public CompletableFuture<Void> deleteColumnFamily(TableName tableName, byte[] columnFamily) {
    return this
        .<Void> newMasterCaller()
        .action(
          (controller, stub) -> this.<DeleteColumnRequest, DeleteColumnResponse, Void> call(
            controller, stub, RequestConverter.buildDeleteColumnRequest(tableName, columnFamily), (
                s, c, req, done) -> s.deleteColumn(c, req, done), (resp) -> null)).call();
  }

  @Override
  public CompletableFuture<Void> modifyColumnFamily(TableName tableName,
    HColumnDescriptor columnFamily) {
    return this
        .<Void> newMasterCaller()
        .action(
          (controller, stub) -> this.<ModifyColumnRequest, ModifyColumnResponse, Void> call(
            controller, stub, RequestConverter.buildModifyColumnRequest(tableName, columnFamily), (
                s, c, req, done) -> s.modifyColumn(c, req, done), (resp) -> null)).call();
  }

  @Override
  public CompletableFuture<Void> createNamespace(NamespaceDescriptor descriptor) {
    return this
        .<Void> newMasterCaller()
        .action(
          (controller, stub) -> this.<CreateNamespaceRequest, CreateNamespaceResponse, Void> call(
            controller, stub, RequestConverter.buildCreateNamespaceRequest(descriptor), (s, c, req,
                done) -> s.createNamespace(c, req, done), (resp) -> null)).call();
  }

  @Override
  public CompletableFuture<Void> modifyNamespace(NamespaceDescriptor descriptor) {
    return this
        .<Void> newMasterCaller()
        .action(
          (controller, stub) -> this.<ModifyNamespaceRequest, ModifyNamespaceResponse, Void> call(
            controller, stub, RequestConverter.buildModifyNamespaceRequest(descriptor), (s, c, req,
                done) -> s.modifyNamespace(c, req, done), (resp) -> null)).call();
  }

  @Override
  public CompletableFuture<Void> deleteNamespace(String name) {
    return this
        .<Void> newMasterCaller()
        .action(
          (controller, stub) -> this.<DeleteNamespaceRequest, DeleteNamespaceResponse, Void> call(
            controller, stub, RequestConverter.buildDeleteNamespaceRequest(name),
            (s, c, req, done) -> s.deleteNamespace(c, req, done), (resp) -> null)).call();
  }

  @Override
  public CompletableFuture<NamespaceDescriptor> getNamespaceDescriptor(String name) {
    return this
        .<NamespaceDescriptor> newMasterCaller()
        .action(
          (controller, stub) -> this
              .<GetNamespaceDescriptorRequest, GetNamespaceDescriptorResponse, NamespaceDescriptor> call(
                controller, stub, RequestConverter.buildGetNamespaceDescriptorRequest(name), (s, c,
                    req, done) -> s.getNamespaceDescriptor(c, req, done), (resp) -> ProtobufUtil
                    .toNamespaceDescriptor(resp.getNamespaceDescriptor()))).call();
  }

  @Override
  public CompletableFuture<List<NamespaceDescriptor>> listNamespaceDescriptors() {
    return this
        .<List<NamespaceDescriptor>> newMasterCaller()
        .action(
          (controller, stub) -> this
              .<ListNamespaceDescriptorsRequest, ListNamespaceDescriptorsResponse, List<NamespaceDescriptor>> call(
                controller, stub, ListNamespaceDescriptorsRequest.newBuilder().build(), (s, c, req,
                    done) -> s.listNamespaceDescriptors(c, req, done),
                ProtobufUtil::toNamespaceDescriptorList)).call();
  }

  @Override
  public CompletableFuture<Boolean> closeRegion(byte[] regionName, Optional<ServerName> serverName) {
    CompletableFuture<Boolean> future = new CompletableFuture<>();
    getRegionLocation(regionName).whenComplete((location, err) -> {
      if (err != null) {
        future.completeExceptionally(err);
        return;
      }
      ServerName server = serverName.isPresent() ? serverName.get() : location.getServerName();
      if (server == null) {
        future.completeExceptionally(new NotServingRegionException(regionName));
      } else {
        closeRegion(location.getRegionInfo(), server).whenComplete((result, err2) -> {
          if (err2 != null) {
            future.completeExceptionally(err2);
          } else {
            future.complete(result);
          }
        });
      }
    });
    return future;
  }

  private CompletableFuture<Boolean> closeRegion(HRegionInfo hri, ServerName serverName) {
    return this
        .<Boolean> newAdminCaller()
        .serverName(serverName)
        .action(
          (controller, stub) -> this.<CloseRegionRequest, CloseRegionResponse, Boolean> adminCall(
            controller, stub, RequestConverter.buildCloseRegionRequest(serverName,
              hri.getRegionName(), false), (s, c, req, done) -> s.closeRegion(c, req, done),
            resp -> resp.getClosed())).call();
  }

  @Override
  public CompletableFuture<List<HRegionInfo>> getOnlineRegions(ServerName serverName) {
    return this
        .<List<HRegionInfo>> newAdminCaller()
        .action(
          (controller, stub) -> this
              .<GetOnlineRegionRequest, GetOnlineRegionResponse, List<HRegionInfo>> adminCall(
                controller, stub, RequestConverter.buildGetOnlineRegionRequest(),
                (s, c, req, done) -> s.getOnlineRegion(c, req, done), ProtobufUtil::getRegionInfos))
        .serverName(serverName).call();
  }

  @Override
  public CompletableFuture<List<HRegionInfo>> getTableRegions(TableName tableName) {
    if (tableName.equals(META_TABLE_NAME)) {
      return connection.getLocator().getRegionLocation(tableName, null, null, operationTimeoutNs)
          .thenApply(loc -> Arrays.asList(loc.getRegionInfo()));
    } else {
      return AsyncMetaTableAccessor.getTableHRegionLocations(metaTable, Optional.of(tableName))
          .thenApply(
            locs -> locs.stream().map(loc -> loc.getRegionInfo()).collect(Collectors.toList()));
    }
  }

  @Override
  public CompletableFuture<Void> flush(TableName tableName) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    getTableHRegionLocations(tableName).whenComplete(
      (locations, err) -> {
        if (err != null) {
          future.completeExceptionally(err);
          return;
        }
        List<CompletableFuture<Void>> flushFutures = new ArrayList<>();
        for (HRegionLocation location : locations) {
          if (location.getRegionInfo() == null || location.getRegionInfo().isOffline()) continue;
          if (location.getServerName() == null) continue;
          flushFutures.add(flushRegion(location.getRegionInfo().getRegionName()));
        }
        if (flushFutures.size() == 0) {
          future.completeExceptionally(new IOException("no regions found for table " + tableName));
          return;
        }
        CompletableFuture
            .allOf(flushFutures.toArray(new CompletableFuture<?>[flushFutures.size()]))
            .whenComplete((ret, err2) -> {
              if (err2 != null) {
                future.completeExceptionally(err2);
              } else {
                future.complete(ret);
              }
            });
      });
    return future;
  }

  @Override
  public CompletableFuture<Void> flushRegion(byte[] regionName) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    getRegionLocation(regionName).whenComplete(
      (location, err) -> {
        if (err != null) {
          future.completeExceptionally(err);
          return;
        }
        ServerName serverName = location.getServerName();
        if (serverName == null) {
          future.completeExceptionally(new NoServerForRegionException(Bytes
              .toStringBinary(regionName)));
          return;
        }

        HRegionInfo regionInfo = location.getRegionInfo();
        this.<Void> newAdminCaller()
            .serverName(serverName)
            .action(
              (controller, stub) -> this.<FlushRegionRequest, FlushRegionResponse, Void> adminCall(
                controller, stub, RequestConverter.buildFlushRegionRequest(regionInfo
                    .getRegionName()), (s, c, req, done) -> s.flushRegion(c, req, done),
                resp -> null)).call().whenComplete((ret, err2) -> {
              if (err2 != null) {
                future.completeExceptionally(err2);
              } else {
                future.complete(ret);
              }
            });
      });
    return future;
  }

  @Override
  public CompletableFuture<Void> compact(TableName tableName, Optional<byte[]> columnFamily) {
    return compact(tableName, columnFamily, false);
  }

  @Override
  public CompletableFuture<Void> compactRegion(byte[] regionName, Optional<byte[]> columnFamily) {
    return compactRegion(regionName, columnFamily, false);
  }

  @Override
  public CompletableFuture<Void> majorCompact(TableName tableName, Optional<byte[]> columnFamily) {
    return compact(tableName, columnFamily, true);
  }

  @Override
  public CompletableFuture<Void> majorCompactRegion(byte[] regionName, Optional<byte[]> columnFamily) {
    return compactRegion(regionName, columnFamily, true);
  }

  @Override
  public CompletableFuture<Void> compactRegionServer(ServerName sn) {
    return compactRegionServer(sn, false);
  }

  @Override
  public CompletableFuture<Void> majorCompactRegionServer(ServerName sn) {
    return compactRegionServer(sn, true);
  }

  private CompletableFuture<Void> compactRegionServer(ServerName sn, boolean major) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    getOnlineRegions(sn).whenComplete((hRegionInfos, err) -> {
      if (err != null) {
        future.completeExceptionally(err);
        return;
      }
      List<CompletableFuture<Void>> compactFutures = new ArrayList<>();
      if (hRegionInfos != null) {
        hRegionInfos.forEach(region -> compactFutures.add(compact(sn, region, major, Optional.empty())));
      }
      CompletableFuture
          .allOf(compactFutures.toArray(new CompletableFuture<?>[compactFutures.size()]))
          .whenComplete((ret, err2) -> {
            if (err2 != null) {
              future.completeExceptionally(err2);
            } else {
              future.complete(ret);
            }
          });
    });
    return future;
  }

  private CompletableFuture<Void> compactRegion(byte[] regionName, Optional<byte[]> columnFamily,
      boolean major) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    getRegionLocation(regionName).whenComplete(
      (location, err) -> {
        if (err != null) {
          future.completeExceptionally(err);
          return;
        }
        ServerName serverName = location.getServerName();
        if (serverName == null) {
          future.completeExceptionally(new NoServerForRegionException(Bytes
              .toStringBinary(regionName)));
          return;
        }
        compact(location.getServerName(), location.getRegionInfo(), major, columnFamily)
            .whenComplete((ret, err2) -> {
              if (err2 != null) {
                future.completeExceptionally(err2);
              } else {
                future.complete(ret);
              }
            });
      });
    return future;
  }

  /**
   * List all region locations for the specific table.
   */
  private CompletableFuture<List<HRegionLocation>> getTableHRegionLocations(TableName tableName) {
    if (TableName.META_TABLE_NAME.equals(tableName)) {
      CompletableFuture<List<HRegionLocation>> future = new CompletableFuture<>();
      // For meta table, we use zk to fetch all locations.
      registry.getMetaRegionLocation().whenComplete((metaRegion, err) -> {
        if (err != null) {
          future.completeExceptionally(err);
        } else if (metaRegion == null) {
          future.completeExceptionally(new IOException("meta region does not found"));
        } else {
          future.complete(Collections.singletonList(metaRegion));
        }
      });
      return future;
    } else {
      // For non-meta table, we fetch all locations by scanning hbase:meta table
      return AsyncMetaTableAccessor.getTableHRegionLocations(metaTable, Optional.of(tableName));
    }
  }

  /**
   * Compact column family of a table, Asynchronous operation even if CompletableFuture.get()
   */
  private CompletableFuture<Void> compact(final TableName tableName, Optional<byte[]> columnFamily,
      final boolean major) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    getTableHRegionLocations(tableName).whenComplete((locations, err) -> {
      if (err != null) {
        future.completeExceptionally(err);
        return;
      }
      List<CompletableFuture<Void>> compactFutures = new ArrayList<>();
      for (HRegionLocation location : locations) {
        if (location.getRegionInfo() == null || location.getRegionInfo().isOffline()) continue;
        if (location.getServerName() == null) continue;
        compactFutures
            .add(compact(location.getServerName(), location.getRegionInfo(), major, columnFamily));
      }
      // future complete unless all of the compact futures are completed.
      CompletableFuture
          .allOf(compactFutures.toArray(new CompletableFuture<?>[compactFutures.size()]))
          .whenComplete((ret, err2) -> {
            if (err2 != null) {
              future.completeExceptionally(err2);
            } else {
              future.complete(ret);
            }
          });
    });
    return future;
  }

  /**
   * Compact the region at specific region server.
   */
  private CompletableFuture<Void> compact(final ServerName sn, final HRegionInfo hri,
      final boolean major, Optional<byte[]> columnFamily) {
    return this
        .<Void> newAdminCaller()
        .serverName(sn)
        .action(
          (controller, stub) -> this.<CompactRegionRequest, CompactRegionResponse, Void> adminCall(
            controller, stub, RequestConverter.buildCompactRegionRequest(hri.getRegionName(),
              major, columnFamily), (s, c, req, done) -> s.compactRegion(c, req, done),
            resp -> null)).call();
  }

  private byte[] toEncodeRegionName(byte[] regionName) {
    try {
      return HRegionInfo.isEncodedRegionName(regionName) ? regionName
          : Bytes.toBytes(HRegionInfo.encodeRegionName(regionName));
    } catch (IOException e) {
      return regionName;
    }
  }

  private void checkAndGetTableName(byte[] encodeRegionName, AtomicReference<TableName> tableName,
      CompletableFuture<TableName> result) {
    getRegionLocation(encodeRegionName).whenComplete(
      (location, err) -> {
        if (err != null) {
          result.completeExceptionally(err);
          return;
        }
        HRegionInfo regionInfo = location.getRegionInfo();
        if (!tableName.compareAndSet(null, regionInfo.getTable())) {
          if (!tableName.get().equals(regionInfo.getTable())) {
            // tables of this two region should be same.
            result.completeExceptionally(new IllegalArgumentException(
                "Cannot merge regions from two different tables " + tableName.get() + " and "
                    + regionInfo.getTable()));
          } else {
            result.complete(tableName.get());
          }
        }
      });
  }

  private CompletableFuture<TableName> checkRegionsAndGetTableName(byte[] encodeRegionNameA,
      byte[] encodeRegionNameB) {
    AtomicReference<TableName> tableNameRef = new AtomicReference<>();
    CompletableFuture<TableName> future = new CompletableFuture<>();

    checkAndGetTableName(encodeRegionNameA, tableNameRef, future);
    checkAndGetTableName(encodeRegionNameB, tableNameRef, future);
    return future;
  }

  @Override
  public CompletableFuture<Void> mergeRegions(byte[] nameOfRegionA, byte[] nameOfRegionB,
      boolean forcible) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    final byte[] encodeRegionNameA = toEncodeRegionName(nameOfRegionA);
    final byte[] encodeRegionNameB = toEncodeRegionName(nameOfRegionB);

    checkRegionsAndGetTableName(encodeRegionNameA, encodeRegionNameB).whenComplete(
      (tableName, err) -> {
        if (err != null) {
          future.completeExceptionally(err);
          return;
        }
        this.<Void> newMasterCaller()
            .action(
              (controller, stub) -> this
                  .<DispatchMergingRegionsRequest, DispatchMergingRegionsResponse, Void> call(
                    controller, stub, RequestConverter.buildDispatchMergingRegionsRequest(
                      encodeRegionNameA, encodeRegionNameB, forcible), (s, c, req, done) -> s
                        .dispatchMergingRegions(c, req, done), (resp) -> null)).call()
            .whenComplete((ret, err2) -> {
              if (err2 != null) {
                future.completeExceptionally(err2);
              } else {
                future.complete(ret);
              }
            });
      });
    return future;
  }

  @Override
  public CompletableFuture<Void> split(TableName tableName) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    tableExists(tableName).whenComplete(
      (exist, error) -> {
        if (error != null) {
          future.completeExceptionally(error);
          return;
        }
        if (!exist) {
          future.completeExceptionally(new TableNotFoundException(tableName));
          return;
        }
        AsyncMetaTableAccessor.getTableHRegionLocations(metaTable, Optional.of(tableName))
            .whenComplete(
              (locations, err2) -> {
                if (err2 != null) {
                  future.completeExceptionally(err2);
                  return;
                }
                if (locations.isEmpty()) {
                  future.complete(null);
                  return;
                }
                List<CompletableFuture<Void>> splitFutures = new ArrayList<>();
                locations.stream().forEach(
                  loc -> {
                    splitFutures.add(split(loc.getServerName(), loc.getRegionInfo(),
                      Optional.empty()));
                  });
                CompletableFuture.allOf(
                  splitFutures.toArray(new CompletableFuture<?>[splitFutures.size()]))
                    .whenComplete((ret, exception) -> {
                      if (exception != null) {
                        future.completeExceptionally(exception);
                        return;
                      }
                      future.complete(ret);
                    });
              });
      });
    return future;
  }

  @Override
  public CompletableFuture<Void> split(TableName tableName, byte[] splitPoint) {
    CompletableFuture<Void> result = new CompletableFuture<>();
    if (splitPoint == null) {
      return failedFuture(new IllegalArgumentException("splitPoint can not be null."));
    }
    connection.getRegionLocator(tableName).getRegionLocation(splitPoint)
        .whenComplete((loc, err) -> {
          if (err != null) {
            result.completeExceptionally(err);
          } else if (loc == null || loc.getRegionInfo() == null) {
            result.completeExceptionally(new IllegalArgumentException(
                "Region does not found: rowKey=" + Bytes.toStringBinary(splitPoint)));
          } else {
            splitRegion(loc.getRegionInfo().getRegionName(), Optional.of(splitPoint))
                .whenComplete((ret, err2) -> {
                  if (err2 != null) {
                    result.completeExceptionally(err2);
                  } else {
                    result.complete(ret);
                  }

                });
          }
        });
    return result;
  }

  @Override
  public CompletableFuture<Void> splitRegion(byte[] regionName, Optional<byte[]> splitPoint) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    getRegionLocation(regionName).whenComplete(
      (location, err) -> {
        HRegionInfo regionInfo = location.getRegionInfo();
        ServerName serverName = location.getServerName();
        if (serverName == null) {
          future.completeExceptionally(new NoServerForRegionException(Bytes
              .toStringBinary(regionName)));
          return;
        }
        split(serverName, regionInfo, splitPoint).whenComplete((ret, err2) -> {
          if (err2 != null) {
            future.completeExceptionally(err2);
          } else {
            future.complete(ret);
          }
        });
      });
    return future;
  }

  private CompletableFuture<Void> split(final ServerName serverName, final HRegionInfo hri,
      Optional<byte[]> splitPoint) {
    if (hri.getStartKey() != null && splitPoint.isPresent()
        && Bytes.compareTo(hri.getStartKey(), splitPoint.get()) == 0) {
      return failedFuture(new IllegalArgumentException(
          "should not give a splitkey which equals to startkey!"));
    }
    TableName tableName = hri.getTable();
    return this
        .<Void> newAdminCaller()
        .action(
          (controller, stub) -> this.<SplitRegionRequest, SplitRegionResponse, Void> adminCall(
            controller, stub, RequestConverter.buildSplitRegionRequest(hri.getRegionName(),
              splitPoint.isPresent() ? splitPoint.get() : null), (s, c, req, done) -> s
                .splitRegion(c, req, done), resp -> null)).serverName(serverName).call();
  }

  @Override
  public CompletableFuture<Void> assign(byte[] regionName) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    getRegionInfo(regionName).whenComplete(
      (regionInfo, err) -> {
        if (err != null) {
          future.completeExceptionally(err);
          return;
        }
        this.<Void> newMasterCaller()
            .action(
              ((controller, stub) -> this.<AssignRegionRequest, AssignRegionResponse, Void> call(
                controller, stub, RequestConverter.buildAssignRegionRequest(regionInfo
                    .getRegionName()), (s, c, req, done) -> s.assignRegion(c, req, done),
                resp -> null))).call().whenComplete((ret, err2) -> {
              if (err2 != null) {
                future.completeExceptionally(err2);
              } else {
                future.complete(ret);
              }
            });
      });
    return future;
  }

  @Override
  public CompletableFuture<Void> unassign(byte[] regionName, boolean forcible) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    getRegionInfo(regionName).whenComplete(
      (regionInfo, err) -> {
        if (err != null) {
          future.completeExceptionally(err);
          return;
        }
        this.<Void> newMasterCaller()
            .action(
              ((controller, stub) -> this
                  .<UnassignRegionRequest, UnassignRegionResponse, Void> call(controller, stub,
                    RequestConverter.buildUnassignRegionRequest(regionInfo.getRegionName(), forcible),
                    (s, c, req, done) -> s.unassignRegion(c, req, done), resp -> null))).call()
            .whenComplete((ret, err2) -> {
              if (err2 != null) {
                future.completeExceptionally(err2);
              } else {
                future.complete(ret);
              }
            });
      });
    return future;
  }

  @Override
  public CompletableFuture<Void> offline(byte[] regionName) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    getRegionInfo(regionName).whenComplete(
      (regionInfo, err) -> {
        if (err != null) {
          future.completeExceptionally(err);
          return;
        }
        this.<Void> newMasterCaller()
            .action(
              ((controller, stub) -> this.<OfflineRegionRequest, OfflineRegionResponse, Void> call(
                controller, stub, RequestConverter.buildOfflineRegionRequest(regionInfo
                    .getRegionName()), (s, c, req, done) -> s.offlineRegion(c, req, done),
                resp -> null))).call().whenComplete((ret, err2) -> {
              if (err2 != null) {
                future.completeExceptionally(err2);
              } else {
                future.complete(ret);
              }
            });
      });
    return future;
  }

  @Override
  public CompletableFuture<Void> move(byte[] regionName, Optional<ServerName> destServerName) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    getRegionInfo(regionName).whenComplete(
      (regionInfo, err) -> {
        if (err != null) {
          future.completeExceptionally(err);
          return;
        }
        this.<Void> newMasterCaller()
            .action(
              (controller, stub) -> this.<MoveRegionRequest, MoveRegionResponse, Void> call(
                controller, stub, RequestConverter.buildMoveRegionRequest(
                  regionInfo.getEncodedNameAsBytes(), destServerName), (s, c, req, done) -> s
                    .moveRegion(c, req, done), resp -> null)).call().whenComplete((ret, err2) -> {
              if (err2 != null) {
                future.completeExceptionally(err2);
              } else {
                future.complete(ret);
              }
            });
      });
    return future;
  }

  @Override
  public CompletableFuture<Void> setQuota(QuotaSettings quota) {
    return this
        .<Void> newMasterCaller()
        .action(
          (controller, stub) -> this.<SetQuotaRequest, SetQuotaResponse, Void> call(controller,
            stub, QuotaSettings.buildSetQuotaRequestProto(quota),
            (s, c, req, done) -> s.setQuota(c, req, done), (resp) -> null)).call();
  }

  @Override
  public CompletableFuture<List<QuotaSettings>> getQuota(QuotaFilter filter) {
    CompletableFuture<List<QuotaSettings>> future = new CompletableFuture<>();
    Scan scan = QuotaTableUtil.makeScan(filter);
    this.connection.getRawTableBuilder(QuotaTableUtil.QUOTA_TABLE_NAME).build()
        .scan(scan, new RawScanResultConsumer() {
          List<QuotaSettings> settings = new ArrayList<>();

          @Override
          public void onNext(Result[] results, ScanController controller) {
            for (Result result : results) {
              try {
                QuotaTableUtil.parseResultToCollection(result, settings);                                                                                                                                    
              } catch (IOException e) { 
                controller.terminate();
                future.completeExceptionally(e);
              }    
            }    
          }    

          @Override
          public void onError(Throwable error) {
            future.completeExceptionally(error);
          }    

          @Override
          public void onComplete() {
            future.complete(settings);
          }    
        });  
    return future;
  }

  @Override
  public CompletableFuture<Void> snapshot(SnapshotDescription snapshotDesc) {
    SnapshotProtos.SnapshotDescription snapshot = ProtobufUtil
        .createHBaseProtosSnapshotDesc(snapshotDesc);
    try {
      ClientSnapshotDescriptionUtils.assertSnapshotRequestIsValid(snapshot);
    } catch (IllegalArgumentException e) {
      return failedFuture(e);
    }
    CompletableFuture<Void> future = new CompletableFuture<>();
    final SnapshotRequest request = SnapshotRequest.newBuilder().setSnapshot(snapshot).build();
    this.<Long> newMasterCaller()
        .action(
          (controller, stub) -> this.<SnapshotRequest, SnapshotResponse, Long> call(controller,
            stub, request, (s, c, req, done) -> s.snapshot(c, req, done),
            resp -> resp.getExpectedTimeout())).call().whenComplete((expectedTimeout, err) -> {
          if (err != null) {
            future.completeExceptionally(err);
            return;
          }
          TimerTask pollingTask = new TimerTask() {
            int tries = 0;
            long startTime = EnvironmentEdgeManager.currentTimeMillis();
            long endTime = startTime + expectedTimeout;
            long maxPauseTime = expectedTimeout / maxAttempts;

            @Override
            public void run(Timeout timeout) throws Exception {
              if (EnvironmentEdgeManager.currentTimeMillis() < endTime) {
                isSnapshotFinished(snapshotDesc).whenComplete((done, err2) -> {
                  if (err2 != null) {
                    future.completeExceptionally(err2);
                  } else if (done) {
                    future.complete(null);
                  } else {
                    // retry again after pauseTime.
                  long pauseTime = ConnectionUtils.getPauseTime(
                    TimeUnit.NANOSECONDS.toMillis(pauseNs), ++tries);
                  pauseTime = Math.min(pauseTime, maxPauseTime);
                  AsyncConnectionImpl.RETRY_TIMER
                      .newTimeout(this, pauseTime, TimeUnit.MILLISECONDS);
                }
              } );
              } else {
                future.completeExceptionally(new SnapshotCreationException("Snapshot '"
                    + snapshot.getName() + "' wasn't completed in expectedTime:" + expectedTimeout
                    + " ms", snapshotDesc));
              }
            }
          };
          AsyncConnectionImpl.RETRY_TIMER.newTimeout(pollingTask, 1, TimeUnit.MILLISECONDS);
        });
    return future;
  }

  @Override
  public CompletableFuture<Boolean> isSnapshotFinished(SnapshotDescription snapshot) {
    return this
        .<Boolean> newMasterCaller()
        .action(
          (controller, stub) -> this.<IsSnapshotDoneRequest, IsSnapshotDoneResponse, Boolean> call(
            controller,
            stub,
            IsSnapshotDoneRequest.newBuilder()
                .setSnapshot(ProtobufUtil.createHBaseProtosSnapshotDesc(snapshot)).build(), (s, c,
                req, done) -> s.isSnapshotDone(c, req, done), resp -> resp.getDone())).call();
  }

  @Override
  public CompletableFuture<Void> restoreSnapshot(String snapshotName) {
    boolean takeFailSafeSnapshot = this.connection.getConfiguration().getBoolean(
      HConstants.SNAPSHOT_RESTORE_TAKE_FAILSAFE_SNAPSHOT,
      HConstants.DEFAULT_SNAPSHOT_RESTORE_TAKE_FAILSAFE_SNAPSHOT);
    return restoreSnapshot(snapshotName, takeFailSafeSnapshot);
  }

  @Override
  public CompletableFuture<Void> restoreSnapshot(String snapshotName, boolean takeFailSafeSnapshot) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    listSnapshots(Optional.of(Pattern.compile(snapshotName))).whenComplete(
      (snapshotDescriptions, err) -> {
        if (err != null) {
          future.completeExceptionally(err);
          return;
        }
        TableName tableName = null;
        if (snapshotDescriptions != null && !snapshotDescriptions.isEmpty()) {
          for (SnapshotDescription snap : snapshotDescriptions) {
            if (snap.getName().equals(snapshotName)) {
              tableName = snap.getTableName();
              break;
            }
          }
        }
        if (tableName == null) {
          future.completeExceptionally(new RestoreSnapshotException(
              "Unable to find the table name for snapshot=" + snapshotName));
          return;
        }
        final TableName finalTableName = tableName;
        tableExists(finalTableName)
            .whenComplete((exists, err2) -> {
              if (err2 != null) {
                future.completeExceptionally(err2);
              } else if (!exists) {
                // if table does not exist, then just clone snapshot into new table.
              completeConditionalOnFuture(future,
                internalRestoreSnapshot(snapshotName, finalTableName));
            } else {
              isTableDisabled(finalTableName).whenComplete(
                (disabled, err4) -> {
                  if (err4 != null) {
                    future.completeExceptionally(err4);
                  } else if (!disabled) {
                    future.completeExceptionally(new TableNotDisabledException(finalTableName));
                  } else {
                    completeConditionalOnFuture(future,
                      restoreSnapshot(snapshotName, finalTableName, takeFailSafeSnapshot));
                  }
                });
            }
          } );
      });
    return future;
  }

  private CompletableFuture<Void> restoreSnapshot(String snapshotName, TableName tableName,
      boolean takeFailSafeSnapshot) {
    if (takeFailSafeSnapshot) {
      CompletableFuture<Void> future = new CompletableFuture<>();
      // Step.1 Take a snapshot of the current state
      String failSafeSnapshotSnapshotNameFormat = this.connection.getConfiguration().get(
        HConstants.SNAPSHOT_RESTORE_FAILSAFE_NAME,
        HConstants.DEFAULT_SNAPSHOT_RESTORE_FAILSAFE_NAME);
      final String failSafeSnapshotSnapshotName = failSafeSnapshotSnapshotNameFormat
          .replace("{snapshot.name}", snapshotName)
          .replace("{table.name}", tableName.toString().replace(TableName.NAMESPACE_DELIM, '.'))
          .replace("{restore.timestamp}", String.valueOf(EnvironmentEdgeManager.currentTimeMillis()));
      LOG.info("Taking restore-failsafe snapshot: " + failSafeSnapshotSnapshotName);
      snapshot(failSafeSnapshotSnapshotName, tableName).whenComplete((ret, err) -> {
        if (err != null) {
          future.completeExceptionally(err);
        } else {
          // Step.2 Restore snapshot
        internalRestoreSnapshot(snapshotName, tableName).whenComplete((void2, err2) -> {
          if (err2 != null) {
            // Step.3.a Something went wrong during the restore and try to rollback.
          internalRestoreSnapshot(failSafeSnapshotSnapshotName, tableName).whenComplete(
            (void3, err3) -> {
              if (err3 != null) {
                future.completeExceptionally(err3);
              } else {
                String msg = "Restore snapshot=" + snapshotName + " failed. Rollback to snapshot="
                    + failSafeSnapshotSnapshotName + " succeeded.";
                future.completeExceptionally(new RestoreSnapshotException(msg));
              }
            });
        } else {
          // Step.3.b If the restore is succeeded, delete the pre-restore snapshot.
          LOG.info("Deleting restore-failsafe snapshot: " + failSafeSnapshotSnapshotName);
          deleteSnapshot(failSafeSnapshotSnapshotName).whenComplete(
            (ret3, err3) -> {
              if (err3 != null) {
                LOG.error(
                  "Unable to remove the failsafe snapshot: " + failSafeSnapshotSnapshotName, err3);
                future.completeExceptionally(err3);
              } else {
                future.complete(ret3);
              }
            });
        }
      } );
      }
    } );
      return future;
    } else {
      return internalRestoreSnapshot(snapshotName, tableName);
    }
  }

  private <T> void completeConditionalOnFuture(CompletableFuture<T> dependentFuture,
      CompletableFuture<T> parentFuture) {
    parentFuture.whenComplete((res, err) -> {
      if (err != null) {
        dependentFuture.completeExceptionally(err);
      } else {
        dependentFuture.complete(res);
      }
    });
  }

  @Override
  public CompletableFuture<Void> cloneSnapshot(String snapshotName, TableName tableName) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    tableExists(tableName).whenComplete((exists, err) -> {
      if (err != null) {
        future.completeExceptionally(err);
      } else if (exists) {
        future.completeExceptionally(new TableExistsException(tableName));
      } else {
        completeConditionalOnFuture(future, internalRestoreSnapshot(snapshotName, tableName));
      }
    });
    return future;
  }

  private CompletableFuture<Void> internalRestoreSnapshot(String snapshotName, TableName tableName) {
    SnapshotProtos.SnapshotDescription snapshot = SnapshotProtos.SnapshotDescription.newBuilder()
        .setName(snapshotName).setTable(tableName.getNameAsString()).build();
    try {
      ClientSnapshotDescriptionUtils.assertSnapshotRequestIsValid(snapshot);
    } catch (IllegalArgumentException e) {
      return failedFuture(e);
    }
    return this.<Void> newMasterCaller()
        .action(
          (controller, stub) -> this.<RestoreSnapshotRequest, RestoreSnapshotResponse, Void> call(
            controller, stub, RestoreSnapshotRequest.newBuilder().setSnapshot(snapshot).build(), (
                s, c, req, done) -> s.restoreSnapshot(c, req, done), resp -> null)).call();
  }

  @Override
  public CompletableFuture<List<SnapshotDescription>> listSnapshots(Optional<Pattern> pattern) {
    CompletableFuture<List<SnapshotDescription>> future = new CompletableFuture<>();
    this.<GetCompletedSnapshotsResponse> newMasterCaller()
        .action(
          (controller, stub) -> this
              .<GetCompletedSnapshotsRequest, GetCompletedSnapshotsResponse, GetCompletedSnapshotsResponse> call(
                controller, stub, GetCompletedSnapshotsRequest.newBuilder().build(), (s, c, req,
                    done) -> s.getCompletedSnapshots(c, req, done), resp -> resp))
        .call()
        .whenComplete(
          (resp, err) -> {
            if (err != null) {
              future.completeExceptionally(err);
              return;
            }
            future.complete(resp
                .getSnapshotsList()
                .stream()
                .map(ProtobufUtil::createSnapshotDesc)
                .filter(
                  snap -> pattern.isPresent() ? pattern.get().matcher(snap.getName()).matches()
                      : true).collect(Collectors.toList()));
          });
    return future;
  }

  @Override
  public CompletableFuture<List<SnapshotDescription>> listTableSnapshots(Pattern tableNamePattern,
      Pattern snapshotNamePattern) {
    CompletableFuture<List<SnapshotDescription>> future = new CompletableFuture<>();
    listTables(Optional.ofNullable(tableNamePattern)).whenComplete(
      (tables, err) -> {
        if (err != null) {
          future.completeExceptionally(err);
          return;
        }
        if (tables == null || tables.size() <= 0) {
          future.complete(Collections.emptyList());
          return;
        }
        List<TableName> tableNames = tables.stream().map(t -> t.getTableName())
            .collect(Collectors.toList());
        listSnapshots(Optional.ofNullable(snapshotNamePattern)).whenComplete(
          (snapshotDescList, err2) -> {
            if (err2 != null) {
              future.completeExceptionally(err2);
              return;
            }
            if (snapshotDescList == null || snapshotDescList.isEmpty()) {
              future.complete(Collections.emptyList());
              return;
            }
            future.complete(snapshotDescList.stream()
                .filter(snap -> (snap != null && tableNames.contains(snap.getTableName())))
                .collect(Collectors.toList()));
          });
      });
    return future;
  }

  @Override
  public CompletableFuture<Void> deleteSnapshot(String snapshotName) {
    return internalDeleteSnapshot(new SnapshotDescription(snapshotName));
  }

  @Override
  public CompletableFuture<Void> deleteSnapshots(Pattern snapshotNamePattern) {
    return deleteTableSnapshots(null, snapshotNamePattern);
  }

  @Override
  public CompletableFuture<Void> deleteTableSnapshots(Pattern tableNamePattern,
      Pattern snapshotNamePattern) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    listTableSnapshots(tableNamePattern, snapshotNamePattern).whenComplete(
      ((snapshotDescriptions, err) -> {
        if (err != null) {
          future.completeExceptionally(err);
          return;
        }
        if (snapshotDescriptions == null || snapshotDescriptions.isEmpty()) {
          future.complete(null);
          return;
        }
        List<CompletableFuture<Void>> deleteSnapshotFutures = new ArrayList<>();
        snapshotDescriptions.forEach(snapDesc -> deleteSnapshotFutures
            .add(internalDeleteSnapshot(snapDesc)));
        CompletableFuture.allOf(
          deleteSnapshotFutures.toArray(new CompletableFuture<?>[deleteSnapshotFutures.size()]))
            .thenAccept(v -> future.complete(v));
      }));
    return future;
  }

  private CompletableFuture<Void> internalDeleteSnapshot(SnapshotDescription snapshot) {
    return this
        .<Void> newMasterCaller()
        .action(
          (controller, stub) -> this.<DeleteSnapshotRequest, DeleteSnapshotResponse, Void> call(
            controller,
            stub,
            DeleteSnapshotRequest.newBuilder()
                .setSnapshot(ProtobufUtil.createHBaseProtosSnapshotDesc(snapshot)).build(), (s, c,
                req, done) -> s.deleteSnapshot(c, req, done), resp -> null)).call();
  }

  /**
   * Get the region location for the passed region name. The region name may be a full region name
   * or encoded region name. If the region does not found, then it'll throw an
   * UnknownRegionException wrapped by a {@link CompletableFuture}
   * @param regionNameOrEncodedRegionName
   * @return region location, wrapped by a {@link CompletableFuture}
   */
  @VisibleForTesting
  CompletableFuture<HRegionLocation> getRegionLocation(byte[] regionNameOrEncodedRegionName) {
    if (regionNameOrEncodedRegionName == null) {
      return failedFuture(new IllegalArgumentException("Passed region name can't be null"));
    }
    try {
      CompletableFuture<Optional<HRegionLocation>> future;
      if (HRegionInfo.isEncodedRegionName(regionNameOrEncodedRegionName)) {
        future = AsyncMetaTableAccessor.getRegionLocationWithEncodedName(metaTable,
          regionNameOrEncodedRegionName);
      } else {
        future = AsyncMetaTableAccessor.getRegionLocation(metaTable, regionNameOrEncodedRegionName);
      }

      CompletableFuture<HRegionLocation> returnedFuture = new CompletableFuture<>();
      future.whenComplete((location, err) -> {
        if (err != null) {
          returnedFuture.completeExceptionally(err);
          return;
        }
        LOG.info("location is " + location);
        if (!location.isPresent() || location.get().getRegionInfo() == null) {
          LOG.info("unknown location is " + location);
          returnedFuture.completeExceptionally(new UnknownRegionException(
              "Invalid region name or encoded region name: "
                  + Bytes.toStringBinary(regionNameOrEncodedRegionName)));
        } else {
          returnedFuture.complete(location.get());
        }
      });
      return returnedFuture;
    } catch (IOException e) {
      return failedFuture(e);
    }
  }

  /**
   * Get the region info for the passed region name. The region name may be a full region name or
   * encoded region name. If the region does not found, then it'll throw an UnknownRegionException
   * wrapped by a {@link CompletableFuture}
   * @param regionNameOrEncodedRegionName
   * @return region info, wrapped by a {@link CompletableFuture}
   */
  private CompletableFuture<HRegionInfo> getRegionInfo(byte[] regionNameOrEncodedRegionName) {
    if (regionNameOrEncodedRegionName == null) {
      return failedFuture(new IllegalArgumentException("Passed region name can't be null"));
    }

    if (Bytes.equals(regionNameOrEncodedRegionName,
      HRegionInfo.FIRST_META_REGIONINFO.getRegionName())
        || Bytes.equals(regionNameOrEncodedRegionName,
          HRegionInfo.FIRST_META_REGIONINFO.getEncodedNameAsBytes())) {
      return CompletableFuture.completedFuture(HRegionInfo.FIRST_META_REGIONINFO);
    }

    CompletableFuture<HRegionInfo> future = new CompletableFuture<>();
    getRegionLocation(regionNameOrEncodedRegionName).whenComplete((location, err) -> {
      if (err != null) {
        future.completeExceptionally(err);
      } else {
        future.complete(location.getRegionInfo());
      }
    });
    return future;
  }

  private byte[][] getSplitKeys(byte[] startKey, byte[] endKey, int numRegions) {
    if (numRegions < 3) {
      throw new IllegalArgumentException("Must create at least three regions");
    } else if (Bytes.compareTo(startKey, endKey) >= 0) {
      throw new IllegalArgumentException("Start key must be smaller than end key");
    }
    if (numRegions == 3) {
      return new byte[][] { startKey, endKey };
    }
    byte[][] splitKeys = Bytes.split(startKey, endKey, numRegions - 3);
    if (splitKeys == null || splitKeys.length != numRegions - 1) {
      throw new IllegalArgumentException("Unable to split key range into enough regions");
    }
    return splitKeys;
  }

  private void verifySplitKeys(byte[][] splitKeys) {
    Arrays.sort(splitKeys, Bytes.BYTES_COMPARATOR);
    // Verify there are no duplicate split keys
    byte[] lastKey = null;
    for (byte[] splitKey : splitKeys) {
      if (Bytes.compareTo(splitKey, HConstants.EMPTY_BYTE_ARRAY) == 0) {
        throw new IllegalArgumentException("Empty split key must not be passed in the split keys.");
      }
      if (lastKey != null && Bytes.equals(splitKey, lastKey)) {
        throw new IllegalArgumentException("All split keys must be unique, " + "found duplicate: "
            + Bytes.toStringBinary(splitKey) + ", " + Bytes.toStringBinary(lastKey));
      }
      lastKey = splitKey;
    }
  }

  private <T> CompletableFuture<T> failedFuture(Throwable error) {
    CompletableFuture<T> future = new CompletableFuture<>();
    future.completeExceptionally(error);
    return future;
  }

  private <T> boolean completeExceptionally(CompletableFuture<T> future, Throwable error) {
    if (error != null) {
      future.completeExceptionally(error);
      return true;
    }
    return false;
  }

  @Override
  public CompletableFuture<ClusterStatus> getClusterStatus() {
    return this
        .<ClusterStatus> newMasterCaller()
        .action(
          (controller, stub) -> this
              .<GetClusterStatusRequest, GetClusterStatusResponse, ClusterStatus> call(controller,
                stub, RequestConverter.buildGetClusterStatusRequest(),
                (s, c, req, done) -> s.getClusterStatus(c, req, done),
                resp ->  ClusterStatus.convert(resp.getClusterStatus()))).call();
  }

  @Override
  public CompletableFuture<Void> shutdown() {
    return this
        .<Void> newMasterCaller()
        .action(
          (controller, stub) -> this.<ShutdownRequest, ShutdownResponse, Void> call(controller,
            stub, ShutdownRequest.newBuilder().build(),
            (s, c, req, done) -> s.shutdown(c, req, done), resp -> null)).call();
  }

  @Override
  public CompletableFuture<Void> stopMaster() {
    return this
        .<Void> newMasterCaller()
        .action(
          (controller, stub) -> this.<StopMasterRequest, StopMasterResponse, Void> call(controller,
            stub, StopMasterRequest.newBuilder().build(),
            (s, c, req, done) -> s.stopMaster(c, req, done), resp -> null)).call();
  }

  @Override
  public CompletableFuture<Void> stopRegionServer(ServerName serverName) {
    StopServerRequest request =
        RequestConverter.buildStopServerRequest("Called by admin client "
            + this.connection.toString());
    return this
        .<Void> newAdminCaller()
        .action(
          (controller, stub) -> this.<StopServerRequest, StopServerResponse, Void> adminCall(
            controller, stub, request, (s, c, req, done) -> s.stopServer(controller, req, done),
            resp -> null)).serverName(serverName).call();
  }

  @Override
  public CompletableFuture<Void> rollWALWriter(ServerName serverName) {
    return this
        .<Void> newAdminCaller()
        .action(
          (controller, stub) -> this.<RollWALWriterRequest, RollWALWriterResponse, Void> adminCall(
            controller, stub, RequestConverter.buildRollWALWriterRequest(),
            (s, c, req, done) -> s.rollWALWriter(controller, req, done), resp -> null))
        .serverName(serverName).call();
  }

  @Override
  public CompletableFuture<CompactionState> getCompactionState(TableName tableName) {
    CompletableFuture<CompactionState> future = new CompletableFuture<>();
    getTableHRegionLocations(tableName).whenComplete(
      (locations, err) -> {
        if (err != null) {
          future.completeExceptionally(err);
          return;
        }
        List<CompactionState> regionStates = new ArrayList<>();
        List<CompletableFuture<CompactionState>> futures = new ArrayList<>();
        locations.stream().filter(loc -> loc.getServerName() != null)
            .filter(loc -> loc.getRegionInfo() != null)
            .filter(loc -> !loc.getRegionInfo().isOffline())
            .map(loc -> loc.getRegionInfo().getRegionName()).forEach(region -> {
              futures.add(getCompactionStateForRegion(region).whenComplete((regionState, err2) -> {
                // If any region compaction state is MAJOR_AND_MINOR
                // the table compaction state is MAJOR_AND_MINOR, too.
                if (err2 != null) {
                  future.completeExceptionally(err2);
                } else if (regionState == CompactionState.MAJOR_AND_MINOR) {

                  future.complete(regionState);
                } else {
                  regionStates.add(regionState);
                }
              }));
            });
        CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[futures.size()]))
            .whenComplete((ret, err3) -> {
              // If future not completed, check all regions's compaction state
              if (!future.isCompletedExceptionally() && !future.isDone()) {
                CompactionState state = CompactionState.NONE;
                for (CompactionState regionState : regionStates) {
                  switch (regionState) {
                  case MAJOR:
                    if (state == CompactionState.MINOR) {
                      future.complete(CompactionState.MAJOR_AND_MINOR);
                    } else {
                      state = CompactionState.MAJOR;
                    }
                    break;
                  case MINOR:
                    if (state == CompactionState.MAJOR) {
                      future.complete(CompactionState.MAJOR_AND_MINOR);
                    } else {
                      state = CompactionState.MINOR;
                    }
                    break;
                  case NONE:
                  default:
                  }
                  if (!future.isDone()) {
                    future.complete(state);
                  }
                }
              }
            });
      });
    return future;
  }

  @Override
  public CompletableFuture<CompactionState> getCompactionStateForRegion(byte[] regionName) {
    CompletableFuture<CompactionState> future = new CompletableFuture<>();
    getRegionLocation(regionName).whenComplete(
      (location, err) -> {
        if (err != null) {
          future.completeExceptionally(err);
          return;
        }
        ServerName serverName = location.getServerName();
        if (serverName == null) {
          future.completeExceptionally(new NoServerForRegionException(Bytes
              .toStringBinary(regionName)));
          return;
        }
        this.<GetRegionInfoResponse> newAdminCaller()
            .action(
              (controller, stub) -> this
                  .<GetRegionInfoRequest, GetRegionInfoResponse, GetRegionInfoResponse> adminCall(
                    controller, stub, RequestConverter.buildGetRegionInfoRequest(location
                        .getRegionInfo().getRegionName(), true), (s, c, req, done) -> s
                        .getRegionInfo(controller, req, done), resp -> resp))
            .serverName(serverName).call().whenComplete((resp2, err2) -> {
              if (err2 != null) {
                future.completeExceptionally(err2);
              } else {
                if (resp2.hasCompactionState()) {
                  future.complete(resp2.getCompactionState());
                } else {
                  future.complete(CompactionState.NONE);
                }
              }
            });
      });
    return future;
  }

  @Override
  public CompletableFuture<Boolean> setBalancerOn(final boolean on) {
    return this
        .<Boolean> newMasterCaller()
        .action(
          (controller, stub) -> this
              .<SetBalancerRunningRequest, SetBalancerRunningResponse, Boolean> call(controller,
                stub, RequestConverter.buildSetBalancerRunningRequest(on, true),
                (s, c, req, done) -> s.setBalancerRunning(c, req, done),
                (resp) -> resp.getPrevBalanceValue())).call();
  }

  @Override
  public CompletableFuture<Boolean> balance() {
    return this
        .<Boolean> newMasterCaller()
        .action(
          (controller, stub) -> this.<BalanceRequest, BalanceResponse, Boolean> call(controller,
            stub, RequestConverter.buildBalanceRequest(),
            (s, c, req, done) -> s.balance(c, req, done), (resp) -> resp.getBalancerRan())).call();
  }

  @Override
  public CompletableFuture<Boolean> setCatalogJanitorOn(boolean enabled) {
    return this
        .<Boolean> newMasterCaller()
        .action(
          (controller, stub) -> this
              .<EnableCatalogJanitorRequest, EnableCatalogJanitorResponse, Boolean> call(
                controller, stub, RequestConverter.buildEnableCatalogJanitorRequest(enabled), (s,
                    c, req, done) -> s.enableCatalogJanitor(c, req, done), (resp) -> resp
                    .getPrevValue())).call();
  }

  @Override
  public CompletableFuture<Boolean> isCatalogJanitorOn() {
    return this
        .<Boolean> newMasterCaller()
        .action(
          (controller, stub) -> this
              .<IsCatalogJanitorEnabledRequest, IsCatalogJanitorEnabledResponse, Boolean> call(
                controller, stub, RequestConverter.buildIsCatalogJanitorEnabledRequest(), (s, c,
                    req, done) -> s.isCatalogJanitorEnabled(c, req, done), (resp) -> resp
                    .getValue())).call();
  }

  @Override
  public CompletableFuture<Integer> runCatalogJanitor() {
    return this
        .<Integer> newMasterCaller()
        .action(
          (controller, stub) -> this.<RunCatalogScanRequest, RunCatalogScanResponse, Integer> call(
            controller, stub, RequestConverter.buildCatalogScanRequest(),
            (s, c, req, done) -> s.runCatalogScan(c, req, done), (resp) -> resp.getScanResult()))
        .call();
  }

  private <T> ServerRequestCallerBuilder<T> newServerCaller() {
    return this.connection.callerFactory.<T> serverRequest()
        .rpcTimeout(rpcTimeoutNs, TimeUnit.NANOSECONDS)
        .operationTimeout(operationTimeoutNs, TimeUnit.NANOSECONDS)
        .pause(pauseNs, TimeUnit.NANOSECONDS).maxAttempts(maxAttempts)
        .startLogErrorsCnt(startLogErrorsCnt);
  }
}