/*
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
package org.apache.hadoop.hbase.master.procedure;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.hadoop.hbase.ConcurrentTableModificationException;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.fs.ErasureCodingUtils;
import org.apache.hadoop.hbase.master.MasterCoprocessorHost;
import org.apache.hadoop.hbase.master.zksyncer.MetaLocationSyncer;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.regionserver.storefiletracker.StoreFileTrackerValidationUtils;
import org.apache.hadoop.hbase.rsgroup.RSGroupInfo;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.ModifyTableState;

@InterfaceAudience.Private
public class ModifyTableProcedure extends AbstractStateMachineTableProcedure<ModifyTableState> {
  private static final Logger LOG = LoggerFactory.getLogger(ModifyTableProcedure.class);

  private TableDescriptor unmodifiedTableDescriptor = null;
  private TableDescriptor modifiedTableDescriptor;
  private boolean deleteColumnFamilyInModify;
  private boolean shouldCheckDescriptor;
  private boolean reopenRegions;
  /**
   * List of column families that cannot be deleted from the hbase:meta table. They are critical to
   * cluster operation. This is a bit of an odd place to keep this list but then this is the tooling
   * that does add/remove. Keeping it local!
   */
  private static final List<byte[]> UNDELETABLE_META_COLUMNFAMILIES =
    Collections.unmodifiableList(Arrays.asList(HConstants.CATALOG_FAMILY, HConstants.TABLE_FAMILY,
      HConstants.REPLICATION_BARRIER_FAMILY, HConstants.NAMESPACE_FAMILY));

  public ModifyTableProcedure() {
    super();
    initialize(null, false);
  }

  public ModifyTableProcedure(final MasterProcedureEnv env, final TableDescriptor htd)
    throws HBaseIOException {
    this(env, htd, null);
  }

  public ModifyTableProcedure(final MasterProcedureEnv env, final TableDescriptor htd,
    final ProcedurePrepareLatch latch) throws HBaseIOException {
    this(env, htd, latch, null, false, true);
  }

  public ModifyTableProcedure(final MasterProcedureEnv env,
    final TableDescriptor newTableDescriptor, final ProcedurePrepareLatch latch,
    final TableDescriptor oldTableDescriptor, final boolean shouldCheckDescriptor,
    final boolean reopenRegions) throws HBaseIOException {
    super(env, latch);
    this.reopenRegions = reopenRegions;
    initialize(oldTableDescriptor, shouldCheckDescriptor);
    this.modifiedTableDescriptor = newTableDescriptor;
    preflightChecks(env, null/* No table checks; if changing peers, table can be online */);
  }

  @Override
  protected void preflightChecks(MasterProcedureEnv env, Boolean enabled) throws HBaseIOException {
    super.preflightChecks(env, enabled);
    if (this.modifiedTableDescriptor.isMetaTable()) {
      // If we are modifying the hbase:meta table, make sure we are not deleting critical
      // column families else we'll damage the cluster.
      Set<byte[]> cfs = this.modifiedTableDescriptor.getColumnFamilyNames();
      for (byte[] family : UNDELETABLE_META_COLUMNFAMILIES) {
        if (!cfs.contains(family)) {
          throw new HBaseIOException(
            "Delete of hbase:meta column family " + Bytes.toString(family));
        }
      }
    }

    if (!reopenRegions) {
      if (this.unmodifiedTableDescriptor == null) {
        throw new HBaseIOException(
          "unmodifiedTableDescriptor cannot be null when this table modification won't reopen regions");
      }
      if (
        !this.unmodifiedTableDescriptor.getTableName()
          .equals(this.modifiedTableDescriptor.getTableName())
      ) {
        throw new HBaseIOException(
          "Cannot change the table name when this modification won't " + "reopen regions.");
      }
      if (
        this.unmodifiedTableDescriptor.getColumnFamilyCount()
            != this.modifiedTableDescriptor.getColumnFamilyCount()
      ) {
        throw new HBaseIOException(
          "Cannot add or remove column families when this modification " + "won't reopen regions.");
      }
      if (
        this.unmodifiedTableDescriptor.getCoprocessorDescriptors().hashCode()
            != this.modifiedTableDescriptor.getCoprocessorDescriptors().hashCode()
      ) {
        throw new HBaseIOException(
          "Can not modify Coprocessor when table modification won't reopen regions");
      }
      final Set<String> s = new HashSet<>(Arrays.asList(TableDescriptorBuilder.REGION_REPLICATION,
        TableDescriptorBuilder.REGION_MEMSTORE_REPLICATION, RSGroupInfo.TABLE_DESC_PROP_GROUP));
      for (String k : s) {
        if (
          isTablePropertyModified(this.unmodifiedTableDescriptor, this.modifiedTableDescriptor, k)
        ) {
          throw new HBaseIOException(
            "Can not modify " + k + " of a table when modification won't reopen regions");
        }
      }
    }
  }

  /**
   * Comparing the value associated with a given key across two TableDescriptor instances'
   * properties.
   * @return True if the table property <code>key</code> is the same in both.
   */
  private boolean isTablePropertyModified(TableDescriptor oldDescriptor,
    TableDescriptor newDescriptor, String key) {
    String oldV = oldDescriptor.getValue(key);
    String newV = newDescriptor.getValue(key);
    if (oldV == null && newV == null) {
      return false;
    } else if (oldV != null && newV != null && oldV.equals(newV)) {
      return false;
    }
    return true;
  }

  private void initialize(final TableDescriptor unmodifiedTableDescriptor,
    final boolean shouldCheckDescriptor) {
    this.unmodifiedTableDescriptor = unmodifiedTableDescriptor;
    this.shouldCheckDescriptor = shouldCheckDescriptor;
    this.deleteColumnFamilyInModify = false;
  }

  @Override
  protected Flow executeFromState(final MasterProcedureEnv env, final ModifyTableState state)
    throws InterruptedException {
    LOG.trace("{} execute state={}", this, state);
    try {
      switch (state) {
        case MODIFY_TABLE_PREPARE:
          prepareModify(env);
          setNextState(ModifyTableState.MODIFY_TABLE_PRE_OPERATION);
          break;
        case MODIFY_TABLE_PRE_OPERATION:
          preModify(env, state);
          // We cannot allow changes to region replicas when 'reopenRegions==false',
          // as this mode bypasses the state management required for modifying region replicas.
          if (reopenRegions) {
            setNextState(ModifyTableState.MODIFY_TABLE_CLOSE_EXCESS_REPLICAS);
          } else {
            setNextState(ModifyTableState.MODIFY_TABLE_UPDATE_TABLE_DESCRIPTOR);
          }
          break;
        case MODIFY_TABLE_CLOSE_EXCESS_REPLICAS:
          if (isTableEnabled(env)) {
            closeExcessReplicasIfNeeded(env);
          }
          setNextState(ModifyTableState.MODIFY_TABLE_UPDATE_TABLE_DESCRIPTOR);
          break;
        case MODIFY_TABLE_UPDATE_TABLE_DESCRIPTOR:
          updateTableDescriptor(env);
          if (reopenRegions) {
            setNextState(ModifyTableState.MODIFY_TABLE_REMOVE_REPLICA_COLUMN);
          } else {
            setNextState(ModifyTableState.MODIFY_TABLE_POST_OPERATION);
          }
          break;
        case MODIFY_TABLE_REMOVE_REPLICA_COLUMN:
          removeReplicaColumnsIfNeeded(env);
          setNextState(ModifyTableState.MODIFY_TABLE_POST_OPERATION);
          break;
        case MODIFY_TABLE_POST_OPERATION:
          postModify(env, state);
          if (reopenRegions) {
            setNextState(ModifyTableState.MODIFY_TABLE_REOPEN_ALL_REGIONS);
          } else
            if (ErasureCodingUtils.needsSync(unmodifiedTableDescriptor, modifiedTableDescriptor)) {
              setNextState(ModifyTableState.MODIFY_TABLE_SYNC_ERASURE_CODING_POLICY);
            } else {
              return Flow.NO_MORE_STATE;
            }
          break;
        case MODIFY_TABLE_REOPEN_ALL_REGIONS:
          if (isTableEnabled(env)) {
            addChildProcedure(ReopenTableRegionsProcedure.throttled(env.getMasterConfiguration(),
              env.getMasterServices().getTableDescriptors().get(getTableName())));
          }
          setNextState(ModifyTableState.MODIFY_TABLE_ASSIGN_NEW_REPLICAS);
          break;
        case MODIFY_TABLE_ASSIGN_NEW_REPLICAS:
          assignNewReplicasIfNeeded(env);
          if (TableName.isMetaTableName(getTableName())) {
            MetaLocationSyncer syncer = env.getMasterServices().getMetaLocationSyncer();
            if (syncer != null) {
              syncer.setMetaReplicaCount(modifiedTableDescriptor.getRegionReplication());
            }
          }
          if (deleteColumnFamilyInModify) {
            setNextState(ModifyTableState.MODIFY_TABLE_DELETE_FS_LAYOUT);
          } else
            if (ErasureCodingUtils.needsSync(unmodifiedTableDescriptor, modifiedTableDescriptor)) {
              setNextState(ModifyTableState.MODIFY_TABLE_SYNC_ERASURE_CODING_POLICY);
            } else {
              return Flow.NO_MORE_STATE;
            }
          break;
        case MODIFY_TABLE_DELETE_FS_LAYOUT:
          deleteFromFs(env, unmodifiedTableDescriptor, modifiedTableDescriptor);
          if (ErasureCodingUtils.needsSync(unmodifiedTableDescriptor, modifiedTableDescriptor)) {
            setNextState(ModifyTableState.MODIFY_TABLE_SYNC_ERASURE_CODING_POLICY);
            break;
          } else {
            return Flow.NO_MORE_STATE;
          }
        case MODIFY_TABLE_SYNC_ERASURE_CODING_POLICY:
          ErasureCodingUtils.sync(env.getMasterFileSystem().getFileSystem(),
            env.getMasterFileSystem().getRootDir(), modifiedTableDescriptor);
          return Flow.NO_MORE_STATE;
        default:
          throw new UnsupportedOperationException("unhandled state=" + state);
      }
    } catch (IOException e) {
      if (isRollbackSupported(state)) {
        setFailure("master-modify-table", e);
      } else {
        LOG.warn("Retriable error trying to modify table={} (in state={})", getTableName(), state,
          e);
      }
    }
    return Flow.HAS_MORE_STATE;
  }

  @Override
  protected void rollbackState(final MasterProcedureEnv env, final ModifyTableState state)
    throws IOException {
    if (
      state == ModifyTableState.MODIFY_TABLE_PREPARE
        || state == ModifyTableState.MODIFY_TABLE_PRE_OPERATION
    ) {
      // nothing to rollback, pre-modify is just checks.
      // TODO: coprocessor rollback semantic is still undefined.
      return;
    }

    // The delete doesn't have a rollback. The execution will succeed, at some point.
    throw new UnsupportedOperationException("unhandled state=" + state);
  }

  @Override
  protected boolean isRollbackSupported(final ModifyTableState state) {
    switch (state) {
      case MODIFY_TABLE_PRE_OPERATION:
      case MODIFY_TABLE_PREPARE:
        return true;
      default:
        return false;
    }
  }

  @Override
  protected void completionCleanup(final MasterProcedureEnv env) {
    releaseSyncLatch();
  }

  @Override
  protected ModifyTableState getState(final int stateId) {
    return ModifyTableState.forNumber(stateId);
  }

  @Override
  protected int getStateId(final ModifyTableState state) {
    return state.getNumber();
  }

  @Override
  protected ModifyTableState getInitialState() {
    return ModifyTableState.MODIFY_TABLE_PREPARE;
  }

  @Override
  protected void serializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.serializeStateData(serializer);

    MasterProcedureProtos.ModifyTableStateData.Builder modifyTableMsg =
      MasterProcedureProtos.ModifyTableStateData.newBuilder()
        .setUserInfo(MasterProcedureUtil.toProtoUserInfo(getUser()))
        .setModifiedTableSchema(ProtobufUtil.toTableSchema(modifiedTableDescriptor))
        .setDeleteColumnFamilyInModify(deleteColumnFamilyInModify)
        .setShouldCheckDescriptor(shouldCheckDescriptor).setReopenRegions(reopenRegions);

    if (unmodifiedTableDescriptor != null) {
      modifyTableMsg
        .setUnmodifiedTableSchema(ProtobufUtil.toTableSchema(unmodifiedTableDescriptor));
    }

    serializer.serialize(modifyTableMsg.build());
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.deserializeStateData(serializer);

    MasterProcedureProtos.ModifyTableStateData modifyTableMsg =
      serializer.deserialize(MasterProcedureProtos.ModifyTableStateData.class);
    setUser(MasterProcedureUtil.toUserInfo(modifyTableMsg.getUserInfo()));
    modifiedTableDescriptor =
      ProtobufUtil.toTableDescriptor(modifyTableMsg.getModifiedTableSchema());
    deleteColumnFamilyInModify = modifyTableMsg.getDeleteColumnFamilyInModify();
    shouldCheckDescriptor =
      modifyTableMsg.hasShouldCheckDescriptor() ? modifyTableMsg.getShouldCheckDescriptor() : false;
    reopenRegions = modifyTableMsg.hasReopenRegions() ? modifyTableMsg.getReopenRegions() : true;

    if (modifyTableMsg.hasUnmodifiedTableSchema()) {
      unmodifiedTableDescriptor =
        ProtobufUtil.toTableDescriptor(modifyTableMsg.getUnmodifiedTableSchema());
    }
  }

  @Override
  public TableName getTableName() {
    return modifiedTableDescriptor.getTableName();
  }

  @Override
  public TableOperationType getTableOperationType() {
    return TableOperationType.EDIT;
  }

  /**
   * Check conditions before any real action of modifying a table.
   */
  private void prepareModify(final MasterProcedureEnv env) throws IOException {
    // Checks whether the table exists
    if (!env.getMasterServices().getTableDescriptors().exists(getTableName())) {
      throw new TableNotFoundException(getTableName());
    }

    // check that we have at least 1 CF
    if (modifiedTableDescriptor.getColumnFamilyCount() == 0) {
      throw new DoNotRetryIOException(
        "Table " + getTableName().toString() + " should have at least one column family.");
    }

    // If descriptor check is enabled, check whether the table descriptor when procedure was
    // submitted matches with the current
    // table descriptor of the table, else retrieve the old descriptor
    // for comparison in order to update the descriptor.
    if (shouldCheckDescriptor) {
      if (
        TableDescriptor.COMPARATOR.compare(unmodifiedTableDescriptor,
          env.getMasterServices().getTableDescriptors().get(getTableName())) != 0
      ) {
        LOG.error("Error while modifying table '" + getTableName().toString()
          + "' Skipping procedure : " + this);
        throw new ConcurrentTableModificationException(
          "Skipping modify table operation on table '" + getTableName().toString()
            + "' as it has already been modified by some other concurrent operation, "
            + "Please retry.");
      }
    } else {
      this.unmodifiedTableDescriptor =
        env.getMasterServices().getTableDescriptors().get(getTableName());
    }

    this.deleteColumnFamilyInModify =
      isDeleteColumnFamily(unmodifiedTableDescriptor, modifiedTableDescriptor);
    if (
      !unmodifiedTableDescriptor.getRegionServerGroup()
        .equals(modifiedTableDescriptor.getRegionServerGroup())
    ) {
      Supplier<String> forWhom = () -> "table " + getTableName();
      RSGroupInfo rsGroupInfo = MasterProcedureUtil.checkGroupExists(
        env.getMasterServices().getRSGroupInfoManager()::getRSGroup,
        modifiedTableDescriptor.getRegionServerGroup(), forWhom);
      MasterProcedureUtil.checkGroupNotEmpty(rsGroupInfo, forWhom);
    }

    // check for store file tracker configurations
    StoreFileTrackerValidationUtils.checkForModifyTable(env.getMasterConfiguration(),
      unmodifiedTableDescriptor, modifiedTableDescriptor, !isTableEnabled(env));
  }

  /**
   * Find out whether all column families in unmodifiedTableDescriptor also exists in the
   * modifiedTableDescriptor.
   * @return True if we are deleting a column family.
   */
  private static boolean isDeleteColumnFamily(TableDescriptor originalDescriptor,
    TableDescriptor newDescriptor) {
    boolean result = false;
    final Set<byte[]> originalFamilies = originalDescriptor.getColumnFamilyNames();
    final Set<byte[]> newFamilies = newDescriptor.getColumnFamilyNames();
    for (byte[] familyName : originalFamilies) {
      if (!newFamilies.contains(familyName)) {
        result = true;
        break;
      }
    }
    return result;
  }

  /**
   * Action before modifying table.
   * @param env   MasterProcedureEnv
   * @param state the procedure state
   */
  private void preModify(final MasterProcedureEnv env, final ModifyTableState state)
    throws IOException, InterruptedException {
    runCoprocessorAction(env, state);
  }

  /**
   * Update descriptor
   * @param env MasterProcedureEnv
   **/
  private void updateTableDescriptor(final MasterProcedureEnv env) throws IOException {
    env.getMasterServices().getTableDescriptors().update(modifiedTableDescriptor);
  }

  /**
   * Removes from hdfs the families that are not longer present in the new table descriptor.
   * @param env MasterProcedureEnv
   */
  private void deleteFromFs(final MasterProcedureEnv env, final TableDescriptor oldTableDescriptor,
    final TableDescriptor newTableDescriptor) throws IOException {
    final Set<byte[]> oldFamilies = oldTableDescriptor.getColumnFamilyNames();
    final Set<byte[]> newFamilies = newTableDescriptor.getColumnFamilyNames();
    for (byte[] familyName : oldFamilies) {
      if (!newFamilies.contains(familyName)) {
        MasterDDLOperationHelper.deleteColumnFamilyFromFileSystem(env, getTableName(),
          getRegionInfoList(env), familyName,
          oldTableDescriptor.getColumnFamily(familyName).isMobEnabled());
      }
    }
  }

  /**
   * remove replica columns if necessary.
   */
  private void removeReplicaColumnsIfNeeded(MasterProcedureEnv env) throws IOException {
    final int oldReplicaCount = unmodifiedTableDescriptor.getRegionReplication();
    final int newReplicaCount = modifiedTableDescriptor.getRegionReplication();
    if (newReplicaCount >= oldReplicaCount) {
      return;
    }
    env.getAssignmentManager().getRegionStateStore().removeRegionReplicas(getTableName(),
      oldReplicaCount, newReplicaCount);
    env.getAssignmentManager().getRegionStates().getRegionsOfTable(getTableName()).stream()
      .filter(r -> r.getReplicaId() >= newReplicaCount)
      .forEach(env.getAssignmentManager().getRegionStates()::deleteRegion);
  }

  private void assignNewReplicasIfNeeded(MasterProcedureEnv env) throws IOException {
    final int oldReplicaCount = unmodifiedTableDescriptor.getRegionReplication();
    final int newReplicaCount = modifiedTableDescriptor.getRegionReplication();
    if (newReplicaCount <= oldReplicaCount) {
      return;
    }
    if (isTableEnabled(env)) {
      List<RegionInfo> newReplicas = env.getAssignmentManager().getRegionStates()
        .getRegionsOfTable(getTableName()).stream().filter(RegionReplicaUtil::isDefaultReplica)
        .flatMap(primaryRegion -> IntStream.range(oldReplicaCount, newReplicaCount).mapToObj(
          replicaId -> RegionReplicaUtil.getRegionInfoForReplica(primaryRegion, replicaId)))
        .collect(Collectors.toList());
      addChildProcedure(env.getAssignmentManager().createAssignProcedures(newReplicas));
    }
  }

  private void closeExcessReplicasIfNeeded(MasterProcedureEnv env) {
    final int oldReplicaCount = unmodifiedTableDescriptor.getRegionReplication();
    final int newReplicaCount = modifiedTableDescriptor.getRegionReplication();
    if (newReplicaCount >= oldReplicaCount) {
      return;
    }
    addChildProcedure(new CloseExcessRegionReplicasProcedure(getTableName(), newReplicaCount));
  }

  /**
   * Action after modifying table.
   * @param env   MasterProcedureEnv
   * @param state the procedure state
   */
  private void postModify(final MasterProcedureEnv env, final ModifyTableState state)
    throws IOException, InterruptedException {
    runCoprocessorAction(env, state);
  }

  /**
   * Coprocessor Action.
   * @param env   MasterProcedureEnv
   * @param state the procedure state
   */
  private void runCoprocessorAction(final MasterProcedureEnv env, final ModifyTableState state)
    throws IOException, InterruptedException {
    final MasterCoprocessorHost cpHost = env.getMasterCoprocessorHost();
    if (cpHost != null) {
      switch (state) {
        case MODIFY_TABLE_PRE_OPERATION:
          cpHost.preModifyTableAction(getTableName(), unmodifiedTableDescriptor,
            modifiedTableDescriptor, getUser());
          break;
        case MODIFY_TABLE_POST_OPERATION:
          cpHost.postCompletedModifyTableAction(getTableName(), unmodifiedTableDescriptor,
            modifiedTableDescriptor, getUser());
          break;
        default:
          throw new UnsupportedOperationException(this + " unhandled state=" + state);
      }
    }
  }

  /**
   * Fetches all Regions for a table. Cache the result of this method if you need to use it multiple
   * times. Be aware that it may change over in between calls to this procedure.
   */
  private List<RegionInfo> getRegionInfoList(final MasterProcedureEnv env) throws IOException {
    return env.getAssignmentManager().getRegionStates().getRegionsOfTable(getTableName());
  }
}
