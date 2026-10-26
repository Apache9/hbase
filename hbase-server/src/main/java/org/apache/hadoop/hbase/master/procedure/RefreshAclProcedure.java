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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.ProcedureYieldException;
import org.apache.hadoop.hbase.procedure2.StateMachineProcedure;
import org.apache.hadoop.hbase.security.access.AclChangeSet;
import org.apache.hadoop.hbase.security.access.AclManager;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.AclEntryUpdate;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.AclOp;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.RefreshAclState;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.RefreshAclStateData;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos;

/**
 * Applies a batch of queued ACL changes: writes them to {@code hbase:acl} and broadcasts the
 * resulting entry states to all registered region servers, as one {@link RefreshAclRemoteProcedure}
 * child per server. The changes are part of this procedure from its creation, so they are persisted
 * with it: a master failover replays them and nothing falls into the gap between a table write and
 * its broadcast.
 * <p>
 * At most one instance is submitted for a burst of changes, see {@link AclManager}; an instance
 * submitted while another one is still running is chained behind it, so the changes reach
 * {@code hbase:acl} in the order they were queued. The broadcast is replayed at-least-once; stale
 * replays are dropped by the timestamp watermark on the servers.
 * </p>
 */
@InterfaceAudience.Private
public class RefreshAclProcedure extends StateMachineProcedure<MasterProcedureEnv, RefreshAclState>
  implements GlobalProcedureInterface {

  private static final Logger LOG = LoggerFactory.getLogger(RefreshAclProcedure.class);

  private List<AclManager.PendingOp> ops;

  /** Entry states derived from the writes, filled in by the apply step and then broadcast. */
  private List<AclEntryUpdate> updates;

  private long ts;

  /** For the framework to create the procedure on replay. */
  public RefreshAclProcedure() {
  }

  public RefreshAclProcedure(List<AclManager.PendingOp> ops) {
    this.ops = ops;
  }

  @Override
  protected Flow executeFromState(MasterProcedureEnv env, RefreshAclState state)
    throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
    switch (state) {
      case REFRESH_ACL_APPLY: {
        // changes drained into a child procedure are only persisted with their parent: their
        // callers are acknowledged here, once this procedure runs and has an id
        for (AclManager.PendingOp op : ops) {
          op.acknowledge(getProcId());
        }
        Map<byte[], AclEntryUpdate> derived = new TreeMap<>(Bytes.BYTES_RAWCOMPARATOR);
        long maxTs = 0L;
        for (int i = 0; i < ops.size(); i++) {
          AclManager.PendingOp op = ops.get(i);
          try {
            maxTs = Math.max(maxTs, env.getMasterServices().getAclManager().applyOp(op, derived));
            // the write landed: acknowledge its caller, old clients return at this point
            ProcedurePrepareLatch.releaseLatch(op.syncLatch, this);
          } catch (IOException e) {
            // surface the failure to the callers waiting on this procedure; the writes are
            // idempotent so a client retry of the change is safe
            LOG.warn("Failed to apply acl change {} of {}", i, this, e);
            setFailure("acl-update", e);
            for (int j = i; j < ops.size(); j++) {
              // writes that will never happen must not leave callers waiting
              ProcedurePrepareLatch.releaseLatch(ops.get(j).syncLatch, this);
            }
            return Flow.NO_MORE_STATE;
          }
        }
        updates = new ArrayList<>(derived.values());
        ts = maxTs;
        setNextState(RefreshAclState.REFRESH_ACL_DISPATCH);
        return Flow.HAS_MORE_STATE;
      }
      case REFRESH_ACL_DISPATCH: {
        RefreshAclRemoteProcedure[] subProcedures =
          env.getMasterServices().getServerManager().getOnlineServersList().stream()
            .map(sn -> new RefreshAclRemoteProcedure(sn, new AclChangeSet(updates, ts)))
            .toArray(RefreshAclRemoteProcedure[]::new);
        if (subProcedures.length > 0) {
          addChildProcedure(subProcedures);
        }
        // no state is needed to wait for them: the framework holds this procedure until its
        // children complete, the next execution lands on the end of the state machine
        return Flow.NO_MORE_STATE;
      }
      default:
        throw new UnsupportedOperationException("unhandled state=" + state);
    }
  }

  @Override
  protected synchronized boolean setTimeoutFailure(MasterProcedureEnv env) {
    // the timeout is only a wake-up to re-check the previous procedure, not a failure
    setState(ProcedureProtos.ProcedureState.RUNNABLE);
    env.getProcedureScheduler().addFront(this);
    return false;
  }

  @Override
  protected void completionCleanup(MasterProcedureEnv env) {
    // release the single-flight gate and submit the queued backlog behind this procedure
    env.getMasterServices().getAclManager().onProcedureComplete(getProcId());
  }

  @Override
  protected void rollbackState(MasterProcedureEnv env, RefreshAclState state)
    throws IOException, InterruptedException {
    // there is no state to roll back: the region server caches are only refreshed forward, a
    // failed push is simply retried with the next change set
    throw new UnsupportedOperationException("RefreshAclProcedure cannot be rolled back");
  }

  /** Visible for testing: the number of queued changes this procedure applies. */
  public int getOpCount() {
    return ops != null ? ops.size() : 0;
  }

  @Override
  public String getGlobalId() {
    return getClass().getSimpleName();
  }

  @Override
  protected RefreshAclState getState(int stateId) {
    return RefreshAclState.forNumber(stateId);
  }

  @Override
  protected int getStateId(RefreshAclState state) {
    return state.getNumber();
  }

  @Override
  protected RefreshAclState getInitialState() {
    return RefreshAclState.REFRESH_ACL_APPLY;
  }

  @Override
  protected RefreshAclState getCurrentState() {
    return super.getCurrentState();
  }

  @Override
  protected void serializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.serializeStateData(serializer);
    RefreshAclStateData.Builder builder = RefreshAclStateData.newBuilder().setTs(ts);
    if (ops != null) {
      for (AclManager.PendingOp op : ops) {
        builder.addOps(op.toProto());
      }
    }
    if (updates != null) {
      builder.addAllUpdates(updates);
    }
    serializer.serialize(builder.build());
  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
    super.deserializeStateData(serializer);
    RefreshAclStateData data = serializer.deserialize(RefreshAclStateData.class);
    ts = data.getTs();
    ops = new ArrayList<>(data.getOpsCount());
    for (AclOp proto : data.getOpsList()) {
      ops.add(new AclManager.PendingOp(proto));
    }
    updates = new ArrayList<>(data.getUpdatesList());
  }

  @Override
  protected void toStringState(StringBuilder builder) {
    super.toStringState(builder);
    builder.append(", ops=").append(ops != null ? ops.size() : 0).append(", ts=").append(ts);
  }
}
