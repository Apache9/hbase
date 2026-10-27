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
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.ProcedureYieldException;
import org.apache.hadoop.hbase.procedure2.StateMachineProcedure;
import org.apache.hadoop.hbase.security.access.AclChangeSet;
import org.apache.hadoop.hbase.security.access.AclManager;
import org.apache.hadoop.hbase.util.RetryCounter;
import org.apache.hadoop.hbase.util.RetryCounterFactory;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.AclEntryUpdate;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.AclOp;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.RefreshAclState;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.RefreshAclStateData;

/**
 * Applies a batch of queued ACL changes to {@code hbase:acl} and broadcasts the resulting entry
 * states to all registered region servers, as one {@link RefreshAclRemoteProcedure} child per
 * server. The changes are part of this procedure from its creation, so they are persisted with it:
 * a master failover replays them and nothing falls into the gap between a table write and its
 * broadcast.
 * <p>
 * The steps are: compute the content to write for the changes that do not carry it and let the
 * procedure store persist it ({@link RefreshAclState#REFRESH_ACL_PREPARE}, skipped when every
 * change carries its exact write content), write the resolved content to {@code hbase:acl}
 * ({@link RefreshAclState#REFRESH_ACL_WRITE}), generate the new AclSnapshot from the entries as the
 * batch left them ({@link RefreshAclState#REFRESH_ACL_SNAPSHOT}), and broadcast those entry states
 * ({@link RefreshAclState#REFRESH_ACL_DISPATCH}).
 * </p>
 * <p>
 * The procedure cannot be rolled back: the region server caches only refresh forward, and once part
 * of the batch is in {@code hbase:acl} there is no undo. A failed step is therefore retried until
 * it succeeds; the resolved content is fixed and the writes are idempotent, so a retry replays them
 * and never recomputes them against a table that already carries part of the batch.
 * </p>
 * <p>
 * At most one instance is submitted for a burst of changes, see {@link AclManager}; while one is
 * running the changes queue up for the next one, so they reach {@code hbase:acl} in the order they
 * were queued. The broadcast is replayed at-least-once; stale replays are dropped by the timestamp
 * watermark on the servers.
 * </p>
 */
@InterfaceAudience.Private
public class RefreshAclProcedure extends StateMachineProcedure<MasterProcedureEnv, RefreshAclState>
  implements GlobalProcedureInterface {

  private static final Logger LOG = LoggerFactory.getLogger(RefreshAclProcedure.class);

  private static final RetryCounterFactory RETRY_FACTORY =
    new RetryCounterFactory(Integer.MAX_VALUE, 100, 1000 * 60 * 5);

  private List<AclManager.PendingOp> ops;

  /** Entry states read after the writes, filled in by the snapshot step and then broadcast. */
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
      case REFRESH_ACL_PREPARE: {
        acknowledgeCallers();
        // compute the content to write for the changes that do not carry it and let the state
        // transition persist it before anything touches hbase:acl: the write step then replays
        // the resolved content and never recomputes it against a table that already carries part
        // of the batch
        retryUntilDone("acl resolve", () -> {
          env.getMasterServices().getAclManager().resolveOps(ops);
          return null;
        });
        setNextState(RefreshAclState.REFRESH_ACL_WRITE);
        return Flow.HAS_MORE_STATE;
      }
      case REFRESH_ACL_WRITE: {
        acknowledgeCallers();
        long maxTs = 0L;
        for (int i = 0; i < ops.size(); i++) {
          AclManager.PendingOp op = ops.get(i);
          // the write content is fixed and the writes are idempotent: retry until they land,
          // there is no rollback of a partial batch to fall back to
          maxTs = Math.max(maxTs, retryUntilDone("acl write " + (i + 1) + " of " + ops.size(),
            () -> env.getMasterServices().getAclManager().writeOp(op)));
          // the write landed: acknowledge its caller, old clients return at this point
          ProcedurePrepareLatch.releaseLatch(op.syncLatch, this);
        }
        ts = maxTs;
        setNextState(RefreshAclState.REFRESH_ACL_SNAPSHOT);
        return Flow.HAS_MORE_STATE;
      }
      case REFRESH_ACL_SNAPSHOT: {
        // generate the new AclSnapshot from the entries as the batch left them: the writes are
        // done and replay exactly, so what is read here is what the batch produced
        updates = retryUntilDone("acl snapshot",
          () -> env.getMasterServices().getAclManager().applyEntryStates(ops, ts));
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

  @FunctionalInterface
  private interface AclStep<T> {
    T run() throws IOException;
  }

  /**
   * Runs a step, retrying with backoff until it succeeds: the procedure cannot be rolled back, so
   * the only way out is forward. An interrupt cuts the backoff short; the framework then reruns the
   * step later, still until it succeeds.
   */
  private <T> T retryUntilDone(String what, AclStep<T> step) throws InterruptedException {
    RetryCounter retry = RETRY_FACTORY.create();
    for (;;) {
      try {
        return step.run();
      } catch (IOException e) {
        long sleepTime = retry.getBackoffTimeAndIncrementAttempts();
        LOG.warn("{} of {}; retrying in {} ms", what, this, sleepTime, e);
        Thread.sleep(sleepTime);
      }
    }
  }

  /**
   * Changes drained into a child procedure are only persisted with their parent: their callers are
   * acknowledged here, once this procedure runs and has an id.
   */
  private void acknowledgeCallers() {
    for (AclManager.PendingOp op : ops) {
      op.acknowledge(getProcId());
    }
  }

  @Override
  protected void completionCleanup(MasterProcedureEnv env) {
    // release the single-flight flag and schedule the queued backlog
    env.getMasterServices().getAclManager().onProcedureComplete();
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
    for (AclManager.PendingOp op : ops) {
      if (op.needsResolve()) {
        return RefreshAclState.REFRESH_ACL_PREPARE;
      }
    }
    // every change carries its exact write content: skip the compute step and write right away
    return RefreshAclState.REFRESH_ACL_WRITE;
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
