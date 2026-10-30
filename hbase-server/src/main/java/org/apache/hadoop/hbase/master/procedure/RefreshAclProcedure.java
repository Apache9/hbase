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
import java.util.concurrent.CompletableFuture;
import java.util.function.LongConsumer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.ProcedureUtil;
import org.apache.hadoop.hbase.procedure2.ProcedureYieldException;
import org.apache.hadoop.hbase.procedure2.StateMachineProcedure;
import org.apache.hadoop.hbase.security.access.AclChangeSet;
import org.apache.hadoop.hbase.security.access.AclManager;
import org.apache.hadoop.hbase.security.access.AclManager.PendingOp;
import org.apache.hadoop.hbase.security.access.AclSnapshot;
import org.apache.hadoop.hbase.security.access.PermissionStorage;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.RetryCounter;
import org.apache.hadoop.hbase.util.RetryCounterFactory;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.AclOp;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.RefreshAclState;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.RefreshAclStateData;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos;

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
 * <p>
 * A cleanup change of a deleted table or namespace carries the id of the delete procedure that
 * requested it. That procedure waits for this one to complete: the delete is only done once its
 * permissions are gone from {@code hbase:acl} and from the region server caches. The link is
 * rebuilt at recovery ({@link #afterReplay}) so a master restart does not schedule the cleanup a
 * second time, and deletions queued together share one procedure and one broadcast.
 * </p>
 */
@InterfaceAudience.Private
public class RefreshAclProcedure extends StateMachineProcedure<MasterProcedureEnv, RefreshAclState>
  implements GlobalProcedureInterface {

  private static final Logger LOG = LoggerFactory.getLogger(RefreshAclProcedure.class);

  private static final RetryCounterFactory RETRY_FACTORY =
    new RetryCounterFactory(Integer.MAX_VALUE, 100, 1000 * 60 * 5);

  private List<AclManager.PendingOp> ops;

  private List<Mutation> aclMutations;

  private List<AclSnapshot.EntryUpdate> aclUpdates;

  private long ts;

  private RetryCounter retryCounter;

  /** For the framework to create the procedure on replay. */
  public RefreshAclProcedure() {
  }

  public RefreshAclProcedure(List<AclManager.PendingOp> ops) {
    this.ops = ops;
  }

  private ProcedureSuspendedException suspend(Configuration conf, LongConsumer backoffConsumer)
    throws ProcedureSuspendedException {
    if (retryCounter == null) {
      retryCounter = ProcedureUtil.createRetryCounter(conf);
    }
    long backoff = retryCounter.getBackoffTimeAndIncrementAttempts();
    backoffConsumer.accept(backoff);
    throw suspend(Math.toIntExact(backoff), true);
  }

  private final void resetRetry() {
    retryCounter = null;
  }

  @Override
  protected synchronized boolean setTimeoutFailure(MasterProcedureEnv env) {
    setState(ProcedureProtos.ProcedureState.RUNNABLE);
    env.getProcedureScheduler().addFront(this);
    return false;
  }

  @Override
  protected Flow executeFromState(MasterProcedureEnv env, RefreshAclState state)
    throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
    switch (state) {
      case REFRESH_ACL_PREPARE: {
        Pair<List<Mutation>, List<AclSnapshot.EntryUpdate>> pair;
        try {
          pair = env.getMasterServices().getAclManager().toMutationsAndUpdates(ops);
        } catch (IOException e) {
          suspend(env.getMasterConfiguration(), backoff -> LOG
            .warn("Failed to compute mutations for writing to acl table, sleep {} ms", backoff, e));
        }
        resetRetry();
        aclMutations = pair.getFirst();
        if (aclMutations.isEmpty()) {
          assert pair.getSecond().isEmpty();
          LOG.info("No acl entry to update, quit...");
          return Flow.NO_MORE_STATE;
        }
        aclUpdates = pair.getSecond();
        ts = aclMutations.stream().mapToLong(Mutation::getTimestamp).max().getAsLong();
        setNextState(RefreshAclState.REFRESH_ACL_WRITE);
        return Flow.HAS_MORE_STATE;
      }
      case REFRESH_ACL_WRITE: {
        try (Table aclTable =
          env.getMasterServices().getConnection().getTable(PermissionStorage.ACL_TABLE_NAME)) {
          Object[] results = new Object[aclMutations.size()];
          aclTable.batch(aclMutations, results);
        } catch (IOException e) {
          suspend(env.getMasterConfiguration(),
            backoff -> LOG.warn("Failed to write to acl table, sleep {} ms", backoff, e));
        }
        resetRetry();
        // the write landed: acknowledge its caller, old clients return at this point
        for (PendingOp op : ops) {
          ProcedurePrepareLatch.releaseLatch(op.syncLatch, this);
        }
        setNextState(RefreshAclState.REFRESH_ACL_DISPATCH);
        return Flow.HAS_MORE_STATE;
      }
      case REFRESH_ACL_SNAPSHOT: {
        // TODO:
        setNextState(RefreshAclState.REFRESH_ACL_DISPATCH);
        return Flow.HAS_MORE_STATE;
      }
      case REFRESH_ACL_DISPATCH: {
        // TODO:
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

  @Override
  protected void afterReplay(MasterProcedureEnv env) {
    // the delete procedures waiting for the cleanup this procedure carries are reloaded without
    // their transient wait fields: rebuild the waits so they keep waiting on this procedure
    // instead of scheduling the cleanup a second time
    for (AclManager.PendingOp op : ops) {
      if (op.ownerProcId() == 0) {
        continue;
      }
      CompletableFuture<Void> wait =
        env.getMasterServices().getAclManager().restoreCleanupWait(op.ownerProcId());
      Procedure<?> owner =
        env.getMasterServices().getMasterProcedureExecutor().getProcedure(op.ownerProcId());
      if (owner instanceof DeleteTableProcedure) {
        ((DeleteTableProcedure) owner).restoreAclCleanupWait(wait);
      } else if (owner instanceof DeleteNamespaceProcedure) {
        ((DeleteNamespaceProcedure) owner).restoreAclCleanupWait(wait);
      }
    }
  }

  @Override
  protected void completionCleanup(MasterProcedureEnv env) {
    // the delete procedures of the cleanup changes in this batch are done waiting on it
    for (AclManager.PendingOp op : ops) {
      if (op.ownerProcId() != 0) {
        env.getMasterServices().getAclManager().completeCleanupWait(op.ownerProcId());
      }
    }
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
    return RefreshAclState.REFRESH_ACL_PREPARE;
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
  }

  @Override
  protected void toStringState(StringBuilder builder) {
    super.toStringState(builder);
    builder.append(", ops=").append(ops != null ? ops.size() : 0).append(", ts=").append(ts);
  }
}
