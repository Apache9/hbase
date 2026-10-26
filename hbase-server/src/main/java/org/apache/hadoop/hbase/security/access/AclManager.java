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
package org.apache.hadoop.hbase.security.access;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.master.procedure.ProcedurePrepareLatch;
import org.apache.hadoop.hbase.master.procedure.RefreshAclProcedure;
import org.apache.hadoop.hbase.security.Superusers;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.ListMultimap;
import org.apache.hbase.thirdparty.com.google.protobuf.ByteString;

import org.apache.hadoop.hbase.shaded.protobuf.generated.AccessControlProtos.Permission.Type;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.AclEntryUpdate;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.AclOp;

/**
 * Master side ACL holder: the {@code hbase:acl} table is the authoritative store, this class owns
 * the master's {@link AclSnapshot} of it and drives every change to it:
 * <ul>
 * <li>grant, revoke and the cleanup of a removed table or namespace are queued here as
 * {@link PendingOp}s and end up in a {@link RefreshAclProcedure}, which writes them to
 * {@code hbase:acl} and then broadcasts the resulting entry states to all region servers;</li>
 * <li>because the writes happen inside the procedure, they are persisted with it before the caller
 * hears back: a master failover replays them from the procedure store and no update can fall into
 * the gap between a table write and its broadcast;</li>
 * <li>the master's own snapshot is updated with every write, so it never lags behind its own
 * changes and needs no periodic reload.</li>
 * </ul>
 * A change is queued in the caller's thread and applied asynchronously:
 * <ul>
 * <li>{@link #grant} and {@link #revoke} hand back a {@link CompletableFuture} with the id of the
 * procedure that will apply the change, so an RPC can answer its client with something to wait on,
 * or block on the {@link ProcedurePrepareLatch} it passes in for the synchronous behaviour old
 * clients expect;</li>
 * <li>{@link #newCleanupProcedure(TableName)} and {@link #newCleanupProcedure(String)} build the
 * cleanup of a deleted table or namespace for the delete procedure to run as one of its children,
 * so the cleanup is persisted with the delete and done before it completes.</li>
 * </ul>
 * A burst of changes is coalesced: at most one procedure is submitted for the queued backlog, and a
 * procedure submitted while another one is still running is chained behind it, so the changes reach
 * {@code hbase:acl} in the order they were queued. Direct writes to {@code hbase:acl} that bypass
 * this class are only picked up by the periodic reload of the region servers.
 */
@InterfaceAudience.Private
public class AclManager implements AclView {

  private static final Logger LOG = LoggerFactory.getLogger(AclManager.class);

  private final Configuration conf;
  private final MasterServices masterServices;

  /** Guards the write path; see the class javadoc. */
  private final Object writeLock = new Object();

  /** The master's current ACL snapshot; replaced as a whole on every change. */
  private volatile AclSnapshot snapshot = AclSnapshot.empty();

  /** Queued changes in arrival order; guarded by {@code this}. */
  private final List<PendingOp> pending = new ArrayList<>();

  /** Number of submitted procedures that have not completed yet; guarded by {@code this}. */
  private int proceduresInFlight;

  /** Id of the last submitted procedure, the one the next one chains behind. */
  private long lastProcedureId;

  /**
   * One queued ACL change. The content is persisted with the procedure that applies it; the ack
   * fields are transient bookkeeping for the caller that queued it.
   */
  public static final class PendingOp {
    private final AclOp.Type type;
    private final UserPermission userPermission;
    private final boolean mergeExisting;
    private final TableName table;
    private final String namespace;

    /** Completed with the id of the procedure that applies this change, at submit time. */
    private final CompletableFuture<Long> ack = new CompletableFuture<>();

    /** Hands the procedure id to the caller of grant or revoke. */
    public void acknowledge(long procedureId) {
      ack.complete(procedureId);
    }

    /** The future completed by {@link #acknowledge(long)}. */
    public CompletableFuture<Long> acknowledged() {
      return ack;
    }

    /** Released once the write of this change landed; for old clients, see the class javadoc. */
    public final ProcedurePrepareLatch syncLatch;

    private PendingOp(AclOp.Type type, UserPermission userPermission, boolean mergeExisting,
      TableName table, String namespace, ProcedurePrepareLatch syncLatch) {
      this.type = type;
      this.userPermission = userPermission;
      this.mergeExisting = mergeExisting;
      this.table = table;
      this.namespace = namespace;
      this.syncLatch = syncLatch;
    }

    static PendingOp grant(UserPermission userPermission, boolean mergeExisting,
      ProcedurePrepareLatch syncLatch) {
      return new PendingOp(AclOp.Type.GRANT, userPermission, mergeExisting, null, null, syncLatch);
    }

    static PendingOp revoke(UserPermission userPermission, ProcedurePrepareLatch syncLatch) {
      return new PendingOp(AclOp.Type.REVOKE, userPermission, false, null, null, syncLatch);
    }

    static PendingOp removeTable(TableName table) {
      return new PendingOp(AclOp.Type.REMOVE_TABLE, null, false, table, null, null);
    }

    static PendingOp removeNamespace(String namespace) {
      return new PendingOp(AclOp.Type.REMOVE_NAMESPACE, null, false, null, namespace, null);
    }

    /** For the procedure: a recovered procedure has no callers to acknowledge any more. */
    public PendingOp(AclOp proto) {
      this.type = proto.getType();
      this.userPermission = proto.hasUserPermission()
        ? ShadedAccessControlUtil.toUserPermission(proto.getUserPermission())
        : null;
      this.mergeExisting = proto.getMergeExisting();
      this.table =
        proto.hasTableName() ? TableName.valueOf(proto.getTableName().toByteArray()) : null;
      this.namespace =
        proto.hasNamespaceName() ? Bytes.toString(proto.getNamespaceName().toByteArray()) : null;
      this.syncLatch = null;
    }

    public AclOp toProto() {
      AclOp.Builder builder = AclOp.newBuilder().setType(type).setMergeExisting(mergeExisting);
      if (userPermission != null) {
        builder.setUserPermission(ShadedAccessControlUtil.toUserPermission(userPermission));
      }
      if (table != null) {
        builder.setTableName(ByteString.copyFrom(table.getName()));
      }
      if (namespace != null) {
        builder.setNamespaceName(ByteString.copyFrom(Bytes.toBytes(namespace)));
      }
      return builder.build();
    }
  }

  public AclManager(Configuration conf, MasterServices masterServices) {
    this.conf = conf;
    this.masterServices = masterServices;
  }

  @Override
  public AclSnapshot current() {
    return snapshot;
  }

  /**
   * Loads the master's snapshot from {@code hbase:acl} and seeds the timestamp clock. Called when
   * this master becomes active; every change made before that is already applied, it was made
   * through a procedure of the previous master and the table is the authoritative store.
   */
  public void initialize() throws IOException {
    if (!AccessChecker.isAuthorizationSupported(conf)) {
      // the flag only turns the checks off (HBASE-13275): no snapshot to load, no region server
      // stores to push to, the writes below still record into hbase:acl and just need a clock
      snapshot = snapshot.withTs(EnvironmentEdgeManager.currentTime());
      return;
    }
    PermissionStorage.AclTableContent content;
    synchronized (writeLock) {
      try {
        content = PermissionStorage.loadAllSnapshot(masterServices.getConnection());
      } catch (TableNotFoundException e) {
        // a fresh cluster: the acl table is created later by the AccessController postStartMaster
        // hook
        content = new PermissionStorage.AclTableContent(Collections.emptyMap(), 0L);
      }
      long seed = Math.max(EnvironmentEdgeManager.currentTime(), content.getMaxTimestamp() + 1);
      snapshot = AclSnapshot.of(content.getPermissions(), seed);
      LOG.info("AclManager initialized, timestamps start at {}", seed);
    }
  }

  /**
   * Next timestamp for an {@code hbase:acl} write, to be stamped into the cells of the write so the
   * cell timestamps carry the master assigned write order. Advances the timestamp clock of the
   * snapshot; callers hold the write lock.
   */
  public long nextTimestamp() {
    long ts = Math.max(EnvironmentEdgeManager.currentTime(), snapshot.getTs() + 1);
    snapshot = snapshot.withTs(ts);
    return ts;
  }

  public long getLastTimestamp() {
    return current().getTs();
  }

  /**
   * Queues a grant. The returned future is completed with the id of the procedure that applies it,
   * so a caller can either wait for that procedure or, with a {@code syncLatch}, return as soon as
   * the write landed.
   */
  public CompletableFuture<Long> grant(UserPermission userPermission,
    boolean mergeExistingPermissions, ProcedurePrepareLatch syncLatch) {
    return enqueue(PendingOp.grant(userPermission, mergeExistingPermissions, syncLatch));
  }

  /** Queues a revoke, see {@link #grant}. */
  public CompletableFuture<Long> revoke(UserPermission userPermission,
    ProcedurePrepareLatch syncLatch) {
    return enqueue(PendingOp.revoke(userPermission, syncLatch));
  }

  /**
   * Builds the procedure removing all permissions of a deleted table: it carries the cleanup
   * together with the changes queued so far, so they reach {@code hbase:acl} in order. The caller
   * runs it as one of its children, which keeps the cleanup persisted with the caller and done
   * before the caller completes.
   */
  public RefreshAclProcedure newCleanupProcedure(TableName tableName) {
    return buildProcedure(PendingOp.removeTable(tableName));
  }

  /**
   * Builds the procedure removing all permissions of a deleted namespace, see
   * {@link #newCleanupProcedure(TableName)}.
   */
  public RefreshAclProcedure newCleanupProcedure(String namespace) {
    return buildProcedure(PendingOp.removeNamespace(namespace));
  }

  private synchronized RefreshAclProcedure buildProcedure(PendingOp cleanup) {
    pending.add(cleanup);
    List<PendingOp> ops = new ArrayList<>(pending);
    pending.clear();
    proceduresInFlight++;
    return new RefreshAclProcedure(ops);
  }

  private synchronized CompletableFuture<Long> enqueue(PendingOp op) {
    pending.add(op);
    submitPending();
    return op.acknowledged();
  }

  /**
   * Takes the queued changes as one procedure and submits it. Nothing is submitted while another
   * acl procedure is running: a burst of changes then lands in one procedure and one broadcast, and
   * the changes reach {@code hbase:acl} in the order they were queued. The drained changes are part
   * of the procedure from its creation, so they are persisted with it before its id is handed out.
   */
  private void submitPending() {
    if (pending.isEmpty() || proceduresInFlight > 0) {
      return;
    }
    List<PendingOp> ops = new ArrayList<>(pending);
    pending.clear();
    RefreshAclProcedure procedure = new RefreshAclProcedure(ops);
    long procedureId = submitProcedure(procedure);
    lastProcedureId = procedureId;
    proceduresInFlight++;
    for (PendingOp op : ops) {
      op.acknowledge(procedureId);
    }
    LOG.debug("Submitted acl update procedure {} with {} changes", procedureId, ops.size());
  }

  /**
   * Called by {@link RefreshAclProcedure} when it terminates; records it as the last procedure of
   * the chain and submits the queued backlog behind it.
   */
  public synchronized void onProcedureComplete(long procedureId) {
    lastProcedureId = procedureId;
    if (proceduresInFlight > 0) {
      proceduresInFlight--;
    }
    submitPending();
  }

  /**
   * Clears the submit state after the procedure executor was restarted under a test: the re-loaded
   * procedures do not run through {@link #submitProcedure} again and would otherwise complete
   * against a count they never incremented.
   */
  public synchronized void reset() {
    pending.clear();
    proceduresInFlight = 0;
  }

  /** Visible for testing: hands the procedure over to the procedure executor. */
  long submitProcedure(RefreshAclProcedure procedure) {
    return masterServices.getMasterProcedureExecutor().submitProcedure(procedure);
  }

  /**
   * Writes one queued change to {@code hbase:acl} and applies it to the local snapshot, recording
   * the derived entry state for the broadcast. Called by the apply step of
   * {@link RefreshAclProcedure}, which also acknowledges the caller once the write landed.
   * @return the master assigned timestamp of the write
   */
  public long applyOp(PendingOp op, Map<byte[], AclEntryUpdate> derived) throws IOException {
    synchronized (writeLock) {
      byte[] entry;
      long ts;
      try (Table t = masterServices.getConnection().getTable(PermissionStorage.ACL_TABLE_NAME)) {
        switch (op.type) {
          case GRANT:
            entry = PermissionStorage.userPermissionRowKey(op.userPermission.getPermission());
            ts = nextTimestamp();
            PermissionStorage.addUserPermission(conf, op.userPermission, t, op.mergeExisting, ts);
            break;
          case REVOKE:
            entry = PermissionStorage.userPermissionRowKey(op.userPermission.getPermission());
            ts = nextTimestamp();
            PermissionStorage.removeUserPermission(conf, op.userPermission, t, ts);
            break;
          case REMOVE_TABLE:
            entry = op.table.getName();
            ts = nextTimestamp();
            PermissionStorage.removeTablePermissions(conf, op.table, t, ts);
            break;
          case REMOVE_NAMESPACE:
            entry = Bytes.toBytes(PermissionStorage.toNamespaceEntry(op.namespace));
            ts = nextTimestamp();
            PermissionStorage.removeNamespacePermissions(conf, op.namespace, t, ts);
            break;
          default:
            throw new UnsupportedOperationException("unhandled acl op=" + op.type);
        }
        ListMultimap<String, UserPermission> perms =
          PermissionStorage.getPermissions(t, entry, null, null, null, false);
        AclSnapshot.EntryUpdate update = perms.isEmpty()
          ? AclSnapshot.EntryUpdate.deleted(entry)
          : AclSnapshot.EntryUpdate.replace(entry, perms);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Applied acl entry {} change at ts {}, deleted={}", Bytes.toStringBinary(entry),
            ts, update.isDeleted());
        }
        // the write is done and the write path is serialized, the state read back is the current
        // one: apply it right away so the master's snapshot never lags behind its own writes
        snapshot = snapshot.applyEntryUpdates(Collections.singletonList(update), ts);
        derived.put(entry, toProto(update));
        return ts;
      } catch (TableNotFoundException e) {
        if (!isCleanupOp(op)) {
          throw e;
        }
        // a cleanup op on a cluster without hbase:acl: nothing was ever recorded for the deleted
        // table or namespace, so there is nothing to clean up
        LOG.debug("No {} yet, nothing to clean up for {}", PermissionStorage.ACL_TABLE_NAME,
          op.type);
        return 0L;
      }
    }
  }

  private static boolean isCleanupOp(PendingOp op) {
    return op.type == AclOp.Type.REMOVE_TABLE || op.type == AclOp.Type.REMOVE_NAMESPACE;
  }

  /**
   * The {@code getUserPermissions} RPC query: collects the permissions of the requested scope,
   * including the superusers for an unfiltered global query (they are not stored in the acl table).
   */
  public List<UserPermission> getUserPermissions(Type permissionType, TableName table,
    String namespace, byte[] cf, byte[] cq, String userName) throws IOException {
    if (permissionType == Type.Table) {
      boolean filter = (cf != null || userName != null) ? true : false;
      return getUserTablePermissions(table, cf, cq, userName, filter);
    }
    if (permissionType == Type.Namespace) {
      return getUserNamespacePermissions(namespace, userName, userName != null ? true : false);
    }
    List<UserPermission> perms = getUserPermissions(userName, userName != null ? true : false);
    // Skip super users when filter user is specified
    if (userName == null) {
      // Adding superusers explicitly to the result set as PermissionStorage do not store
      // them. Also using acl as table name to be inline with the results of global admin and
      // will help in avoiding any leakage of information about being superusers.
      for (String user : Superusers.getSuperUsers()) {
        perms.add(new UserPermission(user,
          Permission.newBuilder().withActions(Permission.Action.values()).build()));
      }
    }
    return perms;
  }

  public List<UserPermission> getUserTablePermissions(TableName table, byte[] cf, byte[] cq,
    String userName, boolean filter) throws IOException {
    return PermissionStorage.getUserTablePermissions(masterServices.getConnection(), table, cf, cq,
      userName, filter);
  }

  public List<UserPermission> getUserNamespacePermissions(String namespace, String userName,
    boolean filter) throws IOException {
    return PermissionStorage.getUserNamespacePermissions(masterServices.getConnection(), namespace,
      userName, filter);
  }

  public List<UserPermission> getUserPermissions(String userName, boolean filter)
    throws IOException {
    return PermissionStorage.getUserPermissions(masterServices.getConnection(), null, null, null,
      userName, filter);
  }

  /**
   * Checks the given permissions of the given user against the authorization cache.
   * @param caller   the request user
   * @param userName the user to check, when different from the caller the caller needs the
   *                 permission to check another user
   */
  public List<Boolean> hasUserPermissions(User caller, String userName,
    List<Permission> permissions) throws IOException {
    if (!caller.getShortName().equals(userName)) {
      List<String> groups = AccessChecker.getUserGroups(userName);
      caller = new AccessChecker.InputUser(userName, groups.toArray(new String[groups.size()]));
    }
    List<Boolean> results = new ArrayList<>(permissions.size());
    AccessChecker accessChecker = masterServices.getAccessChecker();
    if (accessChecker != null) {
      for (Permission permission : permissions) {
        results.add(accessChecker.hasUserPermission(caller, "hasUserPermissions", permission));
      }
    } else {
      for (int i = 0; i < permissions.size(); i++) {
        results.add(true);
      }
    }
    return results;
  }

  private static AclEntryUpdate toProto(AclSnapshot.EntryUpdate update) {
    AclEntryUpdate.Builder builder = AclEntryUpdate.newBuilder()
      .setEntry(ByteString.copyFrom(update.getEntry())).setDeleted(update.isDeleted());
    if (!update.isDeleted()) {
      builder.setPerms(AccessControlUtil.toUserTablePermissions(update.getPermissions()));
    }
    return builder.build();
  }
}
