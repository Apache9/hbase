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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
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

import org.apache.hbase.thirdparty.com.google.common.collect.ArrayListMultimap;
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
 * <li>the master's own snapshot is updated with each applied batch, so it never lags behind its own
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

  /**
   * Whether a {@link RefreshAclProcedure} exists and has not completed yet. Only one runs at a
   * time: while it does, changes queue up and are picked up by the next procedure; guarded by
   * {@code this}.
   */
  private boolean procedureRunning;

  /**
   * One queued ACL change. The content is persisted with the procedure that applies it; the ack
   * fields are transient bookkeeping for the caller that queued it.
   */
  public static final class PendingOp {
    private final AclOp.Type type;
    private UserPermission userPermission;
    private boolean mergeExisting;
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

    /**
     * Set for a cleanup op whose caller waits for the procedure carrying it: completed with that
     * procedure so the caller can run it as its child.
     */
    private CompletableFuture<RefreshAclProcedure> cleanupRequest;

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

    /**
     * Whether the exact content to write is not in the request and has to be computed from what the
     * entry holds: a grant merging with the existing actions, or a partial revoke reducing them.
     * Everything else carries its write content in the request.
     */
    public boolean needsResolve() {
      switch (type) {
        case GRANT:
          return mergeExisting;
        case REVOKE:
          return hasActions(userPermission);
        default:
          return false;
      }
    }

    /**
     * Replaces the requested change with the exact content to write: the action set a merge grant
     * lands on, or what remains of the permission after a partial revoke (an empty set removes the
     * record). Called before anything is written, so the resolved content is persisted with the
     * procedure and the write step just replays it.
     */
    public void resolveTo(Permission.Action[] actions) {
      userPermission = withActions(userPermission, actions);
      mergeExisting = false;
    }

    /** The same scope with the given actions; {@link Permission#setActions} cannot empty a set. */
    private static UserPermission withActions(UserPermission userPermission,
      Permission.Action[] actions) {
      Permission permission = userPermission.getPermission();
      Permission.Builder builder;
      if (permission instanceof TablePermission) {
        TablePermission table = (TablePermission) permission;
        builder = Permission.newBuilder(table.getTableName()).withFamily(table.getFamily())
          .withQualifier(table.getQualifier());
      } else if (permission instanceof NamespacePermission) {
        builder = Permission.newBuilder(((NamespacePermission) permission).getNamespace());
      } else {
        builder = Permission.newBuilder();
      }
      return new UserPermission(userPermission.getUser(), builder.withActions(actions).build());
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
    // rejected here because a write that can never land must not enter a procedure that retries
    // its writes until they do
    if (!hasActions(userPermission)) {
      throw new IllegalArgumentException("No actions associated with " + userPermission.getUser());
    }
    return enqueue(PendingOp.grant(userPermission, mergeExistingPermissions, syncLatch));
  }

  /** Queues a revoke, see {@link #grant}. */
  public CompletableFuture<Long> revoke(UserPermission userPermission,
    ProcedurePrepareLatch syncLatch) {
    return enqueue(PendingOp.revoke(userPermission, syncLatch));
  }

  /**
   * Requests the procedure removing all permissions of a deleted table. It carries the cleanup
   * together with the changes queued in front of it, so they reach {@code hbase:acl} in order. The
   * caller runs it as one of its children, which keeps the cleanup persisted with the caller and
   * done before the caller completes.
   * <p>
   * Only one acl procedure runs at a time: when one is already running, the future is completed
   * once it terminates and the next procedure can be built. Callers on a procedure worker should
   * suspend on the future ({@code ProcedureFutureUtil}) instead of blocking on it.
   * </p>
   */
  public synchronized CompletableFuture<RefreshAclProcedure>
    newCleanupProcedure(TableName tableName) {
    return requestCleanup(PendingOp.removeTable(tableName));
  }

  /**
   * Requests the procedure removing all permissions of a deleted namespace, see
   * {@link #newCleanupProcedure(TableName)}.
   */
  public synchronized CompletableFuture<RefreshAclProcedure> newCleanupProcedure(String namespace) {
    return requestCleanup(PendingOp.removeNamespace(namespace));
  }

  private CompletableFuture<RefreshAclProcedure> requestCleanup(PendingOp cleanup) {
    CompletableFuture<RefreshAclProcedure> request = new CompletableFuture<>();
    cleanup.cleanupRequest = request;
    pending.add(cleanup);
    maybeSchedule();
    return request;
  }

  private synchronized CompletableFuture<Long> enqueue(PendingOp op) {
    pending.add(op);
    maybeSchedule();
    return op.acknowledged();
  }

  /**
   * Builds the next procedure from the queued changes when none is running. Everything up to the
   * first cleanup request goes into it, so the changes reach {@code hbase:acl} in the order they
   * were queued; the procedure is handed to that cleanup's caller to run as its child, or submitted
   * here when the queue only carries grant and revoke changes. The drained changes are part of the
   * procedure from its creation, so they are persisted with it before its id is handed out.
   */
  private void maybeSchedule() {
    if (procedureRunning || pending.isEmpty()) {
      return;
    }
    int cut = pending.size();
    for (int i = 0; i < pending.size(); i++) {
      if (pending.get(i).cleanupRequest != null) {
        cut = i + 1;
        break;
      }
    }
    List<PendingOp> ops = new ArrayList<>(pending.subList(0, cut));
    pending.subList(0, cut).clear();
    RefreshAclProcedure procedure = new RefreshAclProcedure(ops);
    procedureRunning = true;
    PendingOp last = ops.get(ops.size() - 1);
    if (last.cleanupRequest != null) {
      // the caller runs it as its child; the calls of the drained changes are acknowledged when
      // it runs and has an id
      CompletableFuture<RefreshAclProcedure> request = last.cleanupRequest;
      last.cleanupRequest = null;
      request.complete(procedure);
    } else {
      long procedureId = submitProcedure(procedure);
      for (PendingOp op : ops) {
        op.acknowledge(procedureId);
      }
      LOG.debug("Submitted acl update procedure {} with {} changes", procedureId, ops.size());
    }
  }

  /** Called by {@link RefreshAclProcedure} when it terminates; schedules the queued backlog. */
  public synchronized void onProcedureComplete() {
    procedureRunning = false;
    maybeSchedule();
  }

  /**
   * Clears the submit state after the procedure executor was restarted under a test: the re-loaded
   * procedures do not run through {@link #submitProcedure} again and would otherwise complete
   * against a flag they never set.
   */
  public synchronized void reset() {
    pending.clear();
    procedureRunning = false;
  }

  /** Visible for testing: hands the procedure over to the procedure executor. */
  long submitProcedure(RefreshAclProcedure procedure) {
    return masterServices.getMasterProcedureExecutor().submitProcedure(procedure);
  }

  /**
   * Fills in the exact content to write for the changes that do not carry it in the request: the
   * action set a merge grant lands on, and what remains of a permission after a partial revoke.
   * Called by the prepare step of {@link RefreshAclProcedure} before anything is written, so the
   * resolved content is persisted with the procedure and the write step replays it instead of
   * recomputing it against a table that already carries part of the batch. Changes queued behind
   * one another resolve against what the ones in front of them will write, so a batch lands exactly
   * as its changes were queued.
   */
  public void resolveOps(List<PendingOp> ops) throws IOException {
    synchronized (writeLock) {
      try (Table t = masterServices.getConnection().getTable(PermissionStorage.ACL_TABLE_NAME)) {
        // the entry states as this batch changes them, seeded from the table at the first change
        Map<byte[], ListMultimap<String, UserPermission>> states =
          new TreeMap<>(Bytes.BYTES_RAWCOMPARATOR);
        for (PendingOp op : ops) {
          byte[] entry = entryKey(op);
          ListMultimap<String, UserPermission> state = states.get(entry);
          if (state == null) {
            state = ArrayListMultimap
              .create(PermissionStorage.getPermissions(t, entry, null, null, null, false));
            states.put(entry, state);
          }
          if (op.needsResolve()) {
            op.resolveTo(resolveActions(op, state));
          }
          applyToState(op, state);
        }
      }
    }
  }

  /**
   * Writes one change to {@code hbase:acl}. The content is what {@link #resolveOps} computed, or
   * what the request carried: the write only lands fixed bytes and is idempotent, so the caller
   * retries it until it succeeds, see {@link RefreshAclProcedure}.
   * @return the master assigned timestamp of the write, 0 when there was nothing to write
   */
  public long writeOp(PendingOp op) throws IOException {
    synchronized (writeLock) {
      try (Table t = masterServices.getConnection().getTable(PermissionStorage.ACL_TABLE_NAME)) {
        long ts = nextTimestamp();
        switch (op.type) {
          case GRANT:
            PermissionStorage.addUserPermission(conf, op.userPermission, t, false, ts);
            break;
          case REVOKE:
            if (hasActions(op.userPermission)) {
              // a partial revoke: the prepare step resolved it to the actions that remain for the
              // user, and the write lands them as the entry's value
              PermissionStorage.addUserPermission(conf, op.userPermission, t, false, ts);
            } else {
              PermissionStorage.removeUserPermission(conf, op.userPermission, t, ts);
            }
            break;
          case REMOVE_TABLE:
            PermissionStorage.removeTablePermissions(conf, op.table, t, ts);
            break;
          case REMOVE_NAMESPACE:
            PermissionStorage.removeNamespacePermissions(conf, op.namespace, t, ts);
            break;
          default:
            throw new UnsupportedOperationException("unhandled acl op=" + op.type);
        }
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

  /**
   * Reads the entries the batch wrote and generates the new {@link AclSnapshot} from them: what is
   * read here is the state the batch left behind, so the snapshot and the broadcast carry exactly
   * what landed. Called by the snapshot step of {@link RefreshAclProcedure} after the writes are
   * done, and holds the write lock, so nothing else writes {@code hbase:acl} in between.
   * @return the entry states to broadcast
   */
  public List<AclEntryUpdate> applyEntryStates(List<PendingOp> ops, long ts) throws IOException {
    synchronized (writeLock) {
      Map<byte[], AclSnapshot.EntryUpdate> states = new TreeMap<>(Bytes.BYTES_RAWCOMPARATOR);
      try (Table t = masterServices.getConnection().getTable(PermissionStorage.ACL_TABLE_NAME)) {
        for (PendingOp op : ops) {
          byte[] entry = entryKey(op);
          if (states.containsKey(entry)) {
            continue;
          }
          ListMultimap<String, UserPermission> perms =
            PermissionStorage.getPermissions(t, entry, null, null, null, false);
          states.put(entry,
            perms.isEmpty()
              ? AclSnapshot.EntryUpdate.deleted(entry)
              : AclSnapshot.EntryUpdate.replace(entry, perms));
        }
      } catch (TableNotFoundException e) {
        if (ops.stream().anyMatch(op -> !isCleanupOp(op))) {
          throw e;
        }
        // a cleanup batch on a cluster without hbase:acl: there is nothing to clean up and
        // nothing was ever recorded for the deleted tables or namespaces
        return Collections.emptyList();
      }
      List<AclSnapshot.EntryUpdate> updates = new ArrayList<>(states.values());
      if (!updates.isEmpty()) {
        snapshot = snapshot.applyEntryUpdates(updates, ts);
      }
      return updates.stream().map(AclManager::toProto).collect(Collectors.toList());
    }
  }

  private static byte[] entryKey(PendingOp op) {
    switch (op.type) {
      case REMOVE_TABLE:
        return op.table.getName();
      case REMOVE_NAMESPACE:
        return Bytes.toBytes(PermissionStorage.toNamespaceEntry(op.namespace));
      default:
        return PermissionStorage.userPermissionRowKey(op.userPermission.getPermission());
    }
  }

  /**
   * The action set {@link PendingOp#resolveTo} lands on for the change, against the entry state.
   */
  private static Permission.Action[] resolveActions(PendingOp op,
    ListMultimap<String, UserPermission> state) {
    Permission.Action[] current = null;
    for (UserPermission held : state.values()) {
      if (op.userPermission.equalsExceptActions(held)) {
        current = held.getPermission().getActions();
        break;
      }
    }
    Set<Permission.Action> resolved = new TreeSet<>();
    if (current != null) {
      resolved.addAll(Arrays.asList(current));
    }
    if (op.type == AclOp.Type.GRANT) {
      // merged: what the entry holds for the user plus the granted actions
      resolved.addAll(Arrays.asList(op.userPermission.getPermission().getActions()));
    } else {
      // what remains of the user's permission once the revoked actions are dropped
      resolved.removeAll(Arrays.asList(op.userPermission.getPermission().getActions()));
    }
    return resolved.toArray(new Permission.Action[0]);
  }

  /** Records what the change writes into the entry state, so changes queued behind it see it. */
  private static void applyToState(PendingOp op, ListMultimap<String, UserPermission> state) {
    switch (op.type) {
      case REMOVE_TABLE:
      case REMOVE_NAMESPACE:
        state.clear();
        return;
      default:
        break;
    }
    // the change writes one cell of the entry: what it held for the same user and scope is
    // replaced by the value of the write
    for (Map.Entry<String, UserPermission> held : new ArrayList<>(state.entries())) {
      if (op.userPermission.equalsExceptActions(held.getValue())) {
        state.remove(held.getKey(), held.getValue());
      }
    }
    if (op.type == AclOp.Type.GRANT || hasActions(op.userPermission)) {
      state.put(op.userPermission.getUser(), op.userPermission);
    }
  }

  private static boolean hasActions(UserPermission userPermission) {
    return userPermission.getPermission().getActions() != null
      && userPermission.getPermission().getActions().length > 0;
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
