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
import java.util.TreeMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.master.MasterServices;
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

/**
 * Master side ACL holder: the {@code hbase:acl} table is the authoritative store, this class owns
 * the master's {@link AclSnapshot} of it and drives everything around it:
 * <ul>
 * <li>performs permission updates and queries on {@code hbase:acl} (all writes go through this
 * class);</li>
 * <li>assigns the monotonic timestamps of its writes;</li>
 * <li>keeps its snapshot in step with every write (the entry state is applied synchronously, so the
 * master never lags behind its own writes and needs no periodic reload);</li>
 * <li>pushes the changes to all region servers through a single in-flight
 * {@link RefreshAclProcedure}: updates arriving while a push is running are merged into the next
 * one, so at most one broadcast is in flight and pushes are naturally coalesced.</li>
 * </ul>
 * The refresh procedure only updates the region servers; the master applies each change to its own
 * snapshot at write time and is reloaded from the table when it becomes active. Activation also
 * pushes one full round to all region servers, see {@link #initialize()}.
 * <p>
 * The whole write path (timestamp assignment, table write, entry read back, local apply, queueing
 * for broadcast) is serialized, so writes apply locally in their timestamp order and a concurrent
 * write can never overwrite a newer state with an older one. The lock is held across the table
 * write; ACL changes are rare admin operations, this does not hurt.
 * </p>
 * Servers that miss a push (crash, abandonment) converge through their startup load and the
 * periodic reload. Direct writes to {@code hbase:acl} that bypass this class are only picked up by
 * that periodic reload.
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

  /** Latest update per entry; guarded by {@code this}. */
  private final Map<byte[], PendingUpdate> pending = new TreeMap<>(Bytes.BYTES_RAWCOMPARATOR);

  private boolean pushInFlight;

  private static final class PendingUpdate {
    private final long ts;
    private final AclSnapshot.EntryUpdate update;

    PendingUpdate(long ts, AclSnapshot.EntryUpdate update) {
      this.ts = ts;
      this.update = update;
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
   * Seeds the timestamp clock, loads the master's snapshot from {@code hbase:acl} and pushes one
   * full round to all region servers. Called when this master becomes active.
   * <p>
   * The full round closes the crash window of the previous master: a write it made to the table but
   * did not broadcast (crash between the table write and the refresh procedure) would otherwise
   * linger on the region servers until their periodic reload, so for example a revoked permission
   * could survive its revocation. It is submitted inside the write lock together with the snapshot
   * swap, so a later write is queued after it and applies on top of it.
   * </p>
   */
  public void initialize() throws IOException {
    if (!AccessChecker.isAuthorizationSupported(conf)) {
      // the flag only turns the checks off (HBASE-13275): no snapshot to load, no region server
      // stores to push to, the writes below still record into hbase:acl and just need a clock
      snapshot = snapshot.withTs(EnvironmentEdgeManager.currentTime());
      return;
    }
    PermissionStorage.AclTableContent content;
    long seed;
    synchronized (writeLock) {
      try {
        content = PermissionStorage.loadAllSnapshot(masterServices.getConnection());
      } catch (TableNotFoundException e) {
        // a fresh cluster: the acl table is created later by the AccessController postStartMaster
        // hook
        content = new PermissionStorage.AclTableContent(Collections.emptyMap(), 0L);
      }
      seed = Math.max(EnvironmentEdgeManager.currentTime(), content.getMaxTimestamp() + 1);
      // serialized with the write path, so a write either lands before the scan (and is part of
      // the snapshot) or applies its entry update after the swap
      snapshot = AclSnapshot.of(content.getPermissions(), seed);
      // the push timestamp is the snapshot's own, above every write it contains: a push is never
      // dropped as stale by an RS and later writes get higher timestamps and apply on top of it
      submitFullPush(new AclChangeSet(toProto(content.getPermissions()), true, seed));
    }
    LOG.info("AclManager initialized, timestamps start at {}", seed);
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

  /** Grants the given permission, optionally merging it into the existing actions. */
  public void grant(UserPermission userPermission, boolean mergeExistingPermissions)
    throws IOException {
    synchronized (writeLock) {
      byte[] entry = PermissionStorage.userPermissionRowKey(userPermission.getPermission());
      long ts = nextTimestamp();
      try (Table t = masterServices.getConnection().getTable(PermissionStorage.ACL_TABLE_NAME)) {
        PermissionStorage.addUserPermission(conf, userPermission, t, mergeExistingPermissions, ts);
        announce(t, entry, ts);
      }
    }
  }

  /** Revokes the given permission. */
  public void revoke(UserPermission userPermission) throws IOException {
    synchronized (writeLock) {
      byte[] entry = PermissionStorage.userPermissionRowKey(userPermission.getPermission());
      long ts = nextTimestamp();
      try (Table t = masterServices.getConnection().getTable(PermissionStorage.ACL_TABLE_NAME)) {
        PermissionStorage.removeUserPermission(conf, userPermission, t, ts);
        announce(t, entry, ts);
      }
    }
  }

  /** Drops all permissions of a removed table. */
  public void removeTablePermissions(TableName tableName) throws IOException {
    synchronized (writeLock) {
      long ts = nextTimestamp();
      try (Table t = masterServices.getConnection().getTable(PermissionStorage.ACL_TABLE_NAME)) {
        PermissionStorage.removeTablePermissions(conf, tableName, t, ts);
        announce(t, tableName.getName(), ts);
      }
    }
  }

  /** Drops all permissions of a removed namespace. */
  public void removeNamespacePermissions(String namespace) throws IOException {
    synchronized (writeLock) {
      byte[] entry = Bytes.toBytes(PermissionStorage.toNamespaceEntry(namespace));
      long ts = nextTimestamp();
      try (Table t = masterServices.getConnection().getTable(PermissionStorage.ACL_TABLE_NAME)) {
        PermissionStorage.removeNamespacePermissions(conf, namespace, t, ts);
        announce(t, entry, ts);
      }
    }
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

  /**
   * Reads the entry state back after the write landed, applies it to the local snapshot and queues
   * it for broadcast to the region servers.
   */
  private void announce(Table t, byte[] entry, long ts) throws IOException {
    ListMultimap<String, UserPermission> perms =
      PermissionStorage.getPermissions(t, entry, null, null, null, false);
    AclSnapshot.EntryUpdate update = perms.isEmpty()
      ? AclSnapshot.EntryUpdate.deleted(entry)
      : AclSnapshot.EntryUpdate.replace(entry, perms);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Announcing acl entry {} update at ts {}, deleted={}", Bytes.toStringBinary(entry),
        ts, update.isDeleted());
    }
    // the write is done and the write path is serialized, the state read back is the current one:
    // apply it right away so the master's snapshot never lags behind its own writes
    snapshot = snapshot.applyEntryUpdates(Collections.singletonList(update), ts);
    if (AccessChecker.isAuthorizationSupported(conf)) {
      onEntryUpdate(ts, update);
    }
  }

  /**
   * Queues an updated entry for broadcast to all region servers. Called by the write path after it
   * applied the update locally.
   */
  public void onEntryUpdate(long ts, AclSnapshot.EntryUpdate update) {
    synchronized (this) {
      pending.put(update.getEntry(), new PendingUpdate(ts, update));
      maybeSubmitPush();
    }
  }

  /**
   * Takes all pending entry updates as one change set and returns {@code null} when there are none.
   * Called by {@link RefreshAclProcedure} as its first step; the change set is persisted in the
   * procedure store, so it survives a master failover and is replayed at-least-once.
   */
  public synchronized AclChangeSet drainPending() {
    if (pending.isEmpty()) {
      return null;
    }
    long ts = 0L;
    List<AclEntryUpdate> updates = new ArrayList<>(pending.size());
    for (PendingUpdate p : pending.values()) {
      updates.add(toProto(p.update));
      ts = Math.max(ts, p.ts);
    }
    pending.clear();
    return new AclChangeSet(updates, false, ts);
  }

  /**
   * Called when a push procedure terminates (success or not); submits the next one when updates
   * piled up meanwhile.
   */
  public synchronized void onPushComplete() {
    pushInFlight = false;
    maybeSubmitPush();
  }

  public long getLastTimestamp() {
    return current().getTs();
  }

  private void maybeSubmitPush() {
    if (pushInFlight || pending.isEmpty()) {
      return;
    }
    pushInFlight = true;
    submitPushProcedure(new RefreshAclProcedure());
  }

  private synchronized void submitFullPush(AclChangeSet changeSet) {
    pushInFlight = true;
    submitPushProcedure(new RefreshAclProcedure(changeSet));
  }

  /** Visible for testing: hands the push procedure over to the procedure executor. */
  void submitPushProcedure(RefreshAclProcedure procedure) {
    masterServices.getMasterProcedureExecutor().submitProcedure(procedure);
  }

  private static List<AclEntryUpdate>
    toProto(Map<byte[], ListMultimap<String, UserPermission>> snapshot) {
    List<AclEntryUpdate> updates = new ArrayList<>(snapshot.size());
    for (Map.Entry<byte[], ListMultimap<String, UserPermission>> e : snapshot.entrySet()) {
      updates.add(AclEntryUpdate.newBuilder().setEntry(ByteString.copyFrom(e.getKey()))
        .setPerms(AccessControlUtil.toUserTablePermissions(e.getValue())).build());
    }
    return updates;
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
