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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.collect.ArrayListMultimap;
import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableMap;
import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableSet;
import org.apache.hbase.thirdparty.com.google.common.collect.ListMultimap;

/**
 * An immutable ACL snapshot: the materialized permission state of the global, namespace and table
 * scopes of {@code hbase:acl} at one point in time. Every update produces a new snapshot, so
 * readers always see one consistent state, a batch of entry updates included, and never block on
 * writers. ACL changes are rare, rebuilding the affected scope on each change is cheap.
 * <p>
 * The moving parts around this class:
 * <ul>
 * <li>{@link EntryUpdate} is the in-memory form of one ACL change: the replacement of a single
 * entry (one {@code hbase:acl} row), or its deletion. The wire form is {@code AclEntryUpdate}.</li>
 * <li>{@link AclChangeSet} is a batch of those wire form updates to broadcast to the region
 * servers, with the master assigned timestamp it was built from.</li>
 * <li>This class is the state they are applied to: {@link #applyEntryUpdates} applies the entry
 * changes on top of a snapshot, {@link #of} materializes a whole {@code hbase:acl} read (the base a
 * full change set replaces to).</li>
 * </ul>
 * The process side ACL holder ({@link AclManager} on the master, {@code AclStore} on a region
 * server) owns the current snapshot through an {@link AclView} and publishes changes under its
 * update lock; {@link AuthManager} pins one snapshot per authorization decision.
 * </p>
 */
@InterfaceAudience.Private
public final class AclSnapshot {

  /**
   * Replacement of a single ACL entry, or its deletion: the in-memory form of the wire type
   * {@code AclEntryUpdate}. The entry name encoding is the row key of {@code hbase:acl}: table name
   * bytes, {@code @} prefixed namespace name, or {@link PermissionStorage#ACL_GLOBAL_NAME} for the
   * global scope.
   */
  public static final class EntryUpdate {
    private final byte[] entry;
    private final ListMultimap<String, UserPermission> perms;
    private final boolean deleted;

    private EntryUpdate(byte[] entry, ListMultimap<String, UserPermission> perms, boolean deleted) {
      this.entry = entry;
      this.perms = perms;
      this.deleted = deleted;
    }

    /** Replaces the whole permission set of the entry. */
    public static EntryUpdate replace(byte[] entry, ListMultimap<String, UserPermission> perms) {
      return new EntryUpdate(entry, perms, false);
    }

    /** Marks the entry as removed. */
    public static EntryUpdate deleted(byte[] entry) {
      return new EntryUpdate(entry, null, true);
    }

    public byte[] getEntry() {
      return entry;
    }

    public ListMultimap<String, UserPermission> getPermissions() {
      return perms;
    }

    public boolean isDeleted() {
      return deleted;
    }
  }

  private static final AclSnapshot EMPTY =
    new AclSnapshot(0L, ImmutableMap.of(), ImmutableMap.of(), ImmutableMap.of());

  /**
   * The master assigned timestamp of the newest change applied in this snapshot: the watermark of
   * the applied updates on a region server, the timestamp clock of the writes on the master, see
   * {@link AclManager#nextTimestamp()}.
   */
  private final long ts;
  /**
   * Global permissions excluding superuser and supergroup, keyed by user/group entry name. Since
   * every user/group can only have one global permission there is no set per name.
   */
  private final Map<String, GlobalPermission> globalCache;
  /** Namespace permissions: namespace name to (entry name to permissions). */
  private final Map<String, Map<String, Set<NamespacePermission>>> namespaceCache;
  /** Table permissions: table name to (entry name to permissions). */
  private final Map<TableName, Map<String, Set<TablePermission>>> tableCache;

  private AclSnapshot(long ts, Map<String, GlobalPermission> globalCache,
    Map<String, Map<String, Set<NamespacePermission>>> namespaceCache,
    Map<TableName, Map<String, Set<TablePermission>>> tableCache) {
    this.ts = ts;
    this.globalCache = globalCache;
    this.namespaceCache = namespaceCache;
    this.tableCache = tableCache;
  }

  /** The empty snapshot, e.g. before the master has loaded {@code hbase:acl}. */
  public static AclSnapshot empty() {
    return EMPTY;
  }

  /** The master assigned timestamp of the newest change applied in this snapshot. */
  public long getTs() {
    return ts;
  }

  /** This snapshot's content under a new timestamp; advances the holder's timestamp clock. */
  AclSnapshot withTs(long ts) {
    return new AclSnapshot(ts, globalCache, namespaceCache, tableCache);
  }

  /**
   * Applies the changes of a batch of entries on top of this snapshot: each {@link EntryUpdate}
   * replaces the whole permission set of its entry, or removes the entry when it is marked as
   * deleted; the last update of an entry in the batch wins. The batch is applied atomically:
   * readers see either all of its entries or none of them.
   * @param ts the master assigned timestamp the batch was built from
   */
  public AclSnapshot applyEntryUpdates(List<EntryUpdate> updates, long ts) {
    // index the batch by entry name: one update replaces (or drops) the whole scope of its entry
    Map<byte[], EntryUpdate> byEntry = new TreeMap<>(Bytes.BYTES_RAWCOMPARATOR);
    for (EntryUpdate update : updates) {
      byEntry.put(update.getEntry(), update);
    }
    // the single global entry carries the whole global scope: it is replaced as a whole so revoked
    // names disappear. Namespace and table scopes below are one entry per scope, replaced likewise
    ImmutableMap.Builder<String, GlobalPermission> global = ImmutableMap.builder();
    EntryUpdate globalUpdate = byEntry.remove(PermissionStorage.ACL_GLOBAL_NAME);
    if (globalUpdate != null) {
      if (!globalUpdate.isDeleted()) {
        for (Map.Entry<String, UserPermission> e : globalUpdate.getPermissions().entries()) {
          global.put(e.getKey(), toGlobalPermission(e.getValue().getPermission()));
        }
      }
    } else {
      global.putAll(globalCache);
    }
    Map<String, EntryUpdate> namespaceUpdates = new HashMap<>();
    Map<TableName, EntryUpdate> tableUpdates = new HashMap<>();
    for (EntryUpdate update : byEntry.values()) {
      byte[] entry = update.getEntry();
      if (PermissionStorage.isNamespaceEntry(entry)) {
        namespaceUpdates.put(Bytes.toString(PermissionStorage.fromNamespaceEntry(entry)), update);
      } else {
        tableUpdates.put(TableName.valueOf(entry), update);
      }
    }
    ImmutableMap.Builder<String, Map<String, Set<NamespacePermission>>> namespaces =
      ImmutableMap.builder();
    for (Map.Entry<String, Map<String, Set<NamespacePermission>>> e : namespaceCache.entrySet()) {
      if (!namespaceUpdates.containsKey(e.getKey())) {
        namespaces.put(e);
      }
    }
    for (Map.Entry<String, EntryUpdate> e : namespaceUpdates.entrySet()) {
      if (!e.getValue().isDeleted()) {
        namespaces.put(e.getKey(), toPermissionSet(toPermissions(e.getValue().getPermissions())));
      }
    }
    ImmutableMap.Builder<TableName, Map<String, Set<TablePermission>>> tables =
      ImmutableMap.builder();
    for (Map.Entry<TableName, Map<String, Set<TablePermission>>> e : tableCache.entrySet()) {
      if (!tableUpdates.containsKey(e.getKey())) {
        tables.put(e);
      }
    }
    for (Map.Entry<TableName, EntryUpdate> e : tableUpdates.entrySet()) {
      if (!e.getValue().isDeleted()) {
        tables.put(e.getKey(), toPermissionSet(toPermissions(e.getValue().getPermissions())));
      }
    }
    return new AclSnapshot(ts, global.buildKeepingLast(), namespaces.buildKeepingLast(),
      tables.buildKeepingLast());
  }

  /**
   * The snapshot of a full {@code hbase:acl} content, the base a full change set replaces to: the
   * content is applied as entry replacements on the empty snapshot.
   * @param aclTableContent entry name to its permissions, see
   *                        {@link PermissionStorage#loadAllSnapshot}
   * @param ts              the timestamp of the new snapshot, see {@link #getTs()}
   */
  public static AclSnapshot of(Map<byte[], ListMultimap<String, UserPermission>> aclTableContent,
    long ts) {
    List<EntryUpdate> updates = new ArrayList<>(aclTableContent.size());
    for (Map.Entry<byte[], ListMultimap<String, UserPermission>> e : aclTableContent.entrySet()) {
      updates.add(EntryUpdate.replace(e.getKey(), e.getValue()));
    }
    return empty().applyEntryUpdates(updates, ts);
  }

  /** Entry name to its permissions, the immutable replacement of one scope. */
  private static <T extends Permission> Map<String, Set<T>>
    toPermissionSet(ListMultimap<String, T> perms) {
    ImmutableMap.Builder<String, Set<T>> builder = ImmutableMap.builder();
    for (String name : perms.keySet()) {
      builder.put(name, ImmutableSet.copyOf(perms.get(name)));
    }
    return builder.build();
  }

  private static <T extends Permission> ListMultimap<String, T>
    toPermissions(ListMultimap<String, UserPermission> userPerms) {
    ListMultimap<String, T> perms = ArrayListMultimap.create();
    for (Map.Entry<String, UserPermission> e : userPerms.entries()) {
      @SuppressWarnings("unchecked")
      T perm = (T) e.getValue().getPermission();
      perms.put(e.getKey(), perm);
    }
    return perms;
  }

  private static GlobalPermission toGlobalPermission(Permission permission) {
    if (permission instanceof GlobalPermission) {
      return (GlobalPermission) permission;
    }
    return new GlobalPermission(permission.getActions());
  }

  /**
   * Global-scope permission of the given user or group entry name.
   * @return the permission, or {@code null} when the entry has no global grant
   */
  public GlobalPermission getGlobalPermission(String entryName) {
    return globalCache.get(entryName);
  }

  /**
   * Namespace-scope permissions of the given user or group entry name.
   * @return the permissions, empty when the entry has no namespace grant
   */
  public Set<NamespacePermission> getNamespacePermissions(String namespace, String entryName) {
    Map<String, Set<NamespacePermission>> perms = namespaceCache.get(namespace);
    Set<NamespacePermission> entryPerms = perms != null ? perms.get(entryName) : null;
    return entryPerms != null ? entryPerms : Collections.emptySet();
  }

  /**
   * Table-scope permissions of the given user or group entry name.
   * @return the permissions, empty when the entry has no table grant
   */
  public Set<TablePermission> getTablePermissions(TableName table, String entryName) {
    Map<String, Set<TablePermission>> perms = tableCache.get(table);
    Set<TablePermission> entryPerms = perms != null ? perms.get(entryName) : null;
    return entryPerms != null ? entryPerms : Collections.emptySet();
  }
}
