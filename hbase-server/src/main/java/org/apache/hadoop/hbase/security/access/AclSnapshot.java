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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.collect.ArrayListMultimap;
import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableMap;
import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableSet;
import org.apache.hbase.thirdparty.com.google.common.collect.ListMultimap;

/**
 * An immutable ACL snapshot: permission caches for the global, namespace and table scopes, fed from
 * {@code hbase:acl} either as a whole (activation, reload) or through entry level deltas (a
 * grant/revoke). Every update produces a new snapshot, so readers always see one consistent state,
 * a batch of entry updates included, and never block on writers. ACL changes are rare, rebuilding
 * the affected scope on each change is cheap.
 * <p>
 * The process side ACL holder ({@link AclManager} on the master, {@code AclStore} on a region
 * server) owns the current snapshot through an {@link AclView} and publishes changes under its
 * update lock; {@link AuthManager} pins one snapshot per authorization decision.
 * </p>
 */
@InterfaceAudience.Private
public final class AclSnapshot {

  /**
   * Replacement of a single ACL entry, or its deletion. The entry name encoding is the row key of
   * {@code hbase:acl}: table name bytes, {@code @} prefixed namespace name, or
   * {@link PermissionStorage#ACL_GLOBAL_NAME} for global scope.
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
   * the applied updates on a region server, the write timestamp of the newest change on the master.
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

  /**
   * Applies incremental ACL entry updates, each update replaces the whole entry, or removes the
   * entry when it is marked as deleted. The batch is applied atomically: readers see either all of
   * its entries or none of them.
   * @param ts the master assigned timestamp of the writes the batch was built from
   */
  public AclSnapshot applyUpdates(List<EntryUpdate> updates, long ts) {
    Map<String, GlobalPermission> global = new HashMap<>(globalCache);
    Map<String, Map<String, Set<NamespacePermission>>> namespaces = new HashMap<>(namespaceCache);
    Map<TableName, Map<String, Set<TablePermission>>> tables = new HashMap<>(tableCache);
    for (EntryUpdate update : updates) {
      applyEntry(update.getEntry(), update.isDeleted() ? null : update.getPermissions(), global,
        namespaces, tables);
    }
    return new AclSnapshot(ts, ImmutableMap.copyOf(global), ImmutableMap.copyOf(namespaces),
      ImmutableMap.copyOf(tables));
  }

  /**
   * Replaces the whole snapshot with the given content. Callers build the content aside first, so
   * readers never observe a partially loaded snapshot.
   * @param snapshot entry name to its permissions, see {@link PermissionStorage#loadAllSnapshot}
   * @param ts       the timestamp of the new snapshot, see {@link #getTs()}
   */
  public AclSnapshot applyFullSnapshot(Map<byte[], ListMultimap<String, UserPermission>> snapshot,
    long ts) {
    Map<String, GlobalPermission> global = new HashMap<>();
    Map<String, Map<String, Set<NamespacePermission>>> namespaces = new HashMap<>();
    Map<TableName, Map<String, Set<TablePermission>>> tables = new HashMap<>();
    for (Map.Entry<byte[], ListMultimap<String, UserPermission>> e : snapshot.entrySet()) {
      applyEntry(e.getKey(), e.getValue(), global, namespaces, tables);
    }
    return new AclSnapshot(ts, ImmutableMap.copyOf(global), ImmutableMap.copyOf(namespaces),
      ImmutableMap.copyOf(tables));
  }

  private static void applyEntry(byte[] entry, ListMultimap<String, UserPermission> perms,
    Map<String, GlobalPermission> global,
    Map<String, Map<String, Set<NamespacePermission>>> namespaces,
    Map<TableName, Map<String, Set<TablePermission>>> tables) {
    if (PermissionStorage.isGlobalEntry(entry)) {
      // the global entry carries the whole global scope: replace it entirely, so revoked names
      // disappear; same for the namespace and table scopes below
      global.clear();
      if (perms != null) {
        for (Map.Entry<String, UserPermission> e : perms.entries()) {
          global.put(e.getKey(), toGlobalPermission(e.getValue().getPermission()));
        }
      }
    } else if (PermissionStorage.isNamespaceEntry(entry)) {
      String namespace = Bytes.toString(PermissionStorage.fromNamespaceEntry(entry));
      if (perms == null) {
        namespaces.remove(namespace);
      } else {
        namespaces.put(namespace, toPermissionSet(toPermissions(perms)));
      }
    } else {
      if (perms == null) {
        tables.remove(TableName.valueOf(entry));
      } else {
        tables.put(TableName.valueOf(entry), toPermissionSet(toPermissions(perms)));
      }
    }
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
