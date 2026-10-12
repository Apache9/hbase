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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.security.access.Permission.Action;
import org.apache.hadoop.hbase.testclassification.SecurityTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import org.apache.hbase.thirdparty.com.google.common.collect.ArrayListMultimap;
import org.apache.hbase.thirdparty.com.google.common.collect.ListMultimap;

/**
 * Unit test for the apply logic of {@link AclSnapshot}: entry replacement and deletion, whole scope
 * replacement of the global entry, batch semantics and timestamp propagation.
 */
@InterfaceAudience.Private
@Tag(SecurityTests.TAG)
@Tag(SmallTests.TAG)
public class TestAclSnapshot {

  private static final TableName TEST_TABLE = TableName.valueOf("snapshotTable");
  private static final TableName NS_TABLE = TableName.valueOf("snapshotNs", "t");
  private static final String TEST_NAMESPACE = "snapshotNs";

  private static final byte[] GLOBAL_ENTRY = PermissionStorage.ACL_GLOBAL_NAME;
  private static final byte[] NS_ENTRY =
    Bytes.toBytes(PermissionStorage.toNamespaceEntry(TEST_NAMESPACE));

  @Test
  public void testEmptySnapshot() {
    AclSnapshot snapshot = AclSnapshot.empty();
    assertEquals(0, snapshot.getTs());
    assertNull(snapshot.getGlobalPermission("george"));
    assertTrue(snapshot.getTablePermissions(TEST_TABLE, "george").isEmpty());
    assertTrue(snapshot.getNamespacePermissions(TEST_NAMESPACE, "george").isEmpty());
  }

  @Test
  public void testOfMaterializesTheAclTableContent() {
    Map<byte[], ListMultimap<String, UserPermission>> content =
      new TreeMap<>(Bytes.BYTES_RAWCOMPARATOR);
    content.put(GLOBAL_ENTRY, userPerms("george", globalPerm(Action.ADMIN)));
    content.put(NS_ENTRY, userPerms("george", nsPerm(TEST_NAMESPACE, Action.WRITE)));
    content.put(TEST_TABLE.getName(), userPerms("george", tablePerm(TEST_TABLE, Action.READ)));

    AclSnapshot snapshot = AclSnapshot.of(content, 42);
    assertEquals(42, snapshot.getTs());
    assertNotNull(snapshot.getGlobalPermission("george"));
    assertEquals(1, snapshot.getNamespacePermissions(TEST_NAMESPACE, "george").size());
    assertEquals(1, snapshot.getTablePermissions(TEST_TABLE, "george").size());
  }

  @Test
  public void testEntryUpdateReplacesTheWholeEntry() {
    AclSnapshot snapshot = AclSnapshot.empty().applyEntryUpdates(
      Collections.singletonList(AclSnapshot.EntryUpdate.replace(TEST_TABLE.getName(),
        userPerms("george", tablePerm(TEST_TABLE, Action.READ, Action.WRITE)))),
      10);
    assertEquals(1, snapshot.getTablePermissions(TEST_TABLE, "george").size());

    // the entry carries the whole permission set of its scope: a replacement with fewer actions
    // must drop the revoked ones instead of merging into the old content
    snapshot = snapshot.applyEntryUpdates(Collections.singletonList(AclSnapshot.EntryUpdate
      .replace(TEST_TABLE.getName(), userPerms("george", tablePerm(TEST_TABLE, Action.READ)))), 11);
    assertEquals(1, snapshot.getTablePermissions(TEST_TABLE, "george").size());
    assertTrue(hasAction(snapshot.getTablePermissions(TEST_TABLE, "george"), Action.READ));
    assertFalse(hasAction(snapshot.getTablePermissions(TEST_TABLE, "george"), Action.WRITE));

    // ... and it must drop the names that are no longer in it entirely
    snapshot = snapshot.applyEntryUpdates(Collections.singletonList(AclSnapshot.EntryUpdate
      .replace(TEST_TABLE.getName(), userPerms("harold", tablePerm(TEST_TABLE, Action.READ)))), 12);
    assertTrue(snapshot.getTablePermissions(TEST_TABLE, "george").isEmpty());
    assertEquals(1, snapshot.getTablePermissions(TEST_TABLE, "harold").size());
  }

  @Test
  public void testEntryDeletionRemovesTheEntry() {
    Map<byte[], ListMultimap<String, UserPermission>> content =
      new TreeMap<>(Bytes.BYTES_RAWCOMPARATOR);
    content.put(GLOBAL_ENTRY, userPerms("george", globalPerm(Action.ADMIN)));
    content.put(NS_ENTRY, userPerms("george", nsPerm(TEST_NAMESPACE, Action.WRITE)));
    content.put(TEST_TABLE.getName(), userPerms("george", tablePerm(TEST_TABLE, Action.READ)));
    AclSnapshot snapshot = AclSnapshot.of(content, 20);

    snapshot =
      snapshot.applyEntryUpdates(Arrays.asList(AclSnapshot.EntryUpdate.deleted(GLOBAL_ENTRY),
        AclSnapshot.EntryUpdate.deleted(NS_ENTRY),
        AclSnapshot.EntryUpdate.deleted(TEST_TABLE.getName())), 21);
    assertNull(snapshot.getGlobalPermission("george"));
    assertTrue(snapshot.getNamespacePermissions(TEST_NAMESPACE, "george").isEmpty());
    assertTrue(snapshot.getTablePermissions(TEST_TABLE, "george").isEmpty());
  }

  @Test
  public void testGlobalEntryReplacesTheWholeGlobalScope() {
    Map<byte[], ListMultimap<String, UserPermission>> content =
      new TreeMap<>(Bytes.BYTES_RAWCOMPARATOR);
    ListMultimap<String, UserPermission> perms = userPerms("george", globalPerm(Action.ADMIN));
    perms.put("harold", new UserPermission("harold", globalPerm(Action.READ)));
    content.put(GLOBAL_ENTRY, perms);
    AclSnapshot snapshot = AclSnapshot.of(content, 30);
    assertNotNull(snapshot.getGlobalPermission("george"));
    assertNotNull(snapshot.getGlobalPermission("harold"));

    // the global entry is one row for the whole scope: replacing it drops the names no longer in it
    snapshot = snapshot.applyEntryUpdates(Collections.singletonList(
      AclSnapshot.EntryUpdate.replace(GLOBAL_ENTRY, userPerms("george", globalPerm(Action.READ)))),
      31);
    assertNotNull(snapshot.getGlobalPermission("george"));
    assertNull(snapshot.getGlobalPermission("harold"));
    assertFalse(
      hasAction(Collections.singleton(snapshot.getGlobalPermission("george")), Action.ADMIN));
  }

  @Test
  public void testBatchIsAppliedAtomicallyAndLastUpdateOfAnEntryWins() {
    AclSnapshot first = AclSnapshot.empty();
    AclSnapshot snapshot = first.applyEntryUpdates(Arrays.asList(
      AclSnapshot.EntryUpdate.replace(TEST_TABLE.getName(),
        userPerms("george", tablePerm(TEST_TABLE, Action.READ))),
      AclSnapshot.EntryUpdate.replace(TEST_TABLE.getName(),
        userPerms("harold", tablePerm(TEST_TABLE, Action.WRITE))),
      AclSnapshot.EntryUpdate.replace(NS_ENTRY,
        userPerms("george", nsPerm(TEST_NAMESPACE, Action.READ)))),
      40);
    // readers see the whole batch: the later update of the table entry wins
    assertTrue(snapshot.getTablePermissions(TEST_TABLE, "george").isEmpty());
    assertEquals(1, snapshot.getTablePermissions(TEST_TABLE, "harold").size());
    assertEquals(1, snapshot.getNamespacePermissions(TEST_NAMESPACE, "george").size());
    // the batch is atomic: the snapshot it was applied on never changed
    assertTrue(first.getTablePermissions(TEST_TABLE, "george").isEmpty());
  }

  @Test
  public void testTimestamps() {
    AclSnapshot snapshot = AclSnapshot.of(Collections.emptyMap(), 50);
    assertEquals(50, snapshot.getTs());

    snapshot = snapshot.applyEntryUpdates(Collections.singletonList(AclSnapshot.EntryUpdate
      .replace(TEST_TABLE.getName(), userPerms("george", tablePerm(TEST_TABLE, Action.READ)))), 55);
    assertEquals(55, snapshot.getTs());

    // the timestamp clock of a holder moves ahead of the applied content, the content stays
    AclSnapshot advanced = snapshot.withTs(60);
    assertEquals(60, advanced.getTs());
    assertEquals(55, snapshot.getTs());
    assertEquals(1, advanced.getTablePermissions(TEST_TABLE, "george").size());

    // deleting the last entry keeps the batch timestamp
    snapshot = snapshot.applyEntryUpdates(
      Collections.singletonList(AclSnapshot.EntryUpdate.deleted(TEST_TABLE.getName())), 70);
    assertEquals(70, snapshot.getTs());
    assertTrue(snapshot.getTablePermissions(TEST_TABLE, "george").isEmpty());
  }

  @Test
  public void testFullBaseDropsEntriesMissingFromTheContent() {
    Map<byte[], ListMultimap<String, UserPermission>> content =
      new TreeMap<>(Bytes.BYTES_RAWCOMPARATOR);
    content.put(TEST_TABLE.getName(), userPerms("george", tablePerm(TEST_TABLE, Action.READ)));
    AclSnapshot snapshot = AclSnapshot.of(content, 80);
    assertEquals(1, snapshot.getTablePermissions(TEST_TABLE, "george").size());

    // applying only the ns table as a full base: the old table entry is gone, same content
    // applied incrementally would have kept it
    Map<byte[], ListMultimap<String, UserPermission>> newContent =
      new TreeMap<>(Bytes.BYTES_RAWCOMPARATOR);
    newContent.put(NS_TABLE.getName(), userPerms("george", tablePerm(NS_TABLE, Action.READ)));
    AclSnapshot full = AclSnapshot.of(newContent, 81);
    assertTrue(full.getTablePermissions(TEST_TABLE, "george").isEmpty());
    assertEquals(1, full.getTablePermissions(NS_TABLE, "george").size());

    AclSnapshot incremental =
      snapshot.applyEntryUpdates(Collections.singletonList(AclSnapshot.EntryUpdate
        .replace(NS_TABLE.getName(), userPerms("george", tablePerm(NS_TABLE, Action.READ)))), 81);
    assertEquals(1, incremental.getTablePermissions(TEST_TABLE, "george").size());
    assertEquals(1, incremental.getTablePermissions(NS_TABLE, "george").size());
  }

  private static boolean hasAction(Iterable<? extends Permission> perms, Action action) {
    for (Permission perm : perms) {
      if (Arrays.asList(perm.getActions()).contains(action)) {
        return true;
      }
    }
    return false;
  }

  private static Permission tablePerm(TableName table, Action... actions) {
    return Permission.newBuilder(table).withActions(actions).build();
  }

  private static Permission nsPerm(String namespace, Action... actions) {
    return Permission.newBuilder(namespace).withActions(actions).build();
  }

  private static Permission globalPerm(Action... actions) {
    return Permission.newBuilder().withActions(actions).build();
  }

  private static ListMultimap<String, UserPermission> userPerms(String user, Permission perm) {
    ListMultimap<String, UserPermission> perms = ArrayListMultimap.create();
    perms.put(user, new UserPermission(user, perm));
    return perms;
  }
}
