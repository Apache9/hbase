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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.master.MockNoopMasterServices;
import org.apache.hadoop.hbase.master.procedure.ProcedurePrepareLatch;
import org.apache.hadoop.hbase.master.procedure.RefreshAclProcedure;
import org.apache.hadoop.hbase.security.Superusers;
import org.apache.hadoop.hbase.testclassification.SecurityTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.yetus.audience.InterfaceAudience;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@InterfaceAudience.Private
@Tag(SecurityTests.TAG)
@Tag(SmallTests.TAG)
public class TestAclManager {

  private static final TableName TEST_TABLE = TableName.valueOf("aclManagerTable");
  private static final TableName TEST_TABLE2 = TableName.valueOf("aclManagerTable2");

  private final List<RefreshAclProcedure> submitted = new ArrayList<>();
  private long nextProcedureId = 100;

  private AclManager aclManager;

  @BeforeEach
  public void setUp() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    conf.set(Superusers.SUPERUSER_CONF_KEY, "admin");
    Superusers.initialize(conf);
    // this test only exercises the submit pipeline: no acl table access, no permission checks
    aclManager = new AclManager(conf, new MockNoopMasterServices(conf)) {
      @Override
      long submitProcedure(RefreshAclProcedure procedure) {
        submitted.add(procedure);
        return nextProcedureId++;
      }
    };
  }

  @Test
  public void testTimestampsAreMonotonic() {
    long first = aclManager.nextTimestamp();
    assertTrue(aclManager.nextTimestamp() > first);
    assertTrue(aclManager.getLastTimestamp() >= first);
  }

  @Test
  public void testChangesAreCoalescedIntoOneProcedure() {
    CompletableFuture<Long> first = grant(TEST_TABLE, "george");
    // the first change starts a procedure right away and its caller gets the id
    assertEquals(1, submitted.size());
    assertEquals(100L, first.getNow(0L));
    assertEquals(1, submitted.get(0).getOpCount());
    assertEquals(0L, submitted.get(0).getPreviousProcedureId());

    // changes arriving while it runs queue up for the next procedure
    CompletableFuture<Long> second = grant(TEST_TABLE, "hubert");
    CompletableFuture<Long> third = grant(TEST_TABLE2, "harold");
    assertEquals(1, submitted.size());
    assertFalse(second.isDone());

    // the queued backlog lands in one procedure, chained behind the running one
    aclManager.onProcedureComplete();
    assertEquals(2, submitted.size());
    assertEquals(101L, second.getNow(0L));
    assertEquals(101L, third.getNow(0L));
    assertEquals(2, submitted.get(1).getOpCount());
    assertEquals(100L, submitted.get(1).getPreviousProcedureId());

    // nothing queued at completion: no further procedure
    aclManager.onProcedureComplete();
    assertEquals(2, submitted.size());
  }

  @Test
  public void testCleanupSubmitsImmediatelyAndChains() {
    grant(TEST_TABLE, "george");
    assertEquals(1, submitted.size());

    // the cleanup of a deleted table must be submitted before its procedure can go on, even
    // while another acl procedure runs: it chains behind it so the writes stay in order
    long cleanup = aclManager.removeTablePermissions(TEST_TABLE);
    assertEquals(2, submitted.size());
    assertEquals(101L, cleanup);
    assertEquals(100L, submitted.get(1).getPreviousProcedureId());
    assertEquals(1, submitted.get(1).getOpCount());

    long namespaceCleanup = aclManager.removeNamespacePermissions("ns");
    assertEquals(3, submitted.size());
    assertEquals(102L, namespaceCleanup);
    assertEquals(101L, submitted.get(2).getPreviousProcedureId());
  }

  @Test
  public void testChangesReachTheTableInQueuedOrder() {
    grant(TEST_TABLE, "george");
    long cleanup = aclManager.removeTablePermissions(TEST_TABLE);
    CompletableFuture<Long> queued = grant(TEST_TABLE, "hubert");
    // the queued change waits for the next procedure
    assertEquals(2, submitted.size());
    assertFalse(queued.isDone());

    // every procedure chains behind the previous one, so the writes reach the table in the
    // order the changes were queued; the backlog is submitted once they all completed
    aclManager.onProcedureComplete();
    assertFalse(queued.isDone());
    aclManager.onProcedureComplete();
    assertEquals(3, submitted.size());
    assertEquals(101L, submitted.get(2).getPreviousProcedureId());
    assertEquals(102L, queued.getNow(0L));
    assertEquals(cleanup, submitted.get(1).getPreviousProcedureId() + 1);
  }

  private CompletableFuture<Long> grant(TableName table, String user) {
    UserPermission perm = new UserPermission(user,
      Permission.newBuilder(table).withActions(Permission.Action.READ).build());
    return aclManager.grant(perm, false, ProcedurePrepareLatch.getNoopLatch());
  }
}
