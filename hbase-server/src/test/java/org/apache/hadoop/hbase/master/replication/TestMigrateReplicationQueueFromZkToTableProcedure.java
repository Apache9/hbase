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
package org.apache.hadoop.hbase.master.replication;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.PeerProcedureInterface;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.ProcedureTestingUtility;
import org.apache.hadoop.hbase.procedure2.ProcedureYieldException;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos.ProcedureState;

@Category({ MasterTests.class, MediumTests.class })
public class TestMigrateReplicationQueueFromZkToTableProcedure {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestMigrateReplicationQueueFromZkToTableProcedure.class);

  private static final HBaseTestingUtil UTIL = new HBaseTestingUtil();

  @BeforeClass
  public static void setupCluster() throws Exception {
    UTIL.startMiniCluster(1);
  }

  @AfterClass
  public static void cleanupTest() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  private ProcedureExecutor<MasterProcedureEnv> getMasterProcedureExecutor() {
    return UTIL.getHBaseCluster().getMaster().getMasterProcedureExecutor();
  }

  @Before
  public void setup() throws Exception {
    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(getMasterProcedureExecutor(), false);
  }

  @After
  public void tearDown() throws Exception {
    ProcedureTestingUtility.setKillAndToggleBeforeStoreUpdate(getMasterProcedureExecutor(), false);
  }

  private static CountDownLatch PEER_PROC_ARRIVE;

  private static CountDownLatch PEER_PROC_RESUME;

  public static final class FakePeerProcedure extends Procedure<MasterProcedureEnv>
    implements PeerProcedureInterface {

    private String peerId;

    public FakePeerProcedure() {
    }

    public FakePeerProcedure(String peerId) {
      this.peerId = peerId;
    }

    @Override
    public String getPeerId() {
      return peerId;
    }

    @Override
    public PeerOperationType getPeerOperationType() {
      return PeerOperationType.UPDATE_CONFIG;
    }

    @Override
    protected Procedure<MasterProcedureEnv>[] execute(MasterProcedureEnv env)
      throws ProcedureYieldException, ProcedureSuspendedException, InterruptedException {
      PEER_PROC_ARRIVE.countDown();
      PEER_PROC_RESUME.await();
      return null;
    }

    @Override
    protected void rollback(MasterProcedureEnv env) throws IOException, InterruptedException {
      throw new UnsupportedOperationException();
    }

    @Override
    protected boolean abort(MasterProcedureEnv env) {
      return false;
    }

    @Override
    protected void serializeStateData(ProcedureStateSerializer serializer) throws IOException {
    }

    @Override
    protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
    }
  }

  @Test
  public void testWaitUntilNoPeerProcedure() throws Exception {
    PEER_PROC_ARRIVE = new CountDownLatch(1);
    PEER_PROC_RESUME = new CountDownLatch(1);
    ProcedureExecutor<MasterProcedureEnv> procExec = getMasterProcedureExecutor();
    procExec.submitProcedure(new FakePeerProcedure());
    PEER_PROC_ARRIVE.await();
    MigrateReplicationQueueFromZkToTableProcedure proc =
      new MigrateReplicationQueueFromZkToTableProcedure();
    procExec.submitProcedure(proc);
    // make sure we will wait until there is no peer related procedures before proceeding
    UTIL.waitFor(30000, () -> proc.getState() == ProcedureState.WAITING_TIMEOUT);
    PEER_PROC_RESUME.countDown();
    UTIL.waitFor(30000, () -> proc.isSuccess());
  }
}
