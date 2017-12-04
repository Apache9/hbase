/**
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

import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.ProcedureYieldException;
import org.apache.hadoop.hbase.procedure2.StateMachineProcedure;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.*;

/**
 *
 */
public class ModifyPeerConfigProcedure
    extends StateMachineProcedure<MasterProcedureEnv, ReplicationPeerModificationState> {

  @Override
  protected Flow executeFromState(MasterProcedureEnv env, ReplicationPeerModificationState state)
      throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
    // TODO Implement
    // StateMachineProcedure<MasterProcedureEnv,ReplicationPeerModificationState>.executeFromState
    return null;
  }

  @Override
  protected void rollbackState(MasterProcedureEnv env, ReplicationPeerModificationState state)
      throws IOException, InterruptedException {
    // TODO Implement
    // StateMachineProcedure<MasterProcedureEnv,ReplicationPeerModificationState>.rollbackState

  }

  @Override
  protected ReplicationPeerModificationState getState(int stateId) {
    // TODO Implement
    // StateMachineProcedure<MasterProcedureEnv,ReplicationPeerModificationState>.getState
    return null;
  }

  @Override
  protected int getStateId(ReplicationPeerModificationState state) {
    // TODO Implement
    // StateMachineProcedure<MasterProcedureEnv,ReplicationPeerModificationState>.getStateId
    return 0;
  }

  @Override
  protected ReplicationPeerModificationState getInitialState() {
    // TODO Implement
    // StateMachineProcedure<MasterProcedureEnv,ReplicationPeerModificationState>.getInitialState
    return null;
  }

}
