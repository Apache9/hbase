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

import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.procedure.RSProcedureDispatcher.RefreshPeerConfigOperation;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureStateSerializer;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.ProcedureYieldException;
import org.apache.hadoop.hbase.procedure2.RemoteProcedureDispatcher.RemoteOperation;
import org.apache.hadoop.hbase.procedure2.RemoteProcedureDispatcher.RemoteProcedure;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.RefreshPeerConfigRequest;

/**
 *
 */
@InterfaceAudience.Private
public class RefreshPeerConfigProcedure extends Procedure<MasterProcedureEnv>
    implements RemoteProcedure<MasterProcedureEnv, ServerName> {

  private String peerId;

  private RefreshPeerConfigRequest.Type type;

  @Override
  public RemoteOperation remoteCallBuild(MasterProcedureEnv env, ServerName remote) {
    return new RefreshPeerConfigOperation(this, peerId, type, remote);
  }

  @Override
  public void remoteCallCompleted(MasterProcedureEnv env, ServerName remote,
      RemoteOperation response) {
    // TODO Implement
    // RemoteProcedureDispatcher.RemoteProcedure<MasterProcedureEnv,ServerName>.remoteCallCompleted

  }

  @Override
  public void remoteCallFailed(MasterProcedureEnv env, ServerName remote, IOException exception) {
    // TODO Implement
    // RemoteProcedureDispatcher.RemoteProcedure<MasterProcedureEnv,ServerName>.remoteCallFailed

  }

  @Override
  protected Procedure<MasterProcedureEnv>[] execute(MasterProcedureEnv env)
      throws ProcedureYieldException, ProcedureSuspendedException, InterruptedException {
    // TODO Implement Procedure<MasterProcedureEnv>.execute
    return null;
  }

  @Override
  protected void rollback(MasterProcedureEnv env) throws IOException, InterruptedException {
    // TODO Implement Procedure<MasterProcedureEnv>.rollback

  }

  @Override
  protected boolean abort(MasterProcedureEnv env) {
    // TODO Implement Procedure<MasterProcedureEnv>.abort
    return false;
  }

  @Override
  protected void serializeStateData(ProcedureStateSerializer serializer) throws IOException {
    // TODO Implement Procedure<MasterProcedureEnv>.serializeStateData

  }

  @Override
  protected void deserializeStateData(ProcedureStateSerializer serializer) throws IOException {
    // TODO Implement Procedure<MasterProcedureEnv>.deserializeStateData

  }

}
