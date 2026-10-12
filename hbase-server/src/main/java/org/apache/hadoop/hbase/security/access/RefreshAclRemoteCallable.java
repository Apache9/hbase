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
import java.util.List;
import org.apache.hadoop.hbase.executor.EventType;
import org.apache.hadoop.hbase.procedure2.BaseRSProcedureCallable;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.protobuf.InvalidProtocolBufferException;

import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.AclEntryUpdate;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.RefreshAclRemoteStateData;

/**
 * Applies one ACL change set pushed by the master to the local {@link AclStore} (timestamp
 * watermark). The apply is synchronous: when this callable reports completion, the change is
 * already visible to the authorization checks of this region server.
 */
@InterfaceAudience.Private
public class RefreshAclRemoteCallable extends BaseRSProcedureCallable {

  private List<AclSnapshot.EntryUpdate> updates;
  private long ts;

  @Override
  protected byte[] doCall() throws Exception {
    AclStore store = rs.getAclStore();
    if (store == null) {
      // authorization disabled on this server: it keeps no ACL snapshot
      return null;
    }
    store.applyUpdates(ts, updates);
    return null;
  }

  @Override
  protected void initParameter(byte[] parameter) throws InvalidProtocolBufferException {
    RefreshAclRemoteStateData data = RefreshAclRemoteStateData.parseFrom(parameter);
    ts = data.getTs();
    updates = new ArrayList<>(data.getUpdatesCount());
    for (AclEntryUpdate update : data.getUpdatesList()) {
      byte[] entry = update.getEntry().toByteArray();
      updates.add(update.getDeleted()
        ? AclSnapshot.EntryUpdate.deleted(entry)
        : AclSnapshot.EntryUpdate.replace(entry,
          AccessControlUtil.toUserPermission(update.getPerms())));
    }
  }

  @Override
  public EventType getEventType() {
    return EventType.M_RS_REFRESH_ACL;
  }

}
