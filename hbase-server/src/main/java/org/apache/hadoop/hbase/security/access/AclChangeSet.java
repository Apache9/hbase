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

import java.util.List;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProcedureProtos.AclEntryUpdate;

/**
 * One ACL change set to broadcast to the region servers: the entry updates of a drained push round,
 * plus the master assigned timestamp of the writes it was built from. Produced by
 * {@link AclManager} , carried through {@code RefreshAclProcedure} and its per-server children and
 * applied against the region server's {@link AclSnapshot}.
 */
@InterfaceAudience.Private
public final class AclChangeSet {

  private final List<AclEntryUpdate> updates;
  private final boolean full;
  private final long ts;

  public AclChangeSet(List<AclEntryUpdate> updates, boolean full, long ts) {
    this.updates = updates;
    this.full = full;
    this.ts = ts;
  }

  public List<AclEntryUpdate> getUpdates() {
    return updates;
  }

  /**
   * True when {@link #getUpdates()} carries the complete {@code hbase:acl} content and the receiver
   * must replace its whole snapshot with it (apply on an empty base) instead of applying the entry
   * changes on top of the current one. There is one such round: the push a master sends when it
   * becomes active, closing the window of a write the previous master made to the table but did not
   * broadcast before it died; see {@link AclManager#initialize()}.
   */
  public boolean isFull() {
    return full;
  }

  public long getTs() {
    return ts;
  }
}
