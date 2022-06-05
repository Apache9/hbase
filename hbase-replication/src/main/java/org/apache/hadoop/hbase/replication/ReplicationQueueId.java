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
package org.apache.hadoop.hbase.replication;

import java.util.Objects;
import java.util.Optional;
import org.apache.hadoop.hbase.ServerName;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Identifier of a replication queue.
 * <p/>
 * Actually, it is just a (RegionServer, PeerId, SourceRegionServer) tuple, where SourceRegionServer
 * could be null. If SourceRegionServer is not null, it indicates that this is a recovered queue.
 */
@InterfaceAudience.Private
public class ReplicationQueueId {

  private final ServerName serverName;

  private final String peerId;

  private final Optional<ServerName> sourceServerName;

  public ReplicationQueueId(ServerName serverName, String peerId) {
    this.serverName = serverName;
    this.peerId = peerId;
    this.sourceServerName = Optional.empty();
  }

  public ReplicationQueueId(ServerName serverName, String peerId, ServerName sourceServerName) {
    this.serverName = serverName;
    this.peerId = peerId;
    this.sourceServerName = Optional.of(sourceServerName);
  }

  public ServerName getServerName() {
    return serverName;
  }

  public String getPeerId() {
    return peerId;
  }

  public Optional<ServerName> getSourceServerName() {
    return sourceServerName;
  }

  public boolean isRecovered() {
    return sourceServerName.isPresent();
  }

  @Override
  public int hashCode() {
    return Objects.hash(peerId, serverName, sourceServerName);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    {
      if (obj == null) return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    ReplicationQueueId other = (ReplicationQueueId) obj;
    return Objects.equals(peerId, other.peerId) && Objects.equals(serverName, other.serverName)
      && Objects.equals(sourceServerName, other.sourceServerName);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder().append(serverName.toString()).append('-').append(peerId);
    sourceServerName.ifPresent(s -> sb.append('-').append(s.toString()));
    return sb.toString();
  }
}
