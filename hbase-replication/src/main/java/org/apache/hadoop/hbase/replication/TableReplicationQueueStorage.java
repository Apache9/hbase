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
package org.apache.hadoop.hbase.replication;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class TableReplicationQueueStorage implements ReplicationQueueStorage {

  @Override
  public void removeQueue(ServerName serverName, String queueId) throws ReplicationException {
    // TODO Implement ReplicationQueueStorage.removeQueue
    
  }

  @Override
  public void addWAL(ServerName serverName, String queueId, String fileName)
    throws ReplicationException {
    // TODO Implement ReplicationQueueStorage.addWAL
    
  }

  @Override
  public void removeWAL(ServerName serverName, String queueId, String fileName)
    throws ReplicationException {
    // TODO Implement ReplicationQueueStorage.removeWAL
    
  }

  @Override
  public void setWALPosition(ServerName serverName, String queueId, String fileName, long position,
    Map<String, Long> lastSeqIds) throws ReplicationException {
    // TODO Implement ReplicationQueueStorage.setWALPosition
    
  }

  @Override
  public long getLastSequenceId(String encodedRegionName, String peerId)
    throws ReplicationException {
    // TODO Implement ReplicationQueueStorage.getLastSequenceId
    return 0;
  }

  @Override
  public void setLastSequenceIds(String peerId, Map<String, Long> lastSeqIds)
    throws ReplicationException {
    // TODO Implement ReplicationQueueStorage.setLastSequenceIds
    
  }

  @Override
  public void removeLastSequenceIds(String peerId) throws ReplicationException {
    // TODO Implement ReplicationQueueStorage.removeLastSequenceIds
    
  }

  @Override
  public void removeLastSequenceIds(String peerId, List<String> encodedRegionNames)
    throws ReplicationException {
    // TODO Implement ReplicationQueueStorage.removeLastSequenceIds
    
  }

  @Override
  public long getWALPosition(ServerName serverName, String queueId, String fileName)
    throws ReplicationException {
    // TODO Implement ReplicationQueueStorage.getWALPosition
    return 0;
  }

  @Override
  public List<String> getWALsInQueue(ServerName serverName, String queueId)
    throws ReplicationException {
    // TODO Implement ReplicationQueueStorage.getWALsInQueue
    return null;
  }

  @Override
  public List<String> getAllQueues(ServerName serverName) throws ReplicationException {
    // TODO Implement ReplicationQueueStorage.getAllQueues
    return null;
  }

  @Override
  public Pair<String, SortedSet<String>> claimQueue(ServerName sourceServerName, String queueId,
    ServerName destServerName) throws ReplicationException {
    // TODO Implement ReplicationQueueStorage.claimQueue
    return null;
  }

  @Override
  public void removeReplicatorIfQueueIsEmpty(ServerName serverName) throws ReplicationException {
    // TODO Implement ReplicationQueueStorage.removeReplicatorIfQueueIsEmpty
    
  }

  @Override
  public List<ServerName> getListOfReplicators() throws ReplicationException {
    // TODO Implement ReplicationQueueStorage.getListOfReplicators
    return null;
  }

  @Override
  public Set<String> getAllWALs() throws ReplicationException {
    // TODO Implement ReplicationQueueStorage.getAllWALs
    return null;
  }

  @Override
  public void addPeerToHFileRefs(String peerId) throws ReplicationException {
    // TODO Implement ReplicationQueueStorage.addPeerToHFileRefs
    
  }

  @Override
  public void removePeerFromHFileRefs(String peerId) throws ReplicationException {
    // TODO Implement ReplicationQueueStorage.removePeerFromHFileRefs
    
  }

  @Override
  public void addHFileRefs(String peerId, List<Pair<Path, Path>> pairs)
    throws ReplicationException {
    // TODO Implement ReplicationQueueStorage.addHFileRefs
    
  }

  @Override
  public void removeHFileRefs(String peerId, List<String> files) throws ReplicationException {
    // TODO Implement ReplicationQueueStorage.removeHFileRefs
    
  }

  @Override
  public List<String> getAllPeersFromHFileRefsQueue() throws ReplicationException {
    // TODO Implement ReplicationQueueStorage.getAllPeersFromHFileRefsQueue
    return null;
  }

  @Override
  public List<String> getReplicableHFiles(String peerId) throws ReplicationException {
    // TODO Implement ReplicationQueueStorage.getReplicableHFiles
    return null;
  }

  @Override
  public Set<String> getAllHFileRefs() throws ReplicationException {
    // TODO Implement ReplicationQueueStorage.getAllHFileRefs
    return null;
  }

  @Override
  public String getRsNode(ServerName serverName) {
    // TODO Implement ReplicationQueueStorage.getRsNode
    return null;
  }

}
