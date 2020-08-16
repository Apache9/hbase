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

import static org.apache.hadoop.hbase.HConstants.REPLICATION_PEER_FAMILY;
import static org.apache.hadoop.hbase.client.replication.ReplicationPeerConfigUtil.NONE_SYNC_STATE_BYTES;
import static org.apache.hadoop.hbase.client.replication.ReplicationPeerConfigUtil.getEnabledBytes;
import static org.apache.hadoop.hbase.client.replication.ReplicationPeerConfigUtil.toByteArray;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.client.CheckAndMutate;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.replication.ReplicationPeerConfigUtil;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class TableReplicationPeerStorage extends TableReplicationStorageBase
  implements ReplicationPeerStorage {

  private static final byte[] CONFIG_QUALIFIER = Bytes.toBytes("config");

  private static final byte[] STATE_QUALIFIER = Bytes.toBytes("state");

  private static final byte[] SYNC_STATE_QUALIFIER = Bytes.toBytes("sync_state");

  private static final byte[] NEW_SYNC_STATE_QUALIFIER = Bytes.toBytes("new_sync_state");

  public TableReplicationPeerStorage(ReplicationFactoryConfig config) {
    super(config.getConnection());
  }

  @Override
  public void addPeer(String peerId, ReplicationPeerConfig peerConfig, boolean enabled,
    SyncReplicationState syncReplicationState) throws ReplicationException {
    Put put = new Put(appendPrefix(peerId))
      .addColumn(REPLICATION_PEER_FAMILY, CONFIG_QUALIFIER, toByteArray(peerConfig))
      .addColumn(REPLICATION_PEER_FAMILY, STATE_QUALIFIER, getEnabledBytes(enabled))
      .addColumn(REPLICATION_PEER_FAMILY, SYNC_STATE_QUALIFIER, NONE_SYNC_STATE_BYTES);
    try (Table table = getTable()) {
      table.put(put);
    } catch (IOException e) {
      throw new ReplicationException(e);
    }
  }

  @Override
  public void removePeer(String peerId) throws ReplicationException {
    try (Table table = getTable()) {
      table.delete(new Delete(appendPrefix(peerId)));
    } catch (IOException e) {
      throw new ReplicationException(e);
    }
  }

  private CheckAndMutate.Builder ifPeerExists(String peerId) {
    return CheckAndMutate.newBuilder(appendPrefix(peerId)).ifMatches(REPLICATION_PEER_FAMILY,
      CONFIG_QUALIFIER, CompareOperator.NOT_EQUAL, null);
  }

  @Override
  public void setPeerState(String peerId, boolean enabled) throws ReplicationException {
    CheckAndMutate m = ifPeerExists(peerId).build(new Put(appendPrefix(peerId))
      .addColumn(REPLICATION_PEER_FAMILY, STATE_QUALIFIER, getEnabledBytes(enabled)));
    try (Table table = getTable()) {
      if (!table.checkAndMutate(m).isSuccess()) {
        throw new ReplicationException("Replication peer " + peerId + " does not exist");
      }
    } catch (IOException e) {
      throw new ReplicationException("Unable to change state of the peer with id=" + peerId, e);
    }
  }

  @Override
  public void updatePeerConfig(String peerId, ReplicationPeerConfig peerConfig)
    throws ReplicationException {
    CheckAndMutate m = ifPeerExists(peerId).build(new Put(appendPrefix(peerId))
      .addColumn(REPLICATION_PEER_FAMILY, CONFIG_QUALIFIER, toByteArray(peerConfig)));
    try (Table table = getTable()) {
      if (!table.checkAndMutate(m).isSuccess()) {
        throw new ReplicationException("Replication peer " + peerId + " does not exist");
      }
    } catch (IOException e) {
      throw new ReplicationException(
        "There was a problem trying to save changes to the replication peer " + peerId, e);
    }
  }

  @Override
  public List<String> listPeerIds() throws ReplicationException {
    Scan scan = new Scan().addFamily(REPLICATION_PEER_FAMILY).setFilter(new KeyOnlyFilter());
    List<String> peerIds = new ArrayList<>();
    try (Table table = getTable(); ResultScanner scanner = table.getScanner(scan)) {
      for (;;) {
        Result result = scanner.next();
        if (result == null) {
          break;
        }
        peerIds.add(removePrefix(result.getRow()));
      }
    } catch (IOException e) {
      throw new ReplicationException("Cannot get the list of peers", e);
    }
    return peerIds;
  }

  @Override
  public boolean isPeerEnabled(String peerId) throws ReplicationException {
    Get get = new Get(appendPrefix(peerId)).addColumn(REPLICATION_PEER_FAMILY, STATE_QUALIFIER);
    try (Table table = getTable()) {
      Result result = table.get(get);
      if (result.isEmpty()) {
        throw new ReplicationException("Replication peer " + peerId + " does not exist");
      }
      return Arrays.equals(getEnabledBytes(true),
        result.getValue(REPLICATION_PEER_FAMILY, STATE_QUALIFIER));
    } catch (IOException e) {
      throw new ReplicationException("Unable to get status of the peer with id=" + peerId, e);
    }
  }

  @Override
  public ReplicationPeerConfig getPeerConfig(String peerId) throws ReplicationException {
    Get get = new Get(appendPrefix(peerId)).addColumn(REPLICATION_PEER_FAMILY, CONFIG_QUALIFIER);
    try (Table table = getTable()) {
      Result result = table.get(get);
      if (result.isEmpty()) {
        throw new ReplicationException("Replication peer " + peerId + " does not exist");
      }
      byte[] data = result.getValue(REPLICATION_PEER_FAMILY, CONFIG_QUALIFIER);
      return ReplicationPeerConfigUtil.parsePeerFrom(data);
    } catch (IOException e) {
      throw new ReplicationException("Error getting configuration for peer with id=" + peerId, e);
    } catch (DeserializationException e) {
      throw new ReplicationException(
        "Failed to parse replication peer config for peer with id=" + peerId, e);
    }
  }

  @Override
  public void setPeerNewSyncReplicationState(String peerId, SyncReplicationState state)
    throws ReplicationException {
    CheckAndMutate m =
      ifPeerExists(peerId).build(new Put(appendPrefix(peerId)).addColumn(REPLICATION_PEER_FAMILY,
        SYNC_STATE_QUALIFIER, SyncReplicationState.toByteArray(state)));
    try (Table table = getTable()) {
      if (!table.checkAndMutate(m).isSuccess()) {
        throw new ReplicationException("Replication peer " + peerId + " does not exist");
      }
    } catch (IOException e) {
      throw new ReplicationException(
        "Unable to set the new sync replication state for peer with id=" + peerId, e);
    }
  }

  @Override
  public void transitPeerSyncReplicationState(String peerId) throws ReplicationException {
    SyncReplicationState state = getSyncState(peerId, NEW_SYNC_STATE_QUALIFIER);
    CheckAndMutate m = ifPeerExists(peerId).build(new Put(appendPrefix(peerId))
      .addColumn(REPLICATION_PEER_FAMILY, SYNC_STATE_QUALIFIER,
        SyncReplicationState.toByteArray(state))
      .addColumn(REPLICATION_PEER_FAMILY, NEW_SYNC_STATE_QUALIFIER, NONE_SYNC_STATE_BYTES));
    try (Table table = getTable()) {
      if (!table.checkAndMutate(m).isSuccess()) {
        throw new ReplicationException("Replication peer " + peerId + " does not exist");
      }
    } catch (IOException e) {
      throw new ReplicationException(
        "Error transiting sync replication state for peer with id=" + peerId, e);
    }
  }

  private SyncReplicationState getSyncState(String peerId, byte[] qualifier)
    throws ReplicationException {
    try (Table table = getTable()) {
      byte[] data = table.get(
        new Get(appendPrefix(peerId)).addColumn(REPLICATION_PEER_FAMILY, NEW_SYNC_STATE_QUALIFIER))
        .getValue(REPLICATION_PEER_FAMILY, qualifier);
      if (data == null || data.length == 0) {
        throw new ReplicationException(
          "Replication peer sync state shouldn't be empty, peerId=" + peerId);
      }
      return SyncReplicationState.parseFrom(data);
    } catch (IOException e) {
      throw new ReplicationException("Error getting sync replication state of " +
        Bytes.toString(qualifier) + " for peer with id=" + peerId, e);
    }
  }

  @Override
  public SyncReplicationState getPeerSyncReplicationState(String peerId)
    throws ReplicationException {
    return getSyncState(peerId, SYNC_STATE_QUALIFIER);
  }

  @Override
  public SyncReplicationState getPeerNewSyncReplicationState(String peerId)
    throws ReplicationException {
    return getSyncState(peerId, NEW_SYNC_STATE_QUALIFIER);
  }
}
