/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.master.cleaner;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Chore;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.catalog.MetaEditor;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.replication.ReplicationAdmin;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * This chore is to clean up the useless data in hbase:meta which is used by serial replication.
 */
@InterfaceAudience.Private
public class ReplicationMetaCleaner extends Chore {

  private static final Log LOG = LogFactory.getLog(ReplicationMetaCleaner.class);

  private ReplicationAdmin replicationAdmin;
  private MasterServices master;
  private HConnection connection;

  public ReplicationMetaCleaner(MasterServices master, Stoppable stoppable, int period)
      throws IOException {
    super("ReplicationMetaCleaner", period, stoppable);
    this.master = master;
    replicationAdmin = new ReplicationAdmin(master.getConfiguration());
    connection = HConnectionManager.createConnection(master.getConfiguration());
  }

  @Override
  protected void chore() {
    try {
      Map<String, HTableDescriptor> tables = master.getTableDescriptors().getAll();
      Map<String, Set<String>> serialTables = new HashMap<String, Set<String>>();
      for (Map.Entry<String, HTableDescriptor> entry : tables.entrySet()) {
        boolean hasSerialScope = false;
        for (HColumnDescriptor column : entry.getValue().getFamilies()) {
          if (column.getScope() == HConstants.REPLICATION_SCOPE_SERIAL) {
            hasSerialScope = true;
            break;
          }
        }
        if (hasSerialScope) {
          serialTables
              .put(entry.getValue().getTableName().getNameAsString(), new HashSet<String>());
        }
      }
      if (serialTables.isEmpty()) {
        return;
      }

      Map<String, ReplicationPeerConfig> peers = replicationAdmin.listPeerConfigs();
      for (Map.Entry<String, ReplicationPeerConfig> entry : peers.entrySet()) {
        for (Map.Entry<byte[], byte[]> map : entry.getValue().getPeerData()
            .entrySet()) {
          String tableName = Bytes.toString(map.getKey());
          if (serialTables.containsKey(tableName)) {
            serialTables.get(tableName).add(entry.getKey());
            break;
          }
        }
      }

      Map<String, List<Long>> barrierMap = MetaEditor.getAllBarriers(connection);
      for (Map.Entry<String, List<Long>> entry : barrierMap.entrySet()) {
        String encodedName = entry.getKey();
        byte[] encodedBytes = Bytes.toBytes(encodedName);
        boolean canClearRegion = false;
        Map<String, Long> posMap = MetaEditor.getReplicationPositionForAllPeer(
            connection, encodedBytes);
        if (posMap.isEmpty()) {
          continue;
        }

        String tableName = MetaEditor.getSerialReplicationTableName(
            connection, encodedBytes);
        Set<String> confPeers = serialTables.get(tableName);
        if (confPeers == null) {
          // This table doesn't exist or all cf's scope is not serial any more, we can clear meta.
          canClearRegion = true;
        } else {
          if (!allPeersHavePosition(confPeers, posMap)) {
            continue;
          }

          String daughterValue = MetaEditor
              .getSerialReplicationDaughterRegion(connection, encodedBytes);
          if (daughterValue != null) {
            //this region is merged or split
            boolean allDaughterStart = true;
            String[] daughterRegions = daughterValue.split(",");
            for (String daughter : daughterRegions) {
              byte[] region = Bytes.toBytes(daughter);
              if (!MetaEditor.getReplicationBarriers(
                  connection, region).isEmpty() &&
                  !allPeersHavePosition(confPeers,
                      MetaEditor
                          .getReplicationPositionForAllPeer(connection, region))) {
                allDaughterStart = false;
                break;
              }
            }
            if (allDaughterStart) {
              canClearRegion = true;
            }
          }
        }
        if (canClearRegion) {
          Delete delete = new Delete(encodedBytes);
          delete.deleteFamily(HConstants.REPLICATION_POSITION_FAMILY);
          delete.deleteFamily(HConstants.REPLICATION_BARRIER_FAMILY);
          delete.deleteFamily(HConstants.REPLICATION_META_FAMILY);
          HTable metaTable = new HTable(TableName.META_TABLE_NAME, connection);
          metaTable.delete(delete);
          metaTable.close();

        } else {

          // Barriers whose seq is larger than min pos of all peers, and the last barrier whose seq
          // is smaller than min pos should be kept. All other barriers can be deleted.

          long minPos = Long.MAX_VALUE;
          for (Map.Entry<String, Long> pos : posMap.entrySet()) {
            minPos = Math.min(minPos, pos.getValue());
          }
          List<Long> barriers = entry.getValue();
          int index = Collections.binarySearch(barriers, minPos);
          if (index < 0) {
            index = -index - 1;
          }
          Delete delete = new Delete(encodedBytes);
          for (int i = 0; i < index - 1; i++) {
            delete.deleteColumn(HConstants.REPLICATION_BARRIER_FAMILY,
                Bytes.toBytes(barriers.get(i)));
          }
          HTable metaTable = new HTable(TableName.META_TABLE_NAME, connection);
          metaTable.delete(delete);
          metaTable.close();

        }
      }
    } catch (IOException e) {
      LOG.error("Exception during cleaning up.", e);
    }

  }

  private boolean allPeersHavePosition(Set<String> peers, Map<String, Long> posMap)
      throws IOException {
    for (String peer : peers) {
      if (!posMap.containsKey(peer)) {
        return false;
      }
    }
    return true;
  }
}