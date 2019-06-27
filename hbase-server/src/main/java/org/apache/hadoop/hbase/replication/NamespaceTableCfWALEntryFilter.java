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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.regionserver.wal.HLog.Entry;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NamespaceTableCfWALEntryFilter implements WALEntryFilter {

  public static final Logger LOG = LoggerFactory.getLogger(NamespaceTableCfWALEntryFilter.class);
  private final ReplicationPeer peer;

  public NamespaceTableCfWALEntryFilter(ReplicationPeer peer) {
    this.peer = peer;
  }

  @Override
  public Entry filter(Entry entry) {
    TableName tabName = entry.getKey().getTablename();
    ArrayList<KeyValue> kvs = entry.getEdit().getKeyValues();
    String namespace = tabName.getNamespaceAsString();

    ReplicationPeerConfig peerConfig = this.peer.getPeerConfig();
    int size = kvs.size();

    if (peerConfig.replicateAllUserTables()) {
      // Replicate all user tables unless there are exclude-namespaces or exclude-table-cfs
      // config
      Set<String> excludeNamespaces = peerConfig.getExcludeNamespaces();
      Map<TableName, List<String>> excludeTableCFs = peerConfig.getExcludeTableCFsMap();

      // If null means user has explicitly not configured any exclude-namespaces and
      // exclude-table-cfs, so all user tables data are replicated to peer cluster
      if (excludeNamespaces == null && excludeTableCFs == null) {
        return entry;
      }

      // First filter by the exclude-namespaces config
      if (excludeNamespaces != null && excludeNamespaces.contains(namespace)) {
        return null;
      }

      if (excludeTableCFs == null || !excludeTableCFs.containsKey(tabName)) {
        return entry;
      } else {
        List<String> cfs = excludeTableCFs.get(tabName);
        // empty cfs means all cfs of this table are exclude
        if (cfs == null || cfs.isEmpty()) {
          return null;
        }
        for (int i = size - 1; i >= 0; i--) {
          KeyValue kv = kvs.get(i);
          // ignore(remove) kv if its cf is in the exclude cf list
          if (cfs.contains(Bytes.toString(kv.getFamily()))) {
            kvs.remove(i);
          }
        }
      }
    } else {
      // Replicate nothing unless there are namespaces or table-cfs config
      Set<String> namespaces = peerConfig.getNamespaces();
      Map<TableName, List<String>> tableCFs = peerConfig.getTableCFsMap();

      if (namespaces == null && tableCFs == null) {
        return null;
      }

      // First filter by namespaces config
      // If table's namespace in peer config, all the tables data are applicable for replication
      if (namespaces != null && namespaces.contains(namespace)) {
        return entry;
      }

      // Then filter by table-cfs config
      // return null(prevent replicating) if logKey's table isn't in this peer's
      // replicaable namespace list and table list
      if (tableCFs == null || !tableCFs.containsKey(tabName)) {
        return null;
      } else {
        List<String> cfs = (tableCFs == null) ? null : tableCFs.get(tabName);
        for (int i = size - 1; i >= 0; i--) {
          KeyValue kv = kvs.get(i);
          // ignore(remove) kv if its cf isn't in the replicable cf list
          // (empty cfs means all cfs of this table are replicable)
          if ((cfs != null && !cfs.contains(Bytes.toString(kv.getFamily())))) {
            kvs.remove(i);
          }
        }
      }
    }

    if (kvs.size() < size/2) {
      kvs.trimToSize();
    }
    return entry;
  }
}
