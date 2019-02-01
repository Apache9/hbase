/**
 *
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

package org.apache.hadoop.hbase.replication.regionserver;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.protobuf.ReplicationProtbufUtil;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.regionserver.wal.HLog.Entry;
import org.apache.hadoop.hbase.classification.InterfaceAudience;

/**
 * A {@link org.apache.hadoop.hbase.replication.ReplicationEndpoint}
 * implementation for replicating to different namespaces and/or tables in another HBase cluster.
 * Redirection is configured via redirection rules within the configuration field of
 * {@link org.apache.hadoop.hbase.replication.ReplicationPeerConfig}.
 * A redirection rule is a key-value pair of the following format:
 * "src-ns:table" : "dst-ns:table"
 * This configuration can be provided on the HBase shell, for example, as follows:
 * add_peer '9', CLUSTER_KEY => "localhost:2182:/hbase2",
 * TABLE_CFS => {"ns1:t1" => ["cf1"], "ns2:t2" => ["cf1"]},
 * ENDPOINT_CLASSNAME =>
 * 'org.apache.hadoop.hbase.replication.regionserver.RedirectingInterClusterReplicationEndpoint',
 * CONFIG => {"ns1:t1" => "ns1:t2", "ns2:t2" => "ns3:t2"}
 */
@InterfaceAudience.Private
public class RedirectingInterClusterReplicationEndpoint
    extends HBaseInterClusterReplicationEndpoint {
  private Map<TableName, TableName> tableRedirectionsMap = null;
  private static final Log LOG =
      LogFactory.getLog(RedirectingInterClusterReplicationEndpoint.class);

  @Override
  public void peerConfigUpdated(ReplicationPeerConfig rpc){
    tableRedirectionsMap = new HashMap<>();;
    Iterator<String> keys = rpc.getConfiguration().keySet().iterator();
    while (keys.hasNext()){
      String key = keys.next();
      try {
        byte[] keyBytes = TableName.isLegalFullyQualifiedTableName(Bytes.toBytes(key));
        String val = rpc.getConfiguration().get(key);
        byte[] valBytes = TableName.isLegalFullyQualifiedTableName(Bytes.toBytes(val));
        tableRedirectionsMap.put(TableName.valueOf(keyBytes), TableName.valueOf(valBytes));
        LOG.info("Redirecting replication from table " + key + " to table " + val);
      } catch (IllegalArgumentException e) {
        LOG.warn("Found unknown configuration key " + key + "in ReplicationPeerConfiguration");
      }
    }
  }

  /**
   * @param entries {@link java.util.List} of WAL {@link org.apache.hadoop.hbase.regionserver.wal.HLog.Entry}
   *                that are redirected if a corresponding redirection rule
   *                has been configured
   * @return Number of redirected entries
   */
  private int redirectEntries(final List<Entry> entries) {
    int count = 0;
    for (Entry e : entries) {
      TableName redirectedTablename = tableRedirectionsMap.get(e.getKey().getTablename());
      if (redirectedTablename != null) {
        e.getKey().setTablename(redirectedTablename);
        count++;
      }
    }
    return count;
  }

  @Override
  protected void replicateWALEntry(List<Entry> entries, ReplicationSinkManager.SinkPeer sinkPeer)
      throws IOException {
    // Redirect the edits to another table in the target
    int redirected = this.redirectEntries(entries);
    if (redirected != 0) {
      LOG.info("Redirected " + redirected + " WAL entries to peer");
    }
    ReplicationProtbufUtil
        .replicateWALEntry(sinkPeer.getRegionServer(), entries.toArray(new Entry[entries.size()]));
  }
}