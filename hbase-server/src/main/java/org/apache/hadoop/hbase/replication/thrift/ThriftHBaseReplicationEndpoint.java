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

package org.apache.hadoop.hbase.replication.thrift;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.replication.regionserver.HBaseInterClusterReplicationEndpoint;
import org.apache.hadoop.hbase.replication.regionserver.ReplicationSinkManager;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

/**
 * A {@link org.apache.hadoop.hbase.replication.BaseReplicationEndpoint} for replication endpoints whose
 * target cluster is an HBase cluster over thrift.
 */
@InterfaceAudience.Private
public class ThriftHBaseReplicationEndpoint extends HBaseInterClusterReplicationEndpoint {

  private static final Log LOG = LogFactory.getLog(ThriftHBaseReplicationEndpoint.class);
  private ThriftClient client;

  @Override public void init(Context context) throws IOException {
    super.init(context);
    client = new ThriftClient(ctx.getConfiguration(), ctx.getPeerId());
  }

  @Override protected void replicateWALEntry(List<HLog.Entry> entries,
      ReplicationSinkManager.SinkPeer sinkPeer) throws IOException {
    client.shipEdits(sinkPeer.getServerName(), entries);
  }

  @Override public synchronized UUID getPeerUUID() {
    UUID result = null;
    try {
      result = client.getPeerClusterUUID(replicationSinkMgr.getReplicationSink().getServerName());
    } catch (IOException e) {
      LOG.warn("Error connecting to peer to get UUID", e);
    }
    return result;
  }
}
