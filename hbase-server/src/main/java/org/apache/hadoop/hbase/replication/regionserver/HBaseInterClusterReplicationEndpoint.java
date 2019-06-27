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

package org.apache.hadoop.hbase.replication.regionserver;

import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.protobuf.ReplicationProtbufUtil;
import org.apache.hadoop.hbase.regionserver.wal.HLog.Entry;
import org.apache.hadoop.hbase.replication.HBaseReplicationEndpoint;
import org.apache.hadoop.hbase.replication.ReplicationEndpoint;
import org.apache.hadoop.hbase.replication.regionserver.ReplicationSinkManager.SinkPeer;
import org.apache.hadoop.ipc.RemoteException;

import com.google.common.annotations.VisibleForTesting;

/**
 * A {@link ReplicationEndpoint} implementation for replicating to another HBase cluster.
 * For the slave cluster it selects a random number of peers
 * using a replication ratio. For example, if replication ration = 0.1
 * and slave cluster has 100 region servers, 10 will be selected.
 * <p/>
 * A stream is considered down when we cannot contact a region server on the
 * peer cluster for more than 55 seconds by default.
 */
@InterfaceAudience.Private
public class HBaseInterClusterReplicationEndpoint extends HBaseReplicationEndpoint {

  private static final Log LOG = LogFactory.getLog(HBaseInterClusterReplicationEndpoint.class);
  private HConnection conn;

  private Configuration conf;

  // How long should we sleep for each retry
  private long sleepForRetries;

  // Maximum number of retries before taking bold actions
  private int maxRetriesMultiplier;
  // Socket timeouts require even bolder actions since we don't want to DDOS
  private int socketTimeoutMultiplier;
  //Metrics for this source
  private MetricsSource metrics;
  // Handles connecting to peer region servers
  protected ReplicationSinkManager replicationSinkMgr;
  private boolean peersSelected = false;
  private boolean dropOnDeletedTables;

  private static final String REPLICATION_ENDPOINT_RPC_TIMEOUT =
      "hbase.replication.endpoint.rpc.timeout";
  private static final int DEFAULT_REPLICATION_ENDPOINT_RPC_TIMEOUT = 300000;

  @Override
  public void init(Context context) throws IOException {
    super.init(context);
    this.conf = HBaseConfiguration.create(ctx.getConfiguration());
    decorateConf();
    this.maxRetriesMultiplier = this.conf.getInt("replication.source.maxretriesmultiplier", 10);
    this.socketTimeoutMultiplier = this.conf.getInt("replication.source.socketTimeoutMultiplier",
        maxRetriesMultiplier * maxRetriesMultiplier);
    // TODO: This connection is replication specific or we should make it particular to
    // replication and make replication specific settings such as compression or codec to use
    // passing Cells.
    this.conn = HConnectionManager.createConnection(this.conf);
    this.sleepForRetries =
        this.conf.getLong("replication.source.sleepforretries", 1000);
    this.dropOnDeletedTables =
        this.conf.getBoolean(HBaseReplicationEndpoint.REPLICATION_DROP_ON_DELETED_TABLE_KEY, false);
    this.metrics = context.getMetrics();
    // ReplicationQueueInfo parses the peerId out of the znode for us
    this.replicationSinkMgr = new ReplicationSinkManager(conn, ctx.getPeerId(), this, this.conf);
  }

  private void decorateConf() {
    String replicationCodec = conf.get(HConstants.REPLICATION_CODEC_CONF_KEY);
    if (StringUtils.isNotEmpty(replicationCodec)) {
      conf.set(HConstants.RPC_CODEC_CONF_KEY, replicationCodec);
    }
    conf.setInt(HConstants.HBASE_RPC_TIMEOUT_KEY,
        conf.getInt(REPLICATION_ENDPOINT_RPC_TIMEOUT, DEFAULT_REPLICATION_ENDPOINT_RPC_TIMEOUT));
  }

  private void connectToPeers() {
    getRegionServers();

    int sleepMultiplier = 1;

    // Connect to peer cluster first, unless we have to stop
    while (this.isRunning() && replicationSinkMgr.getSinks().size() == 0) {
      replicationSinkMgr.chooseSinks();
      if (this.isRunning() && replicationSinkMgr.getSinks().size() == 0) {
        if (sleepForRetries("Waiting for peers", sleepMultiplier)) {
          sleepMultiplier++;
        }
      }
    }
  }


  private void reconnectToPeerCluster() {
    HConnection connection = null;
    try {
      connection = HConnectionManager.createConnection(this.conf);
    } catch (IOException ioe) {
      LOG.warn("Failed to create connection for peer cluster", ioe);
      IOUtils.closeQuietly(connection);
    }
    if (connection != null) {
      this.conn = connection;
    }
  }

  private List<Entry> filterNotExistTableEdits(final List<Entry> oldEntries) {
    List<Entry> entries = new ArrayList<>();
    Map<TableName, Boolean> existMap = new HashMap<>();
    try (HConnection localConn = HConnectionManager.createConnection(ctx.getLocalConfiguration());
        HBaseAdmin localAdmin = new HBaseAdmin(localConn)) {
      for (Entry e : oldEntries) {
        TableName tableName = e.getKey().getTablename();
        boolean exist = true;
        if (existMap.containsKey(tableName)) {
          exist = existMap.get(tableName);
        } else {
          try {
            exist = localAdmin.tableExists(tableName);
            existMap.put(tableName, exist);
          } catch (IOException iox) {
            LOG.warn("Exception checking for local table " + tableName, iox);
          }
        }
        if (exist) {
          entries.add(e);
        } else {
          // Would potentially be better to retry in one of the outer loops
          // and add a table filter there; but that would break the encapsulation,
          // so we're doing the filtering here.
          LOG.warn(
              "Missing table detected at sink, local table also does not exist, filtering edits for table '"
                  + tableName + "'");
        }
      }
    } catch (IOException iox) {
      LOG.warn("Exception when creating connection to check local table", iox);
      return oldEntries;
    }
    return entries;
  }

  /**
   * Check if there's an {@link TableNotFoundException} in the caused by stacktrace.
   */
  @VisibleForTesting
  public static boolean isTableNotFoundException(Throwable io) {
    if (io instanceof RemoteException) {
      io = ((RemoteException) io).unwrapRemoteException();
    }
    if (io != null && io.getMessage().contains("TableNotFoundException")) {
      return true;
    }
    for (;;) {
      if (io == null) {
        return false;
      }
      if (io instanceof TableNotFoundException) {
        return true;
      }
      io = io.getCause();
    }
  }

  /**
   * Do the shipping logic
   */
  @Override
  public boolean replicate(ReplicateContext replicateContext) {
    List<Entry> entries = replicateContext.getEntries();
    int sleepMultiplier = 1;
    while (this.isRunning()) {
      if (!peersSelected) {
        connectToPeers();
        peersSelected = true;
      }

      if (!isPeerEnabled()) {
        if (sleepForRetries("Replication is disabled", sleepMultiplier)) {
          sleepMultiplier++;
        }
        continue;
      }

      if (conn == null || conn.isClosed()) {
        reconnectToPeerCluster();
      }

      SinkPeer sinkPeer = null;
      try {
        sinkPeer = replicationSinkMgr.getReplicationSink();
        if (LOG.isTraceEnabled()) {
          LOG.trace("Replicating " + entries.size() +
              " entries of total size " + replicateContext.getSize());
        }
        replicateWALEntry(entries, sinkPeer);

        // update metrics
        this.metrics.setAgeOfLastShippedOp(entries.get(entries.size()-1).getKey().getWriteTime());
        return true;

      } catch (IOException ioe) {
        LOG.warn("Replicate edites to peer cluster failed.", ioe);
        // Didn't ship anything, but must still age the last time we did
        this.metrics.refreshAgeOfLastShippedOp();
        if (isTableNotFoundException(ioe)) {
          if (dropOnDeletedTables) {
            // Only filter the edits to replicate and don't change the entries in replicateContext
            // as the upper layer rely on it.
            entries = filterNotExistTableEdits(entries);
            if (entries.isEmpty()) {
              LOG.warn("After filter not exist table's edits, 0 edits to replicate, just return");
              return true;
            }
          }
          // fall through and sleep below
        } else {
          if (ioe instanceof SocketTimeoutException) {
            // This exception means we waited for more than 60s and nothing
            // happened, the cluster is alive and calling it right away
            // even for a test just makes things worse.
            sleepForRetries(
              "Encountered a SocketTimeoutException. Since the "
                  + "call to the remote cluster timed out, which is usually "
                  + "caused by a machine failure or a massive slowdown",
              this.socketTimeoutMultiplier);
          } else if (ioe instanceof ConnectException) {
            LOG.warn("Peer is unavailable, rechecking all sinks: ", ioe);
            replicationSinkMgr.chooseSinks();
          } else {
            LOG.warn("Can't replicate because of a local or network error: ", ioe);
          }
        }

        if (sinkPeer != null) {
          replicationSinkMgr.reportBadSink(sinkPeer);
        }
        if (sleepForRetries("Since we are unable to replicate", sleepMultiplier)) {
          sleepMultiplier++;
        }
      }
    }
    return false; // in case we exited before replicating
  }

  protected void replicateWALEntry(List<Entry> entries,
      SinkPeer sinkPeer) throws IOException {
    ReplicationProtbufUtil.replicateWALEntry(sinkPeer.getRegionServer(),
        entries.toArray(new Entry[entries.size()]));
  }


  @Override
  protected void doStop() {
    disconnect(); //don't call super.doStop()
    if (this.conn != null) {
      try {
        this.conn.close();
        this.conn = null;
      } catch (IOException e) {
        LOG.warn("Failed to close the connection");
      }
    }
    notifyStopped();
  }
}
