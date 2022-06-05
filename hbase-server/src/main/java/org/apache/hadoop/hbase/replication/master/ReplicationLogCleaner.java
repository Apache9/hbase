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
package org.apache.hadoop.hbase.replication.master;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.ServerManager;
import org.apache.hadoop.hbase.master.cleaner.BaseLogCleanerDelegate;
import org.apache.hadoop.hbase.master.replication.ReplicationPeerManager;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationGroupOffset;
import org.apache.hadoop.hbase.replication.ReplicationPeerDescription;
import org.apache.hadoop.hbase.replication.ReplicationQueueData;
import org.apache.hadoop.hbase.replication.ReplicationQueueId;
import org.apache.hadoop.hbase.replication.ReplicationQueueStorage;
import org.apache.hadoop.hbase.replication.ReplicationStorageFactory;
import org.apache.hadoop.hbase.wal.AbstractFSWALProvider;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Predicate;
import org.apache.hbase.thirdparty.com.google.common.collect.Iterables;
import org.apache.hbase.thirdparty.org.apache.commons.collections4.MapUtils;

/**
 * Implementation of a log cleaner that checks if a log is still scheduled for replication before
 * deleting it when its TTL is over.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class ReplicationLogCleaner extends BaseLogCleanerDelegate {
  private static final Logger LOG = LoggerFactory.getLogger(ReplicationLogCleaner.class);
  private ServerManager serverManager;
  private ReplicationPeerManager replicationPeerManager;
  private List<String> peerIds;
  private ReplicationQueueStorage queueStorage;
  private boolean stopped = false;

  @Override
  public void preClean() {
    // server name -> peer Id -> group offsets
    Map<ServerName, Map<String, Map<String, ReplicationGroupOffset>>> onlineServerOffsets =
      new HashMap<>();
    Map<ServerName, Map<String, Map<String, ReplicationGroupOffset>>> deadServerOffsets =
      new HashMap<>();
    try {
      for (ReplicationQueueData queue : queueStorage.listAllQueues()) {
        ReplicationQueueId queueId = queue.getId();
        if (queueId.isRecovered()) {
          deadServerOffsets
            .computeIfAbsent(queueId.getSourceServerName().get(), k -> new HashMap<>())
            .put(queueId.getPeerId(), queue.getOffsets());
        } else {
          onlineServerOffsets
            .computeIfAbsent(queueId.getSourceServerName().get(), k -> new HashMap<>())
            .put(queueId.getPeerId(), queue.getOffsets());
        }
      }
      peerIds = replicationPeerManager.listPeers(null).stream()
        .map(ReplicationPeerDescription::getPeerId).collect(Collectors.toList());
    } catch (ReplicationException e) {
      LOG.warn("Failed to read zookeeper, skipping checking deletable files");
      peerIds = null;
    }
  }

  @Override
  public Iterable<FileStatus> getDeletableFiles(Iterable<FileStatus> files) {
    // all members of this class are null if replication is disabled,
    // so we cannot filter the files
    if (this.getConf() == null || peerIds.isEmpty()) {
      return files;
    }

    if (peerIds == null) {
      return Collections.emptyList();
    }
    return Iterables.filter(files, new Predicate<FileStatus>() {
      @Override
      public boolean apply(FileStatus file) {
        // just for overriding the findbugs NP warnings, as the parameter is marked as Nullable in
        // the guava Predicate.
        if (file == null) {
          return false;
        }
        String wal = file.getPath().getName();
        String walPrefix = AbstractFSWALProvider.getWALPrefixFromWALName(wal);
        boolean logInReplicationQueue = wals.contains(wal);
        if (logInReplicationQueue) {
          LOG.debug("Found up in ZooKeeper, NOT deleting={}", wal);
        }
        return !logInReplicationQueue && (file.getModificationTime() < readZKTimestamp);
      }
    });
  }

  @Override
  public void init(Map<String, Object> params) {
    super.init(params);
    try {
      if (MapUtils.isNotEmpty(params)) {
        Object master = params.get(HMaster.MASTER);
        if (master != null && master instanceof HMaster) {
          zkw = ((HMaster) master).getZooKeeper();
          shareZK = true;
        }
      }
      if (zkw == null) {
        zkw = new ZKWatcher(getConf(), "replicationLogCleaner", null);
      }
      this.queueStorage = ReplicationStorageFactory.getReplicationQueueStorage(zkw, getConf());
    } catch (IOException e) {
      LOG.error("Error while configuring " + this.getClass().getName(), e);
    }
  }

  @InterfaceAudience.Private
  public void setConf(Configuration conf, ZKWatcher zk) {
    super.setConf(conf);
    try {
      this.zkw = zk;
      this.queueStorage = ReplicationStorageFactory.getReplicationQueueStorage(zk, conf);
    } catch (Exception e) {
      LOG.error("Error while configuring " + this.getClass().getName(), e);
    }
  }

  @InterfaceAudience.Private
  public void setConf(Configuration conf, ZKWatcher zk,
    ReplicationQueueStorage replicationQueueStorage) {
    super.setConf(conf);
    this.zkw = zk;
    this.queueStorage = replicationQueueStorage;
  }

  @Override
  public void stop(String why) {
    if (this.stopped) return;
    this.stopped = true;
    if (!shareZK && this.zkw != null) {
      LOG.info("Stopping " + this.zkw);
      this.zkw.close();
    }
  }

  @Override
  public boolean isStopped() {
    return this.stopped;
  }
}
