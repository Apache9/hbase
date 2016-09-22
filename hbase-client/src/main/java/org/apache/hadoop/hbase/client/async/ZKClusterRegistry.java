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
package org.apache.hadoop.hbase.client.async;

import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.toList;
import static org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper.removeMetaData;

import com.google.protobuf.InvalidProtocolBufferException;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.RetryNTimes;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterId;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.zookeeper.ZKConfig;
import org.apache.hadoop.hbase.zookeeper.ZNodePaths;

/**
 * Cache the cluster registry data in memory and use zk watcher to update.
 * <p>
 * The constructor is a blocking operation as we want to fetch cluster id immediately from zk. All
 * other methods will return the cached data in memory immediately.
 */
@InterfaceAudience.Private
class ZKClusterRegistry implements ClusterRegistry {

  private static final Log LOG = LogFactory.getLog(ZKClusterRegistry.class);

  private final CuratorFramework zk;

  private final ZNodePaths znodePaths;

  private final List<NodeCache> metaRegionLocationsWatcher;

  private volatile RegionLocations metaRegionLocations;

  private final PathChildrenCache currentNrHRSWatcher;

  private volatile int currentNrHRS;

  private final NodeCache masterAddressWatcher;

  private volatile ServerName masterAddress;

  private volatile int masterInfoPort;

  private RegionState createRegionState(int replicaId, byte[] data)
      throws InvalidProtocolBufferException {
    if (data == null || data.length == 0) {
      return new RegionState(
          RegionReplicaUtil.getRegionInfoForReplica(HRegionInfo.FIRST_META_REGIONINFO, replicaId),
          RegionState.State.OFFLINE);
    }
    data = removeMetaData(data);
    int prefixLen = ProtobufUtil.lengthOfPBMagic();
    ZooKeeperProtos.MetaRegionServer mrs = ZooKeeperProtos.MetaRegionServer.PARSER.parseFrom(data,
      prefixLen, data.length - prefixLen);
    HBaseProtos.ServerName sn = mrs.getServer();
    ServerName serverName = ServerName.valueOf(sn.getHostName(), sn.getPort(), sn.getStartCode());
    RegionState.State state;
    if (mrs.hasState()) {
      state = RegionState.State.convert(mrs.getState());
    } else {
      state = RegionState.State.OPEN;
    }
    return new RegionState(
        RegionReplicaUtil.getRegionInfoForReplica(HRegionInfo.FIRST_META_REGIONINFO, replicaId),
        state, serverName);
  }

  // All listeners of NodeCaches will be called inside the same thread so no need to lock
  private void updateMetaRegionLocation(int replicaId, ChildData childData) {
    if (childData == null) {
      return;
    }
    RegionState state;
    try {
      state = createRegionState(replicaId, childData.getData());
    } catch (InvalidProtocolBufferException e) {
      LOG.warn("Failed to parse meta region state", e);
      return;
    }
    if (state.getState() != RegionState.State.OPEN || state.getServerName() == null) {
      return;
    }
    RegionLocations oldLocs = metaRegionLocations;
    Deque<HRegionLocation> newLocs = new ArrayDeque<>();
    if (oldLocs != null) {
      Arrays.stream(oldLocs.getRegionLocations())
          .filter(loc -> loc.getRegionInfo().getReplicaId() != replicaId)
          .forEach(loc -> newLocs.add(loc));
    }
    HRegionLocation newLoc = new HRegionLocation(
        RegionReplicaUtil.getRegionInfoForReplica(HRegionInfo.FIRST_META_REGIONINFO, replicaId),
        state.getServerName());
    if (replicaId == HRegionInfo.DEFAULT_REPLICA_ID) {
      newLocs.addFirst(newLoc);
    } else {
      newLocs.addLast(newLoc);
    }
    metaRegionLocations = new RegionLocations(newLocs);
  }

  private void updateCurrentNrHRS() {
    currentNrHRS = currentNrHRSWatcher.getCurrentData().size();
  }

  private void updateMasterAddressAndInfoPort() {
    ChildData childData = masterAddressWatcher.getCurrentData();
    if (childData == null) {
      return;
    }
    byte[] data = childData.getData();
    if (data == null || data.length == 0) {
      return;
    }
    data = removeMetaData(data);
    int prefixLen = ProtobufUtil.lengthOfPBMagic();
    try {
      ZooKeeperProtos.Master masterProto = ZooKeeperProtos.Master.PARSER.parseFrom(data, prefixLen,
        data.length - prefixLen);
      HBaseProtos.ServerName snProto = masterProto.getMaster();
      masterAddress = ServerName.valueOf(snProto.getHostName(), snProto.getPort(),
        snProto.getStartCode());
      masterInfoPort = masterProto.getInfoPort();
    } catch (InvalidProtocolBufferException e) {
      LOG.warn("Failed to parse master address", e);
    }
  }

  ZKClusterRegistry(Configuration conf) {
    this.znodePaths = new ZNodePaths(conf);
    int zkSessionTimeout = conf.getInt(HConstants.ZK_SESSION_TIMEOUT,
      HConstants.DEFAULT_ZK_SESSION_TIMEOUT);
    int zkRetry = conf.getInt("zookeeper.recovery.retry", 3);
    int zkRetryIntervalMs = conf.getInt("zookeeper.recovery.retry.intervalmill", 1000);
    this.zk = CuratorFrameworkFactory.builder()
        .connectString(ZKConfig.getZKQuorumServersString(conf)).sessionTimeoutMs(zkSessionTimeout)
        .retryPolicy(new RetryNTimes(zkRetry, zkRetryIntervalMs))
        .threadFactory(
          Threads.newDaemonThreadFactory(String.format("ZKClusterRegistry-0x%08x", hashCode())))
        .build();
    this.zk.start();

    metaRegionLocationsWatcher = znodePaths.metaReplicaZNodes.entrySet().stream().map(entry -> {
      NodeCache nc = new NodeCache(zk, entry.getValue());
      nc.getListenable().addListener(new NodeCacheListener() {

        @Override
        public void nodeChanged() {
          updateMetaRegionLocation(entry.getKey().intValue(), nc.getCurrentData());
        }
      });
      return nc;
    }).collect(collectingAndThen(toList(), Collections::unmodifiableList));

    currentNrHRSWatcher = new PathChildrenCache(zk, znodePaths.rsZNode, false,
        Threads.newDaemonThreadFactory(String.format("ZKClusterRegistry-0x%08x-RS", hashCode())));
    currentNrHRSWatcher.getListenable().addListener(new PathChildrenCacheListener() {

      @Override
      public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) {
        updateCurrentNrHRS();
      }
    });

    masterAddressWatcher = new NodeCache(zk, znodePaths.masterAddressZNode);
    masterAddressWatcher.getListenable().addListener(new NodeCacheListener() {

      @Override
      public void nodeChanged() {
        updateMasterAddressAndInfoPort();
      }
    });

    try {
      for (NodeCache watcher : metaRegionLocationsWatcher) {
        watcher.start();
      }
      currentNrHRSWatcher.start();
      masterAddressWatcher.start();
    } catch (Exception e) {
      // normal start will not throw any exception.
      throw new AssertionError(e);
    }
  }

  @Override
  public RegionLocations getMetaRegionLocation() {
    return metaRegionLocations;
  }

  @Override
  public String getClusterId() {
    try {
      byte[] data = zk.getData().forPath(znodePaths.clusterIdZNode);
      if (data == null || data.length == 0) {
        return null;
      }
      return ClusterId.parseFrom(removeMetaData(data)).toString();
    } catch (Exception e) {
      LOG.warn("failed to get cluster id", e);
      return null;
    }
  }

  @Override
  public int getCurrentNrHRS() {
    return currentNrHRS;
  }

  @Override
  public ServerName getMasterAddress() {
    return masterAddress;
  }

  @Override
  public int getMasterInfoPort() {
    return masterInfoPort;
  }

  @Override
  public void close() {
    metaRegionLocationsWatcher.forEach(IOUtils::closeQuietly);
    IOUtils.closeQuietly(currentNrHRSWatcher);
    IOUtils.closeQuietly(masterAddressWatcher);
    IOUtils.closeQuietly(zk);
  }
}
