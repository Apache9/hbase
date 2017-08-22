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
package org.apache.hadoop.hbase.client;

import static org.apache.hadoop.hbase.HConstants.DEFAULT_ZK_SESSION_TIMEOUT;
import static org.apache.hadoop.hbase.HConstants.ZK_SESSION_TIMEOUT;
import static org.apache.hadoop.hbase.HRegionInfo.FIRST_META_REGIONINFO;
import static org.apache.hadoop.hbase.protobuf.ProtobufUtil.lengthOfPBMagic;
import static org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper.removeMetaData;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.BackgroundPathable;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.retry.RetryNTimes;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterId;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.zookeeper.ZKConfig;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZNodePaths;
import org.apache.zookeeper.data.Stat;

/**
 * Fetch the registry data from zookeeper.
 */
@InterfaceAudience.Private
class ZKAsyncRegistry implements AsyncRegistry {

  private final CuratorFramework zk;

  private final ZNodePaths znodePaths;

  ZKAsyncRegistry(Configuration conf) {
    this.znodePaths = new ZNodePaths(conf);
    int zkSessionTimeout = conf.getInt(ZK_SESSION_TIMEOUT, DEFAULT_ZK_SESSION_TIMEOUT);
    int zkRetry = conf.getInt("zookeeper.recovery.retry", 3);
    int zkRetryIntervalMs = conf.getInt("zookeeper.recovery.retry.intervalmill", 1000);
    this.zk = CuratorFrameworkFactory.builder()
        .connectString(ZKConfig.getZKQuorumServersString(conf)).sessionTimeoutMs(zkSessionTimeout)
        .retryPolicy(new RetryNTimes(zkRetry, zkRetryIntervalMs))
        .threadFactory(
          Threads.newDaemonThreadFactory(String.format("ZKClusterRegistry-0x%08x", hashCode())))
        .build();
    this.zk.start();
  }

  private interface CuratorEventProcessor<T> {
    T process(CuratorEvent event) throws Exception;
  }

  private static <T> CompletableFuture<T> exec(BackgroundPathable<?> opBuilder, String path,
      CuratorEventProcessor<T> processor) {
    CompletableFuture<T> future = new CompletableFuture<>();
    try {
      opBuilder.inBackground((client, event) -> {
        try {
          future.complete(processor.process(event));
        } catch (Exception e) {
          future.completeExceptionally(e);
        }
      }).withUnhandledErrorListener((msg, e) -> future.completeExceptionally(e)).forPath(path);
    } catch (Exception e) {
      future.completeExceptionally(e);
    }
    return future;
  }

  private static String getClusterId(CuratorEvent event) throws DeserializationException {
    byte[] data = event.getData();
    if (data == null || data.length == 0) {
      return null;
    }
    data = removeMetaData(data);
    return ClusterId.parseFrom(data).toString();
  }

  @Override
  public CompletableFuture<String> getClusterId() {
    return exec(zk.getData(), znodePaths.clusterIdZNode, ZKAsyncRegistry::getClusterId);
  }

  private static ZooKeeperProtos.MetaRegionServer getMetaProto(CuratorEvent event)
      throws IOException {
    byte[] data = event.getData();
    if (data == null || data.length == 0) {
      return null;
    }
    data = removeMetaData(data);
    int prefixLen = lengthOfPBMagic();
    return ZooKeeperProtos.MetaRegionServer.PARSER.parseFrom(data, prefixLen,
      data.length - prefixLen);
  }

  private Pair<RegionState.State, ServerName> getStateAndServerName(
      ZooKeeperProtos.MetaRegionServer proto) {
    RegionState.State state;
    if (proto.hasState()) {
      state = RegionState.State.convert(proto.getState());
    } else {
      state = RegionState.State.OPEN;
    }
    HBaseProtos.ServerName snProto = proto.getServer();
    return Pair.newPair(state,
      ServerName.valueOf(snProto.getHostName(), snProto.getPort(), snProto.getStartCode()));
  }

  @Override
  public CompletableFuture<HRegionLocation> getMetaRegionLocation() {
    CompletableFuture<HRegionLocation> future = new CompletableFuture<>();
    exec(zk.getData(), znodePaths.metaServerZNode, ZKAsyncRegistry::getMetaProto)
        .whenComplete((proto, error) -> {
          if (error != null) {
            future.completeExceptionally(error);
            return;
          }
          if (proto == null) {
            future.completeExceptionally(new IOException("Meta znode is null"));
            return;
          }
          Pair<RegionState.State, ServerName> stateAndServerName = getStateAndServerName(proto);
          if (stateAndServerName.getFirst() != RegionState.State.OPEN) {
            future.completeExceptionally(
              new IOException("Meta region is in state " + stateAndServerName.getFirst()));
            return;
          }
          future
              .complete(new HRegionLocation(FIRST_META_REGIONINFO, stateAndServerName.getSecond()));
        });
    return future;
  }

  private static int getCurrentNrHRS(CuratorEvent event) {
    Stat stat = event.getStat();
    return stat != null ? stat.getNumChildren() : 0;
  }

  @Override
  public CompletableFuture<Integer> getCurrentNrHRS() {
    return exec(zk.checkExists(), znodePaths.rsZNode, ZKAsyncRegistry::getCurrentNrHRS);
  }

  private static ZooKeeperProtos.Master getMasterProto(CuratorEvent event) throws IOException {
    byte[] data = event.getData();
    if (data == null || data.length == 0) {
      return null;
    }
    data = removeMetaData(data);
    int prefixLen = lengthOfPBMagic();
    return ZooKeeperProtos.Master.PARSER.parseFrom(data, prefixLen, data.length - prefixLen);
  }

  @Override
  public CompletableFuture<ServerName> getMasterAddress() {
    return exec(zk.getData(), znodePaths.masterAddressZNode, ZKAsyncRegistry::getMasterProto)
        .thenApply(proto -> {
          if (proto == null) {
            return null;
          }
          HBaseProtos.ServerName snProto = proto.getMaster();
          return ServerName.valueOf(snProto.getHostName(), snProto.getPort(),
            snProto.getStartCode());
        });
  }

  private static ZooKeeperProtos.Table.State getTableState(CuratorEvent event) throws IOException {
    byte[] data = event.getData();
    if (data == null || data.length <= 0) return null;
    try {
      data = removeMetaData(data);
      ProtobufUtil.expectPBMagicPrefix(data);
      ZooKeeperProtos.Table.Builder builder = ZooKeeperProtos.Table.newBuilder();
      int magicLen = ProtobufUtil.lengthOfPBMagic();
      ZooKeeperProtos.Table t = builder.mergeFrom(data, magicLen, data.length - magicLen).build();
      return t.getState();
    } catch (DeserializationException e) {
      throw new IOException(e);
    }
  }

  @Override
  public CompletableFuture<Boolean> isTableEnabled(TableName tableName) {
    return exec(zk.getData(), ZKUtil.joinZNode(znodePaths.tableZNode, tableName.getNameAsString()),
      ZKAsyncRegistry::getTableState).thenApply(state -> {
      return state == ZooKeeperProtos.Table.State.ENABLED;
    });
  }

  @Override
  public CompletableFuture<Boolean> isTableDisabled(TableName tableName) {
    return exec(zk.getData(), ZKUtil.joinZNode(znodePaths.tableZNode, tableName.getNameAsString()),
      ZKAsyncRegistry::getTableState).thenApply(state -> {
      return state == ZooKeeperProtos.Table.State.DISABLED;
    });
  }

  @Override
  public void close() {
    zk.close();
  }
}