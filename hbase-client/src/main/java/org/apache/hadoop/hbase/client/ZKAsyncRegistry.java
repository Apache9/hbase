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

import static org.apache.hadoop.hbase.HRegionInfo.FIRST_META_REGIONINFO;
import static org.apache.hadoop.hbase.protobuf.ProtobufUtil.lengthOfPBMagic;
import static org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper.removeMetaData;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterId;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.zookeeper.ReadOnlyZKClient;
import org.apache.hadoop.hbase.zookeeper.ZNodePaths;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;

import com.google.common.annotations.VisibleForTesting;

/**
 * Fetch the registry data from zookeeper.
 */
@InterfaceAudience.Private
class ZKAsyncRegistry implements AsyncRegistry {

  private final ReadOnlyZKClient zk;

  private final ZNodePaths znodePaths;

  ZKAsyncRegistry(Configuration conf) {
    this.znodePaths = new ZNodePaths(conf);
    this.zk = new ReadOnlyZKClient(conf);
  }

  private interface Converter<T> {
    T convert(byte[] data) throws Exception;
  }

  private <T> CompletableFuture<T> getAndConvert(String path, Converter<T> converter) {
    CompletableFuture<T> future = new CompletableFuture<>();
    zk.get(path).whenComplete((data, error) -> {
      if (error != null) {
        if (!(error instanceof KeeperException) ||
          ((KeeperException) error).code() != Code.NONODE) {
          future.completeExceptionally(error);
          return;
        }
      }
      try {
        future.complete(converter.convert(data));
      } catch (Exception e) {
        future.completeExceptionally(e);
      }
    });
    return future;
  }

  private static String getClusterId(byte[] data) throws DeserializationException {
    if (data == null || data.length == 0) {
      return null;
    }
    data = removeMetaData(data);
    return ClusterId.parseFrom(data).toString();
  }

  @Override
  public CompletableFuture<String> getClusterId() {
    return getAndConvert(znodePaths.clusterIdZNode, ZKAsyncRegistry::getClusterId);
  }

  @VisibleForTesting
  ReadOnlyZKClient getZKClient() {
    return zk;
  }

  private static ZooKeeperProtos.MetaRegionServer getMetaProto(byte[] data) throws IOException {
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
    getAndConvert(znodePaths.metaServerZNode, ZKAsyncRegistry::getMetaProto)
        .whenComplete((proto, error) -> {
          if (error != null) {
            future.completeExceptionally(error);
            return;
          }
          if (proto == null) {
            future.complete(null);
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

  @Override
  public CompletableFuture<Integer> getCurrentNrHRS() {
    return zk.exists(znodePaths.rsZNode).thenApply(s -> s != null ? s.getNumChildren() : 0);
  }

  private static ZooKeeperProtos.Master getMasterProto(byte[] data) throws IOException {
    if (data == null || data.length == 0) {
      return null;
    }
    data = removeMetaData(data);
    int prefixLen = lengthOfPBMagic();
    return ZooKeeperProtos.Master.PARSER.parseFrom(data, prefixLen, data.length - prefixLen);
  }

  @Override
  public CompletableFuture<ServerName> getMasterAddress() {
    return getAndConvert(znodePaths.masterAddressZNode, ZKAsyncRegistry::getMasterProto)
        .thenApply(proto -> {
          if (proto == null) {
            return null;
          }
          HBaseProtos.ServerName snProto = proto.getMaster();
          return ServerName.valueOf(snProto.getHostName(), snProto.getPort(),
            snProto.getStartCode());
        });
  }

  @Override
  public void close() {
    zk.close();
  }
}