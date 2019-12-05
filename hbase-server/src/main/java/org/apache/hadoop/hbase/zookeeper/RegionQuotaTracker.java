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
package org.apache.hadoop.hbase.zookeeper;

import com.google.protobuf.InvalidProtocolBufferException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.QuotaProtos.ThrottleRequest;
import org.apache.hadoop.hbase.quotas.QuotaLimiter;
import org.apache.hadoop.hbase.quotas.RegionQuotaSettings;
import org.apache.hadoop.hbase.quotas.ThrottleType;
import org.apache.hadoop.hbase.quotas.TimeBasedLimiter;
import org.apache.hadoop.hbase.zookeeper.ZKUtil.NodeAndData;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.TimeUnit;
import org.apache.hadoop.hbase.protobuf.generated.QuotaProtos.Throttle;
import org.apache.hadoop.hbase.protobuf.generated.QuotaProtos.TimedQuota;

/**
 * Tracks the region quota which is hard limit up in ZK
 */
@InterfaceAudience.Private
public class RegionQuotaTracker extends ZooKeeperListener {
  private static final Logger LOG = LoggerFactory.getLogger(RegionQuotaTracker.class);
  private final String node;
  private CountDownLatch initialized = new CountDownLatch(1);
  private ConcurrentHashMap<String, QuotaLimiter> regionReadLimiters = new ConcurrentHashMap<>();
  private ConcurrentHashMap<String, QuotaLimiter> regionWriteLimiters = new ConcurrentHashMap<>();

  private static final String READ = "R-";
  private static final String WRITE = "W-";

  public RegionQuotaTracker(ZooKeeperWatcher watcher, Abortable abortable) {
    super(watcher);
    this.node = watcher.znodePaths.regionQuotaZNode;
  }

  public synchronized void start() {
    this.watcher.registerListener(this);
    try {
      if (ZKUtil.watchAndCheckExists(watcher, node)) {
        refreshRegionQuotas();
      }
    } catch (KeeperException e) {
      watcher.abort("Unexpected exception during initialization, aborting", e);
    } finally {
      initialized.countDown();
    }
  }

  public void setRegionQuota(String regionName, ThrottleType type, ThrottleRequest throttle)
      throws KeeperException, IOException {
    String regionZNode = generateZNodeName(regionName, type);
    ZKUtil.createSetData(watcher, regionZNode, throttleToBytes(throttle));
  }

  public void removeRegionQuota(String regionName, ThrottleType type)
      throws KeeperException, IOException {
    String regionZNode = generateZNodeName(regionName, type);
    ZKUtil.deleteNodeFailSilent(watcher, regionZNode);
  }

  public void removeRegionQuota(String regionName) throws KeeperException, IOException {
    removeRegionQuota(regionName, ThrottleType.WRITE_NUMBER);
    removeRegionQuota(regionName, ThrottleType.READ_NUMBER);
  }

  public List<RegionQuotaSettings> listRegionQuotas() throws KeeperException {
    List<RegionQuotaSettings> regionQuotaSettings = new ArrayList<>();
    List<NodeAndData> existing = ZKUtil.getChildDataAndWatchForNewChildren(watcher, node);
    if (existing != null) {
      for (NodeAndData nodeAndData : existing) {
        RegionQuotaSettings regionQuota =
            parseZNodeToRegionQuota(nodeAndData.getNode(), nodeAndData.getData());
        if (regionQuota != null) {
          regionQuotaSettings.add(regionQuota);
        }
      }
    }
    return regionQuotaSettings;
  }

  public ConcurrentHashMap<String, QuotaLimiter> getRegionReadLimiters() {
    return regionReadLimiters;
  }

  public ConcurrentHashMap<String, QuotaLimiter> getRegionWriteLimiters() {
    return regionWriteLimiters;
  }

  @Override
  public void nodeCreated(String path) {
    waitUntilInitialized();
    try {
      if (path.equals(node)) {
        refreshRegionQuotas();
      } else if (node.equals(ZKUtil.getParent(path))) {
        refreshRegionQuotas(path);
      }
    } catch (KeeperException e) {
      LOG.warn("Refresh region quotas error when node {} is created", path, e);
    }
  }

  @Override
  public void nodeDataChanged(String path) {
    waitUntilInitialized();
    if (node.equals(ZKUtil.getParent(path))) {
      refreshRegionQuotas(path);
    }
  }

  @Override
  public void nodeChildrenChanged(String path) {
    waitUntilInitialized();
    try {
      if (node.equals(path)) {
        refreshRegionQuotas();
      }
    } catch (KeeperException e) {
      LOG.warn("Refresh region quotas error when node children {} is created", path, e);
    }
  }

  @Override
  public synchronized void nodeDeleted(String path) {
    waitUntilInitialized();
    if (node.equals(ZKUtil.getParent(path))) {
      removeRegionQuotaZNode(path);
    } else if (node.equals(path)) {
      regionReadLimiters.clear();
      regionWriteLimiters.clear();
    }
  }

  private String generateZNodeName(String regionName, ThrottleType type) throws IOException {
    String typeStr;
    if (type == ThrottleType.READ_NUMBER) {
      typeStr = READ;
    } else if (type == ThrottleType.WRITE_NUMBER) {
      typeStr = WRITE;
    } else {
      throw new DoNotRetryIOException(
          new UnsupportedOperationException("Unknown throttle type: " + type));
    }
    return ZKUtil.joinZNode(node, typeStr + regionName);
  }

  private RegionQuotaSettings parseZNodeToRegionQuota(String path, byte[] data) {
    String node = ZKUtil.getNodeName(path);
    ThrottleType throttleType = getThrottleTypeFromZNode(node);
    String region = getRegionFromZNode(node);
    if (throttleType != null && region != null) {
      ThrottleRequest throttle = bytesToThrottle(node, data);
      if (throttle != null) {
        return new RegionQuotaSettings(region, throttle);
      }
    }
    return null;
  }

  private void refreshRegionQuotas(String znode) {
    try {
      byte[] existing = ZKUtil.getDataAndWatch(watcher, znode);
      RegionQuotaSettings regionQuota = parseZNodeToRegionQuota(znode, existing);
      if (regionQuota != null) {
        refreshRegionQuota(regionQuota);
      }
    } catch (KeeperException e) {
      LOG.error("Refresh region quota error for znode: {}", znode, e);
    }
  }

  private void refreshRegionQuotas() throws KeeperException {
    List<RegionQuotaSettings> regionQuotas = listRegionQuotas();
    for (RegionQuotaSettings regionQuota : regionQuotas) {
      refreshRegionQuota(regionQuota);
    }
  }

  private void refreshRegionQuota(RegionQuotaSettings regionQuota) {
    QuotaLimiter quotaLimiter = buildQuotaLimiter(regionQuota);
    if (quotaLimiter != null) {
      if (regionQuota.getThrottleType() == ThrottleType.READ_NUMBER) {
        regionReadLimiters.put(regionQuota.getRegionName(), quotaLimiter);
      } else if (regionQuota.getThrottleType() == ThrottleType.WRITE_NUMBER) {
        regionWriteLimiters.put(regionQuota.getRegionName(), quotaLimiter);
      } else {
        LOG.warn("Unknown throttle type {} for region {}", regionQuota.getThrottleType(),
          regionQuota.getRegionName());
      }
    }
  }

  private void removeRegionQuotaZNode(String znode) {
    String node = ZKUtil.getNodeName(znode);
    ThrottleType throttleType = getThrottleTypeFromZNode(node);
    String region = getRegionFromZNode(node);
    if (throttleType == null || region == null) {
      return;
    }
    if (throttleType == ThrottleType.READ_NUMBER) {
      regionReadLimiters.remove(region);
    } else if (throttleType == ThrottleType.WRITE_NUMBER) {
      regionWriteLimiters.remove(region);
    } else {
      LOG.warn("Unknown throttle type {} for znode {}", throttleType, znode);
    }
  }

  private byte[] throttleToBytes(ThrottleRequest throttle) {
    return ProtobufUtil.prependPBMagic(throttle.toByteArray());
  }

  private ThrottleRequest bytesToThrottle(String znode, byte[] data) {
    try {
      int pblen = ProtobufUtil.lengthOfPBMagic();
      return ThrottleRequest.newBuilder().mergeFrom(data, pblen, data.length - pblen).build();
    } catch (InvalidProtocolBufferException e) {
      LOG.warn("Invalid throttle bytes for znode: "+znode);
      return null;
    }
  }

  private ThrottleType getThrottleTypeFromZNode(String znode) {
    if (znode.startsWith(READ)) {
      return ThrottleType.READ_NUMBER;
    } else if (znode.startsWith(WRITE)) {
      return ThrottleType.WRITE_NUMBER;
    } else {
      LOG.warn("Unknown throttle type for znode {}", znode);
      return null;
    }
  }

  private String getRegionFromZNode(String znode) {
    if (znode.startsWith(READ)) {
      return znode.substring(READ.length());
    } else if (znode.startsWith(WRITE)) {
      return znode.substring(READ.length());
    } else {
      LOG.warn("Unknown throttle type for znode {}", znode);
      return null;
    }
  }

  private void waitUntilInitialized() {
    try {
      initialized.await();
    } catch (InterruptedException e) {
      LOG.error("wait until initialized error", e);
    }
  }

  private QuotaLimiter buildQuotaLimiter(RegionQuotaSettings regionQuota) {
    ThrottleType throttleType = regionQuota.getThrottleType();
    long limit = regionQuota.getLimit();
    TimeUnit timeUnit = regionQuota.getThrottle().getTimedQuota().getTimeUnit();
    if (throttleType == ThrottleType.READ_NUMBER) {
      Throttle throttle = Throttle.newBuilder()
          .setReadNum(TimedQuota.newBuilder().setSoftLimit(limit).setTimeUnit(timeUnit)).build();
      return TimeBasedLimiter.fromThrottle(throttle);
    } else if (throttleType == ThrottleType.WRITE_NUMBER) {
      Throttle throttle = Throttle.newBuilder()
          .setWriteNum(TimedQuota.newBuilder().setSoftLimit(limit).setTimeUnit(timeUnit)).build();
      return TimeBasedLimiter.fromThrottle(throttle);
    } else {
      LOG.warn("Unknown throttle type {}", throttleType);
      return null;
    }
  }
}
