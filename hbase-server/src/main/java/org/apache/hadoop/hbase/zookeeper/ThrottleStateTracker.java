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

import org.apache.commons.logging.Log;
import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.QuotaProtos;
import org.apache.hadoop.hbase.quotas.ThrottleState;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.KeeperException;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;

/**
 * Tracks the throttle state up in ZK
 */
@InterfaceAudience.Private
public class ThrottleStateTracker extends ZooKeeperNodeTracker {
  private static final Log LOG = LogFactory.getLog(ThrottleStateTracker.class);
  private final RegionServerServices rsServices;

  public ThrottleStateTracker(ZooKeeperWatcher watcher, Abortable abortable) {
    this(watcher, abortable, null);
  }

  public ThrottleStateTracker(ZooKeeperWatcher watcher, Abortable abortable,
      RegionServerServices rsServices) {
    super(watcher, watcher.throttleZNode, abortable);
    this.rsServices = rsServices;
  }

  public ThrottleState getThrottleState() {
    byte[] upData = super.getData(false);
    try {
      if (upData == null) {
        return ThrottleState.ON;
      }
      return ProtobufUtil.toThrottleState(parseFrom(upData));
    } catch (DeserializationException dex) {
      LOG.error("ZK state for throttle could not be parsed " + Bytes.toStringBinary(upData));
      return ThrottleState.ON;
    }
  }

  public void setThrottleState(ThrottleState state) throws KeeperException {
    byte[] upData = toByteArray(state);
    try {
      ZKUtil.setData(watcher, watcher.throttleZNode, upData);
    } catch (KeeperException.NoNodeException nne) {
      ZKUtil.createAndWatch(watcher, watcher.throttleZNode, upData);
    }
    super.nodeDataChanged(watcher.throttleZNode);
  }

  @Override
  public void nodeCreated(String path) {
    if (path.equals(node)) {
      super.nodeCreated(path);
      if (this.rsServices != null) {
        this.rsServices.switchThrottle();
      }
    }
  }

  @Override
  public void nodeDataChanged(String path) {
    if (path.equals(node)) {
      super.nodeDataChanged(path);
      if (this.rsServices != null) {
        this.rsServices.switchThrottle();
      }
    }
  }

  private byte[] toByteArray(ThrottleState state) {
    QuotaProtos.ThrottleState proto = ProtobufUtil.toProtoThrottleState(state);
    return ProtobufUtil.prependPBMagic(Bytes.toBytes(proto.getNumber()));
  }

  private QuotaProtos.ThrottleState parseFrom(byte[] pbBytes) throws DeserializationException {
    ProtobufUtil.expectPBMagicPrefix(pbBytes);
    int magicLen = ProtobufUtil.lengthOfPBMagic();
    return QuotaProtos.ThrottleState.valueOf(Bytes.toInt(pbBytes, magicLen, pbBytes.length
        - magicLen));
  }
}
