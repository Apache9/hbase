/**
 * Copyright 2011 The Apache Software Foundation
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

package org.apache.hadoop.hbase.master;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.ClusterSwitch;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

/**
 * Cluster switch tracker <br>
 * - Balance switch <br>
 * - Major Compaction switch <br>
 */

public class ClusterSwitchTracker {
  private final static Log LOG = LogFactory.getLog(ClusterSwitchTracker.class);

  private ZooKeeperWatcher watcher;
  private volatile boolean balanceSwitch = true;
  private volatile boolean majorCompactionSwitch = true;

  public ClusterSwitchTracker(ZooKeeperWatcher watcher) throws IOException {
    this.watcher = watcher;
    initClusterSwitch();
  }

  public boolean getBalanceSwitch() {
    return balanceSwitch;
  }

  public void setBalanceSwitch(boolean balanceSwitch) {
    this.balanceSwitch = balanceSwitch;
  }

  public boolean getMajorCompactionSwitch() {
    return majorCompactionSwitch;
  }

  public void setMajorCompactionSwitch(boolean majorCompactionSwitch) {
    this.majorCompactionSwitch = majorCompactionSwitch;
  }

  private void initClusterSwitch() throws IOException {
    try {
      if (ZKUtil.checkExists(watcher, watcher.clusterSwitchZNode) == -1) {
        persistClusterSwitch();
        return;
      }
      ClusterSwitch.Builder builder = ClusterSwitch.newBuilder();
      byte[] data = ZKUtil.getData(watcher, watcher.clusterSwitchZNode);
      if (data != null && data.length > 0 && ProtobufUtil.isPBMagicPrefix(data)) {
        builder.mergeFrom(data, ProtobufUtil.lengthOfPBMagic(),
          data.length - ProtobufUtil.lengthOfPBMagic());
        ClusterSwitch switches = builder.build();
        if (switches.hasBalance()) {
          this.balanceSwitch = switches.getBalance();
        }
        if (switches.hasMajorCompaction()) {
          this.majorCompactionSwitch = switches.getMajorCompaction();
        }
        LOG.info("Update cluster switches from zookeeper. BalanceSwitch=" + balanceSwitch
            + " majorCompactionSwitch=" + majorCompactionSwitch);
      } else {
        throw new IOException("Data on zookeeper is not for protobuf.");
      }
    } catch (KeeperException e) {
      throw new IOException("Get cluster switches to zookeeper failed!", e);
    }
  }

  public void persistClusterSwitch() throws IOException {
    try {
      ClusterSwitch.Builder builder = ClusterSwitch.newBuilder();
      builder.setBalance(balanceSwitch);
      builder.setMajorCompaction(majorCompactionSwitch);
      byte[] data = ProtobufUtil.prependPBMagic(builder.build().toByteArray());
      ZKUtil.createSetData(watcher, watcher.clusterSwitchZNode, data);
      LOG.info("Update cluster switches to zookeeper. BalanceSwitch=" + balanceSwitch
        + " majorCompactionSwitch=" + majorCompactionSwitch);
    } catch (KeeperException e) {
      throw new IOException("Update cluster switches to zookeeper failed!", e);
    }
  }
}
