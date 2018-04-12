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
package org.apache.hadoop.hbase.client.replication;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CompoundConfiguration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.ReplicationPeerDescription;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;

public class ReplicationUtils {

  /**
   * Returns the configuration needed to talk to the remote slave cluster.
   */
  public static Configuration getPeerClusterConfiguration(ReplicationPeerDescription peer,
      Configuration baseConf) throws IOException {
    ReplicationPeerConfig peerConfig = peer.getPeerConfig();
    Configuration otherConf;
    try {
      otherConf = HBaseConfiguration.create(baseConf);
      ZKUtil.applyClusterKeyToConf(otherConf, peerConfig.getClusterKey());
    } catch (IOException e) {
      throw new IOException("Can't get peer configuration for peerId=" + peer.getPeerId(), e);
    }

    if (!peerConfig.getConfiguration().isEmpty()) {
      CompoundConfiguration compound = new CompoundConfiguration();
      compound.add(otherConf);
      compound.addStringMap(peerConfig.getConfiguration());
      return compound;
    }

    return otherConf;
  }
}
