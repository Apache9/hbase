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
package org.apache.hadoop.hbase.replication.thrift;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.security.HBaseSaslRpcServer;
import org.apache.hadoop.hbase.security.HBaseSaslRpcServer.QualityOfProtection;
import org.apache.hadoop.hbase.security.User;


public class ThriftUtilities {

  private ThriftUtilities() {
    throw new UnsupportedOperationException("Can't initialize class");
  }

  public static QualityOfProtection getQOP(Configuration conf) {
    QualityOfProtection saslQOP = QualityOfProtection.AUTHENTICATION;
    String rpcProtection = conf.get("hbase.replication.thrift.protection",
        QualityOfProtection.AUTHENTICATION.name().toLowerCase());
    if (QualityOfProtection.INTEGRITY.name().toLowerCase()
        .equals(rpcProtection)) {
      saslQOP = QualityOfProtection.INTEGRITY;
    } else if (QualityOfProtection.PRIVACY.name().toLowerCase().equals(
        rpcProtection)) {
      saslQOP = QualityOfProtection.PRIVACY;
    }
    return saslQOP;
  }

  public static HBaseSaslRpcServer.AuthMethod getAuthType(Configuration conf) {
    return HBaseSaslRpcServer.AuthMethod.valueOf(conf.get(User.HBASE_SECURITY_CONF_KEY, "SIMPLE").toUpperCase());
  }

  public static int getThriftServerPort(Configuration conf) {
    return conf.getInt("hbase.replication.thrift.server.port", -1);
  }

  /*
   * hbase.replication.thrift.peer.<peerId>.port is used for testing purposes only
   * in your production cluster, which would be running on different instances, you do
   * not need to set this field, simply provide the hbase.replication.thrift.server.port
   * value and keep them the same across clusters.
   */
  public static int getDestinationPeerPort(Configuration conf, String peerId) {
    int port = conf.getInt("hbase.replication.thrift.peer." + peerId + ".port", -1);
    // if there is no custom peer port set use the default server port.
    if (port == -1) {
      port = getThriftServerPort(conf);
    }
    return port;
  }

}


