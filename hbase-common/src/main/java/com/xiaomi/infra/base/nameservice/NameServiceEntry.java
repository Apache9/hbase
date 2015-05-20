/**
 * Copyright 2013, Xiaomi.com
 * All rights reserved.
 * Author: yehangjun
 */

package com.xiaomi.infra.base.nameservice;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;

public class NameServiceEntry {
  private final URI uri;
  private final String scheme;
  private final ClusterInfo clusterInfo;
  private final String resource;

  public NameServiceEntry(URI uri)
      throws IOException {
    this.uri = uri;
    scheme = uri.getScheme();

    if (scheme == null) {
      clusterInfo = null;
    } else {
      clusterInfo = new ClusterInfo(uri.getHost(), uri.getPort());
      if (clusterInfo.isZkCluster() && !"zk".equals(scheme)) {
        throw new IOException("Illegal scheme, 'zk' expected: " + scheme);
      }
    }

    // Strip the leading '/' from path.
    String path = uri.getPath();
    if (path.startsWith("/")) {
      path = path.substring(1);
    }
    resource = path;
  }

  /**
   *  The return value could be null.
   */
  public String getScheme() {
    return scheme;
  }

  /**
   *  The return value could be null.
   */
  public ClusterInfo getClusterInfo() {
    return clusterInfo;
  }

  public String getResource() {
    return resource;
  }
  
  public boolean isLocalResource() {
    return scheme == null;
  }

  /**
   * If the entry is compatible with passed in scheme.
   * A local resource (not a full uri) is compatible for any scheme.
   */
  public boolean compatibleWithScheme(String possibleScheme) {
    return this.scheme == null || scheme.equals(possibleScheme);
  }

  /**
   * Set the configuration according to cluster information if necessary.
   * It would duplicate the configuration before modifying it, so the passed in
   * "conf" instance would NOT be changed. 
   */
  // TODO(yehangjun): implement for other schemes, now it's only for hbase.
  public Configuration createClusterConf(Configuration conf)
      throws IOException {
    if (isLocalResource()) {
      // Do nothing for local resource (not a full uri and then doesn't have the
      // cluster information).
      return conf;
    }

    // Duplicate configuration as we need to modify it.
    conf = (conf != null ? new Configuration(conf) : new Configuration());

    ZkClusterInfo zkClusterInfo = clusterInfo.getZkClusterInfo();

    String kerberosPrinciple = "hbase_" +
        zkClusterInfo.getClusterType().toString().toLowerCase() +
        "/hadoop@XIAOMI.HADOOP";

    // Setting configuration for authentication, authorization, and encryption.
    conf.set("hadoop.security.authentication", "kerberos");
    conf.set("hadoop.security.auth_to_local", "RULE:[1:$1] RULE:[2:$1] DEFAULT");
    conf.set("hbase.security.authentication", "kerberos");
    conf.set("hbase.rpc.engine", "org.apache.hadoop.hbase.ipc.SecureRpcEngine");
    conf.set("hbase.master.kerberos.principal", kerberosPrinciple);
    conf.set("hbase.regionserver.kerberos.principal", kerberosPrinciple);

    // Setting configuration for cluster entry address.
    conf.set("hbase.zookeeper.quorum", zkClusterInfo.resolve());
    conf.setInt("hbase.zookeeper.property.clientPort", zkClusterInfo.getPort());
    conf.set("zookeeper.znode.parent", "/hbase/" + clusterInfo.getClusterName());
    
    return conf;
  }
}
