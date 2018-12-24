/**
 * Copyright 2013, Xiaomi.com
 * All rights reserved.
 * Author: yehangjun
 */

package com.xiaomi.infra.base.nameservice;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import org.apache.commons.lang3.StringUtils;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class ZkClusterInfo {

  public static final int DEFAULT_ZOOKEEPER_CLIENT_PORT = 11000;

  public static enum ClusterType {
    SRV, PRC, TST, SEC,
  }

  public static final int CLUSTER_TYPE_LEN = 3;

  private final String clusterName;
  private final ClusterType clusterType;
  private final int port;

  public ZkClusterInfo(String clusterName) throws IOException {
    this(clusterName, -1);
  }

  public ZkClusterInfo(String clusterName, int port) throws IOException {
    this.clusterName = clusterName;
    int length = clusterName.length();

    String clusterTypeName = clusterName.substring(length - CLUSTER_TYPE_LEN, length);
    try {
      clusterType = ClusterType.valueOf(clusterTypeName.toUpperCase());
    } catch (IllegalArgumentException e) {
      throw new IOException("Illegal zookeeper cluster type: " + clusterTypeName);
    }

    this.port = (port == -1) ? DEFAULT_ZOOKEEPER_CLIENT_PORT : port;
  }

  public ClusterType getClusterType() {
    return clusterType;
  }

  public int getPort() {
    return port;
  }

  public String toDnsName() {
    return clusterName + ".observer.zk.hadoop.srv";
  }

  /**
   * Resolve the dns name of zookeeper cluster to a comma-separate ip list. If failed to resolve but
   * it's a "well-known" name (the cluster we have setup for the time being), a pre-defined ip list
   * would be returned as a back off.
   * @return The ip list of the dns name, separated by comma.
   * @throws IOException if dns name is illegal or failed to be resolved.
   */
  public String resolve() throws IOException {
    String dnsName = toDnsName();
    try {
      InetAddress[] allAddresses = InetAddress.getAllByName(dnsName);
      String[] allIps = new String[allAddresses.length];
      for (int i = 0; i < allAddresses.length; ++i) {
        allIps[i] = allAddresses[i].getHostAddress();
      }

      // zk hosts will be used as key of HConnection and SecureClient, we
      // keep the 'allIps' in the same order to share HConnection and SecureClient
      // for HTables. On the other hand, zk client will shuffle provided hosts
      // before connecting to zk servers, this won't make zk client always connect
      // to the same zk server.
      Arrays.sort(allIps);
      return StringUtils.join(allIps, ',');
    } catch (UnknownHostException e) {
      throw new IOException(
          "Failed to resolve dns name and it doesn't hava a back off: " + dnsName);
    }
  }
}
