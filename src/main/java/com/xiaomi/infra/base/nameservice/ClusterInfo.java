/**
 * Copyright 2013, Xiaomi.com
 * All rights reserved.
 * Author: yehangjun
 */

package com.xiaomi.infra.base.nameservice;

import java.io.IOException;

public class ClusterInfo {
  private final ZkClusterInfo zkClusterInfo;
  private final String clusterName;
  private final String clusterSuffixName;
  
  public ClusterInfo(String clusterName)
      throws IOException {
    this(clusterName, -1);
  }

  public ClusterInfo(String clusterName, int port)
      throws IOException {
    this.clusterName = clusterName;
    int pos = clusterName.indexOf('-');
    if (pos <= 0) {
      // The cluster itself is a zookeeper cluster.
      zkClusterInfo = new ZkClusterInfo(clusterName, port);
      clusterSuffixName = null;
    } else {
      zkClusterInfo = new ZkClusterInfo(clusterName.substring(0, pos), port);
      clusterSuffixName = clusterName.substring(pos+1);
    }
  }

  public ZkClusterInfo getZkClusterInfo() {
    return zkClusterInfo;
  }
  
  public String getClusterName() {
    return clusterName;
  }

  public boolean isZkCluster() {
    return clusterSuffixName == null;
  }
}
