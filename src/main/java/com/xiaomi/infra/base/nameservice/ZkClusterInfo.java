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
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ZkClusterInfo {
  private static final Log LOG = LogFactory.getLog(ZkClusterInfo.class);

  private static final Map<String, String> PRE_DEFINED_CLUSTERS =
      new TreeMap<String, String>();

  static {
    // Dumb clusters for unit test.
    PRE_DEFINED_CLUSTERS.put(
      "xmdm-zk-tst.hadoop.srv", "192.168.135.12,192.168.135.34,192.168.135.56");
    PRE_DEFINED_CLUSTERS.put(
      "bjdm-zk-tst.hadoop.srv", "10.235.3.55,10.235.3.57,10.235.3.67");
  }

  public static enum ClusterType {
    SRV,
    PRC,
    TST,
    SEC,
  }

  private final String clusterName;
  private final String cityName;
  private final String idcName;
  private final ClusterType clusterType;
  private final int port;
  
  public ZkClusterInfo(String clusterName)
      throws IOException {
    this(clusterName, -1);
  }
  
  public ZkClusterInfo(String clusterName, int port)
      throws IOException {
    this.clusterName = clusterName;
    int length = clusterName.length();
    String clusterTypeName;
    if (length == 5) {
      cityName = "bj";
      idcName = clusterName.substring(0, 2);
      clusterTypeName = clusterName.substring(2, 5);
    } else if (length == 7) {
      cityName = clusterName.substring(0, 2);
      idcName = clusterName.substring(2, 4);
      clusterTypeName = clusterName.substring(4, 7);
    } else {
      throw new IOException("Illegal zookeeper cluster name: " + clusterName);
    }

    try {
      clusterType = ClusterType.valueOf(clusterTypeName.toUpperCase());
    } catch (IllegalArgumentException e) {
      throw new IOException("Illegal zookeeper cluster type: " + clusterTypeName);
    }
    
    this.port = (port == -1) ? 11000 : port;
  }

  public ClusterType getClusterType() {
    return clusterType;
  }

  public int getPort() {
    return port;
  }

  public String toDnsName() {
    return cityName + idcName + "-zk-" + clusterType.toString().toLowerCase() + ".hadoop.srv";      
  }
  
  /**
   * Resolve the dns name of zookeeper cluster to a comma-separate ip list.
   * If failed to resolve but it's a "well-known" name (the cluster we have
   * setup for the time being), a pre-defined ip list would be returned as a
   * back off.
   * @return The ip list of the dns name, separated by comma.
   * @throws IOException if dns name is illegal or failed to be resolved.
   */
  public String resolve()
      throws IOException {
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
      String preDefinedCluster = PRE_DEFINED_CLUSTERS.get(dnsName);
      if (preDefinedCluster == null) {
        throw new IOException(
            "Failed to resolve dns name and it doesn't hava a back off: " +
            dnsName);
      }
      LOG.warn(
          "Failed to resolve the cluster but a pre-defined ip list is used " +
          "instead: " + dnsName);
      return preDefinedCluster;
    }
  }
}
