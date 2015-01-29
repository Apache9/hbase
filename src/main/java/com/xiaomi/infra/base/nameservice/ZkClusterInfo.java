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
      "xmdm001-zk-tst.hadoop.srv", "192.168.135.12,192.168.135.34,192.168.135.58");
    PRE_DEFINED_CLUSTERS.put(
      "xmd1-zk-tst.hadoop.srv", "192.168.135.12,192.168.135.34,192.168.135.59");
    PRE_DEFINED_CLUSTERS.put(
      "xmd001-zk-tst.hadoop.srv", "192.168.135.12,192.168.135.34,192.168.135.60");
    PRE_DEFINED_CLUSTERS.put(
      "bjdm-zk-tst.hadoop.srv", "10.235.3.55,10.235.3.57,10.235.3.67");
    PRE_DEFINED_CLUSTERS.put(
      "bjdm001-zk-tst.hadoop.srv", "10.235.3.55,10.235.3.57,10.235.3.69");
    PRE_DEFINED_CLUSTERS.put(
      "bjd1-zk-tst.hadoop.srv", "10.235.3.55,10.235.3.57,10.235.3.70");
    PRE_DEFINED_CLUSTERS.put(
      "bjd001-zk-tst.hadoop.srv", "10.235.3.55,10.235.3.57,10.235.3.71");
  }

  public static enum ClusterType {
    SRV,
    PRC,
    TST,
    SEC,
  }

  public static final int REGION_NAME_LEN = 2;
  public static final int IDC_NAME_LEN = 2;
  public static final int CLUSTER_TYPE_LEN = 3;

  private final String clusterName;
  private final String regionName;
  private final String idcAndIndexName;
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
    if (length < IDC_NAME_LEN + CLUSTER_TYPE_LEN) {
      throw new IOException("Illegal zookeeper cluster name: " + clusterName);
    }

    String clusterTypeName = clusterName.substring(length - CLUSTER_TYPE_LEN, length);
    try {
      clusterType = ClusterType.valueOf(clusterTypeName.toUpperCase());
    } catch (IllegalArgumentException e) {
      throw new IOException("Illegal zookeeper cluster type: " + clusterTypeName);
    }
    
    int index = length - CLUSTER_TYPE_LEN - 1;
    while (index >= 0 && Character.isDigit(clusterName.charAt(index)))
      --index;
    ++index;

    if (index <= IDC_NAME_LEN) {
      regionName = "bj";
      idcAndIndexName = clusterName.substring(0, length - CLUSTER_TYPE_LEN);
    } else if (index <= REGION_NAME_LEN + IDC_NAME_LEN) {
      regionName = clusterName.substring(0, REGION_NAME_LEN);
      idcAndIndexName = clusterName.substring(REGION_NAME_LEN, length - CLUSTER_TYPE_LEN);
    } else {
      throw new IOException("Illegal zookeeper cluster name: " + clusterName);
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
    return regionName + idcAndIndexName + "-zk-" + clusterType.toString().toLowerCase() + ".hadoop.srv";
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
