/**
 * Copyright 2013, Xiaomi.com
 * All rights reserved.
 * Author: yehangjun
 */

package com.xiaomi.infra.base.nameservice;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class NameService {
  public final static String HBASE_URI_PREFIX = "hbase://";
  private static boolean hasCheckKerberos = false;

  /**
    * The resource could be a full uri with cluster information, or just a short
    * name and the client should hava a method to resolve the cluster. Take hbase
    * for example:
    *   "table1": just the hbase table name
    *
    *   "hbase://lgprc-test/table1": "table1" on the hbase cluster "lgprc-test",
    *   which is located by the name service.
    *
    *   "hbase://lgprc-test:9800/table1": the zookeeper cluster is running on
    *   port 9800 instead of the default port (11000).
    *
    * When it's a full uri name, the cluster must be setup with kerberos
    * authentication enabled.
    * When it's a just a short form name, the configuration would be loaded from
    * configuration files.
    */
  public static NameServiceEntry resolve(String resource) throws IOException {
    return resolve(resource, new Configuration());
  }

  public static NameServiceEntry resolve(String resource, final Configuration conf)
      throws IOException {
    try {
      URI uri = new URI(resource);
      return resolve(uri, conf);
    } catch (URISyntaxException e) {
      throw new IOException("Illegal uri: " + e.getMessage());
    }
  }

  public static NameServiceEntry resolve(URI uri) throws IOException {
    return resolve(uri, new Configuration());
  }

  public static NameServiceEntry resolve(URI uri, final Configuration conf) throws IOException {
    NameServiceEntry entry = new NameServiceEntry(uri);
    if (!entry.isLocalResource()) {
      checkKerberos(conf);
    }
    return entry;
  }

  private static synchronized void checkKerberos(final Configuration conf) throws IOException {
    if (!hasCheckKerberos) {
      // Creating a Configuration is heavy, just do once.
      hasCheckKerberos = true;
      String value = conf.get(HADOOP_SECURITY_AUTHENTICATION);
      if (!"kerberos".equals(value)) {
        throw new RuntimeException("Kerberos is required for name service, "
            + "please make sure set hadoop.security.authentication to kerberos "
            + "in the core-site.xml, or add "
            + "-Dhadoop.property.hadoop.security.authentication=kerberos on "
            + "JVM startup options.");
      }
    }
  }

  public static TableName resolveTableName(String fullTableName) {
    NameServiceEntry tableEntry = null;
    if (fullTableName.startsWith(NameService.HBASE_URI_PREFIX)) {
      try {
        tableEntry = NameService.resolve(fullTableName);
        if (!tableEntry.compatibleWithScheme("hbase")) {
          throw new IOException("Unrecognized scheme: " + tableEntry.getScheme());
        }
      } catch (IOException e) {
        throw new IllegalArgumentException("Given full table name is illegal", e);
      }
    }
    return TableName.valueOf(tableEntry == null ? fullTableName : tableEntry.getResource());
  }

  public static String resolveClusterUri(String tableName){
    if(tableName.startsWith(NameService.HBASE_URI_PREFIX)) {
      //since fullTableName is hbase://cluster/table, we want hbase://cluster only.
      return tableName.substring(0, tableName.lastIndexOf("/"));
    } else {
      return "";
    }
  }

  public static Configuration createConfigurationByClusterKey(String key) throws IOException {
    return createConfigurationByClusterKey(key, new Configuration());
  }

  public static Configuration createConfigurationByClusterKey(String key, Configuration conf)
      throws IOException {
    NameServiceEntry entry = resolve(key, conf);
    if (!entry.compatibleWithScheme("hbase")) {
      throw new IOException("Unrecognized scheme: " + entry.getScheme());
    }
    // it just copy configuration and change it, the old configuration do not change
    return entry.createClusterConf(conf);
  }
}
