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

public class NameService {
  public final static String HBASE_URI_PREFIX = "hbase://";
  /* Comment this out as it's very tricky and breaks some unit test. It would
   * try to login user using kerberos and some tests don't work.
   * Now the users must set hadoop.security.authentication to kerberos in the
   * core-site.xml, or add -Dhadoop.property.hadoop.security.authentication=kerberos
   * on JVM startup options.
   * See UserGroupInformation.initUGI() for the details.
   * 
  static {
    // We must set "hadoop.security.authentication" to "kerberos" manually
    // before trying to connect to hbase server. Some static methods of class
    // UserGroupInformation (getLoginUser() and isSecurityEnabled() basically)
    // need this key to know if the kerberos is enabled and they create a
    // Configuration by themselves, which would try to load from core-site.xml.
    // If core-site.xml isn't provided, the default value "simple" would be used
    // and even we set it to "kerberos" programmatically later, it doesn't work.
    Configuration conf = new Configuration();
    conf.set("hadoop.security.authentication", "kerberos");
    conf.set("hadoop.security.auth_to_local",
        "RULE:[1:$1] RULE:[2:$1] DEFAULT");
    conf.set("hadoop.security.group.mapping",
        "org.apache.hadoop.security.SimpleGroupsMapping");
    UserGroupInformation.setConfiguration(conf);
  }
  
  public static void initialize() {
    // Do nothing here, the real work is done in the static code block above.
  }
  */
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
  public static NameServiceEntry resolve(String resource)
      throws IOException {
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
  
  public static NameServiceEntry resolve(URI uri)
      throws IOException {
    return resolve(uri, new Configuration());
  }
  
  public static NameServiceEntry resolve(URI uri, final Configuration conf)
      throws IOException {
    NameServiceEntry entry = new NameServiceEntry(uri);
    if (!entry.isLocalResource()) {
      checkKerberos(conf);
    }
    return entry;
  }

  private static synchronized void checkKerberos(final Configuration conf)
      throws IOException {
    if (!hasCheckKerberos) {
      // Creating a Configuration is heavy, just do once.
      hasCheckKerberos = true;
      String value = conf.get(HADOOP_SECURITY_AUTHENTICATION);
      if (!"kerberos".equals(value)) {
        throw new RuntimeException("Kerberos is required for name service, " +
            "please make sure set hadoop.security.authentication to kerberos " +
            "in the core-site.xml, or add " +
            "-Dhadoop.property.hadoop.security.authentication=kerberos on " +
            "JVM startup options.");
      }
    }
  }

  public static Configuration createConfigurationByClusterKey(String key)
    throws IOException {
    return createConfigurationByClusterKey(key, new Configuration());
  }

  public static Configuration createConfigurationByClusterKey(String key, Configuration conf)
    throws IOException {
    NameServiceEntry entry = resolve(key, conf);
    if (!entry.compatibleWithScheme("hbase")) {
      throw new IOException("Unrecognized scheme: " + entry.getScheme());
    }
    // it just copy configuration and change it, the old configuration do not change
    return entry.createClusterConf(null);
  }
}
