/*
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
package org.apache.hadoop.hbase.zookeeper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.zookeeper.client.ZKClientConfig;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableSet;

@Tag(MiscTests.TAG)
@Tag(SmallTests.TAG)
public class TestZKConfig {

  /** Supported ZooKeeper client TLS properties */
  private static final Set<String> ZOOKEEPER_CLIENT_TLS_PROPERTIES = ImmutableSet.of(
    "client.secure", "clientCnxnSocket", "ssl.keyStore.location", "ssl.keyStore.password",
    "ssl.keyStore.passwordPath", "ssl.keyStore.type", "ssl.trustStore.location",
    "ssl.trustStore.password", "ssl.trustStore.passwordPath", "ssl.trustStore.type");

  @Test
  public void testZKConfigLoading() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    // Test that we read only from the config instance
    // (i.e. via hbase-default.xml and hbase-site.xml)
    conf.setInt(HConstants.ZOOKEEPER_CLIENT_PORT, 2181);
    Properties props = ZKConfig.makeZKProps(conf);
    assertEquals("2181", props.getProperty("clientPort"),
      "Property client port should have been default from the HBase config");
  }

  @Test
  public void testGetZooKeeperClusterKey() {
    Configuration conf = HBaseConfiguration.create();
    conf.set(HConstants.ZOOKEEPER_QUORUM, "\tlocalhost\n");
    conf.set(HConstants.ZOOKEEPER_CLIENT_PORT, "3333");
    conf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "hbase");
    String clusterKey = ZKConfig.getZooKeeperClusterKey(conf, "test");
    assertTrue(!clusterKey.contains("\t") && !clusterKey.contains("\n"));
    assertEquals("localhost:3333:hbase,test", clusterKey);
  }

  @Test
  public void testClusterKey() throws Exception {
    testKey("server", 2181, "/hbase");
    testKey("server1,server2,server3", 2181, "/hbase");
    try {
      ZKConfig.validateClusterKey("2181:/hbase");
    } catch (IOException ex) {
      // OK
    }
  }

  @Test
  public void testClusterKeyWithMultiplePorts() throws Exception {
    // server has different port than the default port
    testKey("server1:2182", 2181, "/hbase", true);
    // multiple servers have their own port
    testKey("server1:2182,server2:2183,server3:2184", 2181, "/hbase", true);
    // one server has no specified port, should use default port
    testKey("server1:2182,server2,server3:2184", 2181, "/hbase", true);
    // the last server has no specified port, should use default port
    testKey("server1:2182,server2:2183,server3", 2181, "/hbase", true);
    // multiple servers have no specified port, should use default port for those servers
    testKey("server1:2182,server2,server3:2184,server4", 2181, "/hbase", true);
    // same server, different ports
    testKey("server1:2182,server1:2183,server1", 2181, "/hbase", true);
    // mix of same server/different port and different server
    testKey("server1:2182,server2:2183,server1", 2181, "/hbase", true);
  }

  @Test
  public void testZooKeeperTlsProperties() {
    // Arrange
    Configuration conf = HBaseConfiguration.create();
    for (String p : ZOOKEEPER_CLIENT_TLS_PROPERTIES) {
      conf.set(HConstants.ZK_CFG_PROPERTY_PREFIX + p, p);
      String zkprop = "zookeeper." + p;
      System.clearProperty(zkprop);
    }

    // Act
    ZKClientConfig zkClientConfig = ZKConfig.getZKClientConfig(conf);

    // Assert
    for (String p : ZOOKEEPER_CLIENT_TLS_PROPERTIES) {
      assertEquals(p, zkClientConfig.getProperty("zookeeper." + p),
        "Invalid or unset system property: " + p);
    }
  }

  @Test
  public void testZooKeeperTlsPropertiesHQuorumPeer() {
    // Arrange
    Configuration conf = HBaseConfiguration.create();
    for (String p : ZOOKEEPER_CLIENT_TLS_PROPERTIES) {
      conf.set(HConstants.ZK_CFG_PROPERTY_PREFIX + p, p);
      String zkprop = "zookeeper." + p;
      System.clearProperty(zkprop);
    }

    // Act
    Properties zkProps = ZKConfig.makeZKProps(conf);

    // Assert
    for (String p : ZOOKEEPER_CLIENT_TLS_PROPERTIES) {
      assertEquals(p, zkProps.getProperty(p), "Invalid or unset system property: " + p);
    }
  }

  private void testKey(String ensemble, int port, String znode) throws IOException {
    testKey(ensemble, port, znode, false); // not support multiple client ports
  }

  private void testKey(String ensemble, int port, String znode, Boolean multiplePortSupport)
    throws IOException {
    Configuration conf = new Configuration();
    String key = ensemble + ":" + port + ":" + znode;
    String ensemble2 = null;
    ZKConfig.ZKClusterKey zkClusterKey = ZKConfig.transformClusterKey(key);
    if (multiplePortSupport) {
      ensemble2 = ZKConfig.standardizeZKQuorumServerString(ensemble, Integer.toString(port));
      assertEquals(ensemble2, zkClusterKey.getQuorumString());
    } else {
      assertEquals(ensemble, zkClusterKey.getQuorumString());
    }
    assertEquals(port, zkClusterKey.getClientPort());
    assertEquals(znode, zkClusterKey.getZnodeParent());

    conf = HBaseConfiguration.createClusterConf(conf, key);
    assertEquals(zkClusterKey.getQuorumString(), conf.get(HConstants.ZOOKEEPER_QUORUM));
    assertEquals(zkClusterKey.getClientPort(), conf.getInt(HConstants.ZOOKEEPER_CLIENT_PORT, -1));
    assertEquals(zkClusterKey.getZnodeParent(), conf.get(HConstants.ZOOKEEPER_ZNODE_PARENT));

    String reconstructedKey = ZKConfig.getZooKeeperClusterKey(conf);
    if (multiplePortSupport) {
      String key2 = ensemble2 + ":" + port + ":" + znode;
      assertEquals(key2, reconstructedKey);
    } else {
      assertEquals(key, reconstructedKey);
    }
  }

  private static final Pattern TESTS = Pattern.compile(
    "\\[[A-Z]+\\] Tests run: (\\d+), Failures: (\\d+), Errors: (\\d+), Skipped: (\\d+), Time elapsed: [^-]+ -- in ([A-Za-z.]+)");

  private static final class TestCount {
    int run;
    int failures;
    int errors;
    int skipped;

    @Override
    public int hashCode() {
      return Objects.hash(errors, failures, run, skipped);
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (obj == null) return false;
      if (getClass() != obj.getClass()) return false;
      TestCount other = (TestCount) obj;
      return errors == other.errors && failures == other.failures && run == other.run
        && skipped == other.skipped;
    }

    @Override
    public String toString() {
      return "TestCount [run=" + run + ", failures=" + failures + ", errors=" + errors
        + ", skipped=" + skipped + "]";
    }

  }

  private static TestCount collect(Matcher m) {
    TestCount c = new TestCount();
    c.run = Integer.parseInt(m.group(1));
    c.failures = Integer.parseInt(m.group(2));
    c.errors = Integer.parseInt(m.group(3));
    c.skipped = Integer.parseInt(m.group(4));
    return c;
  }

  private static Map<String, TestCount> read(String file) throws IOException {
    Map<String, TestCount> testCount = new TreeMap<>();
    try (BufferedReader br = Files.newBufferedReader(Paths.get(file), StandardCharsets.UTF_8)) {
      for (String line;;) {
        line = br.readLine();
        if (line == null) {
          break;
        }
        Matcher m = TESTS.matcher(line);
        if (!m.matches()) {
          continue;
        }
        String testName = m.group(5);
        testCount.put(testName, collect(m));
      }
    }
    return testCount;
  }

  public static void main(String[] args) throws Exception {
    Map<String, TestCount> oldCount = read("/home/zhangduo/hbase/old");
    Map<String, TestCount> newCount = read("/home/zhangduo/hbase/new");
    oldCount.forEach((testName, testCount) -> {
      TestCount newTestCount = newCount.get(testName);
      if (newTestCount == null) {
        System.out.println("missed: " + testName);
        return;
      }
      if (!testCount.equals(newTestCount)) {
        System.out.println("diff " + testName + ":");
        System.out.println("\told: " + testCount);
        System.out.println("\tnew: " + newTestCount);
      }
    });
    newCount.forEach((testName, newTestCount) -> {
      if (!oldCount.containsKey(testName)) {
        System.out.println("new: " + testName);
      }
    });
  }
}
