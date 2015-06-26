/**
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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.replication.ReplicationAdmin;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationPeer;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.regionserver.HBaseInterClusterReplicationEndpoint;
import org.apache.hadoop.hbase.replication.regionserver.ReplicationSink;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static com.google.common.collect.Iterables.getOnlyElement;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.fail;

public class ReplicationTestUtils {


  private static final Log LOG = LogFactory.getLog(ReplicationTestUtils.class);
  public static final byte[] DEFAULT_FAMILY = Bytes.toBytes("test_family");
  public static final byte[] DEFAULT_QUALIFIER = Bytes.toBytes("test_qual");
  static final long SLEEP_TIME = 100;
  static final int NB_RETRIES = 200;

  public static Configuration setupConfiguration(HBaseTestingUtility cluster, int thriftServerPort) {
    Configuration configuration = cluster.getConfiguration();
    configuration.setInt("hbase.replication.thrift.server.port", thriftServerPort);
    configuration.setBoolean(ReplicationSink.CONF_KEY_REPLICATION_THRIFT, true);
    configuration.setBoolean("hbase.replication", true);
    return configuration;
  }

  public static void addPeerThriftPort(HBaseTestingUtility cluster, String peerId, int port) {
    cluster.getConfiguration().setInt("hbase.replication.thrift.peer." + peerId + ".port", port);
  }

  public static HTableDescriptor createTestTable() throws Exception {
    HTableDescriptor table = new HTableDescriptor("test_table");
    HColumnDescriptor fam = new HColumnDescriptor(DEFAULT_FAMILY);
    fam.setScope(HConstants.REPLICATION_SCOPE_GLOBAL);
    fam.setMaxVersions(1);
    table.addFamily(fam);
    return table;
  }

  public static void createTableOnCluster(HBaseTestingUtility cluster, HTableDescriptor table)
      throws IOException {
    new HBaseAdmin(cluster.getConfiguration()).createTable(table);
  }

  public static HTable getTestTable(HBaseTestingUtility cluster, HTableDescriptor table)
      throws IOException {
    HTable result = new HTable(cluster.getConfiguration(), table.getName());
    result.setWriteBufferSize(1024);
    return result;
  }

  public static void addReplicationPeer(String peerId, HBaseTestingUtility source,
      HBaseTestingUtility destination) throws IOException, ReplicationException {
     addReplicationPeer(peerId, source, destination, ReplicationPeer.PeerProtocol.THRIFT);
  }

  public static void addReplicationPeer(String peerId, HBaseTestingUtility source,
      HBaseTestingUtility destination, ReplicationPeer.PeerProtocol protocol)
      throws IOException, ReplicationException {
    Configuration configuration = source.getConfiguration();

    ReplicationAdmin admin = new ReplicationAdmin(configuration);
    String endpoint = HBaseInterClusterReplicationEndpoint.class.getName();
    if (protocol.getProtocol() == ZooKeeperProtos.ReplicationPeer.Protocol.THRIFT) {
      endpoint = ThriftHBaseReplicationEndpoint.class.getName();
    }
    ReplicationPeerConfig config =
        new ReplicationPeerConfig().setClusterKey(destination.getClusterKey())
            .setProtocol(protocol).setReplicationEndpointImpl(endpoint);
    admin.addPeer(peerId,config, null);
  }

  public static Result putAndWait(Put put, String value, HTable source, HTable target) throws Exception {
    return putAndWait(put, value, false, source, target);
  }

  public static Result putAndWait(Put put, String value, boolean compareTimestamps, HTable source, HTable target) throws Exception {
    source.put(put);

    Get get = new Get(put.getRow());
    for (int i = 0; i < NB_RETRIES; i++) {
      Result res = target.get(get);
      if (res.size() == 0) {
        LOG.info("Row not available");
        Thread.sleep(SLEEP_TIME);
      } else {
        if (compareTimestamps) {
          if (getOnlyElement(res.getColumn(DEFAULT_FAMILY, DEFAULT_QUALIFIER)).getTimestamp()
              != getOnlyElement(put.get(DEFAULT_FAMILY, DEFAULT_QUALIFIER)).getTimestamp()) {
            LOG.info("Cell timestamps don't match... wait some more");
            Thread.sleep(SLEEP_TIME);
            continue;
          }
          assertArrayEquals(res.value(), Bytes.toBytes(value));
        }
        return res;
      }
    }
    throw new RuntimeException("Waited too much time for put replication");
  }


  public static void deleteAndWait(byte[] row, HTable source, HTable target)
      throws Exception {
    Delete del = new Delete(row);
    source.delete(del);

    Get get = new Get(row);
    for (int i = 0; i < NB_RETRIES; i++) {
      if (i==NB_RETRIES-1) {
        fail("Waited too much time for del replication");
      }
      Result res = target.get(get);
      if (res.size() >= 1) {
        LOG.info("Row not deleted");
        Thread.sleep(SLEEP_TIME);
      } else {
        break;
      }
    }
  }

  public static void assertContainsOnly(HTable table, Set<String> values) throws Exception {
    for (int i = 0; i < NB_RETRIES; i++) {
      Set<String> valuesCopy = Sets.newHashSet(values);
      ResultScanner scanner = table.getScanner(new Scan());
      Result result;
      int tableSize = 0;
      int valuesSize = valuesCopy.size();
      List<String> inTableNotExpected = Lists.newArrayList();
      while ((result = scanner.next()) != null) {
        String value = Bytes.toString(result.getValue(DEFAULT_FAMILY, DEFAULT_QUALIFIER));
        boolean removed = valuesCopy.remove(value);
        if (!removed) {
          inTableNotExpected.add(value);
        }
        tableSize++;
      }
      if (!valuesCopy.isEmpty()) {
        LOG.warn("Table did not have expected values: " + valuesCopy);
        Thread.sleep(SLEEP_TIME);
      } else if (tableSize != valuesSize) {
        LOG.warn("Table had more values (" + tableSize + ") than expected: " + inTableNotExpected);
        Thread.sleep(SLEEP_TIME);
      } else {
        return;
      }
    }
    fail("Waited too much time for replication to sync up");
  }

  public static Put generateRandomPut(String value) {
    return generateRandomPut(value, System.currentTimeMillis());
  }

  public static Put generateRandomPut(String value, long timestamp) {
    Put put = new Put(Bytes.toBytes(UUID.randomUUID().toString()));
    put.add(DEFAULT_FAMILY, DEFAULT_QUALIFIER, timestamp, Bytes.toBytes(value));
    return put;
  }


}
