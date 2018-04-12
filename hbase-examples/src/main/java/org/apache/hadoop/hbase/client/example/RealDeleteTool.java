/**
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
package org.apache.hadoop.hbase.client.example;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.replication.ReplicationUtils;
import org.apache.hadoop.hbase.replication.ReplicationPeerDescription;
import org.apache.hadoop.hbase.snapshot.ExportSnapshot;
import org.apache.hadoop.hbase.util.Bytes;

import com.xiaomi.infra.base.nameservice.NameService;

/**
 * A tool to delete table rows from cluster. Will delete first then take a major compact for table.
 */
public class RealDeleteTool {

  private static final Log LOG = LogFactory.getLog(RealDeleteTool.class);

  private static String cluster;

  private static String tableName;

  private static String fileName;

  private static void printUsageAndExit() {
    System.err.println("Examples:");
    System.err.println(" To delete the rows of table from a cluster");
    System.err.println(" $ bin/hbase " + "org.apache.hadoop.hbase.client.example.RealDeleteTool "
        + "--cluster=hbase://hytst-staging98 --table=test-table rows.file");
    System.err.println();
    System.exit(1);
  }

  private static void praseCommandLine(String[] args) {
    if (args.length < 1) {
      printUsageAndExit();
    }
    for (int i = 0; i < args.length; i++) {
      String cmd = args[i];
      if (cmd.equals("-h") || cmd.startsWith("--h")) {
        printUsageAndExit();
      }
      String clusterKey = "--cluster=";
      if (cmd.startsWith(clusterKey)) {
        cluster = cmd.substring(clusterKey.length());
        continue;
      }
      String tableKey = "--table=";
      if (cmd.startsWith(tableKey)) {
        tableName = cmd.substring(tableKey.length());
        continue;
      }
      if (i == args.length - 1) {
        fileName = cmd;
      } else {
        System.err.println("Invalid argument '" + cmd + "'");
        printUsageAndExit();
      }
    }
    LOG.info(
      "Arguments table: " + tableName + ", cluster: " + cluster + ", rows file: " + fileName);
  }

  private static void realDeleteTableRows(HConnection conn, List<String> rows)
      throws IOException, InterruptedException {
    try (HTableInterface table = conn.getTable(tableName)) {
      for (String row : rows) {
        Delete delete = new Delete(Bytes.toBytes(row));
        table.delete(delete);
      }
    }
    try (HBaseAdmin admin = new HBaseAdmin(conn)) {
      admin.majorCompact(tableName);
    }
  }

  private static List<String> readRowsToDelete() throws IOException {
    List<String> rows = new ArrayList<>();
    try (BufferedReader reader = new BufferedReader(
        new InputStreamReader(new FileInputStream(fileName)))) {
      for (String line = reader.readLine(); line != null; line = reader.readLine()) {
        rows.add(line.trim());
      }
    } catch (FileNotFoundException fnfe) {
      LOG.error("File " + fileName + " not found");
      fnfe.printStackTrace();
    }
    return rows;
  }

  public static void main(String[] args) throws Exception {
    praseCommandLine(args);
    List<String> rowsToDelete = readRowsToDelete();
    if (!cluster.startsWith(NameService.HBASE_URI_PREFIX)) {
      cluster = NameService.HBASE_URI_PREFIX + cluster;
    }
    Configuration conf = HBaseConfiguration.create();
    HBaseConfiguration.merge(conf, NameService.createConfigurationByClusterKey(cluster, conf));
    LOG.info("hbase.zookeeper.quorum=" + conf.get("hbase.zookeeper.quorum"));
    LOG.info(
      "hbase.zookeeper.property.clientPort=" + conf.get("hbase.zookeeper.property.clientPort"));
    LOG.info("zookeeper.znode.parent=" + conf.get("zookeeper.znode.parent"));

    Map<String, HConnection> connMap = new HashMap<>();
    try {
      HConnection conn = HConnectionManager.createConnection(conf);
      connMap.put(cluster, conn);
      try (HBaseAdmin admin = new HBaseAdmin(conn)) {
        List<ReplicationPeerDescription> peers = admin.listReplicationPeers();
        for (ReplicationPeerDescription peer : peers) {
          if (peer.getPeerConfig().needToReplicate(TableName.valueOf(tableName))) {
            Configuration peerConf = ReplicationUtils.getPeerClusterConfiguration(peer, conf);
            String peerClusterKey = peer.getPeerConfig().getClusterKey();
            if (!connMap.containsKey(peerClusterKey)) {
              connMap.put(peerClusterKey, HConnectionManager.createConnection(peerConf));
            }
          }
        }
      }
      for (Map.Entry<String, HConnection> entry : connMap.entrySet()) {
        System.out
            .println("Will delete rows for table: " + tableName + ", cluster: " + entry.getKey());
        realDeleteTableRows(entry.getValue(), rowsToDelete);
      }
    } finally {
      connMap.values().stream().forEach(IOUtils::closeQuietly);
    }
  }
}
