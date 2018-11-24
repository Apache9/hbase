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
package org.apache.hadoop.hbase.replication;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.catalog.MetaEditor;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * A tool to check or fix serial replication stuck problem.
 */
@InterfaceAudience.Private
public class SerialReplicationTool extends Configured implements Tool {
  private static final Log LOG =
      LogFactory.getLog(SerialReplicationTool.class);

  private final int SLEEP_TIME = 60000;

  private HConnection connection;

  private HTableInterface metaTable;

  public SerialReplicationTool(Configuration conf) throws IOException {
    connection = HConnectionManager.createConnection(conf);
    metaTable = connection.getTable(TableName.META_TABLE_NAME);
  }

  /**
   * The guts of the {@link #main} method.
   * Call this method to avoid the {@link #main(String[])} System.exit.
   * @param args
   * @return errCode
   * @throws Exception
   */
  static int innerMain(final Configuration conf, final String [] args) throws Exception {
    return ToolRunner.run(conf, new SerialReplicationTool(conf), args);
  }

  public static void main(String[] args) throws Exception {
    System.exit(innerMain(HBaseConfiguration.create(), args));
  }

  private static void printUsage() {
    System.err
        .printf("Usage: bin/hbase org.apache.hadoop.hbase.replication.SerialReplicationTool [options] peerId");
    System.err.println(" where [options] are:");
    System.err.println("  -h|-help    Show this help and exit.");
    System.err.println("  check       Check if the peer is stuck");
    System.err.println("  fix         Fix the peer stuck by update meta replication positions");
    System.err.println();
  }

  private void cleanUnusedBarriers(String encodedRegionName, List<Long> barriers) 
      throws IOException {
    Delete delete = new Delete(Bytes.toBytes(encodedRegionName));
    for (int i = 0; i < barriers.size() - 1; i++) {
      delete.deleteColumn(HConstants.REPLICATION_BARRIER_FAMILY,
          Bytes.toBytes(barriers.get(i)));
      LOG.info("Cleanup barrier " + barriers.get(i) + " for region=" + encodedRegionName);
    }
    metaTable.delete(delete);
  }

  private void updatePeerReplicationPosition(String peerId, String encodedRegionName, long pos)
      throws IOException {
    Map<String, Long> map = new HashMap<>();
    map.put(encodedRegionName, pos);
    try {
      MetaEditor.updateReplicationPositions(connection, peerId, map);
    } catch (IOException e) {
      LOG.warn("Failed to updateReplicationPositions, retry", e);
      MetaEditor.updateReplicationPositions(connection, peerId, map);
    }
  }

  private void fixStuckPeer(String peerId) throws Exception {
    Map<String, List<Long>> barrierMap = MetaEditor.getAllBarriers(connection);
    Map<String, Long> stuckRegions = getStuckRegions(barrierMap, peerId);
    LOG.info("Totally found " + stuckRegions.size() + " regions maybe stuck peer " + peerId);

    // Sleep again when try to fix
    LOG.info("Will sleep " + SLEEP_TIME + " ms to see whether position in meta was updated");
    Thread.sleep(SLEEP_TIME);

    for (Map.Entry<String, Long> entry : stuckRegions.entrySet()) {
      String encodedRegionName = entry.getKey();
      long posInMeta = MetaEditor.getReplicationPositionForOnePeer(connection,
        Bytes.toBytes(encodedRegionName), peerId);
      if (posInMeta == entry.getValue().longValue()) {
        List<Long> barriers = barrierMap.get(encodedRegionName);
        LOG.info("Peer " + peerId + " still stuck by region=" + encodedRegionName + ", posInMeta="
            + posInMeta + ", barriers=" + Arrays.toString(barriers.toArray())
            + ". Will try to fix it!");
        if (barriers.size() > 0) {
          // update new pos to last barrier - 1
          long newPos = barriers.get(barriers.size() - 1) - 1;
          if (posInMeta < newPos) {
            // cleanup unused barriers
            if (barriers.size() - 1 > 0) {
              cleanUnusedBarriers(encodedRegionName, barriers);
            }
            updatePeerReplicationPosition(peerId, encodedRegionName, newPos);
            LOG.info("Update region=" + encodedRegionName + " posInMeta from " + posInMeta + " to "
                + newPos + ", barriers=" + Arrays.toString(barriers.toArray()) + ", peerId="
                + peerId);
          }
        }
      } else {
        LOG.info("No need to fix for region=" + encodedRegionName + " because posInMeta was updated from "
            + entry.getValue() + " to " + posInMeta);
      }
    }
  }

  private void checkStuckPeer(String peerId) throws Exception {
    Map<String, List<Long>> barrierMap = MetaEditor.getAllBarriers(connection);
    Map<String, Long> stuckRegions = getStuckRegions(barrierMap, peerId);
    LOG.info("Totally found " + stuckRegions.size() + " regions maybe stuck peer " + peerId);
  }

  private Map<String, Long> getStuckRegions(Map<String, List<Long>> barrierMap, String peerId)
      throws Exception {
    Map<String, Long> posInMetaMap = new HashMap<>();
    for (String encodedRegionName : barrierMap.keySet()) {
      long posInMeta = MetaEditor
          .getReplicationPositionForOnePeer(connection, Bytes.toBytes(encodedRegionName), peerId);
      posInMetaMap.put(encodedRegionName, posInMeta);
      LOG.info(
          "Get posInMeta=" + posInMeta + " for region=" + encodedRegionName + ", peerId=" + peerId);
    }

    // Sleep to see whether position in meta was updated
    LOG.info("Will sleep " + SLEEP_TIME + " ms to see whether position in meta was updated");
    Thread.sleep(SLEEP_TIME);

    Map<String, Long> stuckRegions = new HashMap<>();
    for (Map.Entry<String, List<Long>> entry : barrierMap.entrySet()) {
      String encodedRegionName = entry.getKey();
      long posInMeta = MetaEditor.getReplicationPositionForOnePeer(connection,
        Bytes.toBytes(encodedRegionName), peerId);
      if (posInMeta == posInMetaMap.get(encodedRegionName).longValue()) {
        List<Long> barriers = entry.getValue();
        if (barriers.size() > 0) {
          long newPos = barriers.get(barriers.size() - 1) - 1;
          if (posInMeta < newPos) {
            stuckRegions.put(encodedRegionName, posInMeta);
            LOG.info("Peer " + peerId + " maybe stuck by region=" + encodedRegionName + ", posInMeta="
                + posInMeta + ", barriers=" + Arrays.toString(entry.getValue().toArray()));
          } else {
            LOG.info("Not stuck by region=" + encodedRegionName + " because posInMeta=" + posInMeta
                + " is equal or bigger than newPos=" + newPos + ", barriers=" + Arrays
                .toString(barriers.toArray()) + ", peerId=" + peerId);
          }
        } else {
          LOG.info("Not stuck by region=" + encodedRegionName + ", posInMeta=" + posInMeta
              + ", because no barriers");
        }
      } else {
        LOG.info("Not stuck by region=" + encodedRegionName + " because posInMeta was updated from "
            + posInMetaMap.get(encodedRegionName) + " to " + posInMeta);
      }
    }
    return stuckRegions;
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args.length < 1) {
      printUsage();
      return 1;
    }
    if (args[0].equals("-help") || args[0].equals("-h")) {
      printUsage();
      return 1;
    }
    if (args.length != 2) {
      printUsage();
      return 1;
    }
    if (args[0].equals("check")) {
      String peerId = args[1];
      checkStuckPeer(peerId);
    } else if (args[0].equals("fix")) {
      String peerId = args[1];
      fixStuckPeer(peerId);
    } else {
      printUsage();
      return 1;
    }
    metaTable.close();
    connection.close();
    return 0;
  }
}
