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

package org.apache.hadoop.hbase.tool;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import com.google.common.collect.Sets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.replication.ReplicationAdmin;
import org.apache.hadoop.hbase.replication.ReplicationZookeeper;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * HBase replication status reporting tool.
 */
public final class ReplicationStatus implements Tool {
  private static final Log LOG = LogFactory.getLog(ReplicationStatus.class);
  private Configuration conf;

  @Override public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override public Configuration getConf() {
    return conf;
  }

  @Override public int run(String[] args) throws Exception {
    int tail = 1;
    Collection<String> targetPeerIds = null;
    boolean checkLength = false;

    // Process command line args
    for (int i = 0; i < args.length; i++) {
      String cmd = args[i];

      if (cmd.startsWith("-")) {
        if (cmd.equals("-help")) {
          // user asked for help, print the help and quit.
          printUsageAndExit();
        } else if (cmd.equals("-tail")) {
          i++;

          if (i == args.length) {
            System.err.println("-tail needs a numeric value argument.");
            printUsageAndExit();
          }

          try {
            tail = Integer.parseInt(args[i]);
          } catch (NumberFormatException e) {
            System.err.println("-tail needs a numeric value argument.");
            printUsageAndExit();
          }
        } else if (cmd.equals("-peerid")) {
          i++;

          if (i == args.length) {
            System.err.println("-peerid needs a string list argument.");
            printUsageAndExit();
          }

          try {
            if (!args[i].isEmpty()) {
              targetPeerIds = Arrays.asList(args[i].split(","));
            }
          } catch (NumberFormatException e) {
            System.err.println("-peerid needs a string list argument.");
            printUsageAndExit();
          }
        } else if (cmd.equals("-checklength")) {
          checkLength = true;
        } else {
          printUsageAndExit();
        }
      }
    }
    ReplicationAdmin replicationAdmin = new ReplicationAdmin(conf);
    ReplicationZookeeper replicationZookeeper = replicationAdmin.getReplicationZk();
    // Check all peers by default
    if (targetPeerIds == null) {
      Map<String, String> peers = replicationAdmin.listPeers();
      targetPeerIds = peers.keySet();
    }

    // active regionservers
    Set<String> activeRs = new HashSet<String>(replicationZookeeper.getRegisteredRegionServers());
    // regionserver nodes under replication path
    Set<String> repRs = new HashSet<String>(replicationZookeeper.getReplicatingRSZNodeList());
    Set<String> diff1 = Sets.difference(activeRs, repRs);
    Set<String> diff2 = Sets.difference(repRs, activeRs);
    System.out.println("=========================== warnings summary ===========================");
    if (!diff1.isEmpty()) {
      System.out.format("WARNING: Some regionservers are not replicating: %s\n", diff1);
    }
    if (!diff2.isEmpty()) {
      System.out.format("WARNING: Some legacy regionservers still in replication: %s\n", diff2);
    }

    FileSystem fs = FileSystem.get(conf);
    Map<String, Map<String, RegionServerReplicationStatus>> replicationStatus =
        new HashMap<String, Map<String, RegionServerReplicationStatus>>();
    boolean fileLencheckPassed = true;
    for (String rs : repRs) {
      List<String> peerIds = replicationZookeeper.getListPeersForRS(rs);
      for (String peerId : peerIds) {
        // extract the real peerId
        String realPeerId = peerId.split("-")[0];
        if (targetPeerIds.contains(realPeerId)) {
          Map<String, RegionServerReplicationStatus> rsMap = replicationStatus.get(realPeerId);
          if (rsMap == null) {
            rsMap = new HashMap<String, RegionServerReplicationStatus>();
            replicationStatus.put(realPeerId, rsMap);
          }
          List<String> hlogs = replicationZookeeper.getListHLogsForPeerForRS(rs, peerId);
          if (hlogs != null) {
            for (String hlog : hlogs) {
              String rsZNode = replicationZookeeper.getRsZNode(rs);
              Pair<Long, Long> p = replicationZookeeper.getHLogRepPosition(rsZNode, peerId, hlog);
              RegionServerReplicationStatus rsRepStatus = rsMap.get(rs);
              if (rsRepStatus == null) {
                rsRepStatus = new RegionServerReplicationStatus(rs);
                rsMap.put(rs, rsRepStatus);
              }
              if (checkLength) {
                String clusterName = conf.get("hbase.cluster.name");
                // example: /hbase/lgsrv-micloud/.logs/lg-hadoop-srv-st461.bj,12620,1441588425583
                // /lg-hadoop-srv-st461.bj%2C12620%2C1441588425583.1441588432481
                String path = String.format("/hbase/%s/.logs/%s/%s", clusterName, rs, hlog);
                FileStatus status = fs.getFileStatus(new Path(path));
                rsRepStatus.addHLogStatus(new HLogReplicationStatus(peerId, hlog, status.getLen(),
                    p.getFirst(), p.getSecond()));
                if (status.getLen() != p.getFirst()) {
                  fileLencheckPassed = false;
                }
              } else {
                rsRepStatus.addHLogStatus(
                    new HLogReplicationStatus(peerId, hlog, -1, p.getFirst(), p.getSecond()));
              }
            }
          }
        }
      }
    }
    if (!fileLencheckPassed) {
      System.out.println("WARNING: file length differs from zk position," +
          "make sure write is disabled and hlog_roll is done");
    }

    Map<String, SortedSet<RegionServerReplicationStatus>> repStatusByPeer =
        new TreeMap<String, SortedSet<RegionServerReplicationStatus>>();
    for (Map.Entry<String, Map<String, RegionServerReplicationStatus>> e :
        replicationStatus.entrySet()) {
      SortedSet<RegionServerReplicationStatus> sortedRsSet = repStatusByPeer.get(e.getKey());
      if (sortedRsSet == null) {
        sortedRsSet = new TreeSet<RegionServerReplicationStatus>();
        repStatusByPeer.put(e.getKey(), sortedRsSet);
      }
      sortedRsSet.addAll(e.getValue().values());
    }
    // display summary
    System.out.println("====================== replication status summary ======================");
    final String fmtSummary = "%-10s\t%-35s\t%-35s\t%-60s\n";
    System.out.format(fmtSummary, "[peer id]", "[last log entry write time]", "[hfile roll time]", "[most lagged hlog]");
    for (Map.Entry<String, SortedSet<RegionServerReplicationStatus>> e :
        repStatusByPeer.entrySet()) {
      String lastWriteTime = "NA";
      String rollTime = "NA";
      String hlog = "NA";
      if (!e.getValue().isEmpty()) {
        RegionServerReplicationStatus rsStatus = e.getValue().first();
        if (!rsStatus.hlogs.isEmpty()) {
          hlog = rsStatus.regionServer + "/" + rsStatus.hlogs.first().hlog;
          lastWriteTime = fmtTimestamp(rsStatus.hlogs.first().writeTime);
          rollTime = fmtTimestamp(rsStatus.hlogs.first().rollTime);
        }
      }
      System.out.format(fmtSummary, e.getKey(), lastWriteTime, rollTime, hlog);
    }

    if (tail > 0) {
      // display details of the n most lagged node
      System.out.println();
      System.out.println("===================== replication status details =====================");
      System.out.println("# sorted by write time of last replicated log entry of each node/hlog");
      for (Map.Entry<String, SortedSet<RegionServerReplicationStatus>> e :
          repStatusByPeer.entrySet()) {
        System.out.format("peer id: %s\n", e.getKey());
        if (!e.getValue().isEmpty()) {
          int i = 0;
          for (RegionServerReplicationStatus rsStatus : e.getValue()) {
            System.out.format("[regionserver %2d] %s\n", i++, rsStatus);
            if (i >= tail) {
              break;
            }
          }
        } else {
          System.out.println("[No regionservers available]");
        }
      }
    }

    return 0;
  }

  private String fmtTimestamp(long ts) {
    String dateString = "NA";
    if (ts > 0) {
      SimpleDateFormat dateFormat = new SimpleDateFormat("yy/MM/dd-HH:mm:ss");
      dateString = dateFormat.format(new Date(ts));
    }
    return String.format("%s(%d)", dateString, ts);
  }

  private class HLogReplicationStatus implements Comparable<HLogReplicationStatus> {
    private final String hlog;
    private final long position;
    private final long length;
    private final long writeTime;
    private final long rollTime;

    private HLogReplicationStatus(String peerId, String hlog, long length, long position,
        long writeTime) {
      this.hlog = peerId + "/" + hlog;
      this.position = position;
      this.length = length;
      this.writeTime = writeTime;
      // hlog file name format: {host}%2C{port}%2C{start time}.{13 digit roll time}
      int pos = hlog.length() - 13;
      if ('.' != hlog.charAt(pos - 1)) {
        throw new IllegalArgumentException("Invalid hlog file format: " + hlog);
      }
      rollTime = Long.parseLong(hlog.substring(pos, hlog.length()));
    }
    
    private long writeTimeLowerBound() {
      return writeTime <= 0 ? rollTime : writeTime;
    }

    @Override public int compareTo(HLogReplicationStatus that) {
      // Sort by write time of last replicated log entry
      long ret = this.writeTimeLowerBound() - that.writeTimeLowerBound();
      if (ret == 0) {
        ret = this.hlog.compareTo(that.hlog);
        if (ret == 0) {
          if (this.length == -1 || that.length == -1) {
            ret = this.position - that.position;
          } else {
            ret = (this.position - this.length) - (that.position - that.length);
          }
        }
      }
      return ret == 0 ? 0 : (ret < 0 ? -1 : 1);
    }

    @Override public String toString() {
      return String.format("%s: %s, %s, %14d(len:%d, diff:%d)",
          hlog, fmtTimestamp(writeTime), fmtTimestamp(rollTime), position, length, length - position);
    }
  }

  private class RegionServerReplicationStatus implements Comparable<RegionServerReplicationStatus> {
    private final String regionServer;
    private final SortedSet<HLogReplicationStatus> hlogs = new TreeSet<HLogReplicationStatus>();

    private RegionServerReplicationStatus(String regionServer) {
      this.regionServer = regionServer;
    }

    public void addHLogStatus(HLogReplicationStatus hlogStatus) {
      hlogs.add(hlogStatus);
    }

    @Override public int compareTo(RegionServerReplicationStatus that) {
      int ret = 0;
      if (this.hlogs.isEmpty()) {
        ret = that.hlogs.isEmpty() ? 0 : -1;
      } else if (that.hlogs.isEmpty()) {
        ret = 1;
      } else {
        HLogReplicationStatus left = this.hlogs.first();
        HLogReplicationStatus right = that.hlogs.first();
        ret = left.compareTo(right);
      }
      if (ret == 0) {
        ret = this.regionServer.compareTo(that.regionServer);
      }
      return ret;
    }

    @Override public String toString() {
      StringBuilder buff = new StringBuilder();
      buff.append(regionServer);
      buff.append(":\n");
      int i = 0;
      for (HLogReplicationStatus hlog : hlogs) {
        buff.append("[");
        buff.append(Integer.toString(i++));
        buff.append("/");
        buff.append(hlogs.size());
        buff.append("] ");
        buff.append(hlog.toString());
        buff.append("\n");
      }
      return buff.toString();
    }
  }

  private void printUsageAndExit() {
    System.err.printf("Usage: bin/hbase %s [opts]\n", getClass().getName());
    System.err.println(" where [opts] are:");
    System.err.println("   -help          Show this help and exit.");
    System.err.println("   -peerid <List> Peer id(s) to check (default to all peers).");
    System.err.println("   -tail <N>      Show details of the most lagged N nodes (default to 1).");
    System.exit(1);
  }

  public static void main(String[] args) {
    Configuration conf = HBaseConfiguration.create();

    int exitCode = 0;
    try {
      exitCode = ToolRunner.run(conf, new ReplicationStatus(), args);
    } catch (Exception e) {
      LOG.error("Replication status tool exited with exception. ", e);
    }
    System.exit(exitCode);
  }
}
