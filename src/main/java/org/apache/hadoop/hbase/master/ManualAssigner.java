/**
 * Copyright 2010 The Apache Software Foundation
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

package org.apache.hadoop.hbase.master;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HDFSBlocksDistribution;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;

/**
 * Assign the regions of table manually, according the data locality
 */

public class ManualAssigner {
  private static final Log LOG = LogFactory.getLog(ManualAssigner.class);

  private Configuration conf;
  private HBaseAdmin admin;
  private Map<String, RoundRobinSelector<ServerName>> candidates;

  public ManualAssigner(Configuration conf) throws IOException {
    this.conf = conf;
    this.admin = new HBaseAdmin(conf);
    init();
  }

  private void init() throws IOException {
    List<ServerName> rs = admin.getMaster().getOnlineRS();
    LOG.info("All online region servers: " + rs);
    this.candidates = new HashMap<String, RoundRobinSelector<ServerName>>();
    for (ServerName sn : rs) {
      if (!candidates.containsKey(sn.getHostname())) {
        candidates.put(sn.getHostname(), new RoundRobinSelector<ServerName>());
      }
      candidates.get(sn.getHostname()).add(sn);
    }
  }

  public void assignAllTables() throws IOException {
    for (String table : admin.getTableNames()) {
      assign(table);
    }
  }

  public void assign(final String table) throws IOException {
    if (admin.isTableEnabled(table)) {
      LOG.info("Skip diabled table: " + table);
      return;
    }
    LOG.info("Assign region of " + table + " manually, according data locality");

    List<HRegionInfo> regions = admin.getTableRegions(Bytes.toBytes(table));
    HTableDescriptor desc = admin.getTableDescriptor(Bytes.toBytes(table));

    for (HRegionInfo region : regions) {
      HDFSBlocksDistribution dist =
          HRegion.computeHDFSBlocksDistribution(conf, desc, region.getEncodedName());

      if (dist.getTopHosts().size() == 0) continue;

      ServerName dst = selectRegionServer(region, dist);
      if (dst == null) {
        continue;
      }
      admin.move(region.getEncodedNameAsBytes(), Bytes.toBytes(dst.getServerName()));
      LOG.info("move " + region.getEncodedName() + " : " + dst.getServerName());
      Threads.sleep(1000);
    }
  }

  private ServerName selectRegionServer(final HRegionInfo region, HDFSBlocksDistribution dist)
      throws IOException {
    String current = admin.getConnection().locateRegion(region.getRegionName()).getHostname();
    for (String host : dist.getTopHosts()) {
      if (current != null && current.equals(host)) {
        return null;
      }
      LOG.info(region + " : " + host + " " + dist.getBlockLocalityIndex(host));
      RoundRobinSelector<ServerName> selector = candidates.get(host);
      if (selector == null) {
        continue;
      }
      ServerName dst = selector.select();
      if (dst == null) {
        continue;
      }
      LOG.info("Candidate " + dst);
      return dst;
    }
    return null;
  }

  public void close() throws IOException {
    this.admin.close();
  }

  public static void main(String[] args) throws IOException {
    System.out.println("./ManualAssigner [$tableName2] [$tableName2] ...");
    System.out.println("If no table is specified, all tables in the cluster will be reassigned according data locality.");
    ManualAssigner assigner = new ManualAssigner(HBaseConfiguration.create());
    if (args.length == 0) {
      assigner.assignAllTables();
    } else {
      for (String table : args) {
        assigner.assign(table);
      }
    }
    assigner.close();
  }

  class RoundRobinSelector<R> {
    private ArrayList<R> candidates;
    private int next;

    public RoundRobinSelector() {
      this.candidates = new ArrayList<R>();
      this.next = 0;
    }

    public void add(R candidate) {
      this.candidates.add(candidate);
    }

    public R select() {
      if (candidates.size() == 0) return null;
      next = (next) % candidates.size();
      return candidates.get(next++);
    }
  }
}
