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

package org.apache.hadoop.hbase.throughput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.catalog.MetaReader;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Throughput quota meta table data access methods.
 */
public class ThroughputQuotaTable {
  // Internal table for throughput quota limits
  public static final String THROUGHPUT_QUOTA_TABLE_NAME_STR = "_throughput_quota_";
  public static final byte[] THROUGHPUT_QUOTA_TABLE_NAME =
      Bytes.toBytes(THROUGHPUT_QUOTA_TABLE_NAME_STR);

  /*
   * Special table name which is used to limit total throughput of requests to all tables for one
   * user
   */
  public static final String WILDCARD_TABLE_NAME_STR = "*";
  public static final byte[] WILDCARD_TABLE_NAME = Bytes.toBytes(WILDCARD_TABLE_NAME_STR);

  // Column family used to store quota limits
  public static final String THROUGHPUT_QUOTA_FAMILY_STR = "t";
  public static final byte[] THROUGHPUT_QUOTA_FAMILY = Bytes.toBytes(THROUGHPUT_QUOTA_FAMILY_STR);

  // Table descriptor for throughput quota limits internal table
  public static final HTableDescriptor THROUGHPUT_QUOTA_TABLEDESC = new HTableDescriptor(
      THROUGHPUT_QUOTA_TABLE_NAME);

  static {
    HColumnDescriptor hcd = new HColumnDescriptor(THROUGHPUT_QUOTA_FAMILY);
    // Follow the same configurations with ACL table
    hcd.setMaxVersions(10); // Arbitrary number, keep versions to help debugging
    hcd.setCompressionType(Compression.Algorithm.NONE);
    hcd.setInMemory(true);
    hcd.setBlockCacheEnabled(true);
    hcd.setBlocksize(8 * 1024);
    hcd.setTimeToLive(HConstants.FOREVER);
    hcd.setBloomFilterType(StoreFile.BloomType.NONE);
    hcd.setScope(HConstants.REPLICATION_SCOPE_LOCAL);
    THROUGHPUT_QUOTA_TABLEDESC.addFamily(hcd);
  }

  private static Log LOG = LogFactory.getLog(ThroughputQuotaTable.class);

  /**
   * Check for existence of {@code THROUGHPUT_QUOTA_TABLE_NAME_STR} table and create it if it does
   * not exist
   */
  static void init(MasterServices master) throws IOException {
    if (!MetaReader.tableExists(master.getCatalogTracker(), THROUGHPUT_QUOTA_TABLE_NAME_STR)) {
      master.createTable(THROUGHPUT_QUOTA_TABLEDESC, null);
    }
  }

  /**
   * Load and parse all records from a throughput quota region.
   */
  public static List<ThroughputQuota> loadAll(HRegion region) throws IOException {
    if (!isQuotaRegion(region)) {
      throw new IOException("Can only load quota limits from " + THROUGHPUT_QUOTA_TABLE_NAME_STR);
    }

    List<ThroughputQuota> all = new ArrayList<ThroughputQuota>();

    // scan all records on this region
    Scan scan = new Scan();
    scan.addFamily(THROUGHPUT_QUOTA_FAMILY);

    InternalScanner iScanner = null;
    try {
      iScanner = region.getScanner(scan);
      while (true) {
        List<KeyValue> kvs = new ArrayList<KeyValue>();
        boolean hasNext = iScanner.next(kvs).hasNext();

        if (!kvs.isEmpty()) {
          ThroughputQuota quota = parseThroughputQuota(kvs);
          if (quota != null) {
            all.add(quota);
          }
        }

        if (!hasNext) {
          break;
        }
      }
    } finally {
      if (iScanner != null) {
        iScanner.close();
      }
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Loaded quota limits from quota table region: " + all);
    }
    return all;
  }

  /**
   * Load throughput quota from internal meta table.
   */
  public static ThroughputQuota loadThroughputQuota(Configuration conf, byte[] tableName)
      throws IOException {
    ThroughputQuota quota = null;
    HTable table = null;
    try {
      table = new HTable(conf, THROUGHPUT_QUOTA_TABLE_NAME);
      Get get = new Get(tableName);
      get.addFamily(THROUGHPUT_QUOTA_FAMILY);
      Result result = table.get(get);
      quota = parseThroughputQuota(result.list());
    } finally {
      if (table != null) {
        table.close();
      }
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Loaded quota limits for table '" + Bytes.toString(tableName) + "': " + quota);
    }
    return quota;
  }

  /**
   * Save throughput quota to internal meta table.
   */
  public static void saveThroughputQuota(Configuration conf, ThroughputQuota newQuota)
      throws IOException {
    if (newQuota == null || newQuota.getLimits() == null || newQuota.getLimits().isEmpty()) {
      return;
    }
    HTable table = null;
    try {
      table = new HTable(conf, THROUGHPUT_QUOTA_TABLE_NAME);
      byte[] rowKey = Bytes.toBytes(newQuota.getTableName());

      Put put = new Put(rowKey);
      for (Entry<String, EnumMap<RequestType, Double>> e1 : newQuota.getLimits().entrySet()) {
        String userNamePrefix = e1.getKey() + ":";
        for (Entry<RequestType, Double> e2 : e1.getValue().entrySet()) {
          // Each qualifier is formatted as User:RequestType
          byte[] q = Bytes.toBytes(userNamePrefix + (char) e2.getKey().code());
          Double limit = e2.getValue();
          if (limit != null && limit >= 0) {
            put.add(THROUGHPUT_QUOTA_FAMILY, q, Bytes.toBytes(limit.toString()));
          } else {
            // null or negative means no limit
            put.add(THROUGHPUT_QUOTA_FAMILY, q, null);
          }
        }
      }

      // The quotas of unspecified users/request types are unchanged
      table.put(put);
    } finally {
      if (table != null) {
        table.close();
      }
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Saved quota limits: " + newQuota);
    }
  }

  public static void removeThroughputQuota(Configuration conf, byte[] tableName) throws IOException {
    HTable table = null;
    try {
      table = new HTable(conf, THROUGHPUT_QUOTA_TABLE_NAME);
      Delete delete = new Delete(tableName);
      table.delete(delete);
    } finally {
      if (table != null) {
        table.close();
      }
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Removed quota limits of removed table " + Bytes.toString(tableName));
    }
  }

  /**
   * Returns {@code true} if the given region is part of the {@code _throughput_quota_} metadata
   * table.
   */
  protected static boolean isQuotaRegion(HRegion region) {
    return Bytes.equals(THROUGHPUT_QUOTA_TABLE_NAME, region.getTableDesc().getName());
  }

  /*
   * Make sure all these KeyValue entries belongs to a single table.
   */
  private static ThroughputQuota parseThroughputQuota(List<KeyValue> kvs)
      throws IOException {
    if (kvs == null || kvs.isEmpty()) {
      return null;
    }

    byte[] tableName = null;
    Map<String, EnumMap<RequestType, Double>> limits = new HashMap<String, EnumMap<RequestType, Double>>();

    for (KeyValue kv : kvs) {
      if (tableName == null) {
        tableName = kv.getRow();
      }

      String q = Bytes.toString(kv.getQualifier());
      String[] items = q.split(":");
      if (items.length != 2 || items[0].isEmpty() || items[1].length() != 1) {
        throw new IOException("Invalid throughput quota row, unknown qualifier: " + q);
      }
      String userName = items[0];
      RequestType requestType = RequestType.fromCode((byte) items[1].charAt(0));
      String limitStr = Bytes.toString(kv.getValue());
      if (limitStr == null || limitStr.isEmpty()) {
        continue;
      }
      Double limit = Double.valueOf(limitStr);
      if (limit < 0) {
        continue;
      }

      EnumMap<RequestType, Double> reqLimits = limits.get(userName);
      if (reqLimits == null) {
        reqLimits = new EnumMap<RequestType, Double>(RequestType.class);
        limits.put(userName, reqLimits);
      }
      reqLimits.put(requestType, limit);
    }

    if (tableName != null) {
      return new ThroughputQuota(Bytes.toString(tableName), limits);
    } else {
      LOG.warn("Failed to parse quota from non empty row: " + kvs);
      return null;
    }
  }
}
