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

package org.apache.hadoop.hbase.quotas;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.QuotaProtos.Quotas;
import org.apache.hadoop.hbase.protobuf.generated.QuotaProtos.Throttle;
import org.apache.hadoop.hbase.protobuf.generated.QuotaProtos.ThrottleOrBuilder;
import org.apache.hadoop.hbase.protobuf.generated.QuotaProtos.TimedQuota;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Helper class to interact with the quota table
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class QuotaUtil extends QuotaTableUtil {
  private static final Log LOG = LogFactory.getLog(QuotaUtil.class);

  public static final String QUOTA_CONF_KEY = "hbase.quota.enabled";
  private static final boolean QUOTA_ENABLED_DEFAULT = false;
  
  public static final String READ_CAPACITY_UNIT_CONF_KEY = "hbase.read.capacity.unit";
  public static final int DEFAULT_READ_CAPACITY_UNIT = 1024;
  public static final String WRITE_CAPACITY_UNIT_CONF_KEY = "hbase.write.capacity.unit";
  public static final int DEFAULT_WRITE_CAPACITY_UNIT = 1024;

  public static final String THROTTLING_MIN_WAIT_INTERVAL = "hbase.throttling.min.wait.interval";
  public static final long DEFAULT_THROTTLING_MIN_WAIT_INTERVAL = 10;

  //there are many unit test not allow exceed (for the DefaultOperationQuota), so need this conf
  public static final String QUOTA_ALLOW_EXCEED_CONF_KEY = "hbase.quota.allow.exceed";
  public static final boolean DEFAULT_QUOTA_ALLOW_EXCEED = true;

  /** Table descriptor for Quota internal table */
  public static final HTableDescriptor QUOTA_TABLE_DESC =
    new HTableDescriptor(QUOTA_TABLE_NAME);
  static {
    QUOTA_TABLE_DESC.addFamily(
      new HColumnDescriptor(QUOTA_FAMILY_INFO)
        .setScope(HConstants.REPLICATION_SCOPE_LOCAL)
        .setBloomFilterType(BloomType.ROW)
        .setMaxVersions(1)
    );
    QUOTA_TABLE_DESC.addFamily(
      new HColumnDescriptor(QUOTA_FAMILY_USAGE)
        .setScope(HConstants.REPLICATION_SCOPE_LOCAL)
        .setBloomFilterType(BloomType.ROW)
        .setMaxVersions(1)
    );
  }

  /** Returns true if the support for quota is enabled */
  public static boolean isQuotaEnabled(final Configuration conf) {
    return conf.getBoolean(QUOTA_CONF_KEY, QUOTA_ENABLED_DEFAULT);
  }

  /* =========================================================================
   *  Quota "settings" helpers
   */
  public static void addTableQuota(final Configuration conf, final TableName table,
      final Quotas data) throws IOException {
    addQuotas(conf, getTableRowKey(table), data);
  }

  public static void deleteTableQuota(final Configuration conf, final TableName table)
      throws IOException {
    deleteQuotas(conf, getTableRowKey(table));
  }

  public static void addNamespaceQuota(final Configuration conf, final String namespace,
      final Quotas data) throws IOException {
    addQuotas(conf, getNamespaceRowKey(namespace), data);
  }

  public static void deleteNamespaceQuota(final Configuration conf, final String namespace)
      throws IOException {
    deleteQuotas(conf, getNamespaceRowKey(namespace));
  }

  public static void addUserQuota(final Configuration conf, final String user,
      final Quotas data) throws IOException {
    addQuotas(conf, getUserRowKey(user), data);
  }

  public static void addUserQuota(final Configuration conf, final String user,
      final TableName table, final Quotas data) throws IOException {
    addQuotas(conf, getUserRowKey(user),
        getSettingsQualifierForUserTable(table), data);
  }

  public static void addUserQuota(final Configuration conf, final String user,
      final String namespace, final Quotas data) throws IOException {
    addQuotas(conf, getUserRowKey(user),
        getSettingsQualifierForUserNamespace(namespace), data);
  }

  public static void deleteUserQuota(final Configuration conf, final String user)
      throws IOException {
    deleteQuotas(conf, getUserRowKey(user));
  }

  public static void deleteUserQuota(final Configuration conf, final String user,
      final TableName table) throws IOException {
    deleteQuotas(conf, getUserRowKey(user),
        getSettingsQualifierForUserTable(table));
  }

  public static void deleteUserQuota(final Configuration conf, final String user,
      final String namespace) throws IOException {
    deleteQuotas(conf, getUserRowKey(user),
        getSettingsQualifierForUserNamespace(namespace));
  }

  private static void addQuotas(final Configuration conf, final byte[] rowKey,
      final Quotas data) throws IOException {
    addQuotas(conf, rowKey, QUOTA_QUALIFIER_SETTINGS, data);
  }

  private static void addQuotas(final Configuration conf, final byte[] rowKey,
      final byte[] qualifier, final Quotas data) throws IOException {
    Put put = new Put(rowKey);
    put.add(QUOTA_FAMILY_INFO, qualifier, quotasToData(data));
    doPut(conf, put);
  }

  private static void deleteQuotas(final Configuration conf, final byte[] rowKey)
      throws IOException {
    deleteQuotas(conf, rowKey, null);
  }

  private static void deleteQuotas(final Configuration conf, final byte[] rowKey,
      final byte[] qualifier) throws IOException {
    Delete delete = new Delete(rowKey);
    if (qualifier != null) {
      delete.deleteColumns(QUOTA_FAMILY_INFO, qualifier);
    }
    doDelete(conf, delete);
  }
  
  
  // update cluster limit to region server limit
  public static Quotas updateByLocalFactor(Quotas quotas, double factor) {
    Quotas.Builder newQuotas = Quotas.newBuilder(quotas);
    if (newQuotas.hasThrottle()) {
      Throttle.Builder throttle = Throttle.newBuilder(newQuotas.getThrottle());
      if (throttle.hasReqNum()) {
        TimedQuota.Builder timedQuota = TimedQuota.newBuilder(throttle.getReqNum());
        timedQuota.setSoftLimit((long) (timedQuota.getSoftLimit() * factor));
        throttle.setReqNum(timedQuota.build());
      }
      if (throttle.hasReqSize()) {
        TimedQuota.Builder timedQuota = TimedQuota.newBuilder(throttle.getReqSize());
        timedQuota.setSoftLimit((long) (timedQuota.getSoftLimit() * factor));
        throttle.setReqSize(timedQuota.build());
      }
      if (throttle.hasReadNum()) {
        TimedQuota.Builder timedQuota = TimedQuota.newBuilder(throttle.getReadNum());
        timedQuota.setSoftLimit((long) (timedQuota.getSoftLimit() * factor));
        throttle.setReadNum(timedQuota.build());
      }
      if (throttle.hasReadSize()) {
        TimedQuota.Builder timedQuota = TimedQuota.newBuilder(throttle.getReadSize());
        timedQuota.setSoftLimit((long) (timedQuota.getSoftLimit() * factor));
        throttle.setReadSize(timedQuota.build());
      }
      if (throttle.hasWriteNum()) {
        TimedQuota.Builder timedQuota = TimedQuota.newBuilder(throttle.getWriteNum());
        timedQuota.setSoftLimit((long) (timedQuota.getSoftLimit() * factor));
        throttle.setWriteNum(timedQuota.build());
      }
      if (throttle.hasWriteSize()) {
        TimedQuota.Builder timedQuota = TimedQuota.newBuilder(throttle.getWriteSize());
        timedQuota.setSoftLimit((long) (timedQuota.getSoftLimit() * factor));
        throttle.setWriteSize(timedQuota.build());
      }
      newQuotas.setThrottle(throttle.build());
    }
    return newQuotas.build();
  }

  public static Map<String, UserQuotaState> fetchUserQuotas(final Configuration conf,
      final List<Get> gets, final Map<TableName, Double> localTableFactors) throws IOException {
    long nowTs = EnvironmentEdgeManager.currentTimeMillis();
    Result[] results = doGet(conf, gets);

    Map<String, UserQuotaState> userQuotas = new HashMap<String, UserQuotaState>(results.length);
    for (int i = 0; i < results.length; ++i) {
      byte[] key = gets.get(i).getRow();
      assert isUserRowKey(key);
      String user = getUserFromRowKey(key);

      final UserQuotaState quotaInfo = new UserQuotaState(nowTs);
      userQuotas.put(user, quotaInfo);

      if (results[i].isEmpty()) continue;
      assert Bytes.equals(key, results[i].getRow());

      try {
        parseUserResult(user, results[i], new UserQuotasVisitor() {
          @Override
          public void visitUserQuotas(String userName, String namespace, Quotas quotas) {
            quotaInfo.setQuotas(namespace, quotas);
          }

          @Override
          public void visitUserQuotas(String userName, TableName table, Quotas quotas) {
            // update factors first, then fetch quota, but need update by local factor
            // if no local factor, table will get all quota
            Double factor = localTableFactors.get(table);
            if (factor != null) {
              quotas = updateByLocalFactor(quotas, factor);
            }
            quotaInfo.setQuotas(table, quotas);
          }

          @Override
          public void visitUserQuotas(String userName, Quotas quotas) {
            quotaInfo.setQuotas(quotas);
          }
        });
      } catch (IOException e) {
        LOG.error("Unable to parse user '" + user + "' quotas", e);
        userQuotas.remove(user);
      }
    }
    return userQuotas;
  }
  
  public static Map<String, UserQuotaState> fetchUserQuotas(final Configuration conf,
      final List<Get> gets) throws IOException {
    long nowTs = EnvironmentEdgeManager.currentTimeMillis();
    Result[] results = doGet(conf, gets);

    Map<String, UserQuotaState> userQuotas = new HashMap<String, UserQuotaState>(results.length);
    for (int i = 0; i < results.length; ++i) {
      byte[] key = gets.get(i).getRow();
      assert isUserRowKey(key);
      String user = getUserFromRowKey(key);

      final UserQuotaState quotaInfo = new UserQuotaState(nowTs);
      userQuotas.put(user, quotaInfo);

      if (results[i].isEmpty()) continue;
      assert Bytes.equals(key, results[i].getRow());

      try {
        parseUserResult(user, results[i], new UserQuotasVisitor() {
          @Override
          public void visitUserQuotas(String userName, String namespace, Quotas quotas) {
            quotaInfo.setQuotas(namespace, quotas);
          }

          @Override
          public void visitUserQuotas(String userName, TableName table, Quotas quotas) {
            quotaInfo.setQuotas(table, quotas);
          }

          @Override
          public void visitUserQuotas(String userName, Quotas quotas) {
            quotaInfo.setQuotas(quotas);
          }
        });
      } catch (IOException e) {
        LOG.error("Unable to parse user '" + user + "' quotas", e);
        userQuotas.remove(user);
      }
    }
    return userQuotas;
  }

  public static Map<TableName, QuotaState> fetchTableQuotas(final Configuration conf,
      final List<Get> gets) throws IOException {
    return fetchGlobalQuotas("table", conf, gets, new KeyFromRow<TableName>() {
      @Override
      public TableName getKeyFromRow(final byte[] row) {
        assert isTableRowKey(row);
        return getTableFromRowKey(row);
      }
    });
  }

  public static Map<String, QuotaState> fetchNamespaceQuotas(final Configuration conf,
      final List<Get> gets) throws IOException {
    return fetchGlobalQuotas("namespace", conf, gets, new KeyFromRow<String>() {
      @Override
      public String getKeyFromRow(final byte[] row) {
        assert isNamespaceRowKey(row);
        return getNamespaceFromRowKey(row);
      }
    });
  }

  public static <K> Map<K, QuotaState> fetchGlobalQuotas(final String type,
      final Configuration conf, final List<Get> gets, final KeyFromRow<K> kfr) throws IOException {
    long nowTs = EnvironmentEdgeManager.currentTimeMillis();
    Result[] results = doGet(conf, gets);

    Map<K, QuotaState> globalQuotas = new HashMap<K, QuotaState>(results.length);
    for (int i = 0; i < results.length; ++i) {
      byte[] row = gets.get(i).getRow();
      K key = kfr.getKeyFromRow(row);

      QuotaState quotaInfo = new QuotaState(nowTs);
      globalQuotas.put(key, quotaInfo);

      if (results[i].isEmpty()) continue;
      assert Bytes.equals(row, results[i].getRow());

      byte[] data = results[i].getValue(QUOTA_FAMILY_INFO, QUOTA_QUALIFIER_SETTINGS);
      if (data == null) continue;

      try {
        Quotas quotas = quotasFromData(data);
        quotaInfo.setQuotas(quotas);
      } catch (IOException e) {
        LOG.error("Unable to parse " + type + " '" + key + "' quotas", e);
        globalQuotas.remove(key);
      }
    }
    return globalQuotas;
  }

  private static interface KeyFromRow<T> {
    T getKeyFromRow(final byte[] row);
  }

  /* =========================================================================
   *  HTable helpers
   */
  private static void doPut(final Configuration conf, final Put put)
      throws IOException {
    HTable table = new HTable(conf, QuotaUtil.QUOTA_TABLE_NAME);
    try {
      table.put(put);
    } finally {
      table.close();
    }
  }

  private static void doDelete(final Configuration conf, final Delete delete)
      throws IOException {
    HTable table = new HTable(conf, QuotaUtil.QUOTA_TABLE_NAME);
    try {
      table.delete(delete);
    } finally {
      table.close();
    }
  }

  /* =========================================================================
   *  Data Size Helpers
   */
  public static long calculateMutationSize(final Mutation mutation) {
    long size = 0;
    for (Map.Entry<byte [], List<Cell>> entry : mutation.getFamilyCellMap().entrySet()) {
      for (Cell cell : entry.getValue()) {
        size += KeyValueUtil.length(cell);
      }
    }
    return size;
  }

  public static long calculateMutationSize(final RowMutations rowMutations) {
    long size = 0;
    for (Mutation mutation : rowMutations.getMutations()) {
      size += calculateMutationSize(mutation);
    }
    return size;
  }

  public static long calculateMutationSize(final Collection<Mutation> mutations) {
    long size = 0;
    for (Mutation mutation : mutations) {
      size += calculateMutationSize(mutation);
    }
    return size;
  }

  public static long calculateMutationSize(final Mutation[] mutations) {
    long size = 0;
    for (Mutation mutation : mutations) {
      size += calculateMutationSize(mutation);
    }
    return size;
  }

  public static long calculateResultSize(final Result result) {
    long size = 0;
    for (Cell cell : result.rawCells()) {
      size += KeyValueUtil.length(cell);
    }
    return size;
  }

  public static long calculateResultSize(final List<Result> results) {
    long size = 0;
    for (Result result: results) {
      for (Cell cell : result.rawCells()) {
        size += KeyValueUtil.length(cell);
      }
    }
    return size;
  }

  public static long calculateCellsSize(final List<Cell> cells) {
    long size = 0;
    for (Cell cell : cells) {
      size += CellUtil.estimatedSizeOf(cell);
    }
    return size;
  }
}
