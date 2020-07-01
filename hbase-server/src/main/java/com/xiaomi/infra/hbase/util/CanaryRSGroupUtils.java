package com.xiaomi.infra.hbase.util;

import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.rsgroup.RSGroupInfo;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xiaomi.infra.thirdparty.com.google.common.cache.Cache;
import com.xiaomi.infra.thirdparty.com.google.common.cache.CacheBuilder;

@InterfaceAudience.Private
public class CanaryRSGroupUtils {

  private static final Logger LOG = LoggerFactory.getLogger(CanaryRSGroupUtils.class);

  private static Cache<TableName, String> TABLE_RSGROUP_CACHE = CacheBuilder.newBuilder()
      // expires per 1 mins
      .expireAfterWrite(1, TimeUnit.MINUTES)
      .build();

  public static String getCachedRSGroupOfTable(Admin admin, TableName tableName) {
    try {
      return TABLE_RSGROUP_CACHE.get(tableName,
          () -> Optional.ofNullable(admin.getRSGroup(tableName))
              .map(RSGroupInfo::getName)
              .orElse(RSGroupInfo.DEFAULT_GROUP));
    } catch (ExecutionException e) {
      LOG.warn("Failed to find rsgroup for table {}", tableName, e);
      return null;
    }
  }

}
