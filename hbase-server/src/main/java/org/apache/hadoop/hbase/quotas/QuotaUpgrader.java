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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public class QuotaUpgrader {
  private static final Logger LOG = LoggerFactory.getLogger(QuotaUpgrader.class);

  private static void printUsageAndExit() {
    System.err.printf("Usage: org.apache.hadoop.hbase.quotas.QuotaUpgrader [options]");
    System.err.println(" where [options] are:");
    System.err.println("  -h|-help      Show this help and exit.");
    System.err.println("  upgrade regionServerReadCU regionServerWriteCU");
    System.exit(1);
  }

  private void upgrade(long readCU, long writeCU) throws IOException {
    Configuration conf = HBaseConfiguration.create();
    if (QuotaUtil.isQuotaEnabled(conf)) {
      try (Connection connection = ConnectionFactory.createConnection(conf);
          Admin admin = connection.getAdmin()) {
        parseQuotaToBranch2(admin);
        setRegionSeverQuota(admin, readCU, writeCU);
        enableExceedThrottleQuota(admin);
      }
    } else {
      throw new UnsupportedOperationException("Quota is disabled");
    }
  }

  private ThrottleType parseThrottleType(ThrottleType throttleType) {
    ThrottleType targetThrottleType = null;
    if (throttleType == ThrottleType.READ_NUMBER || throttleType == ThrottleType.WRITE_NUMBER
        || throttleType == ThrottleType.REQUEST_NUMBER) {
      switch (throttleType) {
        case READ_NUMBER:
          targetThrottleType = ThrottleType.READ_CAPACITY_UNIT;
          break;
        case WRITE_NUMBER:
          targetThrottleType = ThrottleType.WRITE_CAPACITY_UNIT;
          break;
        case REQUEST_NUMBER:
          targetThrottleType = ThrottleType.REQUEST_CAPACITY_UNIT;
          break;
        default:
      }
    }
    return targetThrottleType;
  }

  private void parseQuotaToBranch2(Admin admin) throws IOException {
    LOG.info("Start parse quota");
    List<QuotaSettings> quotaSettings = admin.getQuota(new QuotaFilter());
    for (QuotaSettings quotaSetting : quotaSettings) {
      if (quotaSetting.getQuotaType() == QuotaType.THROTTLE) {
        ThrottleSettings throttleSettings = (ThrottleSettings) quotaSetting;
        ThrottleType throttleType = throttleSettings.getThrottleType();
        ThrottleType targetThrottleType = parseThrottleType(throttleType);
        if (targetThrottleType == null) {
          LOG.error("Skip parse quota {} because throttle type is {}",
            getQuotaSettingsString(quotaSetting), throttleType);
          continue;
        }
        long softLimit = throttleSettings.getSoftLimit();
        TimeUnit timeUnit = throttleSettings.getTimeUnit();
        String namespace = quotaSetting.getNamespace();
        TableName tableName = quotaSetting.getTableName();
        String userName = quotaSetting.getUserName();
        QuotaScope targetScope = QuotaScope.CLUSTER;
        if (userName != null) {
          if (namespace != null) {
            QuotaSettings targetQuotaSettings = QuotaSettingsFactory.throttleUser(userName,
              namespace, targetThrottleType, softLimit, timeUnit, targetScope);
            admin.setQuota(targetQuotaSettings);
            admin.setQuota(
              QuotaSettingsFactory.unthrottleUserByThrottleType(userName, namespace, throttleType));
            logParseQuota(quotaSetting, targetQuotaSettings);
          } else if (tableName != null) {
            QuotaSettings targetQuotaSettings = QuotaSettingsFactory.throttleUser(userName,
              tableName, targetThrottleType, softLimit, timeUnit, targetScope);
            admin.setQuota(targetQuotaSettings);
            admin.setQuota(
              QuotaSettingsFactory.unthrottleUserByThrottleType(userName, tableName, throttleType));
            logParseQuota(quotaSetting, targetQuotaSettings);
          } else {
            QuotaSettings targetQuotaSettings = QuotaSettingsFactory.throttleUser(userName,
              targetThrottleType, softLimit, timeUnit, targetScope);
            admin.setQuota(targetQuotaSettings);
            admin.setQuota(
              QuotaSettingsFactory.unthrottleUserByThrottleType(userName, throttleType));
            logParseQuota(quotaSetting, targetQuotaSettings);
          }
        } else if (namespace != null) {
          QuotaSettings targetQuotaSettings = QuotaSettingsFactory.throttleNamespace(namespace,
            targetThrottleType, softLimit, timeUnit, targetScope);
          admin.setQuota(targetQuotaSettings);
          admin.setQuota(
            QuotaSettingsFactory.unthrottleNamespaceByThrottleType(namespace, throttleType));
          logParseQuota(quotaSetting, targetQuotaSettings);
        } else if (tableName != null) {
          QuotaSettings targetQuotaSettings = QuotaSettingsFactory.throttleTable(tableName,
            targetThrottleType, softLimit, timeUnit, targetScope);
          admin.setQuota(targetQuotaSettings);
          admin.setQuota(
            QuotaSettingsFactory.unthrottleTableByThrottleType(tableName, throttleType));
          logParseQuota(quotaSetting, targetQuotaSettings);
        } else if (quotaSetting.getRegionServer() != null) {
          // do nothing
        } else {
          LOG.warn("Found unknown quota: {}", quotaSetting);
        }
      }
    }
    LOG.info("Finish parse quota");
  }

  private void logParseQuota(QuotaSettings quotaSettings, QuotaSettings targetQuotaSettings) {
    LOG.info("Parse quota from {} to {}", getQuotaSettingsString(quotaSettings),
      getQuotaSettingsString(targetQuotaSettings));
  }

  private String getQuotaSettingsString(QuotaSettings quotaSetting) {
    return quotaSetting.ownerToString() + quotaSetting.toString();
  }

  private void setRegionSeverQuota(Admin admin, long readCU, long writeCU)
      throws IOException {
    LOG.info("Start set RegionServer CU");
    QuotaSettings readQuotaSettings =
        QuotaSettingsFactory.throttleRegionServer(QuotaTableUtil.QUOTA_REGION_SERVER_ROW_KEY,
          ThrottleType.READ_CAPACITY_UNIT, readCU, TimeUnit.SECONDS);
    QuotaSettings writeQuotaSettings =
        QuotaSettingsFactory.throttleRegionServer(QuotaTableUtil.QUOTA_REGION_SERVER_ROW_KEY,
          ThrottleType.WRITE_CAPACITY_UNIT, writeCU, TimeUnit.SECONDS);
    admin.setQuota(readQuotaSettings);
    admin.setQuota(writeQuotaSettings);
    LOG.info("Finish set RegionServer CU");
  }

  private void enableExceedThrottleQuota(Admin admin) throws IOException {
    LOG.info("Start enable exceed throttle switch");
    admin.exceedThrottleQuotaSwitch(true);
    LOG.info("Finish enable exceed throttle switch");
  }

  public static void main(String[] args) throws Exception {
    if (args.length <= 1) {
      printUsageAndExit();
    }
    if (args[0].equals("-help") || args[0].equals("-h")) {
      printUsageAndExit();
    } else if (args[0].equals("upgrade")) {
      if (args.length < 3) {
        printUsageAndExit();
      } else {
        long readCU = Long.parseLong(args[1]);
        long writeCU = Long.parseLong(args[2]);
        LOG.info("Will set RegionServer readCU to {}, writeCU to {}", readCU, writeCU);
        QuotaUpgrader quotaUpgrader = new QuotaUpgrader();
        quotaUpgrader.upgrade(readCU, writeCU);
      }
    } else {
      printUsageAndExit();
    }
  }
}
