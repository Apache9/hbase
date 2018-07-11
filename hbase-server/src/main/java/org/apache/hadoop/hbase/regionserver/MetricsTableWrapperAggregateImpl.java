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

package org.apache.hadoop.hbase.regionserver;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.CompatibilitySingletonFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.metrics2.MetricsExecutor;

import org.apache.hbase.thirdparty.com.google.common.collect.Sets;

@InterfaceAudience.Private
public class MetricsTableWrapperAggregateImpl implements MetricsTableWrapperAggregate, Closeable {
  private final HRegionServer regionServer;
  private ScheduledExecutorService executor;
  private Runnable runnable;
  private long period;
  private ScheduledFuture<?> tableMetricsUpdateTask;
  private ConcurrentHashMap<TableName, MetricsTableValues> metricsTableMap = new ConcurrentHashMap<>();

  public MetricsTableWrapperAggregateImpl(final HRegionServer regionServer) {
    this.regionServer = regionServer;
    this.period = regionServer.conf.getLong(HConstants.REGIONSERVER_METRICS_PERIOD,
      HConstants.DEFAULT_REGIONSERVER_METRICS_PERIOD) + 1000;
    this.executor = CompatibilitySingletonFactory.getInstance(MetricsExecutor.class).getExecutor();
    this.runnable = new TableMetricsWrapperRunnable();
    this.tableMetricsUpdateTask = this.executor.scheduleWithFixedDelay(this.runnable, period, this.period,
      TimeUnit.MILLISECONDS);
  }

  public class TableMetricsWrapperRunnable implements Runnable {

    @Override
    public void run() {
      Map<TableName, MetricsTableValues> localMetricsTableMap = new HashMap<>();

      for (Region r : regionServer.getOnlineRegionsLocalContext()) {
        TableName tbl= r.getTableDescriptor().getTableName();
        MetricsTableValues metricsTable = localMetricsTableMap.get(tbl);
        if (metricsTable == null) {
          metricsTable = new MetricsTableValues();
          localMetricsTableMap.put(tbl, metricsTable);
        }
        long tempStorefilesSize = 0;
        for (Store store : r.getStores()) {
          tempStorefilesSize += store.getStorefilesSize();
        }
        metricsTable.setMemStoresSize(metricsTable.getMemStoresSize() + r.getMemStoreDataSize());
        metricsTable.setStoreFilesSize(metricsTable.getStoreFilesSize() + tempStorefilesSize);
        metricsTable.setTableSize(metricsTable.getMemStoresSize() + metricsTable.getStoreFilesSize());
        metricsTable.setReadRequestsCount(metricsTable.getReadRequestsCount() + r.getReadRequestsCount());
        metricsTable.setWriteRequestsCount(metricsTable.getWriteRequestsCount() + r.getWriteRequestsCount());
        metricsTable.setTotalRequestsCount(metricsTable.getReadRequestsCount() + metricsTable.getWriteRequestsCount());
        metricsTable.setReadRequestsCountPerSecond(metricsTable.getReadRequestsCountPerSecond() + r.getReadRequestsCountPerSecond());
        metricsTable.setWriteRequestsCountPerSecond(metricsTable.getWriteRequestsCountPerSecond() + r.getWriteRequestsCountPerSecond());
        metricsTable.setGetRequestsCountPerSecond(metricsTable.getGetRequestsCountPerSecond() + r.getGetRequestsCountPerSecond());
        metricsTable.setScanRequestsCountPerSecond(metricsTable.getScanRequestsCountPerSecond() + r.getScanRequestsCountPerSecond());
        metricsTable.setScanRowsCountPerSecond(metricsTable.getScanRowsCountPerSecond() + r.getScanRowsCountPerSecond());
        metricsTable.setReadRequestsByCapacityUnitPerSecond(
          metricsTable.getReadRequestsByCapacityUnitPerSecond()
              + r.getReadRequestsByCapacityUnitPerSecond());
        metricsTable.setWriteRequestsByCapacityUnitPerSecond(
          metricsTable.getWriteRequestsByCapacityUnitPerSecond()
              + r.getWriteRequestsByCapacityUnitPerSecond());
        metricsTable.setReadCellsPerSecond(
          metricsTable.getReadCellsPerSecond() + r.getReadCellsPerSecond());
        metricsTable.setReadRawCellsPerSecond(
          metricsTable.getReadRawCellsPerSecond() + r.getReadRawCellsPerSecond());
      }

      for(Map.Entry<TableName, MetricsTableValues> entry : localMetricsTableMap.entrySet()) {
        TableName tbl = entry.getKey();
        if (metricsTableMap.get(tbl) == null) {
          MetricsTableSource tableSource = CompatibilitySingletonFactory
              .getInstance(MetricsRegionServerSourceFactory.class).createTable(tbl.getNameAsString(),
                MetricsTableWrapperAggregateImpl.this);
          CompatibilitySingletonFactory
          .getInstance(MetricsRegionServerSourceFactory.class).getTableAggregate()
          .register(tbl.getNameAsString(), tableSource);
        }
        metricsTableMap.put(entry.getKey(), entry.getValue());
      }
      Set<TableName> existingTableNames = Sets.newHashSet(metricsTableMap.keySet());
      existingTableNames.removeAll(localMetricsTableMap.keySet());
      MetricsTableAggregateSource agg = CompatibilitySingletonFactory
          .getInstance(MetricsRegionServerSourceFactory.class).getTableAggregate();
      for (TableName table : existingTableNames) {
        agg.deregister(table.getNameAsString());
        if (metricsTableMap.get(table) != null) {
          metricsTableMap.remove(table);
        }
      }
    }
  }

  @Override
  public long getReadRequestsCount(String table) {
    MetricsTableValues metricsTable = metricsTableMap.get(TableName.valueOf(table));
    if (metricsTable == null)
      return 0;
    else
      return metricsTable.getReadRequestsCount();
  }

  @Override
  public long getWriteRequestsCount(String table) {
    MetricsTableValues metricsTable = metricsTableMap.get(TableName.valueOf(table));
    if (metricsTable == null)
      return 0;
    else
      return metricsTable.getWriteRequestsCount();
  }

  @Override
  public long getTotalRequestsCount(String table) {
    MetricsTableValues metricsTable = metricsTableMap.get(TableName.valueOf(table));
    if (metricsTable == null)
      return 0;
    else
      return metricsTable.getTotalRequestsCount();
  }

  @Override
  public long getMemStoresSize(String table) {
    MetricsTableValues metricsTable = metricsTableMap.get(TableName.valueOf(table));
    if (metricsTable == null)
      return 0;
    else
      return metricsTable.getMemStoresSize();
  }

  @Override
  public long getStoreFilesSize(String table) {
    MetricsTableValues metricsTable = metricsTableMap.get(TableName.valueOf(table));
    if (metricsTable == null)
      return 0;
    else
      return metricsTable.getStoreFilesSize();
  }

  @Override
  public long getTableSize(String table) {
    MetricsTableValues metricsTable = metricsTableMap.get(TableName.valueOf(table));
    if (metricsTable == null)
      return 0;
    else
      return metricsTable.getTableSize();
  }

  @Override
  public long getReadRequestsCountPerSecond(String table) {
    MetricsTableValues metricsTable = metricsTableMap.get(TableName.valueOf(table));
    if (metricsTable == null) {
      return 0;
    } else {
      return metricsTable.getReadRequestsCountPerSecond();
    }
  }

  @Override
  public long getWriteRequestsCountPerSecond(String table) {
    MetricsTableValues metricsTable = metricsTableMap.get(TableName.valueOf(table));
    if (metricsTable == null) {
      return 0;
    } else {
      return metricsTable.getWriteRequestsCountPerSecond();
    }
  }

  @Override
  public long getGetRequestsCountPerSecond(String table) {
    MetricsTableValues metricsTable = metricsTableMap.get(TableName.valueOf(table));
    if (metricsTable == null) {
      return 0;
    } else {
      return metricsTable.getGetRequestsCountPerSecond();
    }
  }

  @Override
  public long getScanRequestsCountPerSecond(String table) {
    MetricsTableValues metricsTable = metricsTableMap.get(TableName.valueOf(table));
    if (metricsTable == null) {
      return 0;
    } else {
      return metricsTable.getScanRequestsCountPerSecond();
    }
  }

  @Override
  public long getScanRowsCountPerSecond(String table) {
    MetricsTableValues metricsTable = metricsTableMap.get(TableName.valueOf(table));
    if (metricsTable == null) {
      return 0;
    } else {
      return metricsTable.getScanRowsCountPerSecond();
    }
  }

  @Override
  public long getReadRequestsByCapacityUnitPerSecond(String table) {
    MetricsTableValues metricsTable = metricsTableMap.get(TableName.valueOf(table));
    if (metricsTable == null) {
      return 0;
    } else {
      return metricsTable.getReadRequestsByCapacityUnitPerSecond();
    }
  }

  @Override
  public long getWriteRequestsByCapacityUnitPerSecond(String table) {
    MetricsTableValues metricsTable = metricsTableMap.get(TableName.valueOf(table));
    if (metricsTable == null) {
      return 0;
    } else {
      return metricsTable.getWriteRequestsByCapacityUnitPerSecond();
    }
  }

  @Override
  public long getReadCellsPerSecond(String table) {
    MetricsTableValues metricsTable = metricsTableMap.get(TableName.valueOf(table));
    if (metricsTable == null) {
      return 0;
    } else {
      return metricsTable.getReadCellsPerSecond();
    }
  }

  @Override
  public long getReadRawCellsPerSecond(String table) {
    MetricsTableValues metricsTable = metricsTableMap.get(TableName.valueOf(table));
    if (metricsTable == null) {
      return 0;
    } else {
      return metricsTable.getReadRawCellsPerSecond();
    }
  }

  @Override
  public void close() throws IOException {
    tableMetricsUpdateTask.cancel(true);
  }

  private static class MetricsTableValues {

    private long totalRequestsCount;
    private long readRequestsCount;
    private long writeRequestsCount;
    private long memstoresSize;
    private long storeFilesSize;
    private long tableSize;
    private long readRequestsCountPerSecond;
    private long writeRequestsCountPerSecond;
    private long getRequestsCountPerSecond;
    private long scanRequestsCountPerSecond;
    private long scanRowsCountPerSecond;
    private long readRequestsByCapacityUnitPerSecond;
    private long writeRequestsByCapacityUnitPerSecond;
    private long readCellsPerSecond;
    private long readRawCellsPerSecond;

    public long getTotalRequestsCount() {
      return totalRequestsCount;
    }

    public void setTotalRequestsCount(long totalRequestsCount) {
      this.totalRequestsCount = totalRequestsCount;
    }

    public long getReadRequestsCount() {
      return readRequestsCount;
    }

    public void setReadRequestsCount(long readRequestsCount) {
      this.readRequestsCount = readRequestsCount;
    }

    public long getWriteRequestsCount() {
      return writeRequestsCount;
    }

    public void setWriteRequestsCount(long writeRequestsCount) {
      this.writeRequestsCount = writeRequestsCount;
    }

    public long getMemStoresSize() {
      return memstoresSize;
    }

    public void setMemStoresSize(long memstoresSize) {
      this.memstoresSize = memstoresSize;
    }

    public long getStoreFilesSize() {
      return storeFilesSize;
    }

    public void setStoreFilesSize(long storeFilesSize) {
      this.storeFilesSize = storeFilesSize;
    }

    public long getTableSize() {
      return tableSize;
    }

    public void setTableSize(long tableSize) {
      this.tableSize = tableSize;
    }

    public long getReadRequestsCountPerSecond() {
      return readRequestsCountPerSecond;
    }

    public void setReadRequestsCountPerSecond(long readRequestsCountPerSecond) {
      this.readRequestsCountPerSecond = readRequestsCountPerSecond;
    }

    public long getWriteRequestsCountPerSecond() {
      return writeRequestsCountPerSecond;
    }

    public void setWriteRequestsCountPerSecond(long writeRequestsCountPerSecond) {
      this.writeRequestsCountPerSecond = writeRequestsCountPerSecond;
    }

    public long getGetRequestsCountPerSecond() {
      return getRequestsCountPerSecond;
    }

    public void setGetRequestsCountPerSecond(long getRequestsCountPerSecond) {
      this.getRequestsCountPerSecond = getRequestsCountPerSecond;
    }

    public long getScanRequestsCountPerSecond() {
      return scanRequestsCountPerSecond;
    }

    public void setScanRequestsCountPerSecond(long scanRequestsCountPerSecond) {
      this.scanRequestsCountPerSecond = scanRequestsCountPerSecond;
    }

    public long getScanRowsCountPerSecond() {
      return scanRowsCountPerSecond;
    }

    public void setScanRowsCountPerSecond(long scanRowsCountPerSecond) {
      this.scanRowsCountPerSecond = scanRowsCountPerSecond;
    }

    public long getReadRequestsByCapacityUnitPerSecond() {
      return readRequestsByCapacityUnitPerSecond;
    }

    public void setReadRequestsByCapacityUnitPerSecond(long readRequestsByCapacityUnitPerSecond) {
      this.readRequestsByCapacityUnitPerSecond = readRequestsByCapacityUnitPerSecond;
    }

    public long getWriteRequestsByCapacityUnitPerSecond() {
      return writeRequestsByCapacityUnitPerSecond;
    }

    public void setWriteRequestsByCapacityUnitPerSecond(long writeRequestsByCapacityUnitPerSecond) {
      this.writeRequestsByCapacityUnitPerSecond = writeRequestsByCapacityUnitPerSecond;
    }

    public long getReadCellsPerSecond() {
      return readCellsPerSecond;
    }

    public void setReadCellsPerSecond(long readCellsPerSecond) {
      this.readCellsPerSecond = readCellsPerSecond;
    }

    public long getReadRawCellsPerSecond() {
      return readRawCellsPerSecond;
    }

    public void setReadRawCellsPerSecond(long readRawCellsPerSecond) {
      this.readRawCellsPerSecond = readRawCellsPerSecond;
    }
  }

}
