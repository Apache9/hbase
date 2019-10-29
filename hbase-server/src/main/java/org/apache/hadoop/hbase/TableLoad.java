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
package org.apache.hadoop.hbase;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hbase.protobuf.generated.ClusterStatusProtos;
import org.apache.hadoop.hbase.util.Strings;

import java.util.ArrayList;
import java.util.List;

/**
 * This class is used exporting current state of load on a table.
 */
public class TableLoad {
  /** the table name */
  private String name;
  /** the number of regions for the table */
  private int region;
  /** the number of stores for all regions of the table */
  private int stores;
  /** the number of storefiles for all regions of the table  */
  private int storefiles;
  /** the total size of the store files for all regions of the table , uncompressed, in MB */
  private int storeUncompressedSizeMB;
  /** the current total size of the store files for all regions of the table , in MB */
  private int storefileSizeMB;
  /** the current size of the memstore for all regions of the table , in MB */
  private int memstoreSizeMB;

  /**
   * The current total size of root-level store file indexes for all regions of the table ,
   * in MB. The same as {@link #rootIndexSizeKB} but in MB.
   */
  private int storefileIndexSizeMB;
  /** the current total read request made to all regions of the table  */
  private long readRequestsCount;
  /** the current total write requests made to all regions of the table  */
  private long writeRequestsCount;
  /** the total compacting key values in currently running compaction */
  private long totalCompactingKVs;
  /** the completed count of key values in currently running compaction */
  private long currentCompactedKVs;

  /** The current total size of root-level indexes for the all regions of the table, in KB. */
  private int rootIndexSizeKB;

  /** The total size of all index blocks, not just the root level, in KB. */
  private int totalStaticIndexSizeKB;

  /**
   * The total size of all Bloom filter blocks, not just loaded into the
   * block cache, in KB.
   */
  private int totalStaticBloomSizeKB;

  /** average data locality of all regions of the table */
  private float locality = 0.0f;

  /** the current read requests per second made to all regions of the table  */
  private long readRequestsPerSecond;

  /** the current write requests per second made to all regions of the table  */
  private long writeRequestsPerSecond;

  private long readCellCountPerSecond;

  private long readRawCellCountPerSecond;

  /** the current total read capacity unit made to all regions of the table  */
  private long readRequestsByCapacityUnitPerSecond;
  /** the current total write capacity unit made to all regions of the table  */
  private long writeRequestsByCapacityUnitPerSecond;

  private long userReadRequestsPerSecond;
  private long userWriteRequestsPerSecond;
  private long userReadRequestsByCapacityUnitPerSecond;
  private long userWriteRequestsByCapacityUnitPerSecond;

  /** the total throttled read request count made to all regions of the table  */
  private long throttledReadRequestsCount;
  /** the total throttled write request count made to all regions of the table  */
  private long throttledWriteRequestsCount;

  private long scanCountPerSecond;
  private long scanRowsPerSecond;

  // table latency
  private long getCount;
  private long putCount;
  private long scanCount;
  private long batchCount;
  private long deleteCount;
  private long appendCount;
  private long incrementCount;
  private long getTimeTotal;
  private long putTimeTotal;
  private long scanTimeTotal;
  private long batchTimeTotal;
  private long deleteTimeTotal;
  private long appendTimeTotal;
  private long incrementTimeTotal;
  private long getTimeMax99Percentile;
  private long putTimeMax99Percentile;
  private long scanTimeMax99Percentile;
  private long batchTimeMax99Percentile;

  // approximate row count
  private long approximateRowCount;

  /**
   * family info
   */
  private List<FamilyLoad> familyLoads;

  public TableLoad(final String name) {
    this.name = name;
    this.region = 0;
    this.stores = 0;
    this.storefiles = 0;
    this.storeUncompressedSizeMB = 0;
    this.storefileSizeMB = 0;
    this.memstoreSizeMB = 0;
    this.storefileIndexSizeMB = 0;
    this.rootIndexSizeKB = 0;
    this.totalStaticIndexSizeKB = 0;
    this.totalStaticBloomSizeKB = 0;
    this.readRequestsCount = 0;
    this.writeRequestsCount = 0;
    this.totalCompactingKVs = 0;
    this.currentCompactedKVs = 0;
    this.locality = 0;
    this.readRequestsPerSecond = 0;
    this.writeRequestsPerSecond = 0;
    this.readCellCountPerSecond = 0;
    this.readRawCellCountPerSecond = 0;
    this.readRequestsByCapacityUnitPerSecond = 0;
    this.writeRequestsByCapacityUnitPerSecond = 0;
    this.throttledReadRequestsCount = 0;
    this.throttledWriteRequestsCount = 0;
    this.scanCountPerSecond = 0;
    this.scanRowsPerSecond = 0;
    this.familyLoads = new ArrayList<>();
    this.getCount = 0;
    this.putCount = 0;
    this.scanCount = 0;
    this.batchCount = 0;
    this.deleteCount = 0;
    this.appendCount = 0;
    this.incrementCount = 0;
    this.getTimeTotal = 0;
    this.putTimeTotal = 0;
    this.scanTimeTotal = 0;
    this.batchTimeTotal = 0;
    this.deleteTimeTotal = 0;
    this.appendTimeTotal = 0;
    this.incrementTimeTotal = 0;
    this.approximateRowCount = 0;
    this.getTimeMax99Percentile = 0;
    this.putTimeMax99Percentile = 0;
    this.scanTimeMax99Percentile = 0;
    this.batchTimeMax99Percentile = 0;
    this.userReadRequestsPerSecond = 0;
    this.userWriteRequestsPerSecond = 0;
    this.userReadRequestsByCapacityUnitPerSecond = 0;
    this.userWriteRequestsByCapacityUnitPerSecond = 0;
  }

  public TableLoad(TableName table) {
    this(table.getNameAsString());
  }

  public void updateTableLatency(final ClusterStatusProtos.RegionServerTableLatency tl) {
    if (tl.hasGetTimeMean()) {
      getTimeTotal += tl.getGetTimeMean() * tl.getGetOperationCount();
      getCount += tl.getGetOperationCount();
    }
    if (tl.hasPutTimeMean()) {
      putTimeTotal += tl.getPutTimeMean() * tl.getPutOperationCount();
      putCount += tl.getPutOperationCount();
    }
    if (tl.hasScanTimeMean()) {
      scanTimeTotal += tl.getScanTimeMean() * tl.getScanOperationCount();
      scanCount += tl.getScanOperationCount();
    }
    if (tl.hasBatchTimeMean()) {
      batchTimeTotal += tl.getBatchTimeMean() * tl.getBatchOperationCount();
      batchCount += tl.getBatchOperationCount();
    }
    if (tl.hasDeleteTimeMean()) {
      deleteTimeTotal += tl.getDeleteTimeMean() * tl.getDeleteOperationCount();
      deleteCount += tl.getDeleteOperationCount();
    }
    if (tl.hasAppendTimeMean()) {
      appendTimeTotal += tl.getAppendTimeMean() * tl.getAppendOperationCount();
      appendCount += tl.getAppendOperationCount();
    }
    if (tl.hasIncrementTimeMean()) {
      incrementTimeTotal += tl.getIncrementTimeMean() * tl.getIncrementOperationCount();
      incrementCount += tl.getIncrementOperationCount();
    }
    if (tl.hasGetTime99Percentile() && tl.getGetTime99Percentile() > getTimeMax99Percentile) {
      getTimeMax99Percentile = tl.getGetTime99Percentile();
    }
    if (tl.hasPutTime99Percentile() && tl.getPutTime99Percentile() > putTimeMax99Percentile) {
      putTimeMax99Percentile = tl.getPutTime99Percentile();
    }
    if (tl.hasScanTime99Percentile() && tl.getScanTime99Percentile() > scanTimeMax99Percentile) {
      scanTimeMax99Percentile = tl.getScanTime99Percentile();
    }
    if (tl.hasBatchTime99Percentile() && tl.getBatchTime99Percentile() > batchTimeMax99Percentile) {
      batchTimeMax99Percentile = tl.getBatchTime99Percentile();
    }
  }

  public void updateTableLoad(final RegionLoad regionLoad) {
    this.region++;
    this.stores += regionLoad.getStores();
    this.storefiles += regionLoad.getStorefiles();

    int totalDataSize = storefileSizeMB + regionLoad.getStorefileSizeMB();
    if (totalDataSize == 0) {
      this.locality = 1.0f;
    } else {
      this.locality = (storefileSizeMB * locality + regionLoad.getStorefileSizeMB()
          * regionLoad.getDataLocality())
          / (totalDataSize);
    }

    this.storeUncompressedSizeMB += regionLoad.getStoreUncompressedSizeMB();
    this.storefileSizeMB += regionLoad.getStorefileSizeMB();
    this.memstoreSizeMB += regionLoad.getMemStoreSizeMB();
    this.storefileIndexSizeMB += regionLoad.getStorefileIndexSizeMB();
    this.rootIndexSizeKB += regionLoad.getRootIndexSizeKB();
    this.totalStaticIndexSizeKB += regionLoad.getTotalStaticIndexSizeKB();
    this.totalStaticBloomSizeKB += regionLoad.getTotalStaticBloomSizeKB();
    this.readRequestsCount += regionLoad.getReadRequestsCount();
    this.writeRequestsCount += regionLoad.getWriteRequestsCount();
    this.totalCompactingKVs += regionLoad.getTotalCompactingKVs();
    this.currentCompactedKVs += regionLoad.getCurrentCompactedKVs();
    this.readRequestsPerSecond += regionLoad.getReadRequestsPerSecond();
    this.readCellCountPerSecond += regionLoad.getReadCellCountPerSecond();
    this.readRawCellCountPerSecond += regionLoad.getReadRawCellCountPerSecond();
    this.scanCountPerSecond += regionLoad.getScanCountPerSecond();
    this.scanRowsPerSecond += regionLoad.getScanRowsPerSecond();
    this.writeRequestsPerSecond += regionLoad.getWriteRequestsPerSecond();
    this.readRequestsByCapacityUnitPerSecond += regionLoad.getReadRequestsByCapacityUnitPerSecond();
    this.writeRequestsByCapacityUnitPerSecond += regionLoad.getWriteRequestsByCapacityUnitPerSecond();
    this.throttledReadRequestsCount += regionLoad.getThrottledReadRequestsCount();
    this.throttledWriteRequestsCount += regionLoad.getThrottledWriteRequestsCount();
    this.userReadRequestsPerSecond += regionLoad.getUserReadRequestsPerSecond();
    this.userWriteRequestsPerSecond += regionLoad.getUserWriteRequestsPerSecond();
    this.userReadRequestsByCapacityUnitPerSecond +=
        regionLoad.getUserReadRequestsByCapacityUnitPerSecond();
    this.userWriteRequestsByCapacityUnitPerSecond +=
        regionLoad.getUserWriteRequestsByCapacityUnitPerSecond();

    for (FamilyLoad load : regionLoad.getFamilyLoads()) {
      boolean exist = false;
      for (FamilyLoad existLoad : familyLoads) {
        if (existLoad.getName().equals(load.getName())) {
          existLoad.incrementRowCount(load.getRowCount());
          existLoad.incrementKeyValueCount(load.getKeyValueCount());
          existLoad.incrementDeleteFamilyCount(load.getDeleteFamilyCount());
          existLoad.incrementDeleteKeyValueCount(load.getDeleteKeyValueCount());
          exist = true;
          break;
        }
      }
      if (!exist) {
        familyLoads.add(load);
      }
    }

    for (FamilyLoad load: familyLoads) {
      long rowCount = load.getRowCount() - load.getDeleteFamilyCount();
      if (rowCount > approximateRowCount) {
        approximateRowCount = rowCount;
      }
    }
  }

  public String getName() {
    return name;
  }

  public int getRegion() {
    return region;
  }

  public int getStores() {
    return stores;
  }

  public int getStorefiles() {
    return storefiles;
  }

  public int getStoreUncompressedSizeMB() {
    return storeUncompressedSizeMB;
  }

  public int getStorefileSizeMB() {
    return storefileSizeMB;
  }

  public int getMemstoreSizeMB() {
    return memstoreSizeMB;
  }

  public int getStorefileIndexSizeMB() {
    return storefileIndexSizeMB;
  }

  public long getReadRequestsCount() {
    return readRequestsCount;
  }

  public long getWriteRequestsCount() {
    return writeRequestsCount;
  }

  public long getTotalCompactingKVs() {
    return totalCompactingKVs;
  }

  public long getCurrentCompactedKVs() {
    return currentCompactedKVs;
  }

  public int getRootIndexSizeKB() {
    return rootIndexSizeKB;
  }

  public int getTotalStaticIndexSizeKB() {
    return totalStaticIndexSizeKB;
  }

  public int getTotalStaticBloomSizeKB() {
    return totalStaticBloomSizeKB;
  }

  public float getLocality() {
    return locality;
  }

  public long getReadRequestsPerSecond() {
    return readRequestsPerSecond;
  }

  public long getReadCellCountPerSecond() {
    return readCellCountPerSecond;
  }

  public long getScanCountPerSecond(){
    return this.scanCountPerSecond;
  }

  public long getScanRowsPerSecond(){
    return this.scanRowsPerSecond;
  }

  public long getReadRawCellCountPerSecond() {
    return readRawCellCountPerSecond;
  }

  public long getWriteRequestsPerSecond() {
    return writeRequestsPerSecond;
  }

  public long getReadRequestsByCapacityUnitPerSecond() {
    return readRequestsByCapacityUnitPerSecond;
  }

  public long getWriteRequestsByCapacityUnitPerSecond() {
    return writeRequestsByCapacityUnitPerSecond;
  }

  public long getUserReadRequestsPerSecond() {
    return userReadRequestsPerSecond;
  }

  public long getUserWriteRequestsPerSecond() {
    return userWriteRequestsPerSecond;
  }

  public long getUserReadRequestsByCapacityUnitPerSecond() {
    return userReadRequestsByCapacityUnitPerSecond;
  }

  public long getUserWriteRequestsByCapacityUnitPerSecond() {
    return userWriteRequestsByCapacityUnitPerSecond;
  }

  public long getThrottledReadRequestsCount() {
    return throttledReadRequestsCount;
  }

  public long getThrottledWriteRequestsCount() {
    return throttledWriteRequestsCount;
  }

  public List<FamilyLoad> getFamilyLoads() {
    return familyLoads;
  }

  public long getGetTimeMean() {
    return getCount > 0 ? getTimeTotal / getCount : 0;
  }

  public long getPutTimeMean() {
    return putCount > 0 ? putTimeTotal / putCount : 0;
  }

  public long getScanTimeMean() {
    return scanCount > 0 ? scanTimeTotal / scanCount : 0;
  }

  public long getBatchTimeMean() {
    return batchCount > 0 ? batchTimeTotal / batchCount : 0;
  }

  public long getAppendTimeMean() {
    return appendCount > 0 ? appendTimeTotal / appendCount : 0;
  }

  public long getDeleteTimeMean() {
    return deleteCount > 0 ? deleteTimeTotal / deleteCount : 0;
  }

  public long getIncrementTimeMean() {
    return incrementCount > 0 ? incrementTimeTotal / incrementCount : 0;
  }

  public long getGetTimeMax99Percentile() {
    return getTimeMax99Percentile;
  }

  public long getPutTimeMax99Percentile() {
    return putTimeMax99Percentile;
  }

  public long getScanTimeMax99Percentile() {
    return scanTimeMax99Percentile;
  }

  public long getBatchTimeMax99Percentile() {
    return batchTimeMax99Percentile;
  }

  public long getApproximateRowCount() {
    return approximateRowCount;
  }

  @VisibleForTesting
  protected long getGetCount() {
    return getCount;
  }

  @VisibleForTesting
  protected long getPutCount() {
    return putCount;
  }

  @VisibleForTesting
  protected long getScanCount() {
    return scanCount;
  }

  @VisibleForTesting
  protected long getBatchCount() {
    return batchCount;
  }

  @VisibleForTesting
  protected long getDeleteCount() {
    return deleteCount;
  }

  @VisibleForTesting
  protected long getAppendCount() {
    return appendCount;
  }

  @VisibleForTesting
  protected long getIncrementCount() {
    return incrementCount;
  }

  @Override
  public String toString() {
    StringBuilder sb = Strings.appendKeyValue(new StringBuilder(), "Table name:", name);
    sb = Strings.appendKeyValue(sb, "numberOfRegion",
      Integer.valueOf(this.region));
    sb = Strings.appendKeyValue(sb, "numberOfStores",
      Integer.valueOf(this.stores));
    sb = Strings.appendKeyValue(sb, "numberOfStorefiles",
      Integer.valueOf(this.storefiles));
    sb = Strings.appendKeyValue(sb, "storefileUncompressedSizeMB",
      Integer.valueOf(this.storeUncompressedSizeMB));
    sb = Strings.appendKeyValue(sb, "storefileSizeMB",
        Integer.valueOf(this.storefileSizeMB));
    if (this.storeUncompressedSizeMB != 0) {
      sb = Strings.appendKeyValue(sb, "compressionRatio",
          String.format("%.4f", (float)this.storefileSizeMB/
              (float)this.storeUncompressedSizeMB));
    }
    sb = Strings.appendKeyValue(sb, "memstoreSizeMB",
      Integer.valueOf(this.memstoreSizeMB));
    sb = Strings.appendKeyValue(sb, "storefileIndexSizeMB",
      Integer.valueOf(this.storefileIndexSizeMB));
    sb = Strings.appendKeyValue(sb, "readRequestsCount",
        Long.valueOf(this.readRequestsCount));
    sb = Strings.appendKeyValue(sb, "writeRequestsCount",
        Long.valueOf(this.writeRequestsCount));
    sb = Strings.appendKeyValue(sb, "rootIndexSizeKB",
        Integer.valueOf(this.rootIndexSizeKB));
    sb = Strings.appendKeyValue(sb, "totalStaticIndexSizeKB",
        Integer.valueOf(this.totalStaticIndexSizeKB));
    sb = Strings.appendKeyValue(sb, "totalStaticBloomSizeKB",
      Integer.valueOf(this.totalStaticBloomSizeKB));
    sb = Strings.appendKeyValue(sb, "totalCompactingKVs",
        Long.valueOf(this.totalCompactingKVs));
    sb = Strings.appendKeyValue(sb, "currentCompactedKVs",
        Long.valueOf(this.currentCompactedKVs));
    float compactionProgressPct = Float.NaN;
    if( this.totalCompactingKVs > 0 ) {
      compactionProgressPct = Float.valueOf(
          this.currentCompactedKVs / this.totalCompactingKVs);
    }
    sb = Strings.appendKeyValue(sb, "compactionProgressPct",
        compactionProgressPct);
    sb = Strings.appendKeyValue(sb, "readRequestsPerSecond",
      this.readRequestsPerSecond);
    sb = Strings.appendKeyValue(sb, "writeRequestsPerSecond",
      this.writeRequestsPerSecond);
    sb = Strings.appendKeyValue(sb, "userReadRequestsPerSecond", this.userReadRequestsPerSecond);
    sb = Strings.appendKeyValue(sb, "userWriteRequestsPerSecond", this.userWriteRequestsPerSecond);
    sb = Strings.appendKeyValue(sb, "readCellCountPerSecond",
        this.getReadCellCountPerSecond());
    sb = Strings.appendKeyValue(sb, "readRawCellCountPerSecond",
        this.getReadRawCellCountPerSecond());
    sb = Strings.appendKeyValue(sb, "scanCountPerSecond", this.getScanCountPerSecond());
    sb = Strings.appendKeyValue(sb, "scanRowsPerSecond", this.getScanRowsPerSecond());
    sb = Strings.appendKeyValue(sb, "readRequestsByCapacityUnitPerSecond",
      this.readRequestsByCapacityUnitPerSecond);
    sb = Strings.appendKeyValue(sb, "writeRequestsByCapacityUnitPerSecond",
      this.writeRequestsByCapacityUnitPerSecond);
    sb = Strings.appendKeyValue(sb, "userReadRequestsByCapacityUnitPerSecond",
      this.userReadRequestsByCapacityUnitPerSecond);
    sb = Strings.appendKeyValue(sb, "userWriteRequestsByCapacityUnitPerSecond",
      this.userWriteRequestsByCapacityUnitPerSecond);
    sb = Strings.appendKeyValue(sb, "throttledReadRequestsCount",
      this.throttledReadRequestsCount);
    sb = Strings.appendKeyValue(sb, "throttledWriteRequestsCount",
      this.throttledWriteRequestsCount);
    sb = Strings.appendKeyValue(sb, "familyLoads",
            this.getFamilyLoads().toString());
    sb = Strings.appendKeyValue(sb, "getTimeMean", this.getGetTimeMean());
    sb = Strings.appendKeyValue(sb, "putTimeMean", this.getPutTimeMean());
    sb = Strings.appendKeyValue(sb, "scanTimeMean", this.getScanTimeMean());
    sb = Strings.appendKeyValue(sb, "batchTimeMean", this.getBatchTimeMean());
    sb = Strings.appendKeyValue(sb, "deleteTimeMean", this.getDeleteTimeMean());
    sb = Strings.appendKeyValue(sb, "appendTimeMean", this.getAppendTimeMean());
    sb = Strings.appendKeyValue(sb, "incrementTimeMean", this.getIncrementTimeMean());
    sb = Strings.appendKeyValue(sb, "getTimeMax99Percentile", this.getGetTimeMax99Percentile());
    sb = Strings.appendKeyValue(sb, "putTimeMax99Percentile", this.getPutTimeMax99Percentile());
    sb = Strings.appendKeyValue(sb, "scanTimeMax99Percentile", this.getScanTimeMax99Percentile());
    sb = Strings.appendKeyValue(sb, "batchTimeMax99Percentile", this.getBatchTimeMax99Percentile());
    sb = Strings.appendKeyValue(sb, "approximateRowCount", this.getApproximateRowCount());
    return sb.toString();
  }
}
