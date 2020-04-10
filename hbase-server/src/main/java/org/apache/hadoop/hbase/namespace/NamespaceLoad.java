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
package org.apache.hadoop.hbase.namespace;

import org.apache.hadoop.hbase.TableLoad;
import org.apache.hadoop.hbase.util.Strings;

/**
 * This class is used exporting current state of load on a namespace.
 */
public class NamespaceLoad {
  /** the namespace name */
  private String name;
  /** the number of tables for this namespace */
  private int tables;
  /** the number of regions for the table */
  private int regions;
  /** the number of stores for all tables in this namespace */
  private int stores;
  /** the number of storefiles for all tables in this namespace */
  private int storefiles;
  /** the total size of the store files for all tables in this namespace, uncompressed, in MB */
  private int storeUncompressedSizeMB;
  /** the current total size of the store files for all tables in this namespace, in MB */
  private int storefileSizeMB;
  /** the current size of the memstore for all tables in this namespace, in MB */
  private int memstoreSizeMB;

  /**
   * The current total size of root-level store file indexes for all tables in this namespace, in
   * MB. The same as {@link #rootIndexSizeKB} but in MB.
   */
  private int storefileIndexSizeMB;
  /** the current total read request made to all tables in this namespace */
  private long readRequestsCount;
  /** the current total write requests made to all tables in this namespace */
  private long writeRequestsCount;
  /** the total compacting key values in currently running compaction */
  private long totalCompactingKVs;
  /** the completed count of key values in currently running compaction */
  private long currentCompactedKVs;

  /** The current total size of root-level indexes for all tables in this namespace, in KB. */
  private int rootIndexSizeKB;

  /** The total size of all index blocks, not just the root level, in KB. */
  private int totalStaticIndexSizeKB;

  /**
   * The total size of all Bloom filter blocks, not just loaded into the block cache, in KB.
   */
  private int totalStaticBloomSizeKB;

  /** the current read requests per second made to all tables in this namespace */
  private long readRequestsPerSecond;

  /** the current write requests per second made to all tables in this namespace */
  private long writeRequestsPerSecond;

  private long readCellCountPerSecond;

  private long readRawCellCountPerSecond;

  /**
   * the current total read requests by capacity unit per second made to all tables in this
   * namespace
   */
  private long readRequestsByCapacityUnitPerSecond;
  /**
   * the current total write requests by capacity unit per second made to all tables in this
   * namespace
   */
  private long writeRequestsByCapacityUnitPerSecond;

  private long userReadRequestsPerSecond;
  private long userWriteRequestsPerSecond;
  private long userReadRequestsByCapacityUnitPerSecond;
  private long userWriteRequestsByCapacityUnitPerSecond;

  /** the total throttled read requests count made to all tables in this namespace */
  private long throttledReadRequestsCount;
  /** the total throttled write requests count made to all tables in this namespace */
  private long throttledWriteRequestsCount;

  private long scanCountPerSecond;
  private long scanRowsPerSecond;

  private long approximateRowCount;

  public NamespaceLoad(final String name) {
    this.name = name;
    this.tables = 0;
    this.regions = 0;
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
    this.readRequestsPerSecond = 0;
    this.writeRequestsPerSecond = 0;
    this.readRequestsByCapacityUnitPerSecond = 0;
    this.writeRequestsByCapacityUnitPerSecond = 0;
    this.throttledReadRequestsCount = 0;
    this.throttledWriteRequestsCount = 0;
    this.readCellCountPerSecond = 0;
    this.readRawCellCountPerSecond = 0;
    this.scanCountPerSecond = 0;
    this.scanRowsPerSecond = 0;
    this.userReadRequestsPerSecond = 0;
    this.userWriteRequestsPerSecond = 0;
    this.userReadRequestsByCapacityUnitPerSecond = 0;
    this.userWriteRequestsByCapacityUnitPerSecond = 0;
    this.approximateRowCount = 0;
  }

  public void updateNamespaceLoad(final TableLoad tableLoad) {
    this.tables++;
    this.regions += tableLoad.getRegion();
    this.stores += tableLoad.getStores();
    this.storefiles += tableLoad.getStorefiles();
    this.storeUncompressedSizeMB += tableLoad.getStoreUncompressedSizeMB();
    this.storefileSizeMB += tableLoad.getStorefileSizeMB();
    this.memstoreSizeMB += tableLoad.getMemstoreSizeMB();
    this.storefileIndexSizeMB += tableLoad.getStorefileIndexSizeMB();
    this.rootIndexSizeKB += tableLoad.getRootIndexSizeKB();
    this.totalStaticIndexSizeKB += tableLoad.getTotalStaticIndexSizeKB();
    this.totalStaticBloomSizeKB += tableLoad.getTotalStaticBloomSizeKB();
    this.readRequestsCount += tableLoad.getReadRequestsCount();
    this.writeRequestsCount += tableLoad.getWriteRequestsCount();
    this.totalCompactingKVs += tableLoad.getTotalCompactingKVs();
    this.currentCompactedKVs += tableLoad.getCurrentCompactedKVs();
    this.readRequestsPerSecond += tableLoad.getReadRequestsPerSecond();
    this.writeRequestsPerSecond += tableLoad.getWriteRequestsPerSecond();
    this.readRequestsByCapacityUnitPerSecond += tableLoad.getReadRequestsByCapacityUnitPerSecond();
    this.writeRequestsByCapacityUnitPerSecond += tableLoad
        .getWriteRequestsByCapacityUnitPerSecond();
    this.throttledReadRequestsCount += tableLoad.getThrottledReadRequestsCount();
    this.throttledWriteRequestsCount += tableLoad.getThrottledWriteRequestsCount();
    this.readCellCountPerSecond += tableLoad.getReadCellCountPerSecond();
    this.readRawCellCountPerSecond += tableLoad.getReadRawCellCountPerSecond();
    this.scanCountPerSecond += tableLoad.getScanCountPerSecond();
    this.scanRowsPerSecond += tableLoad.getScanRowsPerSecond();
    this.userReadRequestsPerSecond += tableLoad.getUserReadRequestsPerSecond();
    this.userWriteRequestsPerSecond += tableLoad.getUserWriteRequestsPerSecond();
    this.userReadRequestsByCapacityUnitPerSecond +=
        tableLoad.getUserReadRequestsByCapacityUnitPerSecond();
    this.userWriteRequestsByCapacityUnitPerSecond +=
        tableLoad.getUserWriteRequestsByCapacityUnitPerSecond();
    this.approximateRowCount += tableLoad.getApproximateRowCount();
  }

  public String getName() {
    return name;
  }

  public int getTables() {
    return tables;
  }

  public int getRegions() {
    return regions;
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

  public long getReadRequestsPerSecond() {
    return readRequestsPerSecond;
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

  public long getThrottledReadRequestsCount() {
    return throttledReadRequestsCount;
  }

  public long getThrottledWriteRequestsCount() {
    return throttledWriteRequestsCount;
  }

  public long getReadCellCountPerSecond() {
    return readCellCountPerSecond;
  }

  public long getReadRawCellCountPerSecond() {
    return readRawCellCountPerSecond;
  }

  public long getScanCountPerSecond(){
    return this.scanCountPerSecond;
  }

  public long getScanRowsPerSecond(){
    return this.scanRowsPerSecond;
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

  public long getApproximateRowCount() {
    return approximateRowCount;
  }

  @Override
  public String toString() {
    StringBuilder sb = Strings.appendKeyValue(new StringBuilder(), "Namespace name:", name);
    sb = Strings.appendKeyValue(sb, "tables", Integer.valueOf(this.tables));
    sb = Strings.appendKeyValue(sb, "regions", Integer.valueOf(this.regions));
    sb = Strings.appendKeyValue(sb, "stores", Integer.valueOf(this.stores));
    sb = Strings.appendKeyValue(sb, "Storefiles", Integer.valueOf(this.storefiles));
    sb = Strings.appendKeyValue(sb, "storefileUncompressedSizeMB",
      Integer.valueOf(this.storeUncompressedSizeMB));
    sb = Strings.appendKeyValue(sb, "storefileSizeMB", Integer.valueOf(this.storefileSizeMB));
    if (this.storeUncompressedSizeMB != 0) {
      sb = Strings.appendKeyValue(sb, "compressionRatio",
        String.format("%.4f", (float) this.storefileSizeMB / (float) this.storeUncompressedSizeMB));
    }
    sb = Strings.appendKeyValue(sb, "memstoreSizeMB", Integer.valueOf(this.memstoreSizeMB));
    sb = Strings.appendKeyValue(sb, "storefileIndexSizeMB",
      Integer.valueOf(this.storefileIndexSizeMB));
    sb = Strings.appendKeyValue(sb, "readRequestsCount", Long.valueOf(this.readRequestsCount));
    sb = Strings.appendKeyValue(sb, "writeRequestsCount", Long.valueOf(this.writeRequestsCount));
    sb = Strings.appendKeyValue(sb, "readCellCountPerSecond",
        this.getReadCellCountPerSecond());
    sb = Strings.appendKeyValue(sb, "readRawCellCountPerSecond",
        this.getReadRawCellCountPerSecond());
    sb = Strings.appendKeyValue(sb, "rootIndexSizeKB", Integer.valueOf(this.rootIndexSizeKB));
    sb = Strings.appendKeyValue(sb, "totalStaticIndexSizeKB",
      Integer.valueOf(this.totalStaticIndexSizeKB));
    sb = Strings.appendKeyValue(sb, "totalStaticBloomSizeKB",
      Integer.valueOf(this.totalStaticBloomSizeKB));
    sb = Strings.appendKeyValue(sb, "totalCompactingKVs", Long.valueOf(this.totalCompactingKVs));
    sb = Strings.appendKeyValue(sb, "currentCompactedKVs", Long.valueOf(this.currentCompactedKVs));
    float compactionProgressPct = Float.NaN;
    if (this.totalCompactingKVs > 0) {
      compactionProgressPct = Float.valueOf(this.currentCompactedKVs / this.totalCompactingKVs);
    }
    sb = Strings.appendKeyValue(sb, "compactionProgressPct", compactionProgressPct);
    sb = Strings.appendKeyValue(sb, "readRequestsPerSecond", this.readRequestsPerSecond);
    sb = Strings.appendKeyValue(sb, "writeRequestsPerSecond", this.writeRequestsPerSecond);
    sb = Strings.appendKeyValue(sb, "readRequestsByCapacityUnitPerSecond",
      this.readRequestsByCapacityUnitPerSecond);
    sb = Strings.appendKeyValue(sb, "writeRequestsByCapacityUnitPerSecond",
      this.writeRequestsByCapacityUnitPerSecond);
    sb = Strings.appendKeyValue(sb, "userReadRequestsPerSecond", this.userReadRequestsPerSecond);
    sb = Strings.appendKeyValue(sb, "userWriteRequestsPerSecond", this.userWriteRequestsPerSecond);
    sb = Strings.appendKeyValue(sb, "userReadRequestsByCapacityUnitPerSecond",
      this.userReadRequestsByCapacityUnitPerSecond);
    sb = Strings.appendKeyValue(sb, "userWriteRequestsByCapacityUnitPerSecond",
      this.userWriteRequestsByCapacityUnitPerSecond);
    sb = Strings.appendKeyValue(sb, "throttledReadRequestsCount", this.throttledReadRequestsCount);
    sb = Strings
        .appendKeyValue(sb, "throttledWriteRequestsCount", this.throttledWriteRequestsCount);
    sb = Strings.appendKeyValue(sb, "scanCountPerSecond", Long.valueOf(this.scanCountPerSecond));
    sb = Strings.appendKeyValue(sb, "scanRowsPerSecond", Long.valueOf(this.scanRowsPerSecond));
    sb = Strings.appendKeyValue(sb, "approximateRowCount", Long.valueOf(this.approximateRowCount));
    return sb.toString();
  }
}
