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

import org.apache.hadoop.hbase.HServerLoad.RegionLoad;
import org.apache.hadoop.hbase.util.Strings;

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
  /** the current total get request made to all regions of the table  */

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
  }

  public void updateTableLoad(final RegionLoad regionLoad) {
    this.region++;
    this.stores += regionLoad.getStores();
    this.storefiles += regionLoad.getStorefiles();

    int totalDataSize =
        storeUncompressedSizeMB + regionLoad.getStorefileSizeMB();
    if (totalDataSize == 0) {
      this.locality = 1.0f;
    } else {
      this.locality =
          (storeUncompressedSizeMB * locality + regionLoad.getStorefileSizeMB() + regionLoad
              .getLocality()) / (totalDataSize);
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
    this.writeRequestsPerSecond += regionLoad.getWriteRequestsPerSecond();
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

  public long getWriteRequestsPerSecond() {
    return writeRequestsPerSecond;
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
    return sb.toString();
  }
}
