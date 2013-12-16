/**
 * Copyright 2013 The Apache Software Foundation
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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.VersionedWritable;

/**
 * Table region statistics information which is passed back to a region server periodically. It
 * includes total number of regions for all/each table(s) and number of regions assigned to this region
 * server for all/each table(s).
 */
public class RegionStatistics extends VersionedWritable {
  public static final byte VERSION = 0;


  // region count of all tables
  private int regionCount;

  // region count of all tables
  private int regionCountRS;

  /*
   * a map from table name to (total region count, count of regions assigned to current region
   * server)
   */
  private Map<byte[], Pair<Integer, Integer>> regionCountPerTable;

  // For writable
  public RegionStatistics() {
  }

  public RegionStatistics(int regionCount, int regionCountRS,
      Map<byte[], Pair<Integer, Integer>> regionCountPerTable) {
    super();
    this.regionCount = regionCount;
    this.regionCountRS = regionCountRS;
    this.regionCountPerTable = regionCountPerTable;
  }

  public int getRegionCount() {
    return regionCount;
  }

  public int getRegionCountRS() {
    return regionCountRS;
  }

  public Map<byte[], Pair<Integer, Integer>> getRegionCountPerTable() {
    return regionCountPerTable;
  }

  @Override
  public byte getVersion() {
    return VERSION;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    out.writeInt(this.regionCount);
    out.writeInt(this.regionCountRS);
    if (this.regionCountPerTable == null) {
      out.writeInt(0);
    } else {
      out.writeInt(this.regionCountPerTable.size());
      for (Map.Entry<byte[], Pair<Integer, Integer>> entry : this.regionCountPerTable.entrySet()) {
        out.writeInt(entry.getKey().length);
        out.write(entry.getKey());
        out.writeInt(entry.getValue().getFirst());
        out.writeInt(entry.getValue().getSecond());
      }
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    this.regionCount = in.readInt();
    this.regionCountRS = in.readInt();
    this.regionCountPerTable = new TreeMap<byte[], Pair<Integer, Integer>>(Bytes.BYTES_COMPARATOR);
    int entries = in.readInt();
    while (entries-- > 0) {
      int len = in.readInt();
      if (len <= 0) {
        throw new IOException("Invalid table name");
      }
      byte[] tableName = new byte[len];
      in.readFully(tableName, 0, len);
      int regionCount = in.readInt();
      int regionCountOfRs = in.readInt();
      this.regionCountPerTable.put(tableName, new Pair<Integer, Integer>(regionCount,
          regionCountOfRs));
    }
  }
}
