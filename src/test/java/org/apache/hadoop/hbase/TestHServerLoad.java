/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */

package org.apache.hadoop.hbase;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.io.WritableUtils;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

@Category(SmallTests.class)
public class TestHServerLoad {
  private static long LAST_FLUSH_SEQID = 10;
  
  private byte[] regionLoadV2Bytes() throws IOException {
    ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(byteStream);
    byte[] name = Bytes.toBytes("test,,,1234612746");
    out.writeByte(2);
    WritableUtils.writeVInt(out, name.length);
    out.write(name);
    WritableUtils.writeVInt(out, 0);
    WritableUtils.writeVInt(out, 0);
    WritableUtils.writeVInt(out, 0);
    WritableUtils.writeVInt(out, 0);
    WritableUtils.writeVInt(out, 0);
    WritableUtils.writeVInt(out, 0);
    WritableUtils.writeVLong(out, 0);
    WritableUtils.writeVLong(out, 0);
    WritableUtils.writeVInt(out, 0);
    WritableUtils.writeVInt(out, 0);
    WritableUtils.writeVInt(out, 0);
    WritableUtils.writeVLong(out, 0);
    WritableUtils.writeVLong(out, 0);
    WritableUtils.writeVInt(out, 0);
    out.close();
    return byteStream.toByteArray();
  }
  
  private HServerLoad.RegionLoad createRegionLoad() {
    byte[] name = Bytes.toBytes("test,,,1234612746");
    int stores = 0;
    int storefiles = 0;
    int storeUncompressedSizeMB = 0;
    int storefileSizeMB = 0;
    int memstoreSizeMB = 0;
    int storefileIndexSizeMB = 0;
    int rootIndexSizeKB = 0;
    int totalStaticIndexSizeKB = 0;
    int totalStaticBloomSizeKB = 0;
    long totalCompactingKVs = 0;
    long currentCompactedKVs = 0;

    return new HServerLoad.RegionLoad(name, stores, storefiles,
        storeUncompressedSizeMB, storefileSizeMB, memstoreSizeMB,
        storefileIndexSizeMB, rootIndexSizeKB, totalStaticIndexSizeKB,
        totalStaticBloomSizeKB, 0, 0, 0, totalCompactingKVs, currentCompactedKVs,
        LAST_FLUSH_SEQID, 0.0f);
  }

  @Test
  public void testRegionLoadSerialization() throws IOException {
    HServerLoad.RegionLoad load = createRegionLoad();
    assertEquals(LAST_FLUSH_SEQID, load.getLastFlushSeqId());

    byte[] bytes = Writables.getBytes(load);
    HServerLoad.RegionLoad deserialized = (HServerLoad.RegionLoad) Writables
        .getWritable(bytes, new HServerLoad.RegionLoad());
    
    assertEquals(load.toString(), deserialized.toString());
    assertEquals(LAST_FLUSH_SEQID, deserialized.getLastFlushSeqId());
  }

  @Test
  public void testRegionLoadV2ToV3() throws IOException {
    byte[] bytes = regionLoadV2Bytes();
    HServerLoad.RegionLoad deserialized = (HServerLoad.RegionLoad) Writables
        .getWritable(bytes, new HServerLoad.RegionLoad());
    assertEquals(5, deserialized.getVersion());
    assertEquals(-1, deserialized.getLastFlushSeqId());
  }
}
