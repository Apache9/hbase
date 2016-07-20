 /*
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
package org.apache.hadoop.hbase.io.hfile;

import java.io.IOException;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.io.hfile.BlockType.BlockCategory;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestCacheConfig {
  @Test
  public void testDisableCacheDataBlock() throws IOException {
    Configuration conf = HBaseConfiguration.create();
    CacheConfig cacheConfig = new CacheConfig(conf);
    assertTrue(cacheConfig.shouldCacheBlockOnRead(BlockCategory.DATA));
    assertFalse(cacheConfig.shouldCacheCompressed(BlockCategory.DATA));
    assertFalse(cacheConfig.shouldCacheDataCompressed());
    assertFalse(cacheConfig.shouldCacheDataOnWrite());
    assertTrue(cacheConfig.shouldCacheDataOnRead());
    assertTrue(cacheConfig.shouldCacheBlockOnRead(BlockCategory.INDEX));
    assertTrue(cacheConfig.shouldCacheBlockOnRead(BlockCategory.META));
    assertTrue(cacheConfig.shouldCacheBlockOnRead(BlockCategory.BLOOM));
    assertFalse(cacheConfig.shouldCacheBloomsOnWrite());
    assertFalse(cacheConfig.shouldCacheIndexesOnWrite());

    conf.setBoolean(CacheConfig.CACHE_BLOCKS_ON_WRITE_KEY, true);
    conf.setBoolean(CacheConfig.CACHE_DATA_BLOCKS_COMPRESSED_KEY, true);
    conf.setBoolean(CacheConfig.CACHE_BLOOM_BLOCKS_ON_WRITE_KEY, true);
    conf.setBoolean(CacheConfig.CACHE_INDEX_BLOCKS_ON_WRITE_KEY, true);

    cacheConfig = new CacheConfig(conf);
    assertTrue(cacheConfig.shouldCacheBlockOnRead(BlockCategory.DATA));
    assertTrue(cacheConfig.shouldCacheCompressed(BlockCategory.DATA));
    assertTrue(cacheConfig.shouldCacheDataCompressed());
    assertTrue(cacheConfig.shouldCacheDataOnWrite());
    assertTrue(cacheConfig.shouldCacheDataOnRead());
    assertTrue(cacheConfig.shouldCacheBlockOnRead(BlockCategory.INDEX));
    assertTrue(cacheConfig.shouldCacheBlockOnRead(BlockCategory.META));
    assertTrue(cacheConfig.shouldCacheBlockOnRead(BlockCategory.BLOOM));
    assertTrue(cacheConfig.shouldCacheBloomsOnWrite());
    assertTrue(cacheConfig.shouldCacheIndexesOnWrite());

    conf.setBoolean(CacheConfig.DISABLE_DATA_BLOCKS_CACHE_KEY, true);
    cacheConfig = new CacheConfig(conf);
    assertFalse(cacheConfig.shouldCacheBlockOnRead(BlockCategory.DATA));
    assertFalse(cacheConfig.shouldCacheCompressed(BlockCategory.DATA));
    assertFalse(cacheConfig.shouldCacheDataCompressed());
    assertFalse(cacheConfig.shouldCacheDataOnWrite());
    assertFalse(cacheConfig.shouldCacheDataOnRead());
    assertTrue(cacheConfig.shouldCacheBlockOnRead(BlockCategory.INDEX));
    assertTrue(cacheConfig.shouldCacheBlockOnRead(BlockCategory.META));
    assertTrue(cacheConfig.shouldCacheBlockOnRead(BlockCategory.BLOOM));
    assertTrue(cacheConfig.shouldCacheBloomsOnWrite());
    assertTrue(cacheConfig.shouldCacheIndexesOnWrite());
  }
}
