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

import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionConfiguration;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionWindowFactory;
import org.apache.hadoop.hbase.regionserver.compactions.ExponentialCompactionWindowFactory;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ RegionServerTests.class, SmallTests.class })
public class TestDateTieredCompactionPolicyFreeze extends AbstractTestDateTieredCompactionPolicy {
  @Override
  protected void config() {
    super.config();

    // Set up policy
    conf.set(StoreEngine.STORE_ENGINE_CLASS_KEY,
      "org.apache.hadoop.hbase.regionserver.DateTieredStoreEngine");
    conf.setLong(CompactionConfiguration.DATE_TIERED_MAX_AGE_MILLIS_KEY, 100);
    conf.setLong(CompactionConfiguration.DATE_TIERED_INCOMING_WINDOW_MIN_KEY, 3);
    conf.setClass(CompactionConfiguration.DATE_TIERED_COMPACTION_WINDOW_FACTORY_CLASS_KEY,
      ExponentialCompactionWindowFactory.class, CompactionWindowFactory.class);
    conf.setLong(ExponentialCompactionWindowFactory.BASE_WINDOW_MILLIS_KEY, 6);
    conf.setInt(ExponentialCompactionWindowFactory.WINDOWS_PER_TIER_KEY, 4);
    conf.setBoolean(CompactionConfiguration.DATE_TIERED_SINGLE_OUTPUT_FOR_MINOR_COMPACTION_KEY,
      false);
    conf.setBoolean(CompactionConfiguration.FREEZE_DATE_TIERED_WINDOW_OLDER_THAN_MAX_AGE_KEY, true);
    // Special settings for compaction policy per window
    this.conf.setInt(CompactionConfiguration.HBASE_HSTORE_COMPACTION_MIN_KEY, 2);
    this.conf.setInt(CompactionConfiguration.HBASE_HSTORE_COMPACTION_MAX_KEY, 12);
    this.conf.setFloat(CompactionConfiguration.HBASE_HSTORE_COMPACTION_RATIO_KEY, 1.2F);

    conf.setInt(HStore.BLOCKING_STOREFILES_KEY, 20);
    conf.setLong(HConstants.MAJOR_COMPACTION_PERIOD, 10);
  }

  @Test
  public void test() throws IOException {
    long[] minTimestamps = new long[] { 0, 24, 32, 150 };
    long[] maxTimestamps = new long[] { 12, 30, 56, 160 };
    long[] sizes = new long[] { 10, 20, 10, 20 };

    compactEquals(161, sfCreate(minTimestamps, maxTimestamps, sizes), new long[] { 20, 10 },
      new long[] { 24, 48, 72 }, false, true);
  }

  @Test
  public void testOneFileButOverlap() throws IOException {
    long[] minTimestamps = new long[] { 0, 24, 150 };
    long[] maxTimestamps = new long[] { 12, 56, 160 };
    long[] sizes = new long[] { 10, 20, 10 };

    compactEquals(161, sfCreate(minTimestamps, maxTimestamps, sizes), new long[] { 20 },
      new long[] { 24, 48, 72 }, false, true);
  }

  @Test
  public void testCompacting() throws IOException {
    long[] minTimestamps = new long[] { 0, 12, 24, 32, 150 };
    long[] maxTimestamps = new long[] { 12, 23, 30, 56, 160 };
    long[] sizes = new long[] { 10, 20, 30, 40, 10 };

    List<StoreFile> candidateFiles = sfCreate(minTimestamps, maxTimestamps, sizes);

    compactEquals(161, candidateFiles, Lists.newArrayList(candidateFiles.subList(0, 2)),
      new long[] { 30, 40 }, new long[] { 24, 48, 72 }, false, true);
  }
}
