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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hbase.regionserver.compactions.CompactionConfiguration;
import org.apache.hadoop.hbase.regionserver.compactions.DateTieredCompactionPolicy;
import org.junit.Assert;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

public class TestDateTieredCompactionPolicy extends TestCompactionPolicy {

  @Override
  protected void config() {
    super.config();

    // Set up policy
    conf.setLong(CompactionConfiguration.MAX_AGE_MILLIS_KEY, 100);
    conf.setLong(CompactionConfiguration.INCOMING_WINDOW_MIN_KEY, 3);
    conf.setLong(CompactionConfiguration.BASE_WINDOW_MILLIS_KEY, 6);
    conf.setInt(CompactionConfiguration.WINDOWS_PER_TIER_KEY, 4);
    conf.set(DefaultStoreEngine.DEFAULT_COMPACTION_POLICY_CLASS_KEY,
      DateTieredCompactionPolicy.class.getName());

    // Special settings for compaction policy per window
    this.conf.setInt(CompactionConfiguration.HBASE_HSTORE_COMPACTION_MIN_KEY, 2);
    this.conf.setInt(CompactionConfiguration.HBASE_HSTORE_COMPACTION_MAX_KEY, 12);
    this.conf.setFloat(CompactionConfiguration.HBASE_HSTORE_COMPACTION_RATIO_KEY, 1.2F);
  }

  protected ArrayList<StoreFile> sfCreate(long[] minTimestamps, long[] maxTimestamps, long[] sizes)
      throws IOException {
    ArrayList<Long> ageInDisk = new ArrayList<Long>();
    for (int i = 0; i < sizes.length; i++) {
      ageInDisk.add(0L);
    }

    ArrayList<StoreFile> ret = Lists.newArrayList();
    for (int i = 0; i < sizes.length; i++) {
      MockStoreFile msf = new MockStoreFile(TEST_UTIL, TEST_FILE, sizes[i], ageInDisk.get(i), false,
          i);
      msf.setTimeRangeTracker(new TimeRangeTracker(minTimestamps[i], maxTimestamps[i]));
      ret.add(msf);
    }
    return ret;
  }

  protected void compactEquals(long now, ArrayList<StoreFile> candidates, long... expected)
      throws IOException {
    Assert.assertTrue(((DateTieredCompactionPolicy) store.storeEngine.getCompactionPolicy())
        .needsCompaction(candidates, ImmutableList.<StoreFile> of(), now));

    List<StoreFile> actual = ((DateTieredCompactionPolicy) store.storeEngine.getCompactionPolicy())
        .applyCompactionPolicy(candidates, false, false, now);

    Assert.assertEquals(Arrays.toString(expected), Arrays.toString(getSizes(actual)));
  }
}
