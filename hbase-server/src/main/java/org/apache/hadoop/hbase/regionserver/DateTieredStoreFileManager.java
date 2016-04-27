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

import java.util.Comparator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionConfiguration;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * StoreFileManager for date tiered compaction.
 */
@InterfaceAudience.Private
public class DateTieredStoreFileManager extends DefaultStoreFileManager {
  /**
   * The file metadata fields that contain the freezing window information.
   */
  public static final byte[] FREEZING_WINDOW_START_TIMESTAMP = Bytes
      .toBytes("FREEZING_WINDOW_START_TIMESTAMP");
  public static final byte[] FREEZING_WINDOW_END_TIMESTAMP = Bytes
      .toBytes("FREEZING_WINDOW_END_TIMESTAMP");

  public DateTieredStoreFileManager(CellComparator kvComparator, Configuration conf,
      CompactionConfiguration comConf) {
    super(kvComparator, conf, comConf);
  }

  @Override
  public Comparator<StoreFile> getStoreFileComparator() {
    return StoreFile.Comparators.SEQ_ID_MAX_TIMESTAMP;
  }
}
