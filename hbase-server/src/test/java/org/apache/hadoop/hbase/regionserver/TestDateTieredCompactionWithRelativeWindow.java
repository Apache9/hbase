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

import org.apache.hadoop.hbase.regionserver.compactions.CompactionConfiguration;
import org.apache.hadoop.hbase.regionserver.compactions.DateTieredWindowFactory;
import org.apache.hadoop.hbase.regionserver.compactions.RelativeDateTieredWindowFactory;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestDateTieredCompactionWithRelativeWindow extends TestDateTieredCompactionPolicy {

  @Override
  protected void config() {
    super.config();

    this.conf.setClass(CompactionConfiguration.WINDOW_FACTORY_CLASS,
      RelativeDateTieredWindowFactory.class, DateTieredWindowFactory.class);
  }

  /**
   * Test for incoming window
   * @throws IOException with error
   */
  @Test
  public void incomingWindow() throws IOException {
    long[] minTimestamps = new long[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
    long[] maxTimestamps = new long[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 };
    long[] sizes = new long[] { 30, 31, 32, 33, 34, 20, 91, 22, 93, 24, 95, 10, 11, 12, 13 };

    compactEquals(16, sfCreate(minTimestamps, maxTimestamps, sizes), 13, 12, 11, 10);
  }

  /**
   * Not enough files in incoming window
   * @throws IOException with error
   */
  @Test
  public void NotIncomingWindow() throws IOException {
    long[] minTimestamps = new long[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
    long[] maxTimestamps = new long[] { 1, 2, 3, 4, 5, 6, 14, 15, 16, 17, 18, 19, 20 };
    long[] sizes = new long[] { 30, 31, 32, 33, 34, 20, 21, 22, 23, 24, 125, 10, 11 };

    compactEquals(25, sfCreate(minTimestamps, maxTimestamps, sizes), 24, 23, 22, 21);
  }

  /**
   * Test for file newer than incoming window
   * @throws IOException with error
   */
  @Test
  public void NewerThanIncomingWindow() throws IOException {
    long[] minTimestamps = new long[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
    long[] maxTimestamps = new long[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 18 };
    long[] sizes = new long[] { 30, 31, 32, 33, 34, 20, 21, 22, 23, 24, 95, 10, 11, 12, 13 };

    compactEquals(16, sfCreate(minTimestamps, maxTimestamps, sizes), 13, 12, 11, 10);
  }

  /**
   * Older than now(161) - maxAge(100)
   * @throws IOException with error
   */
  @Test
  public void olderThanMaxAge() throws IOException {
    long[] minTimestamps = new long[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
    long[] maxTimestamps = new long[] { 44, 60, 61, 96, 100, 104, 105, 106, 113, 145, 157 };
    long[] sizes = new long[] { 0, 50, 51, 40, 41, 42, 33, 30, 31, 2, 1 };

    compactEquals(161, sfCreate(minTimestamps, maxTimestamps, sizes), 31, 30, 33, 42, 41, 40);
  }

  /**
   * Out-of-order data
   * @throws IOException with error
   */
  @Test
  public void OutOfOrder() throws IOException {
    long[] minTimestamps = new long[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
    long[] maxTimestamps = new long[] { 0, 13, 3, 10, 11, 1, 2, 12, 14, 15 };
    long[] sizes = new long[] { 30, 31, 32, 33, 34, 22, 28, 23, 24, 1 };

    compactEquals(16, sfCreate(minTimestamps, maxTimestamps, sizes), 1, 24, 23, 28, 22, 34, 33, 32,
      31);
  }
}
