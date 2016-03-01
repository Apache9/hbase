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
package org.apache.hadoop.hbase.regionserver.compactions;

import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceAudience;

/**
 * Generate window whose boundary is relative to now.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class RelativeDateTieredWindowFactory implements DateTieredWindowFactory {

  private final class RelativeWindow implements Window {

    private final long windowStartMillis;

    private final long windowMillis;

    private RelativeWindow(long windowStartMillis, long windowMillis) {
      this.windowStartMillis = windowStartMillis;
      this.windowMillis = windowMillis;
    }

    @Override
    public int compareToTimestamp(long timestamp) {
      long delta = timestamp - windowStartMillis;
      return delta < 0 ? 1 : delta < windowMillis ? 0 : -1;
    }

    @Override
    public Window nextWindow() {
      long nextWindowMillis = windowMillis * times;
      return new RelativeWindow(windowStartMillis - nextWindowMillis, nextWindowMillis);
    }
  }

  private final long baseWindowSizeMillis;

  private final int times;

  public RelativeDateTieredWindowFactory(CompactionConfiguration conf) {
    this.baseWindowSizeMillis = conf.getBaseWindowMillis();
    this.times = conf.getMinFilesToCompact();
  }

  @Override
  public Window getInitialWindow(long now) {
    return new RelativeWindow(now - baseWindowSizeMillis, baseWindowSizeMillis);
  }
}
