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
 * Generate window whose boundary is fixed based on epoch time.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class FixedDateTieredWindowFactory implements DateTieredWindowFactory {

  private final class FixedWindow implements Window {
    /**
     * How big a range of timestamps fit inside the window in milliseconds.
     */
    private final long windowMillis;

    /**
     * A timestamp t is within the window iff t / size == divPosition.
     */
    private final long divPosition;

    private FixedWindow(long baseWindowMillis, long divPosition) {
      this.windowMillis = baseWindowMillis;
      this.divPosition = divPosition;
    }

    @Override
    public int compareToTimestamp(long timestamp) {
      long pos = timestamp / windowMillis;
      return divPosition == pos ? 0 : divPosition < pos ? -1 : 1;
    }

    @Override
    public FixedWindow nextWindow() {
      if (divPosition % windowsPerTier > 0) {
        return new FixedWindow(windowMillis, divPosition - 1);
      } else {
        return new FixedWindow(windowMillis * windowsPerTier, divPosition / windowsPerTier - 1);
      }
    }
  }

  private final long baseWindowSizeMillis;

  private final int windowsPerTier;

  public FixedDateTieredWindowFactory(CompactionConfiguration comConf) {
    this.baseWindowSizeMillis = comConf.getBaseWindowMillis();
    this.windowsPerTier = comConf.getWindowsPerTier();
  }

  @Override
  public FixedWindow getInitialWindow(long now) {
    return new FixedWindow(baseWindowSizeMillis, now / baseWindowSizeMillis);
  }

}
