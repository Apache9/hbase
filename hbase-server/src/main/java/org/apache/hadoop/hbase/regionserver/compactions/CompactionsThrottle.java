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
package org.apache.hadoop.hbase.regionserver.compactions;

import java.io.IOException;
import java.io.InterruptedIOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.util.StringUtils;

/**
 * Limit peak hour compaction speed , slow down when it is too fast. Some applications have a
 * typically peak hour at which time have most request.
 * hbase.regionserver.compaction.peak.start.hour means peak start hour.
 * hbase.regionserver.compaction.peak.end.hour means peak end hour. Between the peak start hour and
 * the peak end hour the request from the client is the most at a day.
 * hbase.regionserver.compaction.speed.check.interval means at which time when we check compact
 * speed. hbase.regionserver.compaction.peak.maxspeed means max speed at peak hour.
 */
@InterfaceAudience.Private
public class CompactionsThrottle {
  private static final Log LOG = LogFactory.getLog(CompactionsThrottle.class);

  private TimeOfDayTracker offPeakHours;

  private final long maxSpeedInPeak;
  private final long maxSpeedInOffPeak;
  private final long checkInterval;
  private int numberOfThrottles = 0;
  private int timeOfThrottles = 0;
  private long start;
  private long end;
  private int bytesWritten = 0;

  public CompactionsThrottle(Configuration conf) {
    offPeakHours = TimeOfDayTracker.getInstance(conf);
    maxSpeedInPeak =
        conf.getLong(CompactionConfiguration.HBASE_HSTORE_PEAK_COMPACTION_SPEED_ALLOWED,
          10L * 1024 * 1024 /* 10 MB/s */);
    maxSpeedInOffPeak =
        conf.getLong(CompactionConfiguration.HBASE_HSTORE_OFFPEAK_COMPACTION_SPEED_ALLOWED,
          30L * 1024 * 1024 /* 30 MB/s */);
    checkInterval =
        conf.getLong(CompactionConfiguration.HBASE_HSTORE_COMPACTION_SPEED_CHECK_INTERVAL,
          10L * 1024 * 1024 /* 10 MB */);
  }

  /**
   * start compaction
   */
  public void startCompaction() {
    start = EnvironmentEdgeManager.currentTimeMillis();
  }

  /**
   * finish compaction
   */
  public void finishCompaction(String region, String family) {
    if (numberOfThrottles > 0) {
      LOG.info("Region '" + region + "' family '" + family + "' 's maxSpeedInPeak is "
          + StringUtils.byteDesc(maxSpeedInPeak) + "/s compaction throttle: sleep number  "
          + numberOfThrottles + " sleep time " + timeOfThrottles + "(ms)");
    }
  }

  /**
   * reset start time
   */
  void resetStartTime() {
    start = EnvironmentEdgeManager.currentTimeMillis();
  }

  /**
   * Peak compaction throttle, if it is peak time and the compaction speed is too fast,
   * sleep for a while to slow down.
   */
  public void throttle(long numOfBytes) throws IOException {
    bytesWritten += numOfBytes;
    if (bytesWritten >= checkInterval) {
      checkAndSlowFastCompact(bytesWritten);
      bytesWritten = 0;
    }
  }

  private void checkAndSlowFastCompact(long numOfBytes, long maxSpeed) throws IOException {
    if (maxSpeed <= 0) {
      return;
    }
    end = EnvironmentEdgeManager.currentTimeMillis();
    long minTimeAllowed = numOfBytes * 1000 / maxSpeed; // ms
    long elapsed = end - start;
    if (elapsed < minTimeAllowed) {
      // too fast
      try {
        // sleep for a while to slow down.
        Thread.sleep(minTimeAllowed - elapsed);
        numberOfThrottles++;
        timeOfThrottles += (minTimeAllowed - elapsed);
      } catch (InterruptedException ie) {
        IOException iie = new InterruptedIOException();
        iie.initCause(ie);
        throw iie;
      }
    }
    resetStartTime();
  }

  private void checkAndSlowFastCompact(long numOfBytes) throws IOException {
    checkAndSlowFastCompact(numOfBytes, offPeakHours.isHourInInterval() ? maxSpeedInOffPeak
        : maxSpeedInPeak);
  }

  /**
   * For test
   */
  public int getNumberOfThrottles() {
    return numberOfThrottles;
  }
}
