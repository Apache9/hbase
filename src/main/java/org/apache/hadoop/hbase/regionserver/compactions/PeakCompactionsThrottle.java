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

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.conf.ConfigurationManager;
import org.apache.hadoop.hbase.conf.ConfigurationObserver;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.RegionServerServices;
import org.apache.hadoop.util.StringUtils;

/**
 * Limit peak hour compaction speed , slow down when it is too fast. 
 */
@InterfaceAudience.Private
public class PeakCompactionsThrottle implements ConfigurationObserver{
  private static final Log LOG = LogFactory.getLog(PeakCompactionsThrottle.class);
  public static final String PEAK_COMPACTION_SPEED_ALLOWED = "hbase.regionserver.compaction.peak.maxspeed";
  public static final String PEAK_COMPACTION_SPEED_CHECK_INTERVAL = "hbase.regionserver.compaction.speed.check.interval";

  private OffPeakHours offPeakHours;
  private RegionServerServices rsServices;
  private AtomicLong maxSpeedInPeak = new AtomicLong(0);
  private AtomicLong checkInterval = new AtomicLong(0);
  private int numberOfThrottles = 0;
  private int timeOfThrottles = 0;
  private long start;
  private long end;
  private long bytesWritten = 0;

  public PeakCompactionsThrottle(Configuration conf, RegionServerServices service) {
    offPeakHours = OffPeakHours.getInstance(conf);
    maxSpeedInPeak.set(conf.getLong(PEAK_COMPACTION_SPEED_ALLOWED, 100 * 1024 * 1024 /* 100 MB/s */));
    checkInterval.set(conf.getLong(PEAK_COMPACTION_SPEED_CHECK_INTERVAL, 10 * 1024 * 1024 * 1024 /* 10 GB */));
    rsServices = service;
  }

  /**
   * start compaction
   */
  public void startCompaction() {
    start = System.currentTimeMillis();
    ConfigurationManager.getInstance().registerObserver(this);
  }

  /**
   * finish compaction
   */
  public void finishCompaction(String region, String family) {
    if (numberOfThrottles > 0) {
      LOG.info("Region '" + region + "' family '" + family + "' 's maxSpeedInPeak is "
          + StringUtils.humanReadableInt(maxSpeedInPeak.get()) + "/s compaction throttle: sleep number  "
          + numberOfThrottles + " sleep time " + timeOfThrottles + "ms");
    }
    ConfigurationManager.getInstance().deregisterObserver(this);
  }

  /**
   * reset start time
   */
  void resetStartTime() {
    start = System.currentTimeMillis();
  }

  /**
   * Peak compaction throttle, if it is peak time and the compaction speed is too fast, sleep for a
   * while to slow down.
   */
  public void throttle(long numOfBytes) throws IOException {
    bytesWritten += numOfBytes;
    if (bytesWritten >= checkInterval.get()) {
      checkAndSlowFastCompact(bytesWritten);
      bytesWritten = 0;
    }
  }

  private void checkAndSlowFastCompact(long numOfBytes) throws IOException {
    if (offPeakHours.isOffPeakHour()) {
      // off peak hour, just return.
      return;
    }
    if (maxSpeedInPeak.get() <= 0) {
      return;
    }
    end = System.currentTimeMillis();
    int currentThreadNum = (rsServices == null ? 1 : rsServices.getCurrentCompactionThreadNum());
    currentThreadNum = Math.max(1, currentThreadNum);
    long minTimeAllowed = (numOfBytes * 1000 * currentThreadNum) / maxSpeedInPeak.get(); // ms
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

  /**
   * For test
   */
  public int getNumberOfThrottles() {
    return numberOfThrottles;
  }

  @Override
  public void notifyOnChange(Configuration conf) {
    maxSpeedInPeak.set(conf.getLong(PEAK_COMPACTION_SPEED_ALLOWED, 100 * 1024 * 1024 /* 100 MB/s */));
    checkInterval.set(conf.getLong(PEAK_COMPACTION_SPEED_CHECK_INTERVAL, 10 * 1024 * 1024 * 1024 /* 10 GB */));
  }
}
