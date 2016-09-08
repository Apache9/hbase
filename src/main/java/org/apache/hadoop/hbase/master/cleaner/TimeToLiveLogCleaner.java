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
package org.apache.hadoop.hbase.master.cleaner;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

/**
 * Log cleaner that uses the timestamp of the hlog to determine if it should
 * be deleted. By default they are allowed to live for 10 minutes.
 */
@InterfaceAudience.Private
public class TimeToLiveLogCleaner extends BaseLogCleanerDelegate implements TimeToLiveCleanable {
  static final Log LOG = LogFactory.getLog(TimeToLiveLogCleaner.class.getName());

  public static final String TTL_CONF_KEY = "hbase.master.logcleaner.ttl";
  // default ttl = 5 minutes
  public static final long DEFAULT_TTL = 60000 * 5;
  public static final long MIN_TTL = 60000;

  public static final String OLDLOG_LIMIT_CONF_KEY = "hbase.master.oldlog.size.limit";
  public static final long DEFAULT_LIMIT = Long.MAX_VALUE;

  // Configured time a log can be kept after it was closed
  private long ttl;
  private boolean stopped = false;
  private long sizeLimit;

  @Override
  public boolean isLogDeletable(FileStatus fStat) {
    long currentTime = EnvironmentEdgeManager.currentTimeMillis();
    long time = fStat.getModificationTime();
    long life = currentTime - time;
    
    if (LOG.isTraceEnabled()) {
      LOG.trace("Log life:" + life + ", ttl:" + ttl + ", current:" + currentTime + ", from: "
          + time);
    }
    if (life < 0) {
      LOG.warn("Found a log (" + fStat.getPath() + ") newer than current time (" + currentTime
          + " < " + time + "), probably a clock skew");
      return false;
    }
    return life > ttl;
  }

  @Override
  public void setConf(Configuration conf) {
    this.ttl = getTTL(conf, TTL_CONF_KEY);
    this.sizeLimit = conf.getLong(OLDLOG_LIMIT_CONF_KEY, DEFAULT_LIMIT);
    super.setConf(conf);
    LOG.info("Initialize TimeToLiveLogCleaner, ttl=" + ttl + ", sizeLimit=" + sizeLimit);
  }

  @Override
  public void stop(String why) {
    this.stopped = true;
  }

  @Override
  public boolean isStopped() {
    return this.stopped;
  }

  @Override
  public void decreaseTTL() {
    ttl = Math.max(ttl / 2, MIN_TTL);
    LOG.info("Decrease ttl to " + ttl);
  }

  @Override
  public void increaseTTL() {
    ttl = Math.min(ttl * 2, getConf().getLong(TTL_CONF_KEY, DEFAULT_TTL));
    LOG.info("Increase ttl to " + ttl);
  }

  @Override
  public boolean isExceedSizeLimit(long size) {
    return size > this.sizeLimit;
  }

  private long getTTL(Configuration conf, String confKey) {
    long confTTL = conf.getLong(confKey, DEFAULT_TTL);
    if (confTTL < MIN_TTL) {
      confTTL = MIN_TTL;
      conf.setLong(confKey, confTTL);
    }
    return confTTL;
  }
}
