/*
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
package org.apache.hadoop.hbase.util;

import static java.lang.Thread.State.BLOCKED;
import static java.lang.Thread.State.NEW;
import static java.lang.Thread.State.RUNNABLE;
import static java.lang.Thread.State.TERMINATED;
import static java.lang.Thread.State.TIMED_WAITING;
import static java.lang.Thread.State.WAITING;

import com.google.common.base.Preconditions;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;

/**
 * Class which sets up a simple thread which runs in a loop sleeping
 * for a config interval. If the VM thread metric number is abnormal,
 * then it'll trigger a thread dump writting into logfile.
 */
@InterfaceAudience.Private
public class JvmThreadMonitor {
  private static final Log LOG = LogFactory.getLog(JvmThreadMonitor.class);
  /** The thread sleep interval */
  private final long SLEEP_INTERVAL_MS;
  private static final String THREAD_SLEEP_KEY = "jvm.threadmonitor.sleep-interval-ms";
  private static final int THREAD_SLEEP_DEFAULT = 60000;

  /** dump thread info if we detect current threadWaiting value less than this threshold */
  private final int threadWaitingThresholdMin;
  private static final String THREAD_WAITING_THRESHOLD_KEY = "jvm.threadmonitor.waiting-threshold-min";
  private static final int THREAD_WAITING_THRESHOLD_DEFAULT = 50;

  /** dump thread info if we detect current threadBlocked value more than this threshold */
  private final int threadBlockedThresholdMax;
  private static final String THREAD_BLOCKED_THRESHOLD_KEY = "jvm.threadmonitor.blocked-threshold-max";
  private static final int THREAD_BLOCKED_THRESHOLD_DEFAULT = 50;

  /** dump thread info if we detect current threadRunnable value more than this threshold */
  private final int threadRunnableThresholdMax;
  private static final String THREAD_RUNNABLE_THRESHOLD_KEY = "jvm.threadmonitor.runnable-threshold-max";
  private static final int THREAD_RUNNABLE_THRESHOLD_DEFAULT = 50;

  private Thread monitorThread;
  private volatile boolean shouldRun = true;

  public JvmThreadMonitor(Configuration conf) {
    this.SLEEP_INTERVAL_MS = conf.getLong(THREAD_SLEEP_KEY, THREAD_SLEEP_DEFAULT);
    this.threadWaitingThresholdMin = conf.getInt(THREAD_WAITING_THRESHOLD_KEY,
      THREAD_WAITING_THRESHOLD_DEFAULT);
    this.threadBlockedThresholdMax = conf.getInt(THREAD_BLOCKED_THRESHOLD_KEY,
      THREAD_BLOCKED_THRESHOLD_DEFAULT);
    this.threadRunnableThresholdMax = conf.getInt(THREAD_RUNNABLE_THRESHOLD_KEY,
      THREAD_RUNNABLE_THRESHOLD_DEFAULT);
  }

  public void start() {
    Preconditions.checkState(monitorThread == null, "Already started");
    monitorThread = new Thread(new Monitor());
    monitorThread.setDaemon(true);
    monitorThread.setName("JvmThreadMonitor");
    monitorThread.start();
  }

  public void stop() {
    shouldRun = false;
    if (monitorThread != null) {
      monitorThread.interrupt();
      try {
        monitorThread.join();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  private class Monitor implements Runnable {
    @Override
    public void run() {
      while (shouldRun) {
        try {
          Thread.sleep(SLEEP_INTERVAL_MS);
        } catch (InterruptedException ie) {
          return;
        }

        ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
        long threadIds[] = threadMXBean.getAllThreadIds();
        ThreadInfo[] threadInfos = threadMXBean.getThreadInfo(threadIds, 0);

        int threadsNew = 0;
        int threadsRunnable = 0;
        int threadsBlocked = 0;
        int threadsWaiting = 0;
        int threadsTimedWaiting = 0;
        int threadsTerminated = 0;

        for (ThreadInfo threadInfo : threadInfos) {
          // threadInfo is null if the thread is not alive or doesn't exist
          if (threadInfo == null) continue;
          Thread.State state = threadInfo.getThreadState();
          if (state == NEW) {
            threadsNew++;
          } else if (state == RUNNABLE) {
            threadsRunnable++;
          } else if (state == BLOCKED) {
            threadsBlocked++;
          } else if (state == WAITING) {
            threadsWaiting++;
          } else if (state == TIMED_WAITING) {
            threadsTimedWaiting++;
          } else if (state == TERMINATED) {
            threadsTerminated++;
          }
        }

        boolean printDump = false;
        if (threadsWaiting < threadWaitingThresholdMin) {
          LOG.warn("dump thread info since threadsWaiting < threadWaitingThresholdMin("
              + threadWaitingThresholdMin + ")");
          printDump = true;
        } else if (threadsBlocked > threadBlockedThresholdMax) {
          LOG.warn("dump thread info since threadsBlocked > threadBlockedThresholdMax("
              + threadBlockedThresholdMax + ")");
          printDump = true;
        } else if (threadsRunnable > threadRunnableThresholdMax) {
          LOG.warn("dump thread info since threadsRunnable > threadRunnableThresholdMax("
              + threadRunnableThresholdMax + ")");
          printDump = true;
        }

        if (printDump) {
          ReflectionUtils
              .logThreadInfo(LOG, "thread dump from JvmThreadMonitor", SLEEP_INTERVAL_MS);
        }
      }
    }
  }
}
