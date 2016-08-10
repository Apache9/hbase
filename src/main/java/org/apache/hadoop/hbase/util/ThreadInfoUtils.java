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
package org.apache.hadoop.hbase.util;

import java.io.PrintStream;
import java.io.PrintWriter;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;

/**
 * A thread info dumper similar to hadoop's ReflectionUtils.
 */
@InterfaceAudience.Private
public class ThreadInfoUtils {

  private static final Log LOG = LogFactory.getLog(ThreadInfoUtils.class);

  private static interface Printer {

    void println(Object o);

    void flush();
  }

  private static final ThreadMXBean THREAD_BEAN = ManagementFactory.getThreadMXBean();

  private static final int MAX_STACK_DEPTH = 20;

  private static final Printer LOG_PRINTER = new Printer() {

    @Override
    public void println(Object o) {
      LOG.info(o);
    }

    @Override
    public void flush() {
    }
  };

  /**
   * Print all of the thread's information and stack traces.
   * @param out the stream to
   * @param title a string title for the stack trace
   */
  public static void printThreadInfo(final PrintStream out, String title) {
    printThreadInfo(new Printer() {

      @Override
      public void println(Object o) {
        out.println(o);
      }

      @Override
      public void flush() {
        out.flush();
      }

    }, title, MAX_STACK_DEPTH);
  }

  /**
   * Print all of the thread's information and stack traces.
   * @param out the writer to
   * @param title a string title for the stack trace
   */
  public static void printThreadInfo(final PrintWriter out, String title) {
    printThreadInfo(new Printer() {

      @Override
      public void println(Object o) {
        out.println(o);
      }

      @Override
      public void flush() {
        out.flush();
      }

    }, title, MAX_STACK_DEPTH);
  }

  private static final ExecutorService LOG_THREAD_EXECUTOR = Executors
      .newSingleThreadExecutor(Threads.newDaemonThreadFactory("ThreadInfoDumper-"));

  private static final long MIN_LOG_DELAY = TimeUnit.MINUTES.toNanos(1);

  private static long LAST_LOG_FINISHED_NANO = -1L;

  private static boolean LOG_TASK_SCHEDULED = false;

  /**
   * Log the current thread stacks at INFO level.
   * <p>
   * Note that, we do not log all thread infos at one time to prevent blocking the normal log
   * output.
   * @param title a descriptive title for the call stacks
   * @param sync whether to wait until the log task finished.
   */
  public static void logThreadInfo(final String title, boolean sync) {
    synchronized (LOG_PRINTER) {
      if (LOG_TASK_SCHEDULED) {
        // there is a log task still running, give up
        return;
      }
      if (System.nanoTime() - LAST_LOG_FINISHED_NANO < MIN_LOG_DELAY) {
        // to prevent log thread info too frequently
        return;
      }
      LOG_TASK_SCHEDULED = true;
    }
    Future<?> future = LOG_THREAD_EXECUTOR.submit(new Runnable() {

      @Override
      public void run() {
        printThreadInfo(LOG_PRINTER, title, MAX_STACK_DEPTH);
        synchronized (LOG_PRINTER) {
          LOG_TASK_SCHEDULED = false;
          LAST_LOG_FINISHED_NANO = System.nanoTime();
        }
      }
    });
    if (sync) {
      try {
        future.get();
      } catch (InterruptedException e) {
      } catch (ExecutionException e) {
        // should not happen
      }
    }
  }

  private static String getTaskName(long id, String name) {
    if (name == null) {
      return Long.toString(id);
    }
    return id + " (" + name + ")";
  }

  private static void printThreadInfo(Printer out, String title, int maxDepth) {
    boolean contention = THREAD_BEAN.isThreadContentionMonitoringEnabled();
    long[] threadIds = THREAD_BEAN.getAllThreadIds();
    out.println("Process Thread Dump: " + title);
    out.println(threadIds.length + " active threads");
    for (long tid : threadIds) {
      ThreadInfo info = THREAD_BEAN.getThreadInfo(tid, maxDepth);
      if (info == null) {
        out.println("  Inactive");
        continue;
      }
      out.println("Thread " + getTaskName(info.getThreadId(), info.getThreadName()) + ":");
      Thread.State state = info.getThreadState();
      out.println("  State: " + state);
      out.println("  Blocked count: " + info.getBlockedCount());
      out.println("  Waited count: " + info.getWaitedCount());
      if (contention) {
        out.println("  Blocked time: " + info.getBlockedTime());
        out.println("  Waited time: " + info.getWaitedTime());
      }
      if (state == Thread.State.WAITING) {
        out.println("  Waiting on " + info.getLockName());
      } else if (state == Thread.State.BLOCKED) {
        out.println("  Blocked on " + info.getLockName());
        out.println("  Blocked by " + getTaskName(info.getLockOwnerId(), info.getLockOwnerName()));
      }
      out.println("  Stack:");
      for (StackTraceElement frame : info.getStackTrace()) {
        out.println("    " + frame.toString());
      }
    }
    out.flush();
  }
}
