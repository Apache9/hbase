/**
 * Copyright 2011 The Apache Software Foundation
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
package org.apache.hadoop.hbase.regionserver;

import static org.apache.hadoop.hbase.zookeeper.ZKSplitLog.Counters.*;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.mutable.MutableInt;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.executor.ExecutorService;
import org.apache.hadoop.hbase.master.SplitLogManager;
import org.apache.hadoop.hbase.regionserver.handler.HLogSplitterHandler;
import org.apache.hadoop.hbase.regionserver.wal.HLogSplitter;
import org.apache.hadoop.hbase.util.CancelableProgressable;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.zookeeper.ZKSplitLog;
import org.apache.hadoop.hbase.zookeeper.ZKSplitLog.TaskState;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperListener;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.hadoop.util.StringUtils;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

/**
 * This worker is spawned in every regionserver (should we also spawn one in
 * the master?). The Worker waits for log splitting tasks to be put up by the
 * {@link SplitLogManager} running in the master and races with other workers
 * in other serves to acquire those tasks. The coordination is done via
 * zookeeper. All the action takes place at /hbase/splitlog znode.
 * <p>
 * If a worker has successfully moved the task from state UNASSIGNED to
 * OWNED then it owns the task. It keeps heart beating the manager by
 * periodically moving the task from UNASSIGNED to OWNED state. On success it
 * moves the task to TASK_DONE. On unrecoverable error it moves task state to
 * ERR. If it cannot continue but wants the master to retry the task then it
 * moves the task state to RESIGNED.
 * <p>
 * The manager can take a task away from a worker by moving the task from
 * OWNED to UNASSIGNED. In the absence of a global lock there is a
 * unavoidable race here - a worker might have just finished its task when it
 * is stripped of its ownership. Here we rely on the idempotency of the log
 * splitting task for correctness
 */
public class SplitLogWorker extends ZooKeeperListener implements Runnable {
  private static final Log LOG = LogFactory.getLog(SplitLogWorker.class);
  
  public static final int DEFAULT_MAX_SPLITTERS = 2;

  Thread worker;
  private final Server server;
  private final int report_period;
  private final ServerName serverName;
  private final TaskExecutor splitTaskExecutor;
  // thread pool which executes recovery work
  private final ExecutorService executorService;
  
  private Object taskReadyLock = new Object();
  volatile int taskReadySeq = 0;
  private volatile String currentTask = null;
  private int currentVersion;
  private volatile boolean exitWorker;
  private Object grabTaskLock = new Object();
  private boolean workerInGrabTask = false;

  protected final AtomicInteger tasksInProgress = new AtomicInteger(0);
  private int maxConcurrentTasks = 0;


  public SplitLogWorker(ZooKeeperWatcher watcher, Configuration conf,
      RegionServerServices server, TaskExecutor splitTaskExecutor) {
    super(watcher);
    this.server = server;
    this.serverName = server.getServerName();
    this.splitTaskExecutor = splitTaskExecutor;
    this.report_period = conf.getInt("hbase.splitlog.report.period",                  
           conf.getInt("hbase.splitlog.manager.timeout", ZKSplitLog.DEFAULT_TIMEOUT) / 3);
    this.executorService = server.getExecutorService();
    this.maxConcurrentTasks = 
        conf.getInt("hbase.regionserver.wal.max.splitters", DEFAULT_MAX_SPLITTERS);
  }

  public SplitLogWorker(ZooKeeperWatcher watcher, final Configuration conf,
      final String serverName, 
      final HRegionServer rs) {
    this(watcher, conf, rs, new TaskExecutor () {
      @Override
      public Status exec(String filename, CancelableProgressable p) {
        Path rootdir;
        FileSystem fs;
        try {
          rootdir = FSUtils.getRootDir(conf);
          fs = rootdir.getFileSystem(conf);
        } catch (IOException e) {
          LOG.warn("could not find root dir or fs", e);
          return Status.RESIGNED;
        }
        // TODO have to correctly figure out when log splitting has been
        // interrupted or has encountered a transient error and when it has
        // encountered a bad non-retry-able persistent error.
        try {          
          String relativeLogPath = getRelativeLogPath(filename);
          if (HLogSplitter.splitLogFile(rootdir,
              fs.getFileStatus(new Path(rootdir, relativeLogPath)), 
              fs, conf, p, rs) == false) {
            return Status.PREEMPTED;
          }
        } catch (InterruptedIOException iioe) {
          LOG.warn("log splitting of " + filename + " interrupted, resigning",
              iioe);
          return Status.RESIGNED;
        } catch (IOException e) {
          Throwable cause = e.getCause();
          if (cause instanceof InterruptedException) {
            LOG.warn("log splitting of " + filename + " interrupted, resigning",
                e);
            return Status.RESIGNED;
          }
          LOG.warn("log splitting of " + filename + " failed, returning error",
              e);
          return Status.ERR;
        }
        return Status.DONE;
      }

      private String getRelativeLogPath(String logPath) {
        StringBuilder sb = new StringBuilder();
        String znodeDelimiter = Character.toString(Path.SEPARATOR_CHAR);
        String[] filenameSplits = logPath.split(znodeDelimiter);
        int len = filenameSplits.length;
        String relativeLogPath = logPath;
        if (len > 3) {
          // the last three terms are .logs/server/log-file
          relativeLogPath = sb.append(filenameSplits[len - 3]).append(znodeDelimiter)
            .append(filenameSplits[len - 2]).append(znodeDelimiter)
            .append(filenameSplits[len - 1]).toString();
        }
        return relativeLogPath;
      }
    });
  }

  @Override
  public void run() {
   try {
    LOG.info("SplitLogWorker " + this.serverName + " starting");
    this.watcher.registerListener(this);
    int res;
    // wait for master to create the splitLogZnode
    res = -1;
    while (res == -1) {
      try {
        res = ZKUtil.checkExists(watcher, watcher.splitLogZNode);
      } catch (KeeperException e) {
        // ignore
        LOG.warn("Exception when checking for " + watcher.splitLogZNode +
            " ... retrying", e);
      }
      if (res == -1) {
        try {
          LOG.info(watcher.splitLogZNode + " znode does not exist," +
              " waiting for master to create one");
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          LOG.debug("Interrupted while waiting for " + watcher.splitLogZNode);
          assert exitWorker == true;
        }
      }
    }

    taskLoop();
   } catch (Throwable t) {
	   // only a logical error can cause here. Printing it out 
	   // to make debugging easier
	   LOG.error("unexpected error ", t);
   } finally {
	   LOG.info("SplitLogWorker " + this.serverName + " exiting");
   }
  }

  /**
   * Wait for tasks to become available at /hbase/splitlog zknode. Grab a task
   * one at a time. This policy puts an upper-limit on the number of
   * simultaneous log splitting that could be happening in a cluster.
   * <p>
   * Synchronization using {@link #task_ready_signal_seq} ensures that it will
   * try to grab every task that has been put up
   */
  private void taskLoop() {
    while (true) {
      int seq_start = taskReadySeq;
      Queue<String> taskQueue = getTaskQueue();
      if (taskQueue == null) {
        LOG.warn("Could not get tasks, did someone remove " +
            this.watcher.splitLogZNode + " ... worker thread exiting.");
        return;
      }
      int numTasks = taskQueue.size();  
      while(!taskQueue.isEmpty()) {
        String task = taskQueue.poll();
        // don't call ZKSplitLog.getNodeName() because that will lead to
        // double encoding of the path name
        if (this.calculateAvailableSplitters(numTasks) > 0) {
          grabTask(ZKUtil.joinZNode(watcher.splitLogZNode, task));
        } else {
          LOG.debug("Current region server " + this.serverName + " has "
              + this.tasksInProgress.get()
              + " tasks in progress and can't take more.");
          break;
        }
        if (exitWorker == true) {
          return;
        }
      }
      synchronized (taskReadyLock) {
        while (seq_start == taskReadySeq) {
          try {
            taskReadyLock.wait();
          } catch (InterruptedException e) {
            LOG.info("SplitLogWorker interrupted while waiting for task," +
              " exiting: " + e.toString());
            assert exitWorker == true;
            return;
          }
        }
      }
    }
  }

  /**
   * try to grab a 'lock' on the task zk node to own and execute the task.
   * <p>
   * @param path zk node for the task
   */
  private void grabTask(String path) {
    Stat stat = new Stat();
    long t = -1;
    byte[] data;
    synchronized (grabTaskLock) {
      currentTask = path;
      workerInGrabTask = true;
      if (Thread.interrupted()) {
        return;
      }
    }
    try {
      try {
        if ((data = ZKUtil.getDataNoWatch(this.watcher, path, stat)) == null) {
          tot_wkr_failed_to_grab_task_no_data.incrementAndGet();
          return;
        }
      } catch (KeeperException e) {
        LOG.warn("Failed to get data for znode " + path, e);
        tot_wkr_failed_to_grab_task_exception.incrementAndGet();
        return;
      }
      if (TaskState.TASK_UNASSIGNED.equals(data) == false) {
        tot_wkr_failed_to_grab_task_owned.incrementAndGet();
        return;
      }

      currentVersion = attemptToOwnTask(true, watcher, serverName.getServerName(), path, stat.getVersion());
      if (currentVersion < 0) {
        tot_wkr_failed_to_grab_task_lost_race.incrementAndGet();
        return;
      }

      if (ZKSplitLog.isRescanNode(watcher, currentTask)) {
        HLogSplitterHandler.endTask(watcher, TaskState.TASK_DONE.get(serverName.getServerName()), tot_wkr_task_acquired_rescan,
          currentTask, currentVersion);
        return;
      }
      
      LOG.info("worker " + serverName + " acquired task " + path);
      tot_wkr_task_acquired.incrementAndGet();
      getDataSetWatchAsync();

      submitTask(path, currentVersion, this.report_period);
      // after a successful submit, sleep a little bit to allow other RSs to grab the rest tasks
      try {
        Random r = new Random();
        int sleepTime = r.nextInt(500) + 500;
        Thread.sleep(sleepTime);
      } catch (InterruptedException e) {
        LOG.warn("Interrupted while yielding for other region servers", e);
        Thread.currentThread().interrupt();
      }
    } finally {
      synchronized (grabTaskLock) {
        workerInGrabTask = false;
        // clear the interrupt from stopTask() otherwise the next task will
        // suffer
        Thread.interrupted();
      }
    }
    return;
  }

  /**
   * Try to own the task by transitioning the zk node data from UNASSIGNED to OWNED.
   * <p>
   * This method is also used to periodically heartbeat the task progress by
   * transitioning the node from OWNED to OWNED.
   * <p>
   * @param isFirstTime
   * @param zkw
   * @param server
   * @param task
   * @param taskZKVersion
   * @return non-negative integer value when task can be owned by current region server otherwise -1
   */
  protected static int attemptToOwnTask(boolean isFirstTime,
      ZooKeeperWatcher zkw, String serverName, String task, int taskZKVersion) {
    int latestZKVersion = -1;
    try {
      Stat stat = zkw.getRecoverableZooKeeper().setData(task, TaskState.TASK_OWNED.get(serverName), taskZKVersion);
      if (stat == null) {
        LOG.warn("zk.setData() returned null for path " + task);
        tot_wkr_task_heartbeat_failed.incrementAndGet();
        return -1;
      }
      latestZKVersion = stat.getVersion();
      tot_wkr_task_heartbeat.incrementAndGet();
      return latestZKVersion;
    } catch (KeeperException e) {
      if (!isFirstTime) {
        if (e.code().equals(KeeperException.Code.NONODE)) {
          LOG.warn("NONODE failed to assert ownership for " + task, e);
        } else if (e.code().equals(KeeperException.Code.BADVERSION)) {
          LOG.warn("BADVERSION failed to assert ownership for " +
              task, e);
        } else {
          LOG.warn("failed to assert ownership for " + task, e);
        }
      }
    } catch (InterruptedException e1) {
      LOG.warn("Interrupted while trying to assert ownership of " +
          task + " " + StringUtils.stringifyException(e1));
      Thread.currentThread().interrupt();
    }
    tot_wkr_task_heartbeat_failed.incrementAndGet();
    return -1;
  }

  /**
   * This function calculates how many splitters it could create based on expected average tasks per
   * RS and the hard limit upper bound(maxConcurrentTasks) set by configuration. <br>
   * At any given time, a RS allows spawn MIN(Expected Tasks/RS, Hard Upper Bound)
   * @param numTasks current total number of available tasks
   * @return
   */
  private int calculateAvailableSplitters(int numTasks) {
    // at lease one RS(itself) available
    int availableRSs = 1;
    try {
      List<String> regionServers = ZKUtil.listChildrenNoWatch(watcher, watcher.rsZNode);
      availableRSs = Math.max(availableRSs, (regionServers == null) ? 0 : regionServers.size());
    } catch (KeeperException e) {
      // do nothing
      LOG.debug("getAvailableRegionServers got ZooKeeper exception", e);
    }
    int expectedTasksPerRS =
        (numTasks / availableRSs) + ((numTasks % availableRSs == 0) ? 0 : 1);
    expectedTasksPerRS = Math.max(1, expectedTasksPerRS); // at least be one
    // calculate how many more splitters we could spawn
    return Math.min(expectedTasksPerRS, this.maxConcurrentTasks)
        - this.tasksInProgress.get();
  }
  
  /**
   * Submit a log split task to executor service
   * @param curTask
   * @param curTaskZKVersion
   */
  void submitTask(final String curTask, final int curTaskZKVersion,
      final int reportPeriod) {
    final MutableInt zkVersion = new MutableInt(curTaskZKVersion);

    CancelableProgressable reporter = new CancelableProgressable() {
      private long last_report_at = 0;

      @Override
      public boolean progress() {
        long t = EnvironmentEdgeManager.currentTimeMillis();
        if ((t - last_report_at) > reportPeriod) {
          last_report_at = t;
          int latestZKVersion =
              attemptToOwnTask(false, watcher, serverName.getServerName(), curTask,
                zkVersion.intValue());
          if (latestZKVersion < 0) {
            LOG.warn("Failed to heartbeat the task" + curTask);
            return false;
          }
          zkVersion.setValue(latestZKVersion);
        }
        return true;
      }
    };

    HLogSplitterHandler hsh =
        new HLogSplitterHandler(server, curTask, zkVersion, reporter,
            this.tasksInProgress, this.splitTaskExecutor);
    this.executorService.submit(hsh);
  }
  
  void getDataSetWatchAsync() {
    this.watcher.getRecoverableZooKeeper().getZooKeeper().
      getData(currentTask, this.watcher,
      new GetDataAsyncCallback(), null);
    tot_wkr_get_data_queued.incrementAndGet();
  }

  void getDataSetWatchSuccess(String path, byte[] data) {
    synchronized (grabTaskLock) {
      if (workerInGrabTask) {
        // currentTask can change but that's ok
        String taskpath = currentTask;
        if (taskpath != null && taskpath.equals(path)) {
          // have to compare data. cannot compare version because then there
          // will be race with attemptToOwnTask()
          // cannot just check whether the node has been transitioned to
          // UNASSIGNED because by the time this worker sets the data watch
          // the node might have made two transitions - from owned by this
          // worker to unassigned to owned by another worker
          if (! TaskState.TASK_OWNED.equals(data, serverName.getServerName()) &&
              ! TaskState.TASK_DONE.equals(data, serverName.getServerName()) &&
              ! TaskState.TASK_ERR.equals(data, serverName.getServerName()) &&
              ! TaskState.TASK_RESIGNED.equals(data, serverName.getServerName())) {
            LOG.info("task " + taskpath + " preempted from " +
                serverName + ", current task state and owner=" +
                new String(data));
            stopTask();
          }
        }
      }
    }
  }

  void getDataSetWatchFailure(String path) {
    synchronized (grabTaskLock) {
      if (workerInGrabTask) {
        // currentTask can change but that's ok
        String taskpath = currentTask;
        if (taskpath != null && taskpath.equals(path)) {
          LOG.info("retrying data watch on " + path);
          tot_wkr_get_data_retry.incrementAndGet();
          getDataSetWatchAsync();
        } else {
          // no point setting a watch on the task which this worker is not
          // working upon anymore
        }
      }
    }
  }




  @Override
  public void nodeDataChanged(String path) {
    // there will be a self generated dataChanged event every time attemptToOwnTask()
    // heartbeats the task znode by upping its version
    synchronized (grabTaskLock) {
      if (workerInGrabTask) {
        // currentTask can change
        String taskpath = currentTask;
        if (taskpath!= null && taskpath.equals(path)) {
          getDataSetWatchAsync();
        }
      }
    }
  }


  private Queue<String> getTaskQueue() {
    long sleepTime = 1000;
    // It will be in loop till it gets the list of children or
    // it will come out if worker thread exited.
    while (!exitWorker) {
      try {
        List<String> childrenPaths = ZKUtil.listChildrenAndWatchForNewChildren(this.watcher,
            this.watcher.splitLogZNode);
        if (childrenPaths != null) {
          List<String> localTasks = new ArrayList<String>();
          List<String> remoteTasks = new ArrayList<String>();
          for (String task: childrenPaths) {
            if (ZKSplitLog.isLocalTask(task, serverName.getHostname())) {
              localTasks.add(task);
            }else {
              remoteTasks.add(task);
            }
          }
          // shuffle the tasks
          Queue<String> queue = new LinkedList<String>();
          // put local tasks first
          Collections.shuffle(localTasks);
          queue.addAll(localTasks);
          
          Collections.shuffle(remoteTasks);
          queue.addAll(remoteTasks);
          return queue;
        }
      } catch (KeeperException e) {
        LOG.warn("Could not get children of znode "
            + this.watcher.splitLogZNode, e);
      }
      try {
        LOG.debug("Retry listChildren of znode " + this.watcher.splitLogZNode
            + " after sleep for " + sleepTime + "ms!");
        Thread.sleep(sleepTime);
      } catch (InterruptedException e1) {
        LOG.warn("Interrupted while trying to get task list ...", e1);
        Thread.currentThread().interrupt();
      }
    }
    return null;
  }


  @Override
  public void nodeChildrenChanged(String path) {
    if(path.equals(watcher.splitLogZNode)) {
      LOG.debug("tasks arrived or departed");
      synchronized (taskReadyLock) {
        taskReadySeq++;
        taskReadyLock.notify();
      }
    }
  }

  /**
   * If the worker is doing a task i.e. splitting a log file then stop the task.
   * It doesn't exit the worker thread.
   */
  void stopTask() {
    LOG.info("Sending interrupt to stop the worker thread");
    worker.interrupt(); // TODO interrupt often gets swallowed, do what else?
  }


  /**
   * start the SplitLogWorker thread
   */
  public void start() {
    worker = new Thread(null, this, "SplitLogWorker-" + serverName);
    exitWorker = false;
    worker.start();
    return;
  }

  /**
   * stop the SplitLogWorker thread
   */
  public void stop() {
    exitWorker = true;
    stopTask();
  }

  /**
   * Asynchronous handler for zk get-data-set-watch on node results.
   */
  class GetDataAsyncCallback implements AsyncCallback.DataCallback {
    private final Log LOG = LogFactory.getLog(GetDataAsyncCallback.class);

    @Override
    public void processResult(int rc, String path, Object ctx, byte[] data,
        Stat stat) {
      tot_wkr_get_data_result.incrementAndGet();
      if (rc != 0) {
        LOG.warn("getdata rc = " + KeeperException.Code.get(rc) + " " + path);
        getDataSetWatchFailure(path);
        return;
      }
      data = watcher.getRecoverableZooKeeper().removeMetaData(data);
      getDataSetWatchSuccess(path, data);
      return;
    }
  }
  
  /**
   * Objects implementing this interface actually do the task that has been
   * acquired by a {@link SplitLogWorker}. Since there isn't a water-tight
   * guarantee that two workers will not be executing the same task therefore it
   * is better to have workers prepare the task and then have the
   * {@link SplitLogManager} commit the work in SplitLogManager.TaskFinisher
   */
  static public interface TaskExecutor {
    static public enum Status {
      DONE(),
      ERR(),
      RESIGNED(),
      PREEMPTED();
    }
    public Status exec(String name, CancelableProgressable p);
  }
}
