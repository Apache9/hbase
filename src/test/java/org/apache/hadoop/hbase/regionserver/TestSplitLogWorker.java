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

import static org.apache.hadoop.hbase.zookeeper.ZKSplitLog.Counters.resetCounters;
import static org.apache.hadoop.hbase.zookeeper.ZKSplitLog.Counters.tot_wkr_failed_to_grab_task_lost_race;
import static org.apache.hadoop.hbase.zookeeper.ZKSplitLog.Counters.tot_wkr_failed_to_grab_task_owned;
import static org.apache.hadoop.hbase.zookeeper.ZKSplitLog.Counters.tot_wkr_preempt_task;
import static org.apache.hadoop.hbase.zookeeper.ZKSplitLog.Counters.tot_wkr_task_acquired;
import static org.apache.hadoop.hbase.zookeeper.ZKSplitLog.Counters.tot_wkr_task_acquired_rescan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MediumTests;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.executor.ExecutorService;
import org.apache.hadoop.hbase.executor.ExecutorService.ExecutorType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CancelableProgressable;
import org.apache.hadoop.hbase.zookeeper.ZKSplitLog;
import org.apache.hadoop.hbase.zookeeper.ZKSplitLog.TaskState;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MediumTests.class)
public class TestSplitLogWorker {
  private static final Log LOG = LogFactory.getLog(TestSplitLogWorker.class);
  static {
    Logger.getLogger("org.apache.hadoop.hbase").setLevel(Level.DEBUG);
  }
  private final static HBaseTestingUtility TEST_UTIL =
    new HBaseTestingUtility();
  private ZooKeeperWatcher zkw;
  private SplitLogWorker slw;
  private ExecutorService executorService;

  private void waitForCounter(AtomicLong ctr, long oldval, long newval,
      long timems) {
    assertTrue("ctr=" + ctr.get() + ", oldval=" + oldval + ", newval=" + newval,
      waitForCounterBoolean(ctr, oldval, newval, timems));
  }

  private boolean waitForCounterBoolean(AtomicLong ctr, long oldval, long newval,
      long timems) {
    long curt = System.currentTimeMillis();
    long endt = curt + timems;
    while (curt < endt) {
      if (ctr.get() <  newval) {
        try {
          Thread.sleep(10);
        } catch (InterruptedException e) {
        }
        curt = System.currentTimeMillis();
      } else {
        assertEquals(newval, ctr.get());
        return true;
      }
    }
    return false;
  }

  @Before
  public void setup() throws Exception {
    TEST_UTIL.startMiniZKCluster();
    zkw = new ZooKeeperWatcher(TEST_UTIL.getConfiguration(),
        "split-log-worker-tests", null);
    ZKUtil.deleteChildrenRecursively(zkw, zkw.baseZNode);
    ZKUtil.createAndFailSilent(zkw, zkw.baseZNode);
    assertTrue(ZKUtil.checkExists(zkw, zkw.baseZNode) != -1);
    LOG.debug(zkw.baseZNode + " created");
    ZKUtil.createAndFailSilent(zkw, zkw.splitLogZNode);
    assertTrue(ZKUtil.checkExists(zkw, zkw.splitLogZNode) != -1);
    LOG.debug(zkw.splitLogZNode + " created");
    resetCounters();
    executorService = new ExecutorService("TestSplitLogWorker");
    executorService.startExecutorService(ExecutorType.RS_LOG_REPLAY_OPS, 10);
  }

  @After
  public void teardown() throws Exception {
    if (executorService != null) {
      executorService.shutdown();
    }
    TEST_UTIL.shutdownMiniZKCluster();
  }

  SplitLogWorker.TaskExecutor neverEndingTask =
    new SplitLogWorker.TaskExecutor() {

      @Override
      public Status exec(String name, CancelableProgressable p) {
        while (true) {
          try {
            Thread.sleep(1000);
          } catch (InterruptedException e) {
            return Status.PREEMPTED;
          }
          if (!p.progress()) {
            return Status.PREEMPTED;
          }
        }
      }

  };

  @Test
  public void testAcquireTaskAtStartup() throws Exception {
    LOG.info("testAcquireTaskAtStartup");
    ZKSplitLog.Counters.resetCounters();
    final ServerName RS = ServerName.parseServerName("rs,1,1");
    RegionServerServices mockedRS = getRegionServer(RS);
    zkw.getRecoverableZooKeeper().create(ZKSplitLog.getEncodedNodeName(zkw, "tatas"),
        TaskState.TASK_UNASSIGNED.get("mgr"), Ids.OPEN_ACL_UNSAFE,
        CreateMode.PERSISTENT);

    SplitLogWorker slw =
        new SplitLogWorker(zkw, TEST_UTIL.getConfiguration(), mockedRS,
            neverEndingTask);
    slw.start();
    try {
      waitForCounter(tot_wkr_task_acquired, 0, 1, 1500);
      assertTrue(TaskState.TASK_OWNED.equals(ZKUtil.getData(zkw,
        ZKSplitLog.getEncodedNodeName(zkw, "tatas")), RS.getServerName()));
    } finally {
      stopSplitLogWorker(slw);
    }
  }

  private void stopSplitLogWorker(final SplitLogWorker slw)
  throws InterruptedException {
    if (slw != null) {
      slw.stop();
      slw.worker.join(3000);
      if (slw.worker.isAlive()) {
        assertTrue(("Could not stop the worker thread slw=" + slw) == null);
      }
    }
  }

  @Test
  public void testRaceForTask() throws Exception {
    LOG.info("testRaceForTask");
    ZKSplitLog.Counters.resetCounters();

    zkw.getRecoverableZooKeeper().create(ZKSplitLog.getEncodedNodeName(zkw, "trft"),
        TaskState.TASK_UNASSIGNED.get("manager"), Ids.OPEN_ACL_UNSAFE,
        CreateMode.PERSISTENT);

    final ServerName SVR1 = ServerName.parseServerName("rs1,1,1");
    final ServerName SVR2 = ServerName.parseServerName("rs2,1,1");
    RegionServerServices mockedRS1 = getRegionServer(SVR1);
    RegionServerServices mockedRS2 = getRegionServer(SVR2);
    SplitLogWorker slw1 =
        new SplitLogWorker(zkw, TEST_UTIL.getConfiguration(), mockedRS1,
            neverEndingTask);
    SplitLogWorker slw2 =
        new SplitLogWorker(zkw, TEST_UTIL.getConfiguration(), mockedRS2,
            neverEndingTask);
    slw1.start();
    slw2.start();
    try {
      waitForCounter(tot_wkr_task_acquired, 0, 1, 1500);
      // Assert that either the tot_wkr_failed_to_grab_task_owned count was set of if
      // not it, that we fell through to the next counter in line and it was set.
      assertTrue(waitForCounterBoolean(tot_wkr_failed_to_grab_task_owned, 0, 1, 1500) ||
        tot_wkr_failed_to_grab_task_lost_race.get() == 1);
      assertTrue(TaskState.TASK_OWNED.equals(ZKUtil.getData(zkw,
        ZKSplitLog.getEncodedNodeName(zkw, "trft")), SVR1.getServerName()) ||
        TaskState.TASK_OWNED.equals(ZKUtil.getData(zkw,
            ZKSplitLog.getEncodedNodeName(zkw, "trft")), SVR2.getServerName()));
    } finally {
      stopSplitLogWorker(slw1);
      stopSplitLogWorker(slw2);
    }
  }

  @Test
  public void testPreemptTask() throws Exception {
    LOG.info("testPreemptTask");
    ZKSplitLog.Counters.resetCounters();
    final ServerName SRV = ServerName.parseServerName("rs,1,1");
    RegionServerServices mockedRS = getRegionServer(SRV);
    SplitLogWorker slw =
        new SplitLogWorker(zkw, TEST_UTIL.getConfiguration(), mockedRS,
            neverEndingTask);
    slw.start();
    try {
      Thread.yield(); // let the worker start
      Thread.sleep(100);

      // this time create a task node after starting the splitLogWorker
      zkw.getRecoverableZooKeeper().create(ZKSplitLog.getEncodedNodeName(zkw, "tpt_task"),
        TaskState.TASK_UNASSIGNED.get("manager"), Ids.OPEN_ACL_UNSAFE,
        CreateMode.PERSISTENT);

      waitForCounter(tot_wkr_task_acquired, 0, 1, 1500);
      assertEquals(1, slw.taskReadySeq);
      assertTrue(TaskState.TASK_OWNED.equals(ZKUtil.getData(zkw,
        ZKSplitLog.getEncodedNodeName(zkw, "tpt_task")),  SRV.getServerName()));

      ZKUtil.setData(zkw, ZKSplitLog.getEncodedNodeName(zkw, "tpt_task"),
        TaskState.TASK_UNASSIGNED.get("manager"));
      waitForCounter(tot_wkr_preempt_task, 0, 1, 1500);
    } finally {
      stopSplitLogWorker(slw);
    }
  }

  @Test
  public void testMultipleTasks() throws Exception {
    LOG.info("testMultipleTasks");
    ZKSplitLog.Counters.resetCounters();
    final ServerName SRV = ServerName.parseServerName("rs,1,1");
    RegionServerServices mockedRS = getRegionServer(SRV);
    SplitLogWorker slw =
        new SplitLogWorker(zkw, TEST_UTIL.getConfiguration(), mockedRS,
            neverEndingTask);
    slw.start();
    try {
      Thread.yield(); // let the worker start
      Thread.sleep(100);

      zkw.getRecoverableZooKeeper().create(ZKSplitLog.getEncodedNodeName(zkw, "tmt_task"),
        TaskState.TASK_UNASSIGNED.get("manager"), Ids.OPEN_ACL_UNSAFE,
        CreateMode.PERSISTENT);

      waitForCounter(tot_wkr_task_acquired, 0, 1, 1500);
      // now the worker is busy doing the above task

      // create another task
      zkw.getRecoverableZooKeeper().create(ZKSplitLog.getEncodedNodeName(zkw, "tmt_task_2"),
        TaskState.TASK_UNASSIGNED.get("manager"), Ids.OPEN_ACL_UNSAFE,
        CreateMode.PERSISTENT);

      // preempt the first task, have it owned by another worker
      ZKUtil.setData(zkw, ZKSplitLog.getEncodedNodeName(zkw, "tmt_task"),
        TaskState.TASK_OWNED.get("another-worker"));
      waitForCounter(tot_wkr_preempt_task, 0, 1, 1500);

      waitForCounter(tot_wkr_task_acquired, 1, 2, 1500);
      assertEquals(2, slw.taskReadySeq);
      assertTrue(TaskState.TASK_OWNED.equals(ZKUtil.getData(zkw,
        ZKSplitLog.getEncodedNodeName(zkw, "tmt_task_2")), SRV.getServerName()));
    } finally {
      stopSplitLogWorker(slw);
    }
  }

  @Test
  public void testRescan() throws Exception {
    LOG.info("testRescan");
    ZKSplitLog.Counters.resetCounters();
    final ServerName SRV = ServerName.parseServerName("rs,1,1");
    RegionServerServices mockedRS = getRegionServer(SRV);
    slw = new SplitLogWorker(zkw, TEST_UTIL.getConfiguration(), mockedRS,
            neverEndingTask);
    slw.start();
    Thread.yield(); // let the worker start
    Thread.sleep(200);

    String task = ZKSplitLog.getEncodedNodeName(zkw, "task");
    zkw.getRecoverableZooKeeper().create(task,
      TaskState.TASK_UNASSIGNED.get("manager"), Ids.OPEN_ACL_UNSAFE,
      CreateMode.PERSISTENT);

    waitForCounter(tot_wkr_task_acquired, 0, 1, 1500);
    // now the worker is busy doing the above task

    // preempt the task, have it owned by another worker
    ZKUtil.setData(zkw, task, TaskState.TASK_UNASSIGNED.get("manager"));
    waitForCounter(tot_wkr_preempt_task, 0, 1, 1500);

    // create a RESCAN node
    String rescan = ZKSplitLog.getEncodedNodeName(zkw, "RESCAN");
    rescan = zkw.getRecoverableZooKeeper().create(rescan,
      TaskState.TASK_UNASSIGNED.get("manager"), Ids.OPEN_ACL_UNSAFE,
      CreateMode.PERSISTENT_SEQUENTIAL);

    waitForCounter(tot_wkr_task_acquired, 1, 2, 1500);
    // RESCAN node might not have been processed if the worker became busy
    // with the above task. preempt the task again so that now the RESCAN
    // node is processed
    ZKUtil.setData(zkw, task, TaskState.TASK_UNASSIGNED.get("manager"));
    waitForCounter(tot_wkr_preempt_task, 1, 2, 1500);
    waitForCounter(tot_wkr_task_acquired_rescan, 0, 1, 1500);

    List<String> nodes = ZKUtil.listChildrenNoWatch(zkw, zkw.splitLogZNode);
    LOG.debug(nodes);
    int num = 0;
    for (String node : nodes) {
      num++;
      if (node.startsWith("RESCAN")) {
        String name = ZKSplitLog.getEncodedNodeName(zkw, node);
        String fn = ZKSplitLog.getFileName(name);
        byte [] data = ZKUtil.getData(zkw, ZKUtil.joinZNode(zkw.splitLogZNode, fn));
        String datastr = Bytes.toString(data);
        assertTrue("data=" + datastr, TaskState.TASK_DONE.equals(data, SRV.getServerName()));
      }
    }
    assertEquals(2, num);
  }
  @Test
  public void testAcquireMultiTasks() throws Exception {
    LOG.info("testAcquireMultiTasks");
    resetCounters();
    final String TATAS = "tatas";
    final ServerName RS = ServerName.parseServerName("rs,1,1");
    final int maxTasks = 3;
    Configuration testConf = HBaseConfiguration.create(TEST_UTIL.getConfiguration());
    testConf.setInt("hbase.regionserver.wal.max.splitters", maxTasks);
    RegionServerServices mockedRS = getRegionServer(RS);

    for (int i = 0; i < maxTasks; i++) {
      zkw.getRecoverableZooKeeper().create(ZKSplitLog.getEncodedNodeName(zkw, TATAS + i),
        TaskState.TASK_UNASSIGNED.get("mgr,1,1"),
        Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    SplitLogWorker slw = new SplitLogWorker(zkw, testConf, mockedRS, neverEndingTask);
    slw.start();
    try {
      waitForCounter(tot_wkr_task_acquired, 0, maxTasks, 3000);
      for (int i = 0; i < maxTasks; i++) {
        byte[] bytes = ZKUtil.getData(zkw, ZKSplitLog.getEncodedNodeName(zkw, TATAS + i));
        assertEquals(RS.getServerName(), TaskState.TASK_OWNED.getWriterName(bytes));
      }
    } finally {
      stopSplitLogWorker(slw);
    }
  }

  /**
   * The test checks SplitLogWorker should not spawn more splitters than expected num of tasks per
   * RS
   * @throws Exception
   */
  @Test
  public void testAcquireMultiTasksByAvgTasksPerRS() throws Exception {
    LOG.info("testAcquireMultiTasks");
    resetCounters();
    final String TATAS = "tatas";
    final ServerName RS = ServerName.parseServerName("rs,1,1");
    final ServerName RS2 = ServerName.parseServerName("rs,1,2");
    final int maxTasks = 3;
    Configuration testConf = HBaseConfiguration.create(TEST_UTIL.getConfiguration());
    testConf.setInt("hbase.regionserver.wal.max.splitters", maxTasks);
    RegionServerServices mockedRS = getRegionServer(RS);

    // create two RS nodes
    zkw.getRecoverableZooKeeper().create(zkw.rsZNode, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    String rsPath = ZKUtil.joinZNode(zkw.rsZNode, RS.getServerName());
    zkw.getRecoverableZooKeeper().create(rsPath, null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
    rsPath = ZKUtil.joinZNode(zkw.rsZNode, RS2.getServerName());
    zkw.getRecoverableZooKeeper().create(rsPath, null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

    for (int i = 0; i < maxTasks; i++) {
      zkw.getRecoverableZooKeeper().create(ZKSplitLog.getEncodedNodeName(zkw, TATAS + i),
        TaskState.TASK_UNASSIGNED.get("mgr,1,1"),
        Ids.OPEN_ACL_UNSAFE,
        CreateMode.PERSISTENT);
    }

    SplitLogWorker slw = new SplitLogWorker(zkw, testConf, mockedRS, neverEndingTask);
    slw.start();
    try {
      int acquiredTasks = 0;
      waitForCounter(tot_wkr_task_acquired, 0, 2, 3000);
      for (int i = 0; i < maxTasks; i++) {
        byte[] bytes = ZKUtil.getData(zkw, ZKSplitLog.getEncodedNodeName(zkw, TATAS + i));
        if (TaskState.TASK_OWNED.equals(bytes, RS.getServerName())) {
          acquiredTasks++;
        }
      }
      assertEquals(2, acquiredTasks);
    } finally {
      stopSplitLogWorker(slw);
    }
  }

  /**
   * Create a mocked region server service instance
   * @param server
   * @return
   */
  private RegionServerServices getRegionServer(ServerName name) {
    RegionServerServices mockedServer = mock(RegionServerServices.class);
    when(mockedServer.getConfiguration()).thenReturn(TEST_UTIL.getConfiguration());
    when(mockedServer.getServerName()).thenReturn(name);
    when(mockedServer.getZooKeeper()).thenReturn(zkw);
    when(mockedServer.isStopped()).thenReturn(false);
    when(mockedServer.getExecutorService()).thenReturn(executorService);

    return mockedServer;
  }

  @org.junit.Rule
  public org.apache.hadoop.hbase.ResourceCheckerJUnitRule cu =
    new org.apache.hadoop.hbase.ResourceCheckerJUnitRule();
}

