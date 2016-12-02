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
package org.apache.hadoop.hbase.master;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.Bytes;

public class TestBalanceThrottlingBase {
  protected static final byte[] FAMILYNAME = Bytes.toBytes("fam");

  void createTable(HBaseTestingUtility testUtil, byte[] table) throws IOException {
    byte[] startKey = new byte[] { 0x00 };
    byte[] stopKey = new byte[] { 0x7f };
    testUtil.createTable(table, new byte[][] { FAMILYNAME }, 1, startKey, stopKey, 100);
  }

  Thread startBalancerChecker(final HMaster master, final AtomicInteger maxCount,
      final AtomicBoolean stop) {
    Runnable checker = new Runnable() {
      @Override
      public void run() {
        while (!stop.get()) {
          maxCount.set(Math.max(maxCount.get(), master.getAssignmentManager()
              .getRegionsInTransitionCount()));
          try {
            Thread.sleep(10);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
      }
    };
    Thread thread = new Thread(checker);
    thread.start();
    return thread;
  }

  void unbalance(HBaseTestingUtility testUtil, HMaster master, byte[] tableName) throws Exception {
    while (master.getAssignmentManager().getRegionsInTransitionCount() > 0) {
      Thread.sleep(100);
    }
    HRegionServer biasedServer = testUtil.getMiniHBaseCluster().getRegionServer(0);
    for (HRegionInfo regionInfo : testUtil.getHBaseAdmin().getTableRegions(tableName)) {
      master.move(regionInfo.getEncodedNameAsBytes(),
        Bytes.toBytes(biasedServer.getServerName().getServerName()));
    }
    while (master.getAssignmentManager().getRegionsInTransitionCount() > 0) {
      Thread.sleep(100);
    }
  }
}
