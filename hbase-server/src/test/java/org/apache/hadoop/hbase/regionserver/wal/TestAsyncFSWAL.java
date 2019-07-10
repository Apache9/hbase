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
package org.apache.hadoop.hbase.regionserver.wal;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.wal.WALProvider.AsyncWriter;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hbase.thirdparty.io.netty.channel.Channel;
import org.apache.hbase.thirdparty.io.netty.channel.EventLoopGroup;
import org.apache.hbase.thirdparty.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.hbase.thirdparty.io.netty.channel.socket.nio.NioSocketChannel;

/**
 * Provides AsyncFSWAL test cases.
 */
@Category({ RegionServerTests.class, MediumTests.class })
public class TestAsyncFSWAL extends AbstractTestFSWAL {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestAsyncFSWAL.class);

  private static EventLoopGroup GROUP;

  private static Class<? extends Channel> CHANNEL_CLASS;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    GROUP = new NioEventLoopGroup(1, Threads.newDaemonThreadFactory("TestAsyncFSWAL"));
    CHANNEL_CLASS = NioSocketChannel.class;
    AbstractTestFSWAL.setUpBeforeClass();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    AbstractTestFSWAL.tearDownAfterClass();
    GROUP.shutdownGracefully();
  }

  @Override
  protected AbstractFSWAL<?> newWAL(FileSystem fs, Path rootDir, String logDir, String archiveDir,
      Configuration conf, List<WALActionsListener> listeners, boolean failIfWALExists,
      String prefix, String suffix) throws IOException {
    AsyncFSWAL wal = new AsyncFSWAL(fs, rootDir, logDir, archiveDir, conf, listeners,
        failIfWALExists, prefix, suffix, GROUP, CHANNEL_CLASS);
    wal.init();
    return wal;
  }

  @Override
  protected AbstractFSWAL<?> newSlowWAL(FileSystem fs, Path rootDir, String logDir,
      String archiveDir, Configuration conf, List<WALActionsListener> listeners,
      boolean failIfWALExists, String prefix, String suffix, final Runnable action)
      throws IOException {
    AsyncFSWAL wal = new AsyncFSWAL(fs, rootDir, logDir, archiveDir, conf, listeners,
        failIfWALExists, prefix, suffix, GROUP, CHANNEL_CLASS) {

      @Override
      void atHeadOfRingBufferEventHandlerAppend() {
        action.run();
        super.atHeadOfRingBufferEventHandlerAppend();
      }
    };
    wal.init();
    return wal;
  }

  private static volatile CountDownLatch ARRIVE_EXECUTE = null;

  private static volatile CountDownLatch ARRIVE_SYNC = null;

  private static volatile CountDownLatch RESUME_SYNC = null;

  /**
   * Testcase for HBASE-22665
   */
  public void testRollWriterHang() throws IOException {
    Configuration conf = new Configuration(CONF);
    conf.setBoolean(AsyncFSWAL.ASYNC_WAL_USE_SHARED_EVENT_LOOP, false);
    String testName = currentTest.getMethodName();
    try (AsyncFSWAL wal = new AsyncFSWAL(FS, CommonFSUtils.getWALRootDir(CONF), DIR.toString(),
        testName, conf, null, true, null, null, GROUP, CHANNEL_CLASS) {

      @Override
      protected ThreadPoolExecutor createConsumeExecutor() {
        return new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<Runnable>(),
            new ThreadFactoryBuilder().setNameFormat("AsyncFSWAL-%d").setDaemon(true).build()) {

          private Runnable delayed;

          @Override
          public void execute(Runnable command) {
            if (ARRIVE_EXECUTE != null) {
              delayed = command;
              ARRIVE_EXECUTE.countDown();
              ARRIVE_EXECUTE = null;
              return;
            }
            super.execute(command);
            if (delayed != null) {
              super.execute(delayed);
              delayed = null;
            }
          }
        };
      }

      @Override
      protected AsyncWriter createWriterInstance(Path path) throws IOException {
        AsyncWriter writer = super.createWriterInstance(path);
        return new AsyncWriter() {

          @Override
          public void close() throws IOException {
            writer.close();
          }

          @Override
          public long getLength() {
            return writer.getLength();
          }

          @Override
          public CompletableFuture<Long> sync() {
            if (ARRIVE_SYNC != null) {
              CompletableFuture<Long> future = new CompletableFuture<>();
              new Thread() {

                @Override
                public void run() {
                  ARRIVE_SYNC.countDown();
                  ARRIVE_SYNC = null;
                  try {
                    RESUME_SYNC.await();
                  } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                  }
                }
                
              }.start();
              return future;
            } else {
              return writer.sync();
            }
          }

          @Override
          public void append(Entry entry) {
            writer.append(entry);
          }
        };
      }
    }) {
      wal.init();
    }
  }
}
