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
package org.apache.hadoop.hbase.io.asyncfs;

import static org.junit.Assert.assertThrows;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.asyncfs.monitor.StreamSlowMonitor;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import org.apache.hbase.thirdparty.io.netty.buffer.ByteBuf;
import org.apache.hbase.thirdparty.io.netty.channel.Channel;
import org.apache.hbase.thirdparty.io.netty.channel.ChannelHandlerContext;
import org.apache.hbase.thirdparty.io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.hbase.thirdparty.io.netty.channel.EventLoop;
import org.apache.hbase.thirdparty.io.netty.channel.EventLoopGroup;
import org.apache.hbase.thirdparty.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.hbase.thirdparty.io.netty.channel.socket.nio.NioSocketChannel;

/**
 * Testcase for HBASE-26679
 */
@Category({ MiscTests.class, SmallTests.class })
public class TestFanOutOneBlockAsyncDFSOutputHang extends AsyncFSTestBase {

  private static DistributedFileSystem FS;

  private static EventLoopGroup EVENT_LOOP_GROUP;

  private static Class<? extends Channel> CHANNEL_CLASS;

  private static StreamSlowMonitor MONITOR;

  private static FanOutOneBlockAsyncDFSOutput OUT;

  @Rule
  public TestName name = new TestName();

  @BeforeClass
  public static void setUp() throws Exception {
    startMiniDFSCluster(2);
    FS = CLUSTER.getFileSystem();
    EVENT_LOOP_GROUP = new NioEventLoopGroup();
    CHANNEL_CLASS = NioSocketChannel.class;
    MONITOR = StreamSlowMonitor.create(UTIL.getConfiguration(), "testMonitor");
    Path f = new Path("/testHang");
    EventLoop eventLoop = EVENT_LOOP_GROUP.next();
    OUT = FanOutOneBlockAsyncDFSOutputHelper.createOutput(FS, f, true, false, (short) 2,
      FS.getDefaultBlockSize(), eventLoop, CHANNEL_CLASS, MONITOR);
  }

  @AfterClass
  public static void tearDown() throws IOException, InterruptedException {
    if (OUT != null) {
      OUT.recoverAndClose(null);
    }
    if (EVENT_LOOP_GROUP != null) {
      EVENT_LOOP_GROUP.shutdownGracefully().sync();
    }
    shutdownMiniDFSCluster();
  }

  @Test
  public void testHang() throws Exception {
    List<Channel> dns = new ArrayList<>(OUT.getDatanodeInfoMap().keySet());
    // simulate slow response
    dns.get(0).pipeline().addFirst(new ChannelInboundHandlerAdapter() {

      @Override
      public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (!(msg instanceof ByteBuf)) {
          ctx.fireChannelRead(msg);
        }
      }
    });
    OUT.write(new byte[1]);
    CompletableFuture<Long> future = OUT.flush(false);
    // make sure dn1 has returned
    Thread.sleep(2000);
    // close dn1, to simulate a failure of dn
    dns.get(1).close();
    // make sure the failure callback has been triggered
    Thread.sleep(2000);
    // we should fail soon with ExecutionException, other than TimeoutException
    assertThrows(ExecutionException.class, () -> future.get(5, TimeUnit.SECONDS));
  }
}
