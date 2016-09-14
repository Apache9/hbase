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
package org.apache.hadoop.hbase.ipc;

import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Threads;

/**
 * Netty based rpc client.
 */
@InterfaceAudience.Private
public class NettyRpcClient extends AbstractRpcClient<NettyConnection> {

  private static final Log LOG = LogFactory.getLog(NettyRpcClient.class);

  private static final ScheduledExecutorService EXECUTOR = Executors.newScheduledThreadPool(1,
    Threads.newDaemonThreadFactory("Idle-Rpc-Conn-Sweeper"));

  final EventLoopGroup group;

  final Class<? extends Channel> channelClass;

  private final boolean shutdownGroupWhenClose;

  private final ScheduledFuture<?> cleanupIdleConnectionTask;

  public NettyRpcClient(Configuration conf, String clusterId) {
    this(conf, clusterId, null);
  }

  public NettyRpcClient(Configuration conf, String clusterId, SocketAddress localAddr) {
    super(conf, clusterId, localAddr);
    Pair<EventLoopGroup, Class<? extends Channel>> groupAndChannelClass = NettyRpcClientConfigHelper
        .getEventLoopConfig(conf);
    if (groupAndChannelClass == null) {
      // Use our own EventLoopGroup.
      this.group = new NioEventLoopGroup(0,
          new DefaultThreadFactory("IPC-NioEventLoopGroup", true, Thread.MAX_PRIORITY));
      this.channelClass = NioSocketChannel.class;
      this.shutdownGroupWhenClose = true;
    } else {
      this.group = groupAndChannelClass.getFirst();
      this.channelClass = groupAndChannelClass.getSecond();
      this.shutdownGroupWhenClose = false;
    }
    cleanupIdleConnectionTask = EXECUTOR.scheduleAtFixedRate(new Runnable() {

      @Override
      public void run() {
        cleanupIdleConnections();
      }
    }, maxIdleTime, maxIdleTime, TimeUnit.MILLISECONDS);
  }

  private void cleanupIdleConnections() {
    long closeBeforeTime = EnvironmentEdgeManager.currentTimeMillis() - maxIdleTime;
    synchronized (connections) {
      for (NettyConnection conn : connections.values()) {
        // remove connection if it has not been chosen by anyone for more than maxIdleTime, and the
        // connection itself has already shutdown. The latter check is because that we may still
        // have some pending calls on connection so we should not shutdown the connection outside.
        // The connection itself will disconnect if there is no pending call for maxIdleTime.
        if (conn.getLastTouched() < closeBeforeTime && !conn.isActive()) {
          LOG.info("Cleanup idle connection to " + conn.remoteId().address);
          connections.removeValue(conn.remoteId(), conn);
        }
      }
    }
  }

  @Override
  protected NettyConnection createConnection(ConnectionId remoteId) throws IOException {
    return new NettyConnection(this, remoteId);
  }

  @Override
  protected void closeInternal() {
    cleanupIdleConnectionTask.cancel(false);
    if (shutdownGroupWhenClose) {
      group.shutdownGracefully();
    }
  }
}
