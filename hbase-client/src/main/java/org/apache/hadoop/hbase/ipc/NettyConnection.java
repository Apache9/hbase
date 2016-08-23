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

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;
import io.netty.util.concurrent.Promise;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.ipc.BufferCallBeforeInitHandler.BufferCallEvent;
import org.apache.hadoop.hbase.security.NettyHBaseSaslRpcClientHandler;
import org.apache.hadoop.hbase.security.SaslChallengeDecoder;
import org.apache.hadoop.hbase.security.SaslUtil.QualityOfProtection;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * A netty based rpc connection.
 */
@InterfaceAudience.Private
public class NettyConnection extends Connection {

  private static final Log LOG = LogFactory.getLog(NettyConnection.class);

  private static final ExecutorService RELOGIN_EXECUTOR = Executors
      .newSingleThreadExecutor(Threads.newDaemonThreadFactory("Relogin"));

  private final NettyRpcClient rpcClient;

  private Channel channel;

  public NettyConnection(NettyRpcClient rpcClient, ConnectionId remoteId) throws IOException {
    super(rpcClient.conf, rpcClient.timeoutTimer, remoteId, rpcClient.clusterId,
        rpcClient.userProvider.isHBaseSecurityEnabled(), rpcClient.pingInterval, rpcClient.codec,
        rpcClient.compressor);
    this.rpcClient = rpcClient;
  }

  private void established(Channel ch) {
    int headerSize = header.getSerializedSize();
    ByteBuf buf = ch.alloc().buffer(4 + headerSize);
    buf.writeInt(headerSize);
    try {
      header.writeTo(new ByteBufOutputStream(buf));
    } catch (IOException e) {
      // should not happen
      throw new RuntimeException(e);
    }
    ch.write(buf);
    ChannelPipeline p = ch.pipeline();
    String addBeforeHandler = p.context(BufferCallBeforeInitHandler.class).name();
    p.addBefore(addBeforeHandler, null,
      new IdleStateHandler(pingInterval, rpcClient.maxIdleTime, 0, TimeUnit.MILLISECONDS));
    p.addBefore(addBeforeHandler, null, new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4));
    p.addBefore(addBeforeHandler, null,
      new NettyRpcDuplexHandler(this, rpcClient.ipcUtil, codec, compressor));
    p.fireUserEventTriggered(BufferCallEvent.success());
  }

  private void relogin(Throwable e) throws IOException {
    if (shouldRelogin(e)) {
      relogin();
    }
  }

  private void failInit(Channel ch, IOException e) {
    synchronized (this) {
      // fail all pending calls
      ch.pipeline().fireUserEventTriggered(BufferCallEvent.fail(e));
      shutdown0();
      return;
    }
  }

  private void saslNegotiate(final Channel ch) {
    UserGroupInformation ticket = getUGI();
    if (ticket == null) {
      failInit(ch, new FatalConnectionException("ticket/user is null"));
      return;
    }
    Promise<Boolean> saslPromise = ch.eventLoop().newPromise();
    ChannelHandler saslHandler;
    try {
      saslHandler = new NettyHBaseSaslRpcClientHandler(saslPromise, ticket, authMethod, token,
          serverPrincipal, rpcClient.fallbackAllowed, this.rpcClient.conf.get(
            "hbase.rpc.protection", QualityOfProtection.AUTHENTICATION.name().toLowerCase()));
    } catch (IOException e) {
      failInit(ch, e);
      return;
    }
    ch.pipeline().addFirst(new SaslChallengeDecoder(), saslHandler);
    saslPromise.addListener(new FutureListener<Boolean>() {

      @Override
      public void operationComplete(Future<Boolean> future) throws Exception {
        if (future.isSuccess()) {
          ChannelPipeline p = ch.pipeline();
          p.remove(SaslChallengeDecoder.class);
          p.remove(NettyHBaseSaslRpcClientHandler.class);
          established(ch);
        } else {
          final Throwable error = future.cause();
          RELOGIN_EXECUTOR.execute(new Runnable() {
            public void run() {
              try {
                relogin(error);
              } catch (IOException e) {
                LOG.warn("relogin failed", e);
              }
            }
          });
          failInit(ch, IPCUtil.toIOE(future.cause()));
        }
      }
    });
  }

  private void connect() {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Connecting to " + remoteId.address);
    }

    this.channel = new Bootstrap().group(rpcClient.group).channel(rpcClient.channelClass)
        .option(ChannelOption.TCP_NODELAY, rpcClient.tcpNoDelay)
        .option(ChannelOption.SO_KEEPALIVE, rpcClient.tcpKeepAlive)
        .option(ChannelOption.CONNECT_TIMEOUT_MILLIS,
          AbstractRpcClient.getSocketTimeout(rpcClient.conf))
        .handler(new BufferCallBeforeInitHandler()).localAddress(rpcClient.localAddr)
        .remoteAddress(remoteId.address).connect().addListener(new ChannelFutureListener() {

          @Override
          public void operationComplete(ChannelFuture future) throws Exception {
            Channel ch = future.channel();
            if (!future.isSuccess()) {
              failInit(ch, IPCUtil.toIOE(future.cause()));
              rpcClient.failedServers.addToFailedServers(remoteId.address, future.cause());
              return;
            }
            ch.write(Unpooled.wrappedBuffer(getConnectionHeaderPreamble()));
            if (useSasl) {
              saslNegotiate(ch);
            } else {
              established(ch);
            }
          }
        }).channel();
  }

  private void shutdown0() {
    if (channel != null) {
      channel.close();
      channel = null;
    }
  }

  /**
   * @return whether we are still connected.
   */
  synchronized boolean isActive() {
    return channel != null;
  }

  @Override
  public synchronized void shutdown() {
    shutdown0();
  }

  @Override
  public synchronized void sendRequest(final Call call) throws IOException {
    if (channel == null) {
      connect();
    }
    if (call.timeout > 0) {
      call.timeoutTask = timeoutTimer.newTimeout(new TimerTask() {

        @Override
        public void run(Timeout timeout) throws Exception {
          call.setTimeout(new IOException(
              "Timed out waiting for response, timeout = " + call.timeout + "ms, waitTime = "
                  + (EnvironmentEdgeManager.currentTimeMillis() - call.startTime) + "ms"));

        }
      }, call.timeout, TimeUnit.MILLISECONDS);
    }
    channel.writeAndFlush(call);
  }

  @Override
  protected void callTimeout(Call call) {
    synchronized (NettyConnection.this) {
      if (channel != null) {
        channel.pipeline().fireUserEventTriggered(new CallTimeoutEvent(call.id));
      }
    }
  }
}
