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

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.RpcCallback;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;
import io.netty.util.concurrent.Promise;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.MetricsConnection;
import org.apache.hadoop.hbase.client.MetricsConnection.CallStats;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.JVM;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.PoolMap;
import org.apache.hadoop.hbase.util.Threads;

/**
 * Netty client for the requests and responses
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class AsyncRpcClient extends AbstractRpcClient {

  private static final Log LOG = LogFactory.getLog(AsyncRpcClient.class);

  public static final String CLIENT_MAX_THREADS = "hbase.rpc.client.threads.max";
  public static final String USE_NATIVE_TRANSPORT = "hbase.rpc.client.nativetransport";
  public static final String USE_GLOBAL_EVENT_LOOP_GROUP = "hbase.rpc.client.globaleventloopgroup";

  private static final ChannelInitializer<SocketChannel> DEFAULT_CHANNEL_INITIALIZER =
      new ChannelInitializer<SocketChannel>() {
    @Override
    protected void initChannel(SocketChannel ch) throws Exception {
      //empty initializer
    }
  };

  protected final AtomicInteger callIdCnt = new AtomicInteger();

  private final PoolMap<Integer, AsyncRpcChannel> connections;

  final FailedServers failedServers;

  @VisibleForTesting
  final Bootstrap bootstrap;

  private final boolean useGlobalEventLoopGroup;

  @VisibleForTesting
  static Pair<EventLoopGroup, Class<? extends Channel>> GLOBAL_EVENT_LOOP_GROUP;

  synchronized static Pair<EventLoopGroup, Class<? extends Channel>>
      getGlobalEventLoopGroup(Configuration conf) {
    if (GLOBAL_EVENT_LOOP_GROUP == null) {
      GLOBAL_EVENT_LOOP_GROUP = createEventLoopGroup(conf);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Create global event loop group "
            + GLOBAL_EVENT_LOOP_GROUP.getFirst().getClass().getSimpleName());
      }
    }
    return GLOBAL_EVENT_LOOP_GROUP;
  }

  private static Pair<EventLoopGroup, Class<? extends Channel>> createEventLoopGroup(
      Configuration conf) {
    // Max amount of threads to use. 0 lets Netty decide based on amount of cores
    int maxThreads = conf.getInt(CLIENT_MAX_THREADS, 0);

    // Config to enable native transport. Does not seem to be stable at time of implementation
    // although it is not extensively tested.
    boolean epollEnabled = conf.getBoolean(USE_NATIVE_TRANSPORT, false);

    // Use the faster native epoll transport mechanism on linux if enabled
    if (epollEnabled && JVM.isLinux() && JVM.isAmd64()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Create EpollEventLoopGroup with maxThreads = " + maxThreads);
      }
      return new Pair<EventLoopGroup, Class<? extends Channel>>(new EpollEventLoopGroup(maxThreads,
          Threads.newDaemonThreadFactory("AsyncRpcChannel")), EpollSocketChannel.class);
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Create NioEventLoopGroup with maxThreads = " + maxThreads);
      }
      return new Pair<EventLoopGroup, Class<? extends Channel>>(new NioEventLoopGroup(maxThreads,
          Threads.newDaemonThreadFactory("AsyncRpcChannel")), NioSocketChannel.class);
    }
  }

  /**
   * Constructor for tests
   *
   * @param configuration      to HBase
   * @param clusterId          for the cluster
   * @param localAddress       local address to connect to
   * @param metrics            the connection metrics
   * @param channelInitializer for custom channel handlers
   */
  protected AsyncRpcClient(Configuration configuration, String clusterId,
      SocketAddress localAddress, MetricsConnection metrics,
      ChannelInitializer<SocketChannel> channelInitializer) {
    super(configuration, clusterId, localAddress, metrics);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Starting async Hbase RPC client");
    }

    Pair<EventLoopGroup, Class<? extends Channel>> eventLoopGroupAndChannelClass;
    this.useGlobalEventLoopGroup = conf.getBoolean(USE_GLOBAL_EVENT_LOOP_GROUP, true);
    if (useGlobalEventLoopGroup) {
      eventLoopGroupAndChannelClass = getGlobalEventLoopGroup(configuration);
    } else {
      eventLoopGroupAndChannelClass = createEventLoopGroup(configuration);
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Use " + (useGlobalEventLoopGroup ? "global" : "individual") + " event loop group "
          + eventLoopGroupAndChannelClass.getFirst().getClass().getSimpleName());
    }

    this.connections = new PoolMap<>(getPoolType(configuration), getPoolSize(configuration));
    this.failedServers = new FailedServers(configuration);

    int operationTimeout = configuration.getInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT,
        HConstants.DEFAULT_HBASE_CLIENT_OPERATION_TIMEOUT);

    // Configure the default bootstrap.
    this.bootstrap = new Bootstrap();
    bootstrap.group(eventLoopGroupAndChannelClass.getFirst())
        .channel(eventLoopGroupAndChannelClass.getSecond())
        .option(ChannelOption.TCP_NODELAY, tcpNoDelay)
        .option(ChannelOption.SO_KEEPALIVE, tcpKeepAlive)
        .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, operationTimeout);
    if (channelInitializer == null) {
      channelInitializer = DEFAULT_CHANNEL_INITIALIZER;
    }
    bootstrap.handler(channelInitializer);
    if (localAddress != null) {
      bootstrap.localAddress(localAddress);
    }
  }

  /** Used in test only. */
  AsyncRpcClient(Configuration configuration) {
    this(configuration, HConstants.CLUSTER_ID_DEFAULT, null, null);
  }

  /** Used in test only. */
  AsyncRpcClient(Configuration configuration,
      ChannelInitializer<SocketChannel> channelInitializer) {
    this(configuration, HConstants.CLUSTER_ID_DEFAULT, null, null, channelInitializer);
  }

  /**
   * Constructor
   *
   * @param configuration to HBase
   * @param clusterId     for the cluster
   * @param localAddress  local address to connect to
   * @param metrics       the connection metrics
   */
  public AsyncRpcClient(Configuration configuration, String clusterId, SocketAddress localAddress,
      MetricsConnection metrics) {
    this(configuration, clusterId, localAddress, metrics, null);
  }

  @Override
  protected void call(final PayloadCarryingRpcController pcrc, MethodDescriptor md, Message param,
      Message returnType, User ticket, InetSocketAddress addr, final RpcCallback<Message> callback,
      CallStats callStats) {
    try {
      final AsyncRpcChannel connection = createRpcChannel(md.getService().getName(), addr, ticket);

      final Promise<Message> promise = connection.callMethod(md, param, pcrc.cellScanner(),
        returnType, getMessageConverterWithRpcController(pcrc), null, pcrc.getCallTimeout(),
        pcrc.getPriority());

      pcrc.notifyOnCancel(new RpcCallback<Object>() {
        @Override
        public void run(Object parameter) {
          // Will automatically fail the promise with CancellationException
          promise.cancel(true);
        }
      });
      promise.addListener(new FutureListener<Message>() {

        @Override
        public void operationComplete(Future<Message> future) throws Exception {
          if (future.isSuccess()) {
            callback.run(future.getNow());
          } else {
            Throwable error = future.cause();
            if (error instanceof IOException) {
              pcrc.setFailed((IOException) error);
            } else {
              pcrc.setFailed(new IOException(error));
            }
            callback.run(null);
          }
        }
      });
    } catch (IOException e) {
      pcrc.setFailed(e);
      callback.run(null);
    }
  }

  private MessageConverter<Message, Message> getMessageConverterWithRpcController(
      final PayloadCarryingRpcController pcrc) {
    return new
      MessageConverter<Message, Message>() {
        @Override
        public Message convert(Message msg, CellScanner cellScanner) {
          pcrc.setCellScanner(cellScanner);
          return msg;
        }
      };
  }

  private boolean closed = false;

  /**
   * Close netty
   */
  public void close() {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Stopping async HBase RPC client");
    }

    synchronized (connections) {
      if (closed) {
        return;
      }
      closed = true;
      for (AsyncRpcChannel conn : connections.values()) {
        conn.close(null);
      }
    }
    // do not close global EventLoopGroup.
    if (!useGlobalEventLoopGroup) {
      bootstrap.config().group().shutdownGracefully();
    }
  }

  /**
   * Create a cell scanner
   *
   * @param cellBlock to create scanner for
   * @return CellScanner
   * @throws java.io.IOException on error on creation cell scanner
   */
  public CellScanner createCellScanner(byte[] cellBlock) throws IOException {
    return ipcUtil.createCellScanner(this.codec, this.compressor, cellBlock);
  }

  /**
   * Build cell block
   *
   * @param cells to create block with
   * @return ByteBuffer with cells
   * @throws java.io.IOException if block creation fails
   */
  public ByteBuffer buildCellBlock(CellScanner cells) throws IOException {
    return ipcUtil.buildCellBlock(this.codec, this.compressor, cells);
  }

  /**
   * Creates an RPC client
   *
   * @param serviceName    name of service
   * @param location       to connect to
   * @param ticket         for current user
   * @return new RpcChannel
   * @throws StoppedRpcClientException when Rpc client is stopped
   * @throws FailedServerException if server failed
   */
  private AsyncRpcChannel createRpcChannel(String serviceName, InetSocketAddress location,
      User ticket) throws StoppedRpcClientException, FailedServerException {
    // Check if server is failed
    if (this.failedServers.isFailedServer(location)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Not trying to connect to " + location +
            " this server is in the failed servers list");
      }
      throw new FailedServerException(
          "This server is in the failed servers list: " + location);
    }

    int hashCode = ConnectionId.hashCode(ticket,serviceName,location);

    AsyncRpcChannel rpcChannel;
    synchronized (connections) {
      if (closed) {
        throw new StoppedRpcClientException();
      }
      rpcChannel = connections.get(hashCode);
      if (rpcChannel != null && !rpcChannel.isAlive()) {
        LOG.debug("Removing dead channel from server="+rpcChannel.getAddress().toString());
        connections.remove(hashCode);
      }
      if (rpcChannel == null) {
        rpcChannel = new AsyncRpcChannel(this.bootstrap, this, ticket, serviceName, location);
        connections.put(hashCode, rpcChannel);
      }
    }

    return rpcChannel;
  }

  /**
   * Interrupt the connections to the given ip:port server. This should be called if the server
   * is known as actually dead. This will not prevent current operation to be retried, and,
   * depending on their own behavior, they may retry on the same server. This can be a feature,
   * for example at startup. In any case, they're likely to get connection refused (if the
   * process died) or no route to host: i.e. there next retries should be faster and with a
   * safe exception.
   *
   * @param sn server to cancel connections for
   */
  @Override
  public void cancelConnections(ServerName sn) {
    synchronized (connections) {
      for (AsyncRpcChannel rpcChannel : connections.values()) {
        if (rpcChannel.isAlive() &&
            rpcChannel.getAddress().getPort() == sn.getPort() &&
            rpcChannel.getAddress().getHostName().contentEquals(sn.getHostname())) {
          LOG.info("The server on " + sn.toString() +
              " is dead - stopping the connection " + rpcChannel.toString());
          rpcChannel.close(null);
        }
      }
    }
  }

  /**
   * Remove connection from pool
   * @param connection to remove
   */
  public void removeConnection(AsyncRpcChannel connection) {
    int connectionHashCode = connection.hashCode();
    synchronized (connections) {
      // we use address as cache key, so we should check here to prevent removing the
      // wrong connection
      AsyncRpcChannel connectionInPool = this.connections.get(connectionHashCode);
      if (connectionInPool != null && connectionInPool.equals(connection)) {
        this.connections.remove(connectionHashCode);
      } else if (LOG.isDebugEnabled()) {
        LOG.debug(String.format("%s already removed, expected instance %08x, actual %08x",
          connection.toString(), System.identityHashCode(connection),
          System.identityHashCode(connectionInPool)));
      }
    }
  }

  /**
   * Get a new timeout on this RPC client
   * @param task to run at timeout
   * @param delay for the timeout
   * @param unit time unit for the timeout
   * @return Timeout
   */
  Timeout newTimeout(TimerTask task, long delay, TimeUnit unit) {
    return WHEEL_TIMER.newTimeout(task, delay, unit);
  }
}
