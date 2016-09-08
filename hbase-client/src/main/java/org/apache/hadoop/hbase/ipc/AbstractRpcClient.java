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
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.protobuf.BlockingRpcChannel;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcChannel;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

import io.netty.util.HashedWheelTimer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.codec.Codec;
import org.apache.hadoop.hbase.codec.KeyValueCodec;
import org.apache.hadoop.hbase.ipc.FailedServers.FailedServer;
import org.apache.hadoop.hbase.protobuf.generated.AuthenticationProtos.TokenIdentifier.Kind;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.security.token.AuthenticationTokenSelector;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.PoolMap;
import org.apache.hadoop.hbase.util.PoolMap.PoolType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.token.TokenSelector;

/**
 * Base class of all RpcClient implementations.
 */
@InterfaceAudience.Private
public abstract class AbstractRpcClient<T extends Connection> implements RpcClient {

  private static final Log LOG = LogFactory.getLog(AbstractRpcClient.class);

  protected final Configuration conf;
  protected final String clusterId;
  protected final SocketAddress localAddr;

  protected final UserProvider userProvider;
  protected final IPCUtil ipcUtil;

  // if the connection is idle for more than this time (in ms), it will be closed at any moment.
  protected final int maxIdleTime;
  protected final int maxRetries; // the max. no. of retries for socket connections
  protected final long failureSleep; // Time to sleep before retry on failure.
  protected final boolean tcpNoDelay; // if T then disable Nagle's Algorithm
  protected final boolean tcpKeepAlive; // if T then use keepalives
  protected final int pingInterval; // how often sends ping to the server in msecs
  protected final FailedServers failedServers;
  protected final Codec codec;
  protected final CompressionCodec compressor;
  protected final boolean fallbackAllowed;

  protected final int clientWarnIpcResponseTime;

  protected int maxConcurrentCallsPerServer;

  protected static final LoadingCache<InetSocketAddress, AtomicInteger> concurrentCounterCache =
      CacheBuilder.newBuilder().expireAfterAccess(1, TimeUnit.HOURS).
          build(new CacheLoader<InetSocketAddress, AtomicInteger>() {
            @Override public AtomicInteger load(InetSocketAddress key) throws Exception {
              return new AtomicInteger(0);
            }
          });

  protected final PoolMap<ConnectionId, T> connections;

  protected final AtomicInteger callIdCnt = new AtomicInteger(0);

  protected final HashedWheelTimer timeoutTimer = new HashedWheelTimer(10, TimeUnit.MILLISECONDS);

  /**
   * Construct an IPC client for the cluster <code>clusterId</code>
   * @param conf configuration
   * @param clusterId the cluster id
   * @param localAddr client socket bind address.
   */
  public AbstractRpcClient(Configuration conf, String clusterId, SocketAddress localAddr) {
    this.userProvider = UserProvider.instantiate(conf);
    this.localAddr = localAddr;
    this.tcpKeepAlive = conf.getBoolean("hbase.ipc.client.tcpkeepalive", true);
    this.clusterId = clusterId != null ? clusterId : HConstants.CLUSTER_ID_DEFAULT;
    this.failureSleep = conf.getLong(HConstants.HBASE_CLIENT_PAUSE,
      HConstants.DEFAULT_HBASE_CLIENT_PAUSE);
    this.maxRetries = conf.getInt("hbase.ipc.client.connect.max.retries", 0);
    this.tcpNoDelay = conf.getBoolean("hbase.ipc.client.tcpnodelay", true);
    this.pingInterval = getPingInterval(conf);
    this.failedServers = new FailedServers(conf);
    this.ipcUtil = new IPCUtil(conf);

    this.maxIdleTime = conf.getInt("hbase.ipc.client.connection.maxidletime", 10000); // 10s
    this.conf = conf;
    this.codec = getCodec();
    this.compressor = getCompressor(conf);
    this.fallbackAllowed = conf.getBoolean(IPC_CLIENT_FALLBACK_TO_SIMPLE_AUTH_ALLOWED_KEY,
      IPC_CLIENT_FALLBACK_TO_SIMPLE_AUTH_ALLOWED_DEFAULT);
    this.clientWarnIpcResponseTime = conf.getInt(CLIENT_WARN_IPC_RESPONSE_TIME,
      DEFAULT_CLIENT_WARN_IPC_RESPONSE_TIME);
    this.connections = new PoolMap<ConnectionId, T>(getPoolType(conf), getPoolSize(conf));
    // login the server principal (if using secure Hadoop)
    if (LOG.isDebugEnabled()) {
      LOG.debug("Codec=" + this.codec + ", compressor=" + this.compressor + ", tcpKeepAlive="
          + this.tcpKeepAlive + ", tcpNoDelay=" + this.tcpNoDelay + ", maxIdleTime="
          + this.maxIdleTime + ", maxRetries=" + this.maxRetries + ", fallbackAllowed="
          + this.fallbackAllowed + ", clientWarnIpcResponseTime=" + this.clientWarnIpcResponseTime
          + ", ping interval=" + this.pingInterval + "ms" + ", bind address="
          + (this.localAddr != null ? this.localAddr : "null"));
    }
    this.maxConcurrentCallsPerServer = conf.getInt(
        HConstants.HBASE_CLIENT_PERSERVER_REQUESTS_THRESHOLD,
        HConstants.DEFAULT_HBASE_CLIENT_PERSERVER_REQUESTS_THRESHOLD);
  }

  /**
   * Return the pool type specified in the configuration, which must be set to either
   * {@link PoolType#RoundRobin} or {@link PoolType#ThreadLocal}, otherwise default to the former.
   * For applications with many user threads, use a small round-robin pool. For applications with
   * few user threads, you may want to try using a thread-local pool. In any case, the number of
   * {@link RpcClientImpl} instances should not exceed the operating system's hard limit on the
   * number of connections.
   * @param config configuration
   * @return either a {@link PoolType#RoundRobin} or {@link PoolType#ThreadLocal}
   */
  private static PoolType getPoolType(Configuration config) {
    return PoolType.valueOf(config.get(HConstants.HBASE_CLIENT_IPC_POOL_TYPE), PoolType.RoundRobin,
      PoolType.ThreadLocal);
  }

  /**
   * Return the pool size specified in the configuration, which is applicable only if the pool type
   * is {@link PoolType#RoundRobin}.
   * @param config
   * @return the maximum pool size
   */
  private static int getPoolSize(Configuration config) {
    return config.getInt(HConstants.HBASE_CLIENT_IPC_POOL_SIZE, 1);
  }

  /**
   * Encapsulate the ugly casting and RuntimeException conversion in private method.
   * <p>
   * Protected as we want to override this method in UT.
   * @return Codec to use on this client.
   */
  protected Codec getCodec() {
    // For NO CODEC, "hbase.client.rpc.codec" must be configured with empty string AND
    // "hbase.client.default.rpc.codec" also -- because default is to do cell block encoding.
    String className = conf.get(HConstants.RPC_CODEC_CONF_KEY, getDefaultCodec(this.conf));
    if (className == null || className.length() == 0) return null;
    try {
      return Class.forName(className).asSubclass(Codec.class).newInstance();
    } catch (Exception e) {
      throw new RuntimeException("Failed getting codec " + className, e);
    }
  }

  @VisibleForTesting
  public static String getDefaultCodec(final Configuration c) {
    // If "hbase.client.default.rpc.codec" is empty string -- you can't set it to null because
    // Configuration will complain -- then no default codec (and we'll pb everything). Else
    // default is KeyValueCodec
    return c.get("hbase.client.default.rpc.codec", KeyValueCodec.class.getCanonicalName());
  }

  /**
   * Encapsulate the ugly casting and RuntimeException conversion in private method.
   * @param conf
   * @return The compressor to use on this client.
   */
  private static CompressionCodec getCompressor(final Configuration conf) {
    String className = conf.get("hbase.client.rpc.compressor", null);
    if (className == null || className.isEmpty()) return null;
    try {
      return (CompressionCodec) Class.forName(className).newInstance();
    } catch (Exception e) {
      throw new RuntimeException("Failed getting compressor " + className, e);
    }
  }

  /**
   * Get the ping interval from configuration; If not set in the configuration, return the default
   * value.
   * @param conf Configuration
   * @return the ping interval
   */
  static int getPingInterval(Configuration conf) {
    return conf.getInt(PING_INTERVAL_NAME, conf.getInt("ipc.ping.interval", DEFAULT_PING_INTERVAL));
  }

  /**
   * @return the socket timeout
   */
  static int getSocketTimeout(Configuration conf) {
    return conf.getInt(SOCKET_TIMEOUT, conf.getInt("ipc.socket.timeout", DEFAULT_SOCKET_TIMEOUT));
  }

  /**
   * Interrupt the connections to the given ip:port server. This should be called if the server is
   * known as actually dead. This will not prevent current operation to be retried, and, depending
   * on their own behavior, they may retry on the same server. This can be a feature, for example at
   * startup. In any case, they're likely to get connection refused (if the process died) or no
   * route to host: i.e. there next retries should be faster and with a safe exception.
   */
  @Override
  public void cancelConnections(ServerName sn, Throwable exc) {
    List<T> toShutdown = new ArrayList<T>();
    synchronized (connections) {
      for (ConnectionId remoteId : connections.keySet()) {
        if (remoteId.address.getPort() == sn.getPort()
            && remoteId.address.getHostName().equals(sn.getHostname())) {
          LOG.info(
            "The server " + sn.getServerName() + " is dead - stopping the connection " + remoteId);
          toShutdown.addAll(connections.removeAll(remoteId));
        }
      }
    }
    if (toShutdown != null) {
      for (T conn : toShutdown) {
        conn.shutdown();
      }
    }
  }

  /**
   * Get a connection from the pool, or create a new one and add it to the pool. Connections to a
   * given host/port are reused.
   */
  private T getConnection(ConnectionId remoteId) throws IOException {
    FailedServer failedServer = failedServers.getFailedServer(remoteId.address);
    if (failedServer != null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Not trying to connect to " + remoteId.address
            + " this server is in the failed servers list");
      }
      throw new FailedServerException("This server is in the failed servers list"
          + failedServer.formatFailedTimeStamp() + ": " + remoteId.address, failedServer.cause);
    }
    T conn;
    synchronized (connections) {
      conn = connections.get(remoteId);
      if (conn == null) {
        conn = createConnection(remoteId);
        connections.put(remoteId, conn);
      }
      conn.setLastTouched(EnvironmentEdgeManager.currentTimeMillis());
    }
    return conn;
  }

  /**
   * Not connected.
   */
  protected abstract T createConnection(ConnectionId remoteId) throws IOException;

  protected int nextCallId() {
    int id, next;
    do {
      id = callIdCnt.get();
      next = id < Integer.MAX_VALUE ? id + 1 : 0;
    } while (!callIdCnt.compareAndSet(id, next));
    return id;
  }

  @Override
  public BlockingRpcChannel createBlockingRpcChannel(final ServerName sn, final User ticket,
      final int rpcTimeout) {
    return new BlockingRpcChannelImplementation(this, sn, ticket, rpcTimeout);
  }

  @Override
  public RpcChannel createRpcChannel(ServerName sn, User ticket, int rpcTimeout) {
    return new RpcChannelImplementation(this, sn, ticket, rpcTimeout);
  }

  /**
   * Make a blocking call. Throws exceptions if there are network problems or if the remote code
   * threw an exception.
   * @param md
   * @param pcrc
   * @param param
   * @param returnType
   * @param addr
   * @param ticket Be careful which ticket you pass. A new user will mean a new Connection.
   *          {@link UserProvider#getCurrent()} makes a new instance of User each time so will be a
   *          new Connection each time.
   * @return A pair with the Message response and the Cell data (if any).
   * @throws InterruptedException
   * @throws IOException
   */
  Message callBlockingMethod(MethodDescriptor md, PayloadCarryingRpcController pcrc, Message param,
      Message returnType, final User ticket, final InetSocketAddress addr) throws ServiceException {
    BlockingRpcCallback<Message> callback = new BlockingRpcCallback<Message>();
    callMethod(md, pcrc, param, returnType, ticket, addr, callback);
    Message ret;
    try {
      ret = callback.get();
    } catch (Exception e) {
      throw new ServiceException(e);
    }
    if (pcrc.failed()) {
      throw new ServiceException(pcrc.getError());
    }
    return ret;
  }

  void callMethod(MethodDescriptor md, PayloadCarryingRpcController pcrc, Message param,
      Message returnType, final User ticket, final InetSocketAddress addr,
      RpcCallback<Message> callback) {
    CellScanner cells = null;
    if (pcrc != null) {
      cells = pcrc.cellScanner();
      // Clear it here so we don't by mistake try and these cells processing results.
      pcrc.setCellScanner(null);
    }
    call(md, pcrc, param, cells, returnType, ticket, addr, callback);
  }

  private void onCallFinished(Call call, PayloadCarryingRpcController pcrc, InetSocketAddress addr,
      RpcCallback<Message> callback) {
    long callTime = EnvironmentEdgeManager.currentTimeMillis() - call.startTime;
    if (LOG.isTraceEnabled()) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Call: " + call.md.getName() + ", callTime: " + callTime + "ms");
      }
    }
    if (callTime > this.clientWarnIpcResponseTime) {
      LOG.warn("Slow ipc call, MethodName=" + call.md.getName() + ", consume time=" + callTime
          + ", remote address:" + addr);
    }
    if (call.error != null) {
      if (call.error instanceof RemoteException) {
        call.error.fillInStackTrace();
        pcrc.setFailed(call.error);
      } else {
        pcrc.setFailed(IPCUtil.wrapException(addr, call.error));
      }
      callback.run(null);
    } else {
      if (call.cells != null) {
        pcrc.setCellScanner(call.cells);
      }
      callback.run(call.response);
    }
  }

  /**
   * Make a call, passing <code>param</code>, to the IPC server running at <code>address</code>
   * which is servicing the <code>protocol</code> protocol, with the <code>ticket</code>
   * credentials.
   * @param md
   * @param pcrc
   * @param param
   * @param addr
   * @param returnType
   * @param ticket Be careful which ticket you pass. A new user will mean a new Connection.
   *          {@link UserProvider#getCurrent()} makes a new instance of User each time so will be a
   *          new Connection each time.
   * @return A pair with the Message response and the Cell data (if any).
   * @throws InterruptedException
   * @throws IOException
   */
  private void call(MethodDescriptor md, final PayloadCarryingRpcController pcrc, Message param,
      CellScanner cells, Message returnType, User ticket, final InetSocketAddress addr,
      final RpcCallback<Message> callback) {
    final AtomicInteger counter = concurrentCounterCache.getUnchecked(addr);
    Call call = new Call(nextCallId(), md, param, cells, returnType, pcrc.getTimeout(),
        pcrc.getPriority(), new RpcCallback<Call>() {

          @Override
          public void run(Call call) {
            counter.decrementAndGet();
            onCallFinished(call, pcrc, addr, callback);
          }
        });
    ConnectionId remoteId = new ConnectionId(ticket, md.getService().getName(), addr);

    int count = counter.incrementAndGet();
    try {
      if (count > maxConcurrentCallsPerServer) {
        throw new ServerBusyException(addr, count);
      }
      T connection = getConnection(remoteId);
      connection.sendRequest(call);
    } catch (Exception e) {
      call.setException(IPCUtil.toIOE(e));
    }
  }

  private static abstract class HBaseRpcChannel {

    protected final InetSocketAddress addr;
    protected final AbstractRpcClient<?> rpcClient;
    protected final int rpcTimeout;
    protected final User ticket;

    protected HBaseRpcChannel(AbstractRpcClient<?> rpcClient, final ServerName sn,
        final User ticket, final int rpcTimeout) {
      this.addr = new InetSocketAddress(sn.getHostname(), sn.getPort());
      this.rpcClient = rpcClient;
      // Set the rpc timeout to be the minimum of configured timeout and whatever the current
      // thread local setting is.
      this.rpcTimeout = rpcTimeout;
      this.ticket = ticket;
    }

    protected void setTimeout(PayloadCarryingRpcController controller) {
      if (controller.getTimeout() <= 0) {
        controller.setTimeout(getRpcTimeout(rpcTimeout));
      }
    }
  }

  /**
   * Blocking rpc channel that goes via hbase rpc.
   */
  // Public so can be subclassed for tests.
  public static class BlockingRpcChannelImplementation extends HBaseRpcChannel
      implements BlockingRpcChannel {

    protected BlockingRpcChannelImplementation(AbstractRpcClient<?> rpcClient, final ServerName sn,
        final User ticket, final int rpcTimeout) {
      super(rpcClient, sn, ticket, rpcTimeout);
    }

    @Override
    public Message callBlockingMethod(MethodDescriptor method, RpcController controller,
        Message request, Message responsePrototype) throws ServiceException {
      PayloadCarryingRpcController pcrc;
      if (controller == null) {
        pcrc = new PayloadCarryingRpcController();
      } else {
        pcrc = (PayloadCarryingRpcController) controller;
      }
      setTimeout(pcrc);
      return rpcClient.callBlockingMethod(method, pcrc, request, responsePrototype, ticket, addr);
    }
  }

  /**
   * Rpc channel that goes via hbase rpc.
   */
  // Public so can be subclassed for tests.
  public static class RpcChannelImplementation extends HBaseRpcChannel implements RpcChannel {

    protected RpcChannelImplementation(AbstractRpcClient<?> rpcClient, final ServerName sn,
        final User ticket, final int rpcTimeout) {
      super(rpcClient, sn, ticket, rpcTimeout);
    }

    @Override
    public void callMethod(MethodDescriptor method, RpcController controller, Message request,
        Message responsePrototype, RpcCallback<Message> done) {
      PayloadCarryingRpcController pcrc = (PayloadCarryingRpcController) Preconditions.checkNotNull(
        controller, "must provide a controller because the exception will be passed by it.");
      setTimeout(pcrc);
      rpcClient.callMethod(method, pcrc, request, responsePrototype, ticket, addr, done);
    }
  }

  protected static final Map<Kind, TokenSelector<? extends TokenIdentifier>> TOKEN_HANDLERS = new HashMap<Kind, TokenSelector<? extends TokenIdentifier>>();

  static {
    TOKEN_HANDLERS.put(Kind.HBASE_AUTH_TOKEN, new AuthenticationTokenSelector());
  }
  // thread-specific RPC timeout, which may override that of what was passed in.
  // This is used to change dynamically the timeout (for read only) when retrying: if
  // the time allowed for the operation is less than the usual socket timeout, then
  // we lower the timeout. This is subject to race conditions, and should be used with
  // extreme caution.
  private static final ThreadLocal<Integer> RPC_TIMEOUT = new ThreadLocal<Integer>() {

    @Override
    protected Integer initialValue() {
      return HConstants.DEFAULT_HBASE_CLIENT_OPERATION_TIMEOUT;
    }
  };

  public static void setRpcTimeout(int t) {
    RPC_TIMEOUT.set(t);
  }

  public static int getRpcTimeout() {
    return RPC_TIMEOUT.get();
  }

  /**
   * Returns the lower of the thread-local RPC time from {@link #setRpcTimeout(int)} and the given
   * default timeout.
   */
  public static int getRpcTimeout(int defaultTimeout) {
    return Math.min(defaultTimeout, getRpcTimeout());
  }

  public static void resetRpcTimeout() {
    RPC_TIMEOUT.remove();
  }
}
