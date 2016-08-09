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

import java.net.SocketAddress;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.codec.Codec;
import org.apache.hadoop.hbase.codec.KeyValueCodec;
import org.apache.hadoop.hbase.protobuf.generated.AuthenticationProtos.TokenIdentifier.Kind;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.security.token.AuthenticationTokenSelector;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.token.TokenSelector;

/**
 * Base class of all RpcClient implementations.
 */
@InterfaceAudience.Private
public abstract class AbstractRpcClient implements RpcClient {

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
    // login the server principal (if using secure Hadoop)
    if (LOG.isDebugEnabled()) {
      LOG.debug("Codec=" + this.codec + ", compressor=" + this.compressor + ", tcpKeepAlive="
          + this.tcpKeepAlive + ", tcpNoDelay=" + this.tcpNoDelay + ", maxIdleTime="
          + this.maxIdleTime + ", maxRetries=" + this.maxRetries + ", fallbackAllowed="
          + this.fallbackAllowed + ", clientWarnIpcResponseTime=" + this.clientWarnIpcResponseTime
          + ", ping interval=" + this.pingInterval + "ms" + ", bind address="
          + (this.localAddr != null ? this.localAddr : "null"));
    }
  }

  /**
   * Encapsulate the ugly casting and RuntimeException conversion in private method.
   * @return Codec to use on this client.
   */
  Codec getCodec() {
    // For NO CODEC, "hbase.client.rpc.codec" must be configured with empty string AND
    // "hbase.client.default.rpc.codec" also -- because default is to do cell block encoding.
    String className = conf.get(HConstants.RPC_CODEC_CONF_KEY, getDefaultCodec(this.conf));
    if (className == null || className.length() == 0) return null;
    try {
      return (Codec) Class.forName(className).newInstance();
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

  protected final static Map<Kind, TokenSelector<? extends TokenIdentifier>> TOKEN_HANDLERS
    = new HashMap<Kind, TokenSelector<? extends TokenIdentifier>>();

  static {
    TOKEN_HANDLERS.put(Kind.HBASE_AUTH_TOKEN, new AuthenticationTokenSelector());
  }

  // thread-specific RPC timeout, which may override that of what was passed in.
  // This is used to change dynamically the timeout (for read only) when retrying: if
  // the time allowed for the operation is less than the usual socket timeout, then
  // we lower the timeout. This is subject to race conditions, and should be used with
  // extreme caution.
  private static ThreadLocal<Integer> RPC_TIMEOUT = new ThreadLocal<Integer>() {

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
    return Math.min(defaultTimeout, RPC_TIMEOUT.get());
  }

  public static void resetRpcTimeout() {
    RPC_TIMEOUT.remove();
  }
}
