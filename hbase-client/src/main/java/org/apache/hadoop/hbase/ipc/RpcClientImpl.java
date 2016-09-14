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

package org.apache.hadoop.hbase.ipc;

import com.google.protobuf.Message;

import java.io.IOException;
import java.net.Socket;
import java.net.SocketAddress;

import javax.net.SocketFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.codec.Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.net.NetUtils;

/**
 * Does RPC against a cluster. Manages connections per regionserver in the cluster.
 * <p>
 * See HBaseServer
 */
@InterfaceAudience.Private
public class RpcClientImpl extends AbstractRpcClient<ConnectionImpl> {

  public static final Log LOG = LogFactory.getLog(RpcClientImpl.class);

  protected final SocketFactory socketFactory; // how to create sockets

  /**
   * Creates a connection. Can be overridden by a subclass for testing.
   * @param remoteId - the ConnectionId to use for the connection creation.
   */
  protected ConnectionImpl createConnection(ConnectionId remoteId, final Codec codec,
      final CompressionCodec compressor) throws IOException {
    return new ConnectionImpl(this, remoteId);
  }

  @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "IS2_INCONSISTENT_SYNC", justification = "Presume sync not needed setting socket timeout")
  static void setSocketTimeout(final Socket socket, final int rpcTimeout)
      throws java.net.SocketException {
    if (socket == null) return;
    socket.setSoTimeout(rpcTimeout);
  }

  /**
   * Construct an IPC cluster client whose values are of the {@link Message} class.
   * @param conf configuration
   * @param clusterId
   * @param factory socket factory
   */
  RpcClientImpl(Configuration conf, String clusterId, SocketFactory factory) {
    this(conf, clusterId, factory, null);
  }

  /**
   * Construct an IPC cluster client whose values are of the {@link Message} class.
   * @param conf configuration
   * @param clusterId
   * @param factory socket factory
   * @param localAddr client socket bind address
   */
  RpcClientImpl(Configuration conf, String clusterId, SocketFactory factory,
      SocketAddress localAddr) {
    super(conf, clusterId, localAddr);
    this.socketFactory = factory;
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
   * Construct an IPC client for the cluster <code>clusterId</code> with the default SocketFactory
   * @param conf configuration
   * @param clusterId
   */
  public RpcClientImpl(Configuration conf, String clusterId) {
    this(conf, clusterId, NetUtils.getDefaultSocketFactory(conf), null);
  }

  /**
   * Construct an IPC client for the cluster <code>clusterId</code> with the default SocketFactory
   * @param conf configuration
   * @param clusterId
   * @param localAddr client socket bind address.
   */
  public RpcClientImpl(Configuration conf, String clusterId, SocketAddress localAddr) {
    this(conf, clusterId, NetUtils.getDefaultSocketFactory(conf), localAddr);
  }

  /**
   * Return the socket factory of this client
   * @return this client's socket factory
   */
  SocketFactory getSocketFactory() {
    return socketFactory;
  }

  @Override
  protected ConnectionImpl createConnection(ConnectionId remoteId) throws IOException {
    return new ConnectionImpl(this, remoteId);
  }

  @Override
  protected void closeInternal() {
  }
}
