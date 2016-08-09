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

import com.google.protobuf.BlockingRpcChannel;

import java.io.Closeable;

import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.security.User;

/**
 * Interface for RpcClient implementations so HConnectionManager can handle it.
 */
@InterfaceAudience.Private
public interface RpcClient extends Closeable {

  static final String PING_INTERVAL_NAME = "hbase.ipc.ping.interval";
  static final String SOCKET_TIMEOUT = "hbase.ipc.socket.timeout";
  static final int DEFAULT_PING_INTERVAL = 60000; // 1 min
  static final int DEFAULT_SOCKET_TIMEOUT = 20000; // 20 seconds
  static final int PING_CALL_ID = -1;

  static final String IPC_CLIENT_FALLBACK_TO_SIMPLE_AUTH_ALLOWED_KEY = "hbase.ipc.client.fallback-to-simple-auth-allowed";
  static final boolean IPC_CLIENT_FALLBACK_TO_SIMPLE_AUTH_ALLOWED_DEFAULT = false;

  static final int DEFAULT_CLIENT_WARN_IPC_RESPONSE_TIME = 100; // milliseconds
  static final String CLIENT_WARN_IPC_RESPONSE_TIME = "hbase.client.ipc.warn.response.time";

  static final String SOCKET_TIMEOUT_CONNECT = "hbase.ipc.client.socket.timeout.connect";
  /**
   * How long we wait when we wait for an answer. It's not the operation time, it's the time we wait
   * when we start to receive an answer, when the remote write starts to send the data.
   */
  static final String SOCKET_TIMEOUT_READ = "hbase.ipc.client.socket.timeout.read";
  static final String SOCKET_TIMEOUT_WRITE = "hbase.ipc.client.socket.timeout.write";
  static final int DEFAULT_SOCKET_TIMEOUT_CONNECT = 10000; // 10 seconds
  static final int DEFAULT_SOCKET_TIMEOUT_READ = 20000; // 20 seconds
  static final int DEFAULT_SOCKET_TIMEOUT_WRITE = 60000; // 60 second

  /**
   * Creates a "channel" that can be used by a blocking protobuf service. Useful setting up protobuf
   * blocking stubs.
   * @param sn
   * @param ticket
   * @param rpcTimeout
   * @return A blocking rpc channel that goes via this rpc client instance.
   */
  BlockingRpcChannel createBlockingRpcChannel(ServerName sn, User ticket, int rpcTimeout);

  /**
   * Interrupt the connections to the given ip:port server. This should be called if the server is
   * known as actually dead. This will not prevent current operation to be retried, and, depending
   * on their own behavior, they may retry on the same server. This can be a feature, for example at
   * startup. In any case, they're likely to get connection refused (if the process died) or no
   * route to host: i.e. there next retries should be faster and with a safe exception.
   */
  void cancelConnections(ServerName sn, Throwable exc);

  /**
   * Stop all threads related to this client. No further calls may be made using this client.
   */
  @Override
  void close();
}
