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
package org.apache.hadoop.hbase.client.async;

import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.ipc.RpcClient;
import org.apache.hadoop.hbase.ipc.RpcClientFactory;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ClientService;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.CollectionUtils;
import org.apache.hadoop.hbase.util.Threads;

/**
 *
 */
@InterfaceAudience.Private
class AsyncConnectionImpl implements AsyncConnection {

  private static final Log LOG = LogFactory.getLog(AsyncConnectionImpl.class);

  private static final HashedWheelTimer RETRY_TIMER = new HashedWheelTimer(
      Threads.newDaemonThreadFactory("Async-Client-Retry-Timer"), 10, TimeUnit.MILLISECONDS);

  private static final String RESOLVE_HOSTNAME_ON_FAIL_KEY = "hbase.resolve.hostnames.on.failure";

  private final Configuration conf;

  private final User user;

  private final ClusterRegistry registry;

  private final String clusterId;

  private final int rpcTimeout;

  private final RpcClient rpcClient;

  final RpcControllerFactory rpcControllerFactory;

  private final boolean hostnameCanChange;

  final RegionLocatorImpl locator;

  public AsyncConnectionImpl(Configuration conf, User user) throws IOException {
    this.conf = conf;
    this.user = user;

    this.locator = new RegionLocatorImpl(conf);

    // action below will not throw exception so no need to catch and close.
    this.registry = ClusterRegistryFactory.getRegistry(conf);
    this.clusterId = Optional.ofNullable(registry.getClusterId()).orElseGet(() -> {
      if (LOG.isDebugEnabled()) {
        LOG.debug("cluster id came back null, using default " + HConstants.CLUSTER_ID_DEFAULT);
      }
      return HConstants.CLUSTER_ID_DEFAULT;
    });
    this.rpcClient = RpcClientFactory.createClient(conf, clusterId);
    this.rpcControllerFactory = RpcControllerFactory.instantiate(conf);
    this.hostnameCanChange = conf.getBoolean(RESOLVE_HOSTNAME_ON_FAIL_KEY, true);
    this.rpcTimeout = conf.getInt(HConstants.HBASE_RPC_TIMEOUT_KEY,
      HConstants.DEFAULT_HBASE_RPC_TIMEOUT);
  }

  @Override
  public Configuration getConfiguration() {
    return conf;
  }

  @Override
  public void close() {
    IOUtils.closeQuietly(locator);
    IOUtils.closeQuietly(rpcClient);
    IOUtils.closeQuietly(registry);
  }

  @Override
  public AsyncRegionLocator getRegionLocator(TableName tableName) {
    return new AsyncRegionLocatorImpl(tableName, locator);
  }

  void retryAfter(long delay, TimeUnit unit, Runnable action) {
    RETRY_TIMER.newTimeout(new TimerTask() {

      @Override
      public void run(Timeout timeout) throws Exception {
        action.run();
      }
    }, delay, unit);
  }

  private final ConcurrentMap<String, ClientService.Interface> rsStubs = new ConcurrentHashMap<>();

  private String getStubKey(String serviceName, ServerName serverName) {
    // Sometimes, servers go down and they come back up with the same hostname but a different
    // IP address. Force a resolution of the rsHostname by trying to instantiate an
    // InetSocketAddress, and this way we will rightfully get a new stubKey.
    // Also, include the hostname in the key so as to take care of those cases where the
    // DNS name is different but IP address remains the same.
    String hostname = serverName.getHostname();
    int port = serverName.getPort();
    if (hostnameCanChange) {
      try {
        InetAddress ip = InetAddress.getByName(hostname);
        return serviceName + "@" + hostname + "-" + ip.getHostAddress() + ":" + port;
      } catch (UnknownHostException e) {
        LOG.warn("Can not resolve " + hostname + ", please check your network", e);
      }
    }
    return serviceName + "@" + hostname + ":" + port;
  }

  private ClientService.Interface createRegionServerStub(ServerName serverName) {
    try {
      return ClientService.newStub(rpcClient.createRpcChannel(serverName, user, rpcTimeout));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  ClientService.Interface getRegionServerStub(ServerName serverName) throws IOException {
    try {
      return CollectionUtils.computeIfAbsent(rsStubs,
        getStubKey(ClientService.Interface.class.getSimpleName(), serverName),
        () -> createRegionServerStub(serverName));
    } catch (UncheckedIOException e) {
      throw e.getCause();
    }
  }

  @Override
  public AsyncTable getTable(TableName tableName) {
    return new AsyncTableImpl(this, tableName);
  }
}
