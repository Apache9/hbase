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
package org.apache.hadoop.hbase;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.ConnectionUtils;
import org.apache.hadoop.hbase.conf.ConfigurationObserver;
import org.apache.hadoop.hbase.io.ByteBuffAllocator;
import org.apache.hadoop.hbase.ipc.HBaseRPCErrorHandler;
import org.apache.hadoop.hbase.ipc.PriorityFunction;
import org.apache.hadoop.hbase.ipc.RpcScheduler;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.ipc.RpcServer.BlockingServiceAndInterface;
import org.apache.hadoop.hbase.ipc.RpcServerFactory;
import org.apache.hadoop.hbase.ipc.RpcServerInterface;
import org.apache.hadoop.hbase.net.Address;
import org.apache.hadoop.hbase.regionserver.RpcSchedulerFactory;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.access.AccessChecker;
import org.apache.hadoop.hbase.security.access.NoopAccessChecker;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.hadoop.hbase.security.access.ZKPermissionWatcher;
import org.apache.hadoop.hbase.util.DNS;
import org.apache.hadoop.hbase.util.OOMEChecker;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.protobuf.Message;

import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.RequestHeader;

/**
 * Base class for Master and RegionServer RpcServices.
 */
@InterfaceAudience.Private
public abstract class HBaseRpcServicesBase<S extends Thread & Server>
  implements HBaseRPCErrorHandler, PriorityFunction, ConfigurationObserver {

  private static final Logger LOG = LoggerFactory.getLogger(HBaseRpcServicesBase.class);

  protected final S server;

  // Server to handle client requests.
  protected final RpcServer rpcServer;

  private final InetSocketAddress isa;

  protected final PriorityFunction priority;
  
  private AccessChecker accessChecker;

  private ZKPermissionWatcher zkPermissionWatcher;

  protected HBaseRpcServicesBase(S server, String processName) throws IOException {
    this.server = server;
    Configuration conf = server.getConfiguration();
    final RpcSchedulerFactory rpcSchedulerFactory;
    try {
      rpcSchedulerFactory = getRpcSchedulerFactoryClass(conf).asSubclass(RpcSchedulerFactory.class)
        .getDeclaredConstructor().newInstance();
    } catch (NoSuchMethodException | InvocationTargetException | InstantiationException
      | IllegalAccessException e) {
      throw new IllegalArgumentException(e);
    }
    String hostname = DNS.getHostname(conf, getDNSServerType());
    int port = conf.getInt(getPortConfigName(), getDefaultPort());
    // Creation of a HSA will force a resolve.
    final InetSocketAddress initialIsa = new InetSocketAddress(hostname, port);
    final InetSocketAddress bindAddress = new InetSocketAddress(getHostname(conf, hostname), port);
    if (initialIsa.getAddress() == null) {
      throw new IllegalArgumentException("Failed resolve of " + initialIsa);
    }
    priority = createPriority();
    // Using Address means we don't get the IP too. Shorten it more even to just the host name
    // w/o the domain.
    final String name = processName + "/" +
      Address.fromParts(initialIsa.getHostName(), initialIsa.getPort()).toStringWithoutDomain();
    server.setName(name);
    // Set how many times to retry talking to another server over Connection.
    ConnectionUtils.setServerSideHConnectionRetriesConfig(conf, name, LOG);
    boolean reservoirEnabled = conf.getBoolean(ByteBuffAllocator.ALLOCATOR_POOL_ENABLED_KEY, true);
    try {
      // use final bindAddress for this server.
      rpcServer = RpcServerFactory.createRpcServer(server, name, getServices(), bindAddress, conf,
        rpcSchedulerFactory.create(conf, this, server), reservoirEnabled);
    } catch (BindException be) {
      throw new IOException(be.getMessage() + ". To switch ports use the '" + getPortConfigName() +
        "' configuration property.", be.getCause() != null ? be.getCause() : be);
    }
    final InetSocketAddress address = rpcServer.getListenerAddress();
    if (address == null) {
      throw new IOException("Listener channel is closed");
    }
    // Set our address, however we need the final port that was given to rpcServer
    isa = new InetSocketAddress(initialIsa.getHostName(), address.getPort());
    rpcServer.setErrorHandler(this);
  }

  protected abstract DNS.ServerType getDNSServerType();

  protected abstract String getHostname(Configuration conf, String defaultHostname);

  protected abstract String getPortConfigName();

  protected abstract int getDefaultPort();

  protected abstract PriorityFunction createPriority();

  protected abstract Class<?> getRpcSchedulerFactoryClass(Configuration conf);

  protected abstract List<BlockingServiceAndInterface> getServices();

  protected final void internalStart(ZKWatcher zkWatcher) {
    if (AccessChecker.isAuthorizationSupported(getConfiguration())) {
      accessChecker = new AccessChecker(getConfiguration());
    } else {
      accessChecker = new NoopAccessChecker(getConfiguration());
    }
    zkPermissionWatcher =
      new ZKPermissionWatcher(zkWatcher, accessChecker.getAuthManager(), getConfiguration());
    try {
      zkPermissionWatcher.start();
    } catch (KeeperException e) {
      LOG.error("ZooKeeper permission watcher initialization failed", e);
    }
    rpcServer.start();
  }

  protected final void requirePermission(String request, Permission.Action perm) throws IOException {
    if (accessChecker != null) {
      accessChecker.requirePermission(RpcServer.getRequestUser().orElse(null), request, null, perm);
    }
  }

  public AccessChecker getAccessChecker() {
    return accessChecker;
  }

  public ZKPermissionWatcher getZkPermissionWatcher() {
    return zkPermissionWatcher;
  }

  protected final void internalStop() {
    if (zkPermissionWatcher != null) {
      zkPermissionWatcher.close();
    }
    rpcServer.stop();
  }
  
  public Configuration getConfiguration() {
    return server.getConfiguration();
  }

  public S getServer() {
    return server;
  }

  public InetSocketAddress getSocketAddress() {
    return isa;
  }

  public RpcServerInterface getRpcServer() {
    return rpcServer;
  }

  public RpcScheduler getRpcScheduler() {
    return rpcServer.getScheduler();
  }

  @Override
  public int getPriority(RequestHeader header, Message param, User user) {
    return priority.getPriority(header, param, user);
  }

  @Override
  public long getDeadline(RequestHeader header, Message param) {
    return priority.getDeadline(header, param);
  }

  /**
   * Check if an OOME and, if so, abort immediately to avoid creating more objects.
   * @return True if we OOME'd and are aborting.
   */
  @Override
  public boolean checkOOME(Throwable e) {
    return OOMEChecker.exitIfOOME(e, getClass().getSimpleName());
  }

  @Override
  public void onConfigurationChange(Configuration conf) {
    if (rpcServer instanceof ConfigurationObserver) {
      ((ConfigurationObserver) rpcServer).onConfigurationChange(conf);
    }
  }
}
