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
package org.apache.hadoop.hbase.client;

import com.google.common.annotations.VisibleForTesting;
import com.xiaomi.infra.base.nameservice.NameService;
import com.xiaomi.infra.base.nameservice.NameServiceEntry;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.RegionTooBusyException;
import org.apache.hadoop.hbase.RetryImmediatelyException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.exceptions.RegionMovedException;
import org.apache.hadoop.hbase.exceptions.RegionOpeningException;
import org.apache.hadoop.hbase.quotas.RpcThrottlingException;
import org.apache.hadoop.hbase.quotas.ThrottlingException;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.ExceptionUtil;
import org.apache.hadoop.hbase.util.ReflectionUtils;
import org.apache.hadoop.ipc.RemoteException;

/**
 * A non-instantiable class that manages creation of {@link HConnection}s.
 * <p>
 * The simplest way to use this class is by using {@link #createConnection(Configuration)}. This
 * creates a new {@link HConnection} to the cluster that is managed by the caller. From this
 * {@link HConnection} {@link HTableInterface} implementations are retrieved with
 * {@link HConnection#getTable(byte[])}. Example:
 * 
 * <pre>
 * {
 *   &#64;code
 *   HConnection connection = HConnectionManager.createConnection(config);
 *   HTableInterface table = connection.getTable("table1");
 *   try {
 *     // Use the table as needed, for a single operation and a single thread
 *   } finally {
 *     table.close();
 *     connection.close();
 *   }
 * }
 * </pre>
 * <p>
 * This class has a static Map of {@link HConnection} instances keyed by {@link HConnectionKey}; A
 * {@link HConnectionKey} is identified by a set of {@link Configuration} properties. Invocations of
 * {@link #getConnection(Configuration)} that pass the same {@link Configuration} instance will
 * return the same {@link HConnection} instance ONLY WHEN the set of properties are the same (i.e.
 * if you change properties in your {@link Configuration} instance, such as RPC timeout, the codec
 * used, HBase will create a new {@link HConnection} instance. For more details on how this is done
 * see {@link HConnectionKey}).
 * <p>
 * Sharing {@link HConnection} instances is usually what you want; all clients of the
 * {@link HConnection} instances share the HConnections' cache of Region locations rather than each
 * having to discover for itself the location of meta, etc. But sharing connections makes clean up
 * of {@link HConnection} instances a little awkward. Currently, clients cleanup by calling
 * {@link #deleteConnection(Configuration)}. This will shutdown the zookeeper connection the
 * HConnection was using and clean up all HConnection resources as well as stopping proxies to
 * servers out on the cluster. Not running the cleanup will not end the world; it'll just stall the
 * closeup some and spew some zookeeper connection failed messages into the log. Running the cleanup
 * on a {@link HConnection} that is subsequently used by another will cause breakage so be careful
 * running cleanup.
 * <p>
 * To create a {@link HConnection} that is not shared by others, you can set property
 * "hbase.client.instance.id" to a unique value for your {@link Configuration} instance, like the
 * following:
 * 
 * <pre>
 * {@code
 * conf.set("hbase.client.instance.id", "12345");
 * HConnection connection = HConnectionManager.getConnection(conf);
 * // Use the connection to your hearts' delight and then when done...
 * conf.set("hbase.client.instance.id", "12345");
 * HConnectionManager.deleteConnection(conf, true);
 * }
 * </pre>
 * <p>
 * Cleanup used to be done inside in a shutdown hook. On startup we'd register a shutdown hook that
 * called {@link #deleteAllConnections()} on its way out but the order in which shutdown hooks run
 * is not defined so were problematic for clients of HConnection that wanted to register their own
 * shutdown hooks so we removed ours though this shifts the onus for cleanup to the client.
 */
@SuppressWarnings("serial")
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class HConnectionManager {
  static final Log LOG = LogFactory.getLog(HConnectionManager.class);

  public static final String RETRIES_BY_SERVER_KEY = "hbase.client.retries.by.server";

  // An LRU Map of HConnectionKey -> HConnection (TableServer). All
  // access must be synchronized. This map is not private because tests
  // need to be able to tinker with it.
  static final Map<HConnectionKey, HConnectionImplementation> CONNECTION_INSTANCES;

  public static final int MAX_CACHED_CONNECTION_INSTANCES;

  static {
    // We set instances to one more than the value specified for {@link
    // HConstants#ZOOKEEPER_MAX_CLIENT_CNXNS}. By default, the zk default max
    // connections to the ensemble from the one client is 30, so in that case we
    // should run into zk issues before the LRU hit this value of 31.
    MAX_CACHED_CONNECTION_INSTANCES =
        HBaseConfiguration.create().getInt(HConstants.ZOOKEEPER_MAX_CLIENT_CNXNS,
          HConstants.DEFAULT_ZOOKEPER_MAX_CLIENT_CNXNS) + 1;
    CONNECTION_INSTANCES = new LinkedHashMap<HConnectionKey, HConnectionImplementation>(
        (int) (MAX_CACHED_CONNECTION_INSTANCES / 0.75F) + 1, 0.75F, true) {
      @Override
      protected boolean removeEldestEntry(
          Map.Entry<HConnectionKey, HConnectionImplementation> eldest) {
        return size() > MAX_CACHED_CONNECTION_INSTANCES;
      }
    };
  }

  /*
   * Non-instantiable.
   */
  private HConnectionManager() {
    super();
  }

  /**
   * @param conn The connection for which to replace the generator.
   * @param cnm Replaces the nonce generator used, for testing.
   * @return old nonce generator.
   */
  @VisibleForTesting
  public static NonceGenerator injectNonceGeneratorForTesting(HConnection conn,
      NonceGenerator cnm) {
    NonceGenerator ng = conn.getNonceGenerator();
    LOG.warn("Nonce generator is being replaced by test code for " + cnm.getClass().getName());
    ((HConnectionImplementation) conn).nonceGenerator = cnm;
    return ng;
  }

  /**
   * Get the connection that goes with the passed <code>conf</code> configuration instance. If no
   * current connection exists, method creates a new connection and keys it using
   * connection-specific properties from the passed {@link Configuration}; see
   * {@link HConnectionKey}.
   * @param conf configuration
   * @return HConnection object for <code>conf</code>
   * @throws ZooKeeperConnectionException
   */
  @Deprecated
  public static HConnection getConnection(final Configuration conf) throws IOException {
    HConnectionKey connectionKey = new HConnectionKey(conf);
    synchronized (CONNECTION_INSTANCES) {
      HConnectionImplementation connection = CONNECTION_INSTANCES.get(connectionKey);
      if (connection == null) {
        connection = (HConnectionImplementation) createConnection(conf, true);
        CONNECTION_INSTANCES.put(connectionKey, connection);
      } else if (connection.isClosed()) {
        HConnectionManager.deleteConnection(connectionKey, true);
        connection = (HConnectionImplementation) createConnection(conf, true);
        CONNECTION_INSTANCES.put(connectionKey, connection);
      }
      connection.incCount();
      return connection;
    }
  }

  public static HConnection createConnection(Configuration conf, String clusterUri)
      throws IOException {
    return createConnection(conf, clusterUri, null);
  }

  public static HConnection createConnection(Configuration conf, String clusterUri,
      ExecutorService pool) throws IOException {
    NameServiceEntry entry = NameService.resolve(clusterUri, conf);
    if (entry.getScheme() == null || !entry.getScheme().equals("hbase")) {
      throw new IOException("Unrecognized scheme: " + entry.getScheme() + ", scheme must be hbase");
    }
    conf = entry.createClusterConf(conf);
    return createConnection(conf, pool);
  }

  /**
   * Create a new HConnection instance using the passed <code>conf</code> instance.
   * <p>
   * Note: This bypasses the usual HConnection life cycle management done by
   * {@link #getConnection(Configuration)}. The caller is responsible for calling
   * {@link HConnection#close()} on the returned connection instance. This is the recommended way to
   * create HConnections. {@code
   * HConnection connection = HConnectionManager.createConnection(conf);
   * HTableInterface table = connection.getTable("mytable");
   * table.get(...);
   * ...
   * table.close();
   * connection.close();
   * }
   * @param conf configuration
   * @return HConnection object for <code>conf</code>
   * @throws ZooKeeperConnectionException
   */
  public static HConnection createConnection(Configuration conf) throws IOException {
    UserProvider provider = UserProvider.instantiate(conf);
    return createConnection(conf, false, null, provider.getCurrent());
  }

  /**
   * Create a new HConnection instance using the passed <code>conf</code> instance.
   * <p>
   * Note: This bypasses the usual HConnection life cycle management done by
   * {@link #getConnection(Configuration)}. The caller is responsible for calling
   * {@link HConnection#close()} on the returned connection instance. This is the recommended way to
   * create HConnections. {@code
   * ExecutorService pool = ...;
   * HConnection connection = HConnectionManager.createConnection(conf, pool);
   * HTableInterface table = connection.getTable("mytable");
   * table.get(...);
   * ...
   * table.close();
   * connection.close();
   * }
   * @param conf configuration
   * @param pool the thread pool to use for batch operation in HTables used via this HConnection
   * @return HConnection object for <code>conf</code>
   * @throws ZooKeeperConnectionException
   */
  public static HConnection createConnection(Configuration conf, ExecutorService pool)
      throws IOException {
    UserProvider provider = UserProvider.instantiate(conf);
    return createConnection(conf, false, pool, provider.getCurrent());
  }

  /**
   * Create a new HConnection instance using the passed <code>conf</code> instance.
   * <p>
   * Note: This bypasses the usual HConnection life cycle management done by
   * {@link #getConnection(Configuration)}. The caller is responsible for calling
   * {@link HConnection#close()} on the returned connection instance. This is the recommended way to
   * create HConnections. {@code
   * ExecutorService pool = ...;
   * HConnection connection = HConnectionManager.createConnection(conf, pool);
   * HTableInterface table = connection.getTable("mytable");
   * table.get(...);
   * ...
   * table.close();
   * connection.close();
   * }
   * @param conf configuration
   * @param user the user the connection is for
   * @return HConnection object for <code>conf</code>
   * @throws ZooKeeperConnectionException
   */
  public static HConnection createConnection(Configuration conf, User user) throws IOException {
    return createConnection(conf, false, null, user);
  }

  /**
   * Create a new HConnection instance using the passed <code>conf</code> instance.
   * <p>
   * Note: This bypasses the usual HConnection life cycle management done by
   * {@link #getConnection(Configuration)}. The caller is responsible for calling
   * {@link HConnection#close()} on the returned connection instance. This is the recommended way to
   * create HConnections. {@code
   * ExecutorService pool = ...;
   * HConnection connection = HConnectionManager.createConnection(conf, pool);
   * HTableInterface table = connection.getTable("mytable");
   * table.get(...);
   * ...
   * table.close();
   * connection.close();
   * }
   * @param conf configuration
   * @param pool the thread pool to use for batch operation in HTables used via this HConnection
   * @param user the user the connection is for
   * @return HConnection object for <code>conf</code>
   * @throws ZooKeeperConnectionException
   */
  public static HConnection createConnection(Configuration conf, ExecutorService pool, User user)
      throws IOException {
    return createConnection(conf, false, pool, user);
  }

  @Deprecated
  static HConnection createConnection(final Configuration conf, final boolean managed)
      throws IOException {
    UserProvider provider = UserProvider.instantiate(conf);
    return createConnection(conf, managed, null, provider.getCurrent());
  }

  @Deprecated
  static HConnection createConnection(final Configuration conf, final boolean managed,
      final ExecutorService pool, final User user) throws IOException {
    String className =
        conf.get("hbase.client.connection.impl", HConnectionImplementation.class.getName());
    Class<?> clazz = null;
    try {
      clazz = Class.forName(className);
    } catch (ClassNotFoundException e) {
      throw new IOException(e);
    }
    try {
      // Default HCM#HCI is not accessible; make it so before invoking.
      Constructor<?> constructor = clazz.getDeclaredConstructor(Configuration.class, boolean.class,
        ExecutorService.class, User.class);
      constructor.setAccessible(true);
      return (HConnection) constructor.newInstance(conf, managed, pool, user);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  /**
   * Delete connection information for the instance specified by passed configuration. If there are
   * no more references to the designated connection connection, this method will then close
   * connection to the zookeeper ensemble and let go of all associated resources.
   * @param conf configuration whose identity is used to find {@link HConnection} instance.
   * @deprecated
   */
  public static void deleteConnection(Configuration conf) {
    deleteConnection(new HConnectionKey(conf), false);
  }

  /**
   * Cleanup a known stale connection. This will then close connection to the zookeeper ensemble and
   * let go of all resources.
   * @param connection
   * @deprecated
   */
  public static void deleteStaleConnection(HConnection connection) {
    deleteConnection(connection, true);
  }

  /**
   * Delete information for all connections. Close or not the connection, depending on the
   * staleConnection boolean and the ref count. By default, you should use it with staleConnection
   * to true.
   * @deprecated
   */
  public static void deleteAllConnections(boolean staleConnection) {
    synchronized (CONNECTION_INSTANCES) {
      Set<HConnectionKey> connectionKeys = new HashSet<HConnectionKey>();
      connectionKeys.addAll(CONNECTION_INSTANCES.keySet());
      for (HConnectionKey connectionKey : connectionKeys) {
        deleteConnection(connectionKey, staleConnection);
      }
      CONNECTION_INSTANCES.clear();
    }
  }

  /**
   * Delete information for all connections..
   * @deprecated kept for backward compatibility, but the behavior is broken. HBASE-8983
   */
  @Deprecated
  public static void deleteAllConnections() {
    deleteAllConnections(false);
  }

  @Deprecated
  static void deleteConnection(HConnection connection, boolean staleConnection) {
    synchronized (CONNECTION_INSTANCES) {
      for (Entry<HConnectionKey, HConnectionImplementation> e : CONNECTION_INSTANCES.entrySet()) {
        if (e.getValue() == connection) {
          deleteConnection(e.getKey(), staleConnection);
          break;
        }
      }
    }
  }

  @Deprecated
  private static void deleteConnection(HConnectionKey connectionKey, boolean staleConnection) {
    synchronized (CONNECTION_INSTANCES) {
      HConnectionImplementation connection = CONNECTION_INSTANCES.get(connectionKey);
      if (connection != null) {
        connection.decCount();
        if (connection.isZeroReference() || staleConnection) {
          CONNECTION_INSTANCES.remove(connectionKey);
          connection.internalClose();
        }
      } else {
        LOG.error("Connection not found in the list, can't delete it " + "(connection key="
            + connectionKey + "). May be the key was modified?",
          new Exception());
      }
    }
  }

  /**
   * It is provided for unit test cases which verify the behavior of region location cache prefetch.
   * @return Number of cached regions for the table.
   * @throws ZooKeeperConnectionException
   */
  static int getCachedRegionCount(Configuration conf, final TableName tableName)
      throws IOException {
    return execute(new HConnectable<Integer>(conf) {
      @Override
      public Integer connect(HConnection connection) {
        return ((HConnectionImplementation) connection).getNumberOfCachedRegionLocations(tableName);
      }
    });
  }

  /**
   * This convenience method invokes the given {@link HConnectable#connect} implementation using a
   * {@link HConnection} instance that lasts just for the duration of the invocation.
   * @param <T> the return type of the connect method
   * @param connectable the {@link HConnectable} instance
   * @return the value returned by the connect method
   * @throws IOException
   */
  @InterfaceAudience.Private
  public static <T> T execute(HConnectable<T> connectable) throws IOException {
    if (connectable == null || connectable.conf == null) {
      return null;
    }
    Configuration conf = connectable.conf;
    HConnection connection = HConnectionManager.getConnection(conf);
    boolean connectSucceeded = false;
    try {
      T returnValue = connectable.connect(connection);
      connectSucceeded = true;
      return returnValue;
    } finally {
      try {
        connection.close();
      } catch (Exception e) {
        ExceptionUtil.rethrowIfInterrupt(e);
        if (connectSucceeded) {
          throw new IOException("The connection to " + connection + " could not be deleted.", e);
        }
      }
    }
  }

  /**
   * The record of errors for servers.
   */
  static class ServerErrorTracker {
    // We need a concurrent map here, as we could have multiple threads updating it in parallel.
    private final ConcurrentMap<HRegionLocation, ServerErrors> errorsByServer =
        new ConcurrentHashMap<HRegionLocation, ServerErrors>();
    private final long canRetryUntil;
    private final int maxRetries;
    private final String startTrackingTime;
    private final boolean ignoreThrottlingException;

    public ServerErrorTracker(long timeout, int maxRetries) {
      this(timeout, maxRetries, false);
    }

    public ServerErrorTracker(long timeout, int maxRetries, boolean ignoreThrottlingException) {
      this.maxRetries = maxRetries;
      this.canRetryUntil = EnvironmentEdgeManager.currentTimeMillis() + timeout;
      this.startTrackingTime = new Date().toString();
      this.ignoreThrottlingException = ignoreThrottlingException;
    }

    /**
     * We stop to retry when we have exhausted BOTH the number of retries and the time allocated.
     */
    boolean canRetryMore(int numRetry) {
      // If there is a single try we must not take into account the time.
      return numRetry < maxRetries
          || (maxRetries > 1 && EnvironmentEdgeManager.currentTimeMillis() < this.canRetryUntil);
    }

    boolean canRetryMore(int numRetry, Throwable t) {
      if (ignoreThrottlingException && (t instanceof ThrottlingException
          || t instanceof RpcThrottlingException)) {
        return true;
      }
      return canRetryMore(numRetry);
    }

    /**
     * Calculates the back-off time for a retrying request to a particular server.
     * @param server The server in question.
     * @param basePause The default hci pause.
     * @return The time to wait before sending next request.
     */
    long calculateBackoffTime(HRegionLocation server, long basePause) {
      long result;
      ServerErrors errorStats = errorsByServer.get(server);
      if (errorStats != null) {
        result = ConnectionUtils.getPauseTime(basePause, errorStats.retries.get());
      } else {
        result = 0; // yes, if the server is not in our list we don't wait before retrying.
      }
      return result;
    }

    /**
     * Reports that there was an error on the server to do whatever bean-counting necessary.
     * @param server The server in question.
     */
    void reportServerError(HRegionLocation server) {
      ServerErrors errors = errorsByServer.get(server);
      if (errors != null) {
        errors.addError();
      } else {
        errors = errorsByServer.putIfAbsent(server, new ServerErrors());
        if (errors != null) {
          errors.addError();
        }
      }
    }

    String getStartTrackingTime() {
      return startTrackingTime;
    }

    /**
     * The record of errors for a server.
     */
    private static class ServerErrors {
      public final AtomicInteger retries = new AtomicInteger(0);

      public void addError() {
        retries.incrementAndGet();
      }
    }
  }

  /**
   * Look for an exception we know in the remote exception: - hadoop.ipc wrapped exceptions - nested
   * exceptions Looks for: RegionMovedException / RegionOpeningException / RegionTooBusyException /
   * ThrottlingException / RpcThrottlingException
   * @return null if we didn't find the exception, the exception otherwise.
   */
  public static Throwable findException(Object exception) {
    if (exception == null || !(exception instanceof Throwable)) {
      return null;
    }
    Throwable cur = (Throwable) exception;
    while (cur != null) {
      if (cur instanceof RegionMovedException || cur instanceof RegionOpeningException
          || cur instanceof RegionTooBusyException || cur instanceof ThrottlingException
          || cur instanceof RpcThrottlingException || cur instanceof RetryImmediatelyException) {
        return cur;
      }
      if (cur instanceof RemoteException) {
        RemoteException re = (RemoteException) cur;
        cur = re.unwrapRemoteException(RegionOpeningException.class, RegionMovedException.class,
          RegionTooBusyException.class);
        if (cur == null) {
          cur = re.unwrapRemoteException();
        }
        // unwrapRemoteException can return the exception given as a parameter when it cannot
        // unwrap it. In this case, there is no need to look further
        // noinspection ObjectEquality
        if (cur == re) {
          return null;
        }
      } else {
        cur = cur.getCause();
      }
    }

    return null;
  }

  /**
   * Set the number of retries to use serverside when trying to communicate with another server over
   * {@link HConnection}. Used updating catalog tables, etc. Call this method before we create any
   * Connections.
   * @param c The Configuration instance to set the retries into.
   * @param log Used to log what we set in here.
   */
  public static void setServerSideHConnectionRetries(final Configuration c, final String sn,
      final Log log) {
    int hcRetries = c.getInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER,
      HConstants.DEFAULT_HBASE_CLIENT_RETRIES_NUMBER);
    // Go big. Multiply by 10. If we can't get to meta after this many retries
    // then something seriously wrong.
    int serversideMultiplier = c.getInt(HConstants.HBASE_CLIENT_SERVERSIDE_RETRIES_MULTIPLIER,
      HConstants.DEFAULT_HBASE_CLIENT_SERVERSIDE_RETRIES_MULTIPLIER);
    int retries = hcRetries * serversideMultiplier;
    c.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, retries);
    log.debug(sn + " HConnection server-to-server retries=" + retries);
  }
  
  public static final String HBASE_CLIENT_ASYNC_CONNECTION_IMPL = "hbase.client.async.connection.impl";


  /**
   * Call {@link #createAsyncConnection(Configuration)} using default HBaseConfiguration.
   * @see #createAsyncConnection(Configuration)
   * @return AsyncConnection object wrapped by CompletableFuture
   */
  public static CompletableFuture<AsyncConnection> createAsyncConnection() {
    return createAsyncConnection(HBaseConfiguration.create());
  }

  /**
   * Call {@link #createAsyncConnection(Configuration, User)} using the given {@code conf} and a
   * User object created by {@link UserProvider}. The given {@code conf} will also be used to
   * initialize the {@link UserProvider}.
   * @param conf configuration
   * @return AsyncConnection object wrapped by CompletableFuture
   * @see #createAsyncConnection(Configuration, User)
   * @see UserProvider
   */
  public static CompletableFuture<AsyncConnection> createAsyncConnection(Configuration conf) {
    User user;
    try {
      user = UserProvider.instantiate(conf).getCurrent();
    } catch (IOException e) {
      CompletableFuture<AsyncConnection> future = new CompletableFuture<>();
      future.completeExceptionally(e);
      return future;
    }
    return createAsyncConnection(conf, user);
  }

  /**
   * Create a new AsyncConnection instance using the passed {@code conf} and {@code user}.
   * AsyncConnection encapsulates all housekeeping for a connection to the cluster. All tables and
   * interfaces created from returned connection share zookeeper connection, meta cache, and
   * connections to region servers and masters.
   * <p>
   * The caller is responsible for calling {@link AsyncConnection#close()} on the returned
   * connection instance.
   * <p>
   * Usually you should only create one AsyncConnection instance in your code and use it everywhere
   * as it is thread safe.
   * @param conf configuration
   * @param user the user the asynchronous connection is for
   * @return AsyncConnection object wrapped by CompletableFuture
   * @throws IOException
   */
  public static CompletableFuture<AsyncConnection> createAsyncConnection(Configuration conf,
      User user) {
    CompletableFuture<AsyncConnection> future = new CompletableFuture<>();
    AsyncRegistry registry = AsyncRegistryFactory.getRegistry(conf);
    registry.getClusterId().whenComplete((clusterId, error) -> {
      if (error != null) {
        future.completeExceptionally(error);
        return;
      }
      if (clusterId == null) {
        future.completeExceptionally(new IOException("clusterid came back null"));
        return;
      }
      Class<? extends AsyncConnection> clazz = conf.getClass(HBASE_CLIENT_ASYNC_CONNECTION_IMPL,
        AsyncConnectionImpl.class, AsyncConnection.class);
      try {
        future.complete(ReflectionUtils.newInstance(clazz, conf, registry, clusterId, user));
      } catch (Exception e) {
        future.completeExceptionally(e);
      }
    });
    return future;
  }
}
