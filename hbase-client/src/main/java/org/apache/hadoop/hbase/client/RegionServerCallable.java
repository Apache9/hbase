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

import com.google.common.base.Throwables;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ClientService;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Implementations call a RegionServer and implement {@link #call()}. Passed to a
 * {@link RpcRetryingCaller} so we retry on fail.
 * @param <T> the class that the ServerCallable handles
 */
@InterfaceAudience.Private
public abstract class RegionServerCallable<T> implements RetryingCallable<T> {
  // Public because used outside of this package over in ipc.
  static final Log LOG = LogFactory.getLog(RegionServerCallable.class);
  protected final HConnection connection;
  protected final TableName tableName;
  protected final byte[] row;
  protected HRegionLocation location;
  private ClientService.BlockingInterface stub;
  private final HBaseRpcController controller;
  protected boolean serverHasMoreResultsContext;
  protected boolean serverHasMoreResults;

  protected final static int MIN_WAIT_DEAD_SERVER = 10000;

  /**
   * @param connection Connection to use.
   * @param tableName Table name to which <code>row</code> belongs.
   * @param row The row we want in <code>tableName</code>.
   */
  public RegionServerCallable(HConnection connection, TableName tableName, byte[] row) {
    this.connection = connection;
    this.tableName = tableName;
    this.row = row;
    this.controller = connection.getRpcControllerFactory().newController();
  }

  protected HRegionLocation getRegionLocation(TableName tableName, byte[] row, boolean reload,
      int callTimeout) throws IOException {
    HRegionLocation loc;
    try {
      loc = connection.getRegionLocationAsync(tableName, row, reload).get(callTimeout,
        TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      throw (InterruptedIOException) new InterruptedIOException().initCause(e);
    } catch (ExecutionException e) {
      Throwables.propagateIfPossible(e.getCause(), IOException.class);
      throw new IOException(e);
    } catch (TimeoutException e) {
      throw new IOException("Timeout when waiting for location, tableName=" + tableName + ", row=" +
          Bytes.toString(row) + ", reload=" + reload);
    }
    if (loc == null) {
      throw new IOException("Failed to find location, tableName=" + tableName + ", row=" +
          Bytes.toStringBinary(row) + ", reload=" + reload);
    }
    return loc;
  }

  /**
   * Prepare for connection to the server hosting region with row from tablename. Does lookup to
   * find region location and hosting server.
   * @param reload Set this to true if connection should re-find the region
   * @throws IOException e
   */
  @Override
  public void prepare(int callTimeout, boolean reload) throws IOException {
    // check table state if this is a retry
    if (reload && !tableName.equals(TableName.META_TABLE_NAME) &&
        getConnection().isTableDisabled(tableName)) {
      throw new TableNotEnabledException(tableName.getNameAsString() + " is disabled.");
    }
    this.location = getRegionLocation(tableName, row, false, callTimeout);
    setStub(getConnection().getClient(getLocation().getServerName()));
  }

  /**
   * @return {@link HConnection} instance used by this Callable.
   */
  HConnection getConnection() {
    return this.connection;
  }

  protected ClientService.BlockingInterface getStub() {
    return this.stub;
  }

  protected HBaseRpcController getController() {
    return controller;
  }

  void setStub(final ClientService.BlockingInterface stub) {
    this.stub = stub;
  }

  protected HRegionLocation getLocation() {
    return this.location;
  }

  protected void setLocation(final HRegionLocation location) {
    this.location = location;
  }

  public TableName getTableName() {
    return this.tableName;
  }

  public byte[] getRow() {
    return this.row;
  }

  @Override
  public void throwable(Throwable t, boolean retrying) {
    if (t instanceof SocketTimeoutException || t instanceof ConnectException ||
        t instanceof RetriesExhaustedException ||
        (location != null && getConnection().isDeadServer(location.getServerName()))) {
      // if thrown these exceptions, we clear all the cache entries that
      // map to that slow/dead server; otherwise, let cache miss and ask
      // hbase:meta again to find the new location
      if (this.location != null) {
        getConnection().clearCaches(location.getServerName());
      }
    } else if (location != null) {
      getConnection().updateCachedLocations(tableName, row, t, location);
    }
  }

  @Override
  public String getExceptionMessageAdditionalDetail() {
    return "row '" + Bytes.toString(row) + "' on table '" + tableName + "' at " + location;
  }

  @Override
  public long sleep(long pause, int tries) {
    return ConnectionUtils.getPauseTime(pause, tries);
  }

  /**
   * @return the HRegionInfo for the current region
   */
  public HRegionInfo getHRegionInfo() {
    if (this.location == null) {
      return null;
    }
    return this.location.getRegionInfo();
  }

  /**
   * Should the client attempt to fetch more results from this region
   * @return True if the client should attempt to fetch more results, false otherwise.
   */
  protected boolean getServerHasMoreResults() {
    assert serverHasMoreResultsContext;
    return this.serverHasMoreResults;
  }

  protected void setServerHasMoreResults(boolean serverHasMoreResults) {
    this.serverHasMoreResults = serverHasMoreResults;
  }

  /**
   * Did the server respond with information about whether more results might exist. Not guaranteed
   * to respond with older server versions
   * @return True if the server responded with information about more results.
   */
  protected boolean hasMoreResultsContext() {
    return serverHasMoreResultsContext;
  }

  protected void setHasMoreResultsContext(boolean serverHasMoreResultsContext) {
    this.serverHasMoreResultsContext = serverHasMoreResultsContext;
  }

  @Override
  public T call(int callTimeout) throws Exception {
    controller.reset();
    controller.setPriority(tableName);
    controller.setCallTimeout(callTimeout);
    return rpcCall();
  }

  protected abstract T rpcCall() throws Exception;
}
