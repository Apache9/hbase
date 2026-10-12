/*
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
package org.apache.hadoop.hbase.security.access;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ChoreService;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.RetryCounter;
import org.apache.hadoop.hbase.util.RetryCounterFactory;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Region server side ACL holder: owns the region server's current {@link AclSnapshot} of
 * {@code hbase:acl} and feeds it from three sources, all funneled through this class:
 * <ul>
 * <li><b>startup load</b>: {@link #reloadFromTable()} after registering with the master, non
 * superuser requests are held until it completes;</li>
 * <li><b>master pushes</b>: {@link #applyUpdates} from {@code RefreshAclRemoteCallable}; applies
 * are synchronous, so a completed refresh procedure means the change is already visible;</li>
 * <li><b>periodic reload</b>: {@link #reloadFromTable()} on an internal chore, the fallback for
 * lost pushes and for direct writes that bypass the master.</li>
 * </ul>
 * Every batch carries the master assigned timestamp of the writes it was built from; batches whose
 * timestamp is not newer than the one in the current snapshot are dropped, so stale deliveries
 * (e.g. a remote procedure replayed after a master failover) cannot regress the snapshot or
 * resurrect deleted entries. {@link #reloadFromTable()} never moves the timestamp backwards either.
 * <p>
 * Updates publish a new immutable snapshot through {@link AclView}; authorization checks pin the
 * current one and never block on this class. Applies and reloads only serialize among themselves on
 * this object: a reload holds it for the duration of its {@code hbase:acl} scan, so an update
 * arriving during the scan is applied after the swap and can never be lost to it.
 * </p>
 */
@InterfaceAudience.Private
public final class AclStore implements Closeable, AclView {

  /** Period of the periodic full reload. */
  public static final String ACL_RELOAD_PERIOD_MSEC_KEY = "hbase.security.acl.reload.period.msec";

  private static final int DEFAULT_ACL_RELOAD_PERIOD_MSEC = 5 * 60 * 1000;

  private static final Logger LOG = LoggerFactory.getLogger(AclStore.class);

  private final Configuration conf;
  private final Stoppable stopper;
  private final ScheduledChore reloadChore;

  /** Created on the first table read and reused; an ACL store does not touch the table often. */
  private Connection connection;

  private volatile AclSnapshot snapshot = AclSnapshot.empty();

  public AclStore(Configuration conf, Stoppable stopper) {
    this.conf = conf;
    this.stopper = stopper;
    this.reloadChore = new ScheduledChore("AclReloadChore", stopper,
      conf.getInt(ACL_RELOAD_PERIOD_MSEC_KEY, DEFAULT_ACL_RELOAD_PERIOD_MSEC)) {
      @Override
      protected void chore() {
        try {
          reloadFromTable();
        } catch (IOException e) {
          LOG.warn("Failed to reload ACLs from {}, keeping the current snapshot",
            PermissionStorage.ACL_TABLE_NAME, e);
        }
      }
    };
  }

  @Override
  public AclSnapshot current() {
    return snapshot;
  }

  /**
   * Loads the full ACL content of {@code hbase:acl} in the background after registering with the
   * master, retrying until it succeeds or the server stops, then calls {@code onLoaded}. Until then
   * non superuser requests are held in {@code RSRpcServices#checkOpen()}. A missing acl table
   * (fresh cluster) counts as loaded with an empty snapshot.
   */
  public void loadAsync(Runnable onLoaded) {
    Thread loader = new Thread(() -> load(onLoaded), "AclCacheLoader");
    loader.setDaemon(true);
    loader.start();
  }

  private void load(Runnable onLoaded) {
    RetryCounter retry = new RetryCounterFactory(Integer.MAX_VALUE, 1000, 1000 * 60 * 5).create();
    while (!stopper.isStopped()) {
      try {
        reloadFromTable();
        onLoaded.run();
        LOG.info("ACL snapshot loaded, applied timestamp {}", getAppliedTimestamp());
        return;
      } catch (IOException e) {
        long sleepTime = retry.getBackoffTimeAndIncrementAttempts();
        LOG.warn("Failed to load ACL snapshot; sleeping {} ms and then retrying.", sleepTime, e);
        // an interrupt only cuts the backoff short (e.g. at shutdown); loading gives up when the
        // server is stopped, not before
        try {
          Thread.sleep(sleepTime);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
        }
      }
    }
  }

  /** Schedules the periodic full reload. */
  public void start(ChoreService choreService) {
    choreService.scheduleChore(reloadChore);
  }

  /** Timestamp of the newest change applied to the current snapshot. */
  public long getAppliedTimestamp() {
    return snapshot.getTs();
  }

  /**
   * Applies a batch of incremental entry updates pushed by the master. Dropped when not newer than
   * the current snapshot, see the class javadoc. Does not block authorization checks.
   */
  public synchronized void applyUpdates(long ts, List<AclSnapshot.EntryUpdate> updates) {
    AclSnapshot current = snapshot;
    if (ts <= current.getTs()) {
      LOG.debug("Dropping stale ACL update batch: ts={}, applied={}", ts, current.getTs());
      return;
    }
    snapshot = current.applyEntryUpdates(updates, ts);
  }

  /**
   * Reads the whole {@code hbase:acl} table and swaps it in as the new snapshot, never losing a
   * concurrent update and never moving the timestamp backwards. Blocks until done. A missing acl
   * table (fresh cluster) is loaded as empty content.
   */
  public synchronized void reloadFromTable() throws IOException {
    PermissionStorage.AclTableContent content;
    try {
      content = PermissionStorage.loadAllSnapshot(getConnection());
    } catch (TableNotFoundException e) {
      content = new PermissionStorage.AclTableContent(Collections.emptyMap(), 0L);
    }
    snapshot = AclSnapshot.of(content.getPermissions(),
      Math.max(snapshot.getTs(), content.getMaxTimestamp()));
  }

  private synchronized Connection getConnection() throws IOException {
    if (connection == null) {
      connection = ConnectionFactory.createConnection(conf);
    }
    return connection;
  }

  @Override
  public synchronized void close() {
    reloadChore.cancel(true);
    if (connection != null) {
      try {
        connection.close();
      } catch (IOException e) {
        LOG.warn("Failed to close the ACL connection", e);
      }
    }
  }
}
