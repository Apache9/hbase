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
package org.apache.hadoop.hbase.zookeeper;

import static org.apache.hadoop.hbase.HConstants.DEFAULT_ZK_SESSION_TIMEOUT;
import static org.apache.hadoop.hbase.HConstants.ZK_SESSION_TIMEOUT;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

/**
 * A very simple read only zookeeper implementation without watcher support.
 * <p>
 * TODO: session expire
 */
@InterfaceAudience.Private
public final class ReadOnlyZKClient implements Closeable {

  private static final Log LOG = LogFactory.getLog(ReadOnlyZKClient.class);

  private final String connectString;

  private final int sessionTimeoutMs;

  private final int maxRetries;

  private final int retryIntervalMs;

  private final int keepAliveTimeMs;

  private static abstract class Task implements Delayed {

    public long time = System.nanoTime();

    public int retries;

    public boolean needZk() {
      return false;
    }

    public void exec(ZooKeeper zk) {
    }

    public void connectFailed(IOException e) {
    }

    public void closed(IOException e) {
    }

    @Override
    public int compareTo(Delayed o) {
      Task that = (Task) o;
      int c = Long.compare(time, that.time);
      if (c != 0) {
        return c;
      }
      return Integer.compare(System.identityHashCode(this), System.identityHashCode(that));
    }

    @Override
    public long getDelay(TimeUnit unit) {
      return unit.convert(time - System.nanoTime(), TimeUnit.NANOSECONDS);
    }

    public boolean delay(long intervalMs, int maxRetries) {
      if (retries >= maxRetries) {
        return false;
      }
      retries++;
      time = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(intervalMs);
      return true;
    }
  }

  private static final Task CLOSE = new Task() {
  };

  private final DelayQueue<Task> tasks = new DelayQueue<>();

  private ZooKeeper zookeeper;

  public ReadOnlyZKClient(Configuration conf) {
    this.connectString = ZKConfig.getZKQuorumServersString(conf);
    this.sessionTimeoutMs = conf.getInt(ZK_SESSION_TIMEOUT, DEFAULT_ZK_SESSION_TIMEOUT);
    this.maxRetries = conf.getInt("zookeeper.recovery.retry", 30);
    this.retryIntervalMs = conf.getInt("zookeeper.recovery.retry.intervalmill", 1000);
    this.keepAliveTimeMs = conf.getInt("zookeeper.keep-alive.time", 60000);
    Thread t = new Thread(this::run, "ReadOnlyZKClient");
    t.setDaemon(true);
    t.start();
  }

  private final class GetTask extends Task {

    private final String path;

    private final CompletableFuture<byte[]> future;

    public GetTask(String path, CompletableFuture<byte[]> future) {
      this.path = path;
      this.future = future;
    }

    @Override
    public boolean needZk() {
      return true;
    }

    @Override
    public void exec(ZooKeeper zk) {
      zk.getData(path, false, (rc, path, ctx, data, stat) -> {
        tasks.add(new Task() {

          @Override
          public void exec(ZooKeeper zk) {
            Code code = Code.get(rc);
            if (code == Code.OK) {
              future.complete(data);
            } else if (code == Code.NONODE) {
              future.completeExceptionally(KeeperException.create(code, path));
            } else {
              if (GetTask.this.delay(retryIntervalMs, maxRetries)) {
                LOG.warn("failed to fetch data for " + path + ", code = " + code + ", retries = " +
                    GetTask.this.retries);
                tasks.add(GetTask.this);
              } else {
                LOG.warn("failed to fetch data for " + path + ", code = " + code + ", retries = " +
                    GetTask.this.retries + ", give up");
                future.completeExceptionally(KeeperException.create(code, path));
              }
            }
          }
        });
      }, null);
    }

    @Override
    public void connectFailed(IOException e) {
      if (delay(retryIntervalMs, maxRetries)) {
        LOG.warn(
          "failed to connect to zk when fetching data for " + path + ", retries = " + retries, e);
        tasks.add(this);
      } else {
        LOG.warn("failed to connect to zk when fetching data for " + path + ", retries = " +
            retries + ", give up",
          e);
        future.completeExceptionally(e);
      }
    }

    @Override
    public void closed(IOException e) {
      future.completeExceptionally(e);
    }
  }

  public CompletableFuture<byte[]> get(String path) {
    CompletableFuture<byte[]> future = new CompletableFuture<>();
    tasks.add(new GetTask(path, future));
    return future;
  }

  private final class ExistsTask extends Task {

    private final String path;

    private final CompletableFuture<Stat> future;

    public ExistsTask(String path, CompletableFuture<Stat> future) {
      this.path = path;
      this.future = future;
    }

    @Override
    public boolean needZk() {
      return true;
    }

    @Override
    public void exec(ZooKeeper zk) {
      zk.exists(path, false, (rc, path, ctx, stat) -> {
        Code code = Code.get(rc);
        if (code == Code.OK) {
          future.complete(stat);
        } else {
          if (ExistsTask.this.delay(retryIntervalMs, maxRetries)) {
            LOG.warn("failed to check existence for " + path + ", code = " + code + ", retries = " +
                ExistsTask.this.retries);
            tasks.add(ExistsTask.this);
          } else {
            LOG.warn("failed to check existence for " + path + ", code = " + code + ", retries = " +
                ExistsTask.this.retries + ", give up");
            future.completeExceptionally(KeeperException.create(code, path));
          }
        }
      }, null);
    }

    @Override
    public void connectFailed(IOException e) {
      if (delay(retryIntervalMs, maxRetries)) {
        LOG.warn(
          "failed to connect to zk when checking existence for " + path + ", retries = " + retries,
          e);
        tasks.add(this);
      } else {
        LOG.warn("failed to connect to zk when checking existence for " + path + ", retries = " +
            retries + ", give up",
          e);
        future.completeExceptionally(e);
      }
    }

    @Override
    public void closed(IOException e) {
      future.completeExceptionally(e);
    }
  }

  public CompletableFuture<Stat> exists(String path) {
    CompletableFuture<Stat> future = new CompletableFuture<>();
    tasks.add(new ExistsTask(path, future));
    return future;
  }

  private void closeZk() {
    if (zookeeper != null) {
      try {
        zookeeper.close();
      } catch (InterruptedException e) {
      }
      zookeeper = null;
    }
  }

  private ZooKeeper getZk() throws IOException {
    if (zookeeper == null) {
      zookeeper = new ZooKeeper(connectString, sessionTimeoutMs, e -> {
      });
    }
    return zookeeper;
  }

  private void run() {
    for (;;) {
      Task task;
      try {
        task = tasks.poll(keepAliveTimeMs, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        continue;
      }
      if (task == CLOSE) {
        break;
      }
      if (task == null) {
        closeZk();
        continue;
      }
      if (!task.needZk()) {
        task.exec(null);
      } else {
        ZooKeeper zk;
        try {
          zk = getZk();
        } catch (IOException e) {
          task.connectFailed(e);
          continue;
        }
        task.exec(zk);
      }

    }
    closeZk();
    IOException error = new IOException("Client already closed");
    Arrays.stream(tasks.toArray(new Task[0])).forEach(t -> t.closed(error));
    tasks.clear();
  }

  @Override
  public void close() throws IOException {
    tasks.add(CLOSE);
  }
}
