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
package org.apache.hadoop.hbase.regionserver.wal;

import io.netty.channel.EventLoop;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;

import java.io.IOException;
import java.nio.channels.CompletionHandler;
import java.util.ArrayDeque;
import java.util.Comparator;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.wal.DefaultWALProvider;
import org.apache.hadoop.hbase.wal.DefaultWALProvider.AsyncWriter;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.htrace.NullScope;
import org.apache.htrace.Trace;
import org.apache.htrace.TraceScope;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 *
 */
@InterfaceAudience.Private
public class AsyncFSHLog extends AbstractFSHLog<AsyncWriter> {

  private static final Log LOG = LogFactory.getLog(AsyncFSHLog.class);

  private static final class Payload {

    public final FSWALEntry entry;

    public final SyncFuture sync;

    public final Promise<Void> roll;

    public Payload(FSWALEntry entry) {
      this.entry = entry;
      this.sync = null;
      this.roll = null;
    }

    public Payload(SyncFuture sync) {
      this.entry = null;
      this.sync = sync;
      this.roll = null;
    }

    public Payload(Promise<Void> roll) {
      this.entry = null;
      this.sync = null;
      this.roll = roll;
    }
  }

  private final EventLoop eventLoop;

  private final Deque<Payload> queue;

  private long nextSeqId = 1L;

  private boolean consumerScheduled;

  // new writer is created and we are waiting for old writer to be closed.
  private boolean waitingRoll;

  // writer is broken and rollWriter is needed. When true, will not replace writer directly without
  // setting waitingRoll to true.
  private boolean writerBroken;

  private final long batchSize;

  private final ExecutorService closeExecutor = Executors.newCachedThreadPool(
    new ThreadFactoryBuilder().setDaemon(true).setNameFormat("Close-WAL-Writer-%d").build());

  // We first poll FSWALEntry from queue to toWriteEntries under lock, and then poll it from
  // toWriteEntries to unackedEntries after append to Writer, and remove it from unackedEntries
  // after sync succeeded.
  private final Deque<FSWALEntry> toWriteEntries = new ArrayDeque<FSWALEntry>();

  private final Deque<FSWALEntry> unackedEntries = new ArrayDeque<FSWALEntry>();

  private final PriorityQueue<SyncFuture> futures = new PriorityQueue<SyncFuture>(11,
      SEQ_COMPARATOR);

  private Promise<Void> rollPromise;

  public AsyncFSHLog(FileSystem fs, Path rootDir, String logDir, String archiveDir,
      Configuration conf, List<WALActionsListener> listeners, boolean failIfWALExists,
      String prefix, String suffix, EventLoop eventLoop)
          throws FailedLogCloseException, IOException {
    super(fs, rootDir, logDir, archiveDir, conf, listeners, failIfWALExists, prefix, suffix);
    this.eventLoop = eventLoop;
    int maxHandlersCount = conf.getInt(HConstants.REGION_SERVER_HANDLER_COUNT, 200);
    queue = new ArrayDeque<Payload>(maxHandlersCount * 3);
    batchSize = conf.getLong(HConstants.WAL_BATCH_SIZE, HConstants.DEFAULT_WAL_BATCH_SIZE);
    rollWriter();
  }

  private void tryFinishRoll() {
    if (rollPromise != null && unackedEntries.isEmpty()) {
      rollPromise.trySuccess(null);
      rollPromise = null;
    }
  }

  private void sync(final AsyncWriter writer, final long doneSeqId) {
    writer.sync(new CompletionHandler<Long, Void>() {

      @Override
      public void completed(Long result, Void attachment) {
        highestSyncedSequence.set(doneSeqId);
        finishSync();
        for (SyncFuture future; (future = futures.peek()) != null;) {
          if (future.getRingBufferSequence() <= doneSeqId) {
            future.done(doneSeqId, null);
            futures.remove();
          } else {
            break;
          }
        }
        for (Iterator<FSWALEntry> iter = unackedEntries.iterator(); iter.hasNext();) {
          if (iter.next().getSequence() <= doneSeqId) {
            iter.remove();
          } else {
            break;
          }
        }
        tryFinishRoll();
        if (!rollWriterLock.tryLock()) {
          return;
        }
        try {
          if (writer.getLength() >= logrollsize) {
            requestLogRoll();
          }
        } finally {
          rollWriterLock.unlock();
        }
      }

      @Override
      public void failed(Throwable exc, Void attachment) {
        LOG.warn("sync failed", exc);
        for (Iterator<FSWALEntry> iter = unackedEntries.descendingIterator(); iter.hasNext();) {
          toWriteEntries.addFirst(iter.next());
        }
        tryFinishRoll();
        synchronized (queue) {
          if (writerBroken) {
            return;
          }
          writerBroken = true;
        }
        if (!rollWriterLock.tryLock()) {
          return;
        }
        try {
          requestLogRoll();
        } finally {
          rollWriterLock.unlock();
        }
      }
    }, null);
  }

  private void finishSync() {
    long doneSeqId = highestSyncedSequence.get();
    for (SyncFuture future; (future = futures.peek()) != null;) {
      if (future.getRingBufferSequence() <= doneSeqId) {
        future.done(doneSeqId, null);
        futures.remove();
      } else {
        break;
      }
    }
  }

  private void consume() {
    final AsyncWriter writer = this.writer;
    finishSync();
    if (toWriteEntries.isEmpty()) {
      tryFinishRoll();
      return;
    }
    long currentLength = writer.getLength();
    long doneSeqId = -1L;
    for (Iterator<FSWALEntry> iter = toWriteEntries.iterator(); iter.hasNext();) {
      FSWALEntry entry = iter.next();
      boolean appended;
      try {
        appended = append(writer, entry);
      } catch (IOException e) {
        throw new AssertionError("should not happen", e);
      }
      doneSeqId = entry.getSequence();
      iter.remove();
      if (appended) {
        unackedEntries.addLast(entry);
        if (writer.getLength() - currentLength >= batchSize) {
          break;
        }
      }
    }
    if (writer.getLength() > currentLength) {
      sync(writer, doneSeqId);
    } else {
      highestSyncedSequence.set(doneSeqId);
      finishSync();
    }
  }

  private static final Comparator<SyncFuture> SEQ_COMPARATOR = new Comparator<SyncFuture>() {

    @Override
    public int compare(SyncFuture o1, SyncFuture o2) {
      int c = Long.compare(o1.getRingBufferSequence(), o2.getRingBufferSequence());
      return c != 0 ? c : Integer.compare(System.identityHashCode(o1), System.identityHashCode(o2));
    }
  };

  private final Runnable consumer = new Runnable() {

    @Override
    public void run() {
      boolean localWaitingRoll;
      synchronized (queue) {
        assert consumerScheduled;
        if (writerBroken || waitingRoll) {
          consumerScheduled = false;
          // waiting for reschedule after rollWriter.
          return;
        }
        for (Payload p; (p = queue.pollFirst()) != null;) {
          if (p.entry != null) {
            toWriteEntries.addLast(p.entry);
          } else if (p.sync != null) {
            futures.add(p.sync);
          } else {
            rollPromise = p.roll;
            waitingRoll = true;
            break;
          }
        }
        localWaitingRoll = waitingRoll;
      }
      consume();
      synchronized (queue) {
        if (localWaitingRoll) {
          if (toWriteEntries.isEmpty()) {
            consumerScheduled = false;
            return;
          }
        } else {
          if (queue.isEmpty() && toWriteEntries.isEmpty()) {
            consumerScheduled = false;
            return;
          }
        }
      }
      // reschedule if we still have something to write.
      eventLoop.execute(this);
    }
  };

  private boolean shouldScheduleConsumer() {
    if (writerBroken || waitingRoll) {
      return false;
    }
    if (consumerScheduled) {
      return false;
    }
    consumerScheduled = true;
    return true;
  }

  @Override
  public long append(HTableDescriptor htd, HRegionInfo hri, WALKey key, WALEdit edits,
      boolean inMemstore) throws IOException {
    boolean scheduleTask;
    long seqId;
    synchronized (queue) {
      if (this.closed) throw new IOException("Cannot append; log is closed");
      seqId = nextSeqId++;
      FSWALEntry entry = new FSWALEntry(seqId, key, edits, htd, hri, inMemstore);
      scheduleTask = shouldScheduleConsumer();
      queue.add(new Payload(entry));
    }
    if (scheduleTask) {
      eventLoop.execute(consumer);
    }
    return seqId;
  }

  @Override
  public void sync() throws IOException {
    TraceScope scope = Trace.startSpan("FSHLog.sync");
    try {
      SyncFuture future = new SyncFuture();
      boolean scheduleTask;
      synchronized (queue) {
        scheduleTask = shouldScheduleConsumer();
        future.reset(nextSeqId - 1, scope.detach());
        queue.addLast(new Payload(future));
      }
      if (scheduleTask) {
        eventLoop.execute(consumer);
      }
      scope = Trace.continueSpan(blockOnSync(future));
    } finally {
      assert scope == NullScope.INSTANCE || !scope.isDetached();
      scope.close();
    }
  }

  @Override
  public void sync(long txid) throws IOException {
    if (highestSyncedSequence.get() >= txid) {
      return;
    }
    TraceScope scope = Trace.startSpan("FSHLog.sync");
    try {
      SyncFuture future = new SyncFuture();
      future.reset(txid, scope.detach());
      boolean scheduleTask;
      synchronized (queue) {
        scheduleTask = shouldScheduleConsumer();
        queue.addLast(new Payload(future));
      }
      if (scheduleTask) {
        eventLoop.execute(consumer);
      }
      scope = Trace.continueSpan(blockOnSync(future));
    } finally {
      assert scope == NullScope.INSTANCE || !scope.isDetached();
      scope.close();
    }
  }

  @Override
  protected AsyncWriter createWriterInstance(Path path) throws IOException {
    return DefaultWALProvider.createAsyncWriter(conf, fs, path, false, eventLoop);
  }

  private void waitForSafePoint() {
    Future<Void> roll;
    boolean scheduleTask;
    synchronized (queue) {
      if (!writerBroken && this.writer != null) {
        Promise<Void> promise = eventLoop.newPromise();
        scheduleTask = queue.isEmpty();
        queue.addLast(new Payload(promise));
        roll = promise;
      } else {
        roll = eventLoop.newSucceededFuture(null);
        scheduleTask = false;
      }
    }
    if (scheduleTask) {
      eventLoop.execute(consumer);
    }
    roll.awaitUninterruptibly();
  }

  @Override
  protected long doReplaceWriter(Path oldPath, Path newPath, AsyncWriter nextWriter)
      throws IOException {
    waitForSafePoint();
    final AsyncWriter oldWriter = this.writer;
    this.writer = nextWriter;
    boolean scheduleTask;
    synchronized (queue) {
      writerBroken = waitingRoll = false;
      if (queue.isEmpty() && toWriteEntries.isEmpty()) {
        scheduleTask = false;
      } else {
        scheduleTask = consumerScheduled = true;
      }
    }
    if (scheduleTask) {
      eventLoop.execute(consumer);
    }
    long oldFileLen;
    if (oldWriter != null) {
      oldFileLen = oldWriter.getLength();
      closeExecutor.execute(new Runnable() {

        @Override
        public void run() {
          try {
            oldWriter.close();
          } catch (IOException e) {
            LOG.warn("close old writer failed", e);
          }
        }
      });
    } else {
      oldFileLen = 0L;
    }
    return oldFileLen;
  }

  @Override
  protected void doShutdown() throws IOException {
    waitForSafePoint();
    this.writer.close();
    this.writer = null;
    closeExecutor.shutdown();
  }

  @Override
  protected void doAppend(AsyncWriter writer, FSWALEntry entry) {
    writer.append(entry);
  }
}
