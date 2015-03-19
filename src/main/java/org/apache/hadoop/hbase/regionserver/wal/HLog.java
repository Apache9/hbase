/**
 * Copyright 2010 The Apache Software Foundation
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
package org.apache.hadoop.hbase.regionserver.wal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.Syncable;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseFileSystem;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.ipc.CallerDisconnectedException;
import org.apache.hadoop.hbase.ipc.HBaseServer;
import org.apache.hadoop.hbase.ipc.RpcCallContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.hadoop.hbase.util.DrainBarrier;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.HasThread;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.StringUtils;

import com.google.common.collect.Lists;
import com.xiaomi.infra.hbase.trace.TracerUtils;

/**
 * HLog stores all the edits to the HStore.  Its the hbase write-ahead-log
 * implementation.
 *
 * It performs logfile-rolling, so external callers are not aware that the
 * underlying file is being rolled.
 *
 * <p>
 * There is one HLog per RegionServer.  All edits for all Regions carried by
 * a particular RegionServer are entered first in the HLog.
 *
 * <p>
 * Each HRegion is identified by a unique long <code>int</code>. HRegions do
 * not need to declare themselves before using the HLog; they simply include
 * their HRegion-id in the <code>append</code> or
 * <code>completeCacheFlush</code> calls.
 *
 * <p>
 * An HLog consists of multiple on-disk files, which have a chronological order.
 * As data is flushed to other (better) on-disk structures, the log becomes
 * obsolete. We can destroy all the log messages for a given HRegion-id up to
 * the most-recent CACHEFLUSH message from that HRegion.
 *
 * <p>
 * It's only practical to delete entire files. Thus, we delete an entire on-disk
 * file F when all of the messages in F have a log-sequence-id that's older
 * (smaller) than the most-recent CACHEFLUSH message for every HRegion that has
 * a message in F.
 *
 * <p>
 * Synchronized methods can never execute in parallel. However, between the
 * start of a cache flush and the completion point, appends are allowed but log
 * rolling is not. To prevent log rolling taking place during this period, a
 * separate reentrant lock is used.
 *
 * <p>To read an HLog, call {@link #getReader(org.apache.hadoop.fs.FileSystem,
 * org.apache.hadoop.fs.Path, org.apache.hadoop.conf.Configuration)}.
 *
 */
public class HLog implements Syncable {
  static final Log LOG = LogFactory.getLog(HLog.class);
  public static final byte [] METAFAMILY = Bytes.toBytes("METAFAMILY");
  static final byte [] METAROW = Bytes.toBytes("METAROW");

  /** File Extension used while splitting an HLog into regions (HBASE-2312) */
  public static final String SPLITTING_EXT = "-splitting";
  public static final boolean SPLIT_SKIP_ERRORS_DEFAULT = false;
  /** The META region's HLog filename extension */
  public static final String META_HLOG_FILE_EXTN = ".meta";
  public static final String SEPARATE_HLOG_FOR_META = "hbase.regionserver.separate.hlog.for.meta";
  
  /**
   * Temporary subdirectory of the region directory used for compaction output.
   */
  public static final String HLOG_TEMP_DIR = ".tmp";
  
  /*
   * Name of directory that holds recovered edits written by the wal log
   * splitting code, one per region
   */
  public static final String RECOVERED_EDITS_DIR = "recovered.edits";
  private static final Pattern EDITFILES_NAME_PATTERN =
    Pattern.compile("-?[0-9]+");
  public static final String RECOVERED_LOG_TMPFILE_SUFFIX = ".temp";
  
  private static final int DEFAULT_SLOW_SYNC_TIME = 100; // in ms

  private static final BlockingQueue<Long> fsWriteLatenciesNanos = 
      new ArrayBlockingQueue<Long>(1024 * 1024 / 8);
  
  private final FileSystem fs;
  private final Path dir;
  private final Configuration conf;
  private final HLogFileSystem hlogFs;
  // Listeners that are called on WAL events.
  private List<WALActionsListener> listeners =
    new CopyOnWriteArrayList<WALActionsListener>();
  private final long blocksize;
  private final String prefix;
  private final AtomicLong unflushedEntries = new AtomicLong(0);
  private final AtomicLong syncedTillHere = new AtomicLong(0);
  private long lastDeferredTxid;
  private final Path oldLogDir;
  private volatile boolean logRollRunning;

  // all writes pending on AsyncWrite/AsyncFlush thread with txid
  // <= failedTxid will fail by throwing asyncIOE
  private final AtomicLong failedTxid = new AtomicLong(0);
  private IOException asyncIOE = null;

  private static Class<? extends Writer> logWriterClass;
  private static Class<? extends Reader> logReaderClass;

  private final int slowSyncLogMs;
  private final AtomicInteger slowSyncRequestRollCounter = new AtomicInteger(0);
  private final int slowSyncRequestRollCountThreshold;
  private final int slowSyncRequestRollMsThreshold;
  private final int syncLogMode;
  private WALCoprocessorHost coprocessorHost;

  static void resetLogReaderClass() {
    HLog.logReaderClass = null;
  }

  // used by testing only
  static void resetLogWriterClass() {
    HLog.logWriterClass = null;
  }

  private FSDataOutputStream hdfs_out; // FSDataOutputStream associated with the current SequenceFile.writer
  // Minimum tolerable replicas, if the actual value is lower than it, 
  // rollWriter will be triggered
  private int minTolerableReplication;
  private Method getNumCurrentReplicas; // refers to DFSOutputStream.getNumCurrentReplicas
  private Method getPipeLine; // refers to DFSOutputStream.getPipeLine
  final static Object [] NO_ARGS = new Object []{};

  /** The barrier used to ensure that close() waits for all log rolls and flushes to finish. */
  private DrainBarrier closeBarrier = new DrainBarrier();
  
  public interface Reader {
    void init(FileSystem fs, Path path, Configuration c) throws IOException;
    void close() throws IOException;
    Entry next() throws IOException;
    Entry next(Entry reuse) throws IOException;
    void seek(long pos) throws IOException;
    long getPosition() throws IOException;
    void reset() throws IOException;
  }

  public interface Writer {
    void init(FileSystem fs, Path path, Configuration c) throws IOException;
    void close() throws IOException;
    void sync(boolean force) throws IOException;
    void append(Entry entry) throws IOException;
    long getLength() throws IOException;
  }

  /*
   * Current log file.
   */
  Writer writer;

  /*
   * Map of all log files but the current one.
   */
  final SortedMap<Long, Path> outputfiles = Collections.
      synchronizedSortedMap(new TreeMap<Long, Path>());

  private Lock outputfilesLock = new ReentrantLock();

  /**
   * This lock synchronizes all operations on oldestUnflushedSeqNums and oldestFlushingSeqNums, with
   * the exception of append's putIfAbsent into oldestUnflushedSeqNums. We only use these to find
   * out the low bound seqNum, or to find regions with old seqNums to force flush them, so we don't
   * care about these numbers messing with anything.
   */
  private final Object oldestSeqNumsLock = new Object();

  /**
   * This lock makes sure only one log roll runs at the same time. Should not be taken while any
   * other lock is held. We don't just use synchronized because that results in bogus and tedious
   * findbugs warning when it thinks synchronized controls writer thread safety
   */
  private final ReentrantLock rollWriterLock = new ReentrantLock(true);

  /**
   * Map of encoded region names to their oldest sequence/edit id in their
   * memstore.
   */
  private final ConcurrentSkipListMap<byte [], Long> oldestUnflushedSeqNums =
    new ConcurrentSkipListMap<byte [], Long>(Bytes.BYTES_COMPARATOR);

  /**
   * Map of encoded region names to their oldest sequence/edit id in their memstore;
   * contains the regions that are currently flushing. That way we can store two numbers for
   * flushing and non-flushing (oldestUnflushedSeqNums) memstore for the same region.
   */
  private final Map<byte[], Long> oldestFlushingSeqNums = new HashMap<byte[], Long>(); 
  
  private volatile boolean closed = false;

  private final AtomicLong logSeqNum = new AtomicLong(0);

  private boolean forMeta = false;

  // The timestamp (in ms) when the log file was created.
  private volatile long filenum = -1;

  //number of transactions in the current Hlog.
  private final AtomicInteger numEntries = new AtomicInteger(0);

  // If live datanode count is lower than the default replicas value,
  // RollWriter will be triggered in each sync(So the RollWriter will be
  // triggered one by one in a short time). Using it as a workaround to slow
  // down the roll frequency triggered by checkLowReplication().
  private AtomicInteger consecutiveLogRolls = new AtomicInteger(0);
  private final int lowReplicationRollLimit;

  // If consecutiveLogRolls is larger than lowReplicationRollLimit,
  // then disable the rolling in checkLowReplication().
  // Enable it if the replications recover.
  private volatile boolean lowReplicationRollEnabled = true;

  // If > than this size, roll the log. This is typically 0.95 times the size
  // of the default Hdfs block size.
  private final long logrollsize;

  // size of current log 
  private long curLogSize = 0;

  // The total size of hlog
  private AtomicLong totalLogSize = new AtomicLong(0);

  // We synchronize on updateLock to prevent updates and to prevent a log roll
  // during an update
  // locked during appends
  private final Object updateLock = new Object();
  private final Object pendingWritesLock = new Object();

  private final boolean enabled;

  /*
   * If more than this many logs, force flush of oldest region to oldest edit
   * goes to disk.  If too many and we crash, then will take forever replaying.
   * Keep the number of logs tidy.
   */
  private final int maxLogs;
  private static boolean isWALCompressionEnabled;

  // List of pending writes to the HLog. There corresponds to transactions
  // that have not yet returned to the client. We keep them cached here
  // instead of writing them to HDFS piecemeal, because the HDFS write-
  // method is pretty heavyweight as far as locking is concerned. The-
  // goal is to increase the batchsize for writing-to-hdfs as well as
  // sync-to-hdfs, so that we can get better system throughput.
  private List<Entry> pendingWrites = new LinkedList<Entry>();

  private final AsyncWriter    asyncWriter;
  private final AsyncSyncer[]  asyncSyncers;
  private final AsyncNotifier  asyncNotifier;

  /** Number of log close errors tolerated before we abort */
  private final int closeErrorsTolerated;

  private final AtomicInteger closeErrorCount = new AtomicInteger();

  /**
   * Pattern used to validate a HLog file name
   */
  private static final Pattern pattern = 
      Pattern.compile(".*\\.\\d*("+HLog.META_HLOG_FILE_EXTN+")*");

  private Map<Writer, Long> unclosedWriters = new HashMap<Writer, Long>();

  public static class Metric {
    public long min = Long.MAX_VALUE;
    public long max = 0;
    public long total = 0;
    public int count = 0;

    synchronized void inc(final long val) {
      min = Math.min(min, val);
      max = Math.max(max, val);
      total += val;
      ++count;
    }

    synchronized Metric get() {
      Metric copy = new Metric();
      copy.min = min;
      copy.max = max;
      copy.total = total;
      copy.count = count;
      this.min = Long.MAX_VALUE;
      this.max = 0;
      this.total = 0;
      this.count = 0;
      return copy;
    }
  }

  // For measuring latency of writes
  private static Metric writeTime = new Metric();
  private static Metric writeSize = new Metric();
  // For measuring latency of syncs
  private static Metric syncTime = new Metric();
  //For measuring slow HLog appends
  private static AtomicLong slowHLogAppendCount = new AtomicLong();
  private static Metric slowHLogAppendTime = new Metric();
  
  public static Metric getWriteTime() {
    return writeTime.get();
  }

  public static Metric getWriteSize() {
    return writeSize.get();
  }

  public static Metric getSyncTime() {
    return syncTime.get();
  }

  public static long getSlowAppendCount() {
    return slowHLogAppendCount.get();
  }
  
  public static Metric getSlowAppendTime() {
    return slowHLogAppendTime.get();
  }

  public static double getHLogCompressionRatio() {
    if (isWALCompressionEnabled) {
      long compressedSize = HLogKey.compressedSize + KeyValueCompression.compressedSize;
      long uncompressedSize = HLogKey.uncompressedSize + KeyValueCompression.uncompressedSize;
      return uncompressedSize == 0 ? 1 : (double) compressedSize / (double) uncompressedSize;
    }
    return 1;
  }

  /**
   * Constructor.
   *
   * @param fs filesystem handle
   * @param dir path to where hlogs are stored
   * @param oldLogDir path to where hlogs are archived
   * @param conf configuration to use
   * @throws IOException
   */
  public HLog(final FileSystem fs, final Path dir, final Path oldLogDir,
              final Configuration conf)
  throws IOException {
    this(fs, dir, oldLogDir, conf, null, true, null, false);
  }

  /**
   * Create an edit log at the given <code>dir</code> location.
   *
   * You should never have to load an existing log. If there is a log at
   * startup, it should have already been processed and deleted by the time the
   * HLog object is started up.
   *
   * @param fs filesystem handle
   * @param dir path to where hlogs are stored
   * @param oldLogDir path to where hlogs are archived
   * @param conf configuration to use
   * @param listeners Listeners on WAL events. Listeners passed here will
   * be registered before we do anything else; e.g. the
   * Constructor {@link #rollWriter()}.
   * @param prefix should always be hostname and port in distributed env and
   *        it will be URL encoded before being used.
   *        If prefix is null, "hlog" will be used
   * @throws IOException
   */
  public HLog(final FileSystem fs, final Path dir, final Path oldLogDir,
      final Configuration conf, final List<WALActionsListener> listeners,
      final String prefix) throws IOException {
    this(fs, dir, oldLogDir, conf, listeners, true, prefix, false);
  }

  /**
   * Create an edit log at the given <code>dir</code> location.
   *
   * You should never have to load an existing log. If there is a log at
   * startup, it should have already been processed and deleted by the time the
   * HLog object is started up.
   *
   * @param fs filesystem handle
   * @param dir path to where hlogs are stored
   * @param oldLogDir path to where hlogs are archived
   * @param conf configuration to use
   * @param listeners Listeners on WAL events. Listeners passed here will
   * be registered before we do anything else; e.g. the
   * Constructor {@link #rollWriter()}.
   * @param failIfLogDirExists If true IOException will be thrown if dir already exists.
   * @param prefix should always be hostname and port in distributed env and
   *        it will be URL encoded before being used.
   *        If prefix is null, "hlog" will be used
   * @param forMeta if this hlog is meant for meta updates
   * @throws IOException
   */
  public HLog(final FileSystem fs, final Path dir, final Path oldLogDir,
      final Configuration conf, final List<WALActionsListener> listeners,
      final boolean failIfLogDirExists, final String prefix, boolean forMeta)
  throws IOException {
    super();
    this.fs = fs;
    this.dir = dir;
    this.conf = conf;
    this.hlogFs = new HLogFileSystem(conf);
    if (listeners != null) {
      for (WALActionsListener i: listeners) {
        registerWALActionsListener(i);
      }
    }
    this.blocksize = conf.getLong("hbase.regionserver.hlog.blocksize",
        FSUtils.getDefaultBlockSize(this.fs, this.dir));
    // Roll at 95% of block size.
    float multi = conf.getFloat("hbase.regionserver.logroll.multiplier", 0.95f);
    this.logrollsize = (long)(this.blocksize * multi);
    boolean dirExists = false;
    if (failIfLogDirExists && (dirExists = this.fs.exists(dir))) {
      throw new IOException("Target HLog directory already exists: " + dir);
    }
    if (!dirExists && !HBaseFileSystem.makeDirOnFileSystem(fs, dir)) {
      throw new IOException("Unable to mkdir " + dir);
    }
    this.oldLogDir = oldLogDir;
    if (!fs.exists(oldLogDir) && !HBaseFileSystem.makeDirOnFileSystem(fs, oldLogDir)) {
      throw new IOException("Unable to mkdir " + this.oldLogDir);
    }
    this.forMeta = forMeta;
    this.maxLogs = conf.getInt("hbase.regionserver.maxlogs", 32);
    this.isWALCompressionEnabled = conf.getBoolean(HConstants.ENABLE_WAL_COMPRESSION, false);
    this.minTolerableReplication = conf.getInt(
        "hbase.regionserver.hlog.tolerable.lowreplication",
        FSUtils.getDefaultReplication(this.fs, this.dir));
    this.lowReplicationRollLimit = conf.getInt(
        "hbase.regionserver.hlog.lowreplication.rolllimit", 5);
    this.enabled = conf.getBoolean("hbase.regionserver.hlog.enabled", true);
    this.closeErrorsTolerated = conf.getInt(
        "hbase.regionserver.logroll.errors.tolerated", 0);
    
    this.slowSyncLogMs = conf.getInt("hbase.regionserver.hlog.slowsync",
      DEFAULT_SLOW_SYNC_TIME);
    LOG.info("HLog configuration: blocksize=" +
      StringUtils.byteDesc(this.blocksize) +
      ", rollsize=" + StringUtils.byteDesc(this.logrollsize) +
      ", enabled=" + this.enabled);
    // If prefix is null||empty then just name it hlog
    this.prefix = prefix == null || prefix.isEmpty() ?
        "hlog" : URLEncoder.encode(prefix, "UTF8");
    // rollWriter sets this.hdfs_out if it can.
    rollWriter();

    // handle the reflection necessary to call getNumCurrentReplicas()
    this.getNumCurrentReplicas = getGetNumCurrentReplicas(this.hdfs_out);
    this.getPipeLine = getGetPipeline(this.hdfs_out);

    final String n = Thread.currentThread().getName();

    asyncWriter = new AsyncWriter(n + "-WAL.AsyncWriter");
    asyncWriter.start();

    int syncerNums = conf.getInt("hbase.hlog.asyncer.number", 5);
    asyncSyncers = new AsyncSyncer[syncerNums];
    for (int i = 0; i < asyncSyncers.length; ++i) {
      asyncSyncers[i] = new AsyncSyncer(n + "-WAL.AsyncSyncer" + i);
      asyncSyncers[i].start();
    }

    asyncNotifier = new AsyncNotifier(n + "-WAL.AsyncNotifier");
    asyncNotifier.start();
    slowSyncRequestRollCountThreshold = conf.getInt(
      "hbase.regionserver.hlog.slowsync.request.roll.count.threshold", 10);
    slowSyncRequestRollMsThreshold = conf.getInt(
      "hbase.regionserver.hlog.slowsync.request.roll.ms.threshold", 1000);
    syncLogMode = conf.getInt("hbase.regionserver.hlog.sync.mode", 0);
    coprocessorHost = new WALCoprocessorHost(this, conf);
  }

  /**
   * Find the 'getNumCurrentReplicas' on the passed <code>os</code> stream.
   * @return Method or null.
   */
  private Method getGetNumCurrentReplicas(final FSDataOutputStream os) {
    Method m = null;
    if (os != null) {
      Class<? extends OutputStream> wrappedStreamClass = os.getWrappedStream()
          .getClass();
      try {
        m = wrappedStreamClass.getDeclaredMethod("getNumCurrentReplicas",
            new Class<?>[] {});
        m.setAccessible(true);
      } catch (NoSuchMethodException e) {
        LOG.info("FileSystem's output stream doesn't support"
            + " getNumCurrentReplicas; --HDFS-826 not available; fsOut="
            + wrappedStreamClass.getName());
      } catch (SecurityException e) {
        LOG.info("Doesn't have access to getNumCurrentReplicas on "
            + "FileSystems's output stream --HDFS-826 not available; fsOut="
            + wrappedStreamClass.getName(), e);
        m = null; // could happen on setAccessible()
      }
    }
    if (m != null) {
      LOG.info("Using getNumCurrentReplicas--HDFS-826");
    }
    return m;
  }
  
  /**
   * Find the 'getPipeline' on the passed <code>os</code> stream.
   * @return Method or null.
   */
  private Method getGetPipeline(final FSDataOutputStream os) {
    Method m = null;
    if (os != null) {
      Class<? extends OutputStream> wrappedStreamClass = os.getWrappedStream()
          .getClass();
      try {
        m = wrappedStreamClass.getDeclaredMethod("getPipeline",
          new Class<?>[] {});
        m.setAccessible(true);
      } catch (NoSuchMethodException e) {
        LOG.info("FileSystem's output stream doesn't support"
            + " getPipeline; not available; fsOut="
            + wrappedStreamClass.getName());
      } catch (SecurityException e) {
        LOG.info(
          "Doesn't have access to getPipeline on "
              + "FileSystems's output stream ; fsOut="
              + wrappedStreamClass.getName(), e);
        m = null; // could happen on setAccessible()
      }
    }
    return m;
  }
  
  public void registerWALActionsListener(final WALActionsListener listener) {
    this.listeners.add(listener);
  }

  public boolean unregisterWALActionsListener(final WALActionsListener listener) {
    return this.listeners.remove(listener);
  }

  /**
   * @return Current state of the monotonically increasing file id.
   */
  public long getFilenum() {
    return this.filenum;
  }

  /**
   * Called by HRegionServer when it opens a new region to ensure that log
   * sequence numbers are always greater than the latest sequence number of the
   * region being brought on-line.
   *
   * @param newvalue We'll set log edit/sequence number to this value if it
   * is greater than the current value.
   */
  public void setSequenceNumber(final long newvalue) {
    for (long id = this.logSeqNum.get(); id < newvalue &&
        !this.logSeqNum.compareAndSet(id, newvalue); id = this.logSeqNum.get()) {
      // This could spin on occasion but better the occasional spin than locking
      // every increment of sequence number.
      LOG.debug("Changed sequenceid from " + logSeqNum + " to " + newvalue);
    }
  }

  /**
   * @return log sequence number
   */
  public long getSequenceNumber() {
    return logSeqNum.get();
  }

  /**
   * Method used internal to this class and for tests only.
   * @return The wrapped stream our writer is using; its not the
   * writer's 'out' FSDatoOutputStream but the stream that this 'out' wraps
   * (In hdfs its an instance of DFSDataOutputStream).
   */
  // usage: see TestLogRolling.java
  OutputStream getOutputStream() {
    return this.hdfs_out.getWrappedStream();
  }

  /**
   * Roll the log writer. That is, start writing log messages to a new file.
   *
   * The implementation is synchronized in order to make sure there's one rollWriter
   * running at any given time. 
   *
   * @return If lots of logs, flush the returned regions so next time through
   * we can clean logs. Returns null if nothing to flush.  Names are actual
   * region names as returned by {@link HRegionInfo#getEncodedName()}
   * @throws org.apache.hadoop.hbase.regionserver.wal.FailedLogCloseException
   * @throws IOException
   */
  public byte [][] rollWriter() throws FailedLogCloseException, IOException {
    return rollWriter(false);
  }

  /**
   * Roll the log writer. That is, start writing log messages to a new file.
   *
   * The implementation is synchronized in order to make sure there's one rollWriter
   * running at any given time. 
   *
   * @param force If true, force creation of a new writer even if no entries
   * have been written to the current writer
   * @return If lots of logs, flush the returned regions so next time through
   * we can clean logs. Returns null if nothing to flush.  Names are actual
   * region names as returned by {@link HRegionInfo#getEncodedName()}
   * @throws org.apache.hadoop.hbase.regionserver.wal.FailedLogCloseException
   * @throws IOException
   */
  public byte [][] rollWriter(boolean force)
      throws FailedLogCloseException, IOException {
    rollWriterLock.lock();
    try {
      // Return if nothing to flush.
      if (!force && this.writer != null && this.numEntries.get() <= 0) {
        return null;
      }
    	this.outputfilesLock.lock();
      byte [][] regionsToFlush = null;
      try {
        this.logRollRunning = true;
        boolean isClosed = closed;
        if (isClosed || !closeBarrier.beginOp()) {
          LOG.debug("HLog " + (isClosed ? "closed" : "closing") + ". Skipping rolling of writer");
          return regionsToFlush;
        }
        // Do all the preparation outside of the updateLock to block
        // as less as possible the incoming writes
        long currentFilenum = this.filenum;
        Path oldPath = null;
        if (currentFilenum > 0) {
          //computeFilename  will take care of meta hlog filename
          oldPath = computeFilename(currentFilenum);
        }
        Path newPath = computeNewFilename();
        // Tell our listeners that a new log is about to be created
        if (!this.listeners.isEmpty()) {
          for (WALActionsListener i : this.listeners) {
            i.preLogRoll(oldPath, newPath);
          }
        }
        HLog.Writer nextWriter = this.createWriterInstance(fs, newPath, conf);
        // Can we get at the dfsclient outputstream?  If an instance of
        // SFLW, it'll have done the necessary reflection to get at the
        // protected field name.
        FSDataOutputStream nextHdfsOut = null;
        if (nextWriter instanceof SequenceFileLogWriter) {
          nextHdfsOut = ((SequenceFileLogWriter)nextWriter).getWriterFSDataOutputStream();
          //perform the costly allocateBlock and sync before we get the lock to roll writers
          try {
            nextWriter.sync(syncLogMode == SyncLogMode.HSYNC_ALWAYS.ordinal());
          } catch (IOException e) {
            //optimization failed, no need to abort here.
            LOG.warn("pre-sync failed", e);
          }
        }

        Path oldFile = null;
        int numEntriesSize = 0;
        synchronized (updateLock) {
          // Clean up current writer.
          oldFile = cleanupCurrentWriter(currentFilenum);
          this.writer = nextWriter;
          this.hdfs_out = nextHdfsOut;
          numEntriesSize = this.numEntries.getAndSet(0);
          slowSyncRequestRollCounter.set(0); // reset this counter after a rolling done
        }
        // Tell our listeners that a new log was created
        if (!this.listeners.isEmpty()) {
          for (WALActionsListener i : this.listeners) {
            i.postLogRoll(oldPath, newPath);
          }
        }
  			long oldFileLen = 0;
        if (oldFile != null) {
          oldFileLen = this.fs.getFileStatus(oldFile).getLen();
          this.totalLogSize.addAndGet(oldFileLen);
        }
        LOG.info((oldFile != null?
            "Roll " + FSUtils.getPath(oldFile) + ", entries=" +
            this.numEntries.get() +
            ", filesize=" + oldFileLen + ". ": "") +
            " for " + FSUtils.getPath(newPath));

        cleanupUnclosedWriters();
        // Can we delete any of the old log files?
        if (getNumLogFiles() > 0) {
          cleanOldLogs();
          regionsToFlush = getRegionsToForceFlush();
        }
      } finally {
        this.logRollRunning = false;
        this.outputfilesLock.unlock();
        closeBarrier.endOp();
      }
      return regionsToFlush;
    } finally {
      rollWriterLock.unlock();
    }
  }

  private void cleanupUnclosedWriters() {
    if (unclosedWriters.size() == 0) {
      return;
    }
    LOG.warn("There are " + unclosedWriters.size() + " unclosed writers");
    List<Writer> writers = new LinkedList<Writer>();
    for (Map.Entry<Writer, Long> entry : this.unclosedWriters.entrySet()) {
      Writer writer = entry.getKey();
      long lastAttempt = entry.getValue().longValue();
      // retry every 10 min
      if (EnvironmentEdgeManager.currentTimeMillis() - lastAttempt > 1000 * 60 * 10) {
        writers.add(writer);
      }
    }
    LOG.warn("Try to cleanup " + writers.size() + " unclosed writers");
    for (Writer writer : writers) {
      try {
        writer.close();
        unclosedWriters.remove(writer);
      } catch (IOException e) {
        LOG.error("Cleanup unclosed writer failed.", e);
        unclosedWriters.put(writer, EnvironmentEdgeManager.currentTimeMillis());
      }
    }
  }

  /**
   * This method allows subclasses to inject different writers without having to
   * extend other methods like rollWriter().
   * 
   * @param fs
   * @param path
   * @param conf
   * @return Writer instance
   * @throws IOException
   */
  protected Writer createWriterInstance(final FileSystem fs, final Path path,
      final Configuration conf) throws IOException {
    if (forMeta) {
      //TODO: set a higher replication for the hlog files (HBASE-6773)
    }
    return this.hlogFs.createWriter(fs, conf, path);
  }

  /**
   * Get a reader for the WAL.
   * The proper way to tail a log that can be under construction is to first use this method
   * to get a reader then call {@link HLog.Reader#reset()} to see the new data. It will also
   * take care of keeping implementation-specific context (like compression).
   * @param fs
   * @param path
   * @param conf
   * @return A WAL reader.  Close when done with it.
   * @throws IOException
   */
  public static Reader getReader(final FileSystem fs, final Path path,
                                 Configuration conf)
      throws IOException {
    try {

      if (logReaderClass == null) {

        logReaderClass = conf.getClass("hbase.regionserver.hlog.reader.impl",
            SequenceFileLogReader.class, Reader.class);
      }


      HLog.Reader reader = logReaderClass.newInstance();
      reader.init(fs, path, conf);
      return reader;
    } catch (IOException e) {
      throw e;
    }
    catch (Exception e) {
      throw new IOException("Cannot get log reader", e);
    }
  }

  /**
   * Get a writer for the WAL.
   * @param path
   * @param conf
   * @return A WAL writer.  Close when done with it.
   * @throws IOException
   */
  public static Writer createWriter(final FileSystem fs,
      final Path path, Configuration conf)
  throws IOException {
    try {
      if (logWriterClass == null) {
        logWriterClass = conf.getClass("hbase.regionserver.hlog.writer.impl",
            SequenceFileLogWriter.class, Writer.class);
      }
      HLog.Writer writer = (HLog.Writer) logWriterClass.newInstance();
      writer.init(fs, path, conf);
      return writer;
    } catch (Exception e) {
      throw new IOException("cannot get log writer", e);
    }
  }

  /*
   * Clean up old commit logs.
   * @return If lots of logs, flush the returned region so next time through
   * we can clean logs. Returns null if nothing to flush.  Returns array of
   * encoded region names to flush.
   * @throws IOException
   */
  private void cleanOldLogs() throws IOException {
    long oldestOutstandingSeqNum = Long.MAX_VALUE;
    synchronized (oldestSeqNumsLock) {
      Long oldestFlushing = (oldestFlushingSeqNums.size() > 0) ? 
          Collections.min(oldestFlushingSeqNums.values()) : Long.MAX_VALUE;
      Long oldestUnflushed = (oldestUnflushedSeqNums.size() > 0) ? 
          Collections.min(oldestUnflushedSeqNums.values()) : Long.MAX_VALUE;
      oldestOutstandingSeqNum = Math.min(oldestFlushing, oldestUnflushed);
    }
    
    // Get the set of all log files whose last sequence number is smaller than
    // the oldest edit's sequence number.
    TreeSet<Long> sequenceNumbers =
      new TreeSet<Long>(this.outputfiles.headMap(
        (Long.valueOf(oldestOutstandingSeqNum))).keySet());
    // Now remove old log files (if any)
    if (LOG.isDebugEnabled()) {
      if (sequenceNumbers.size() > 0) {
        // Find associated region; helps debugging.
        //byte [] oldestRegion = getOldestRegion(oldestOutstandingSeqNum);
        LOG.debug("Found " + sequenceNumbers.size() + " hlogs to remove" +
          " out of total " + this.outputfiles.size() + ";" +
          " oldest outstanding sequenceid is " + oldestOutstandingSeqNum);
      }
    }
    for (Long seq : sequenceNumbers) {
      archiveLogFile(this.outputfiles.remove(seq), seq);
    }
  }

  /**
   * Return regions that have edits that are equal or less than a certain sequence number.
   * Static due to some old unit test.
   * @param walSeqNum The sequence number to compare with.
   * @param regionsToSeqNums Encoded region names to sequence ids
   * @return All regions whose seqNum <= walSeqNum. Null if no regions found.
   */
  static byte[][] findMemstoresWithEditsEqualOrOlderThan(
      final long walSeqNum, final Map<byte[], Long> regionsToSeqNums) {
    List<byte[]> regions = null;
    for (Map.Entry<byte[], Long> e : regionsToSeqNums.entrySet()) {
      if (e.getValue().longValue() <= walSeqNum) {
        if (regions == null) regions = new ArrayList<byte[]>();
        regions.add(e.getKey());
      }
    }
    return regions == null ? null : regions
        .toArray(new byte[][] { HConstants.EMPTY_BYTE_ARRAY });
  }

  private byte[][] getRegionsToForceFlush() throws IOException {
    // If too many log files, figure which regions we need to flush.
    // Array is an array of encoded region names.
    byte [][] regions = null;
    int logCount = this.outputfiles == null? 0: this.outputfiles.size();
    if (logCount > this.maxLogs && logCount > 0) {
      // This is an array of encoded region names.
      synchronized (oldestSeqNumsLock) {
        regions = findMemstoresWithEditsEqualOrOlderThan(this.outputfiles.firstKey(),
              this.oldestUnflushedSeqNums);
      }
      if (regions != null) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < regions.length; i++) {
          if (i > 0) sb.append(", ");
          sb.append(Bytes.toStringBinary(regions[i]));
        }
        LOG.info("Too many hlogs: logs=" + logCount + ", maxlogs=" +
           this.maxLogs + "; forcing flush of " + regions.length + " regions(s): " +
           sb.toString());
      }
    }
    return regions;
  }

  /*
   * Cleans up current writer closing and adding to outputfiles.
   * Presumes we're operating inside an updateLock scope.
   * @return Path to current writer or null if none.
   * @throws IOException
   */
  Path cleanupCurrentWriter(final long currentfilenum) throws IOException {
    Path oldFile = null;
    if (this.writer != null) {
      try {
        // Wait till all current transactions are written to the hlog.
        // No new transactions can occur because we have the updatelock.
        if (this.unflushedEntries.get() != this.syncedTillHere.get()) {
          LOG.debug("cleanupCurrentWriter " +
                   " waiting for transactions to get synced " +
                   " total " + this.unflushedEntries.get() +
                   " synced till here " + this.syncedTillHere.get());
          sync();
        }
      } catch (IOException e) {
        LOG.error("Sync before closing current writer failed!", e);
      }
      // Close the current writer, get a new one.
      try {
        this.writer.close();
        closeErrorCount.set(0);
      } catch (IOException e) {
        LOG.error("Failed close of HLog writer", e);
        int errors = closeErrorCount.incrementAndGet();
        if (errors <= closeErrorsTolerated && !hasDeferredEntries()) {
          unclosedWriters.put(this.writer, EnvironmentEdgeManager.currentTimeMillis());
          LOG.warn("Riding over HLog close failure! error count="+errors);
        } else {
          if (hasDeferredEntries()) {
            LOG.error("Aborting due to unflushed edits in HLog");
          }
          // Failed close of log file.  Means we're losing edits.  For now,
          // shut ourselves down to minimize loss.  Alternative is to try and
          // keep going.  See HBASE-930.
          FailedLogCloseException flce =
            new FailedLogCloseException("#" + currentfilenum);
          flce.initCause(e);
          throw flce;
        }
      }
      this.writer = null;
      if (currentfilenum >= 0) {
        oldFile = computeFilename(currentfilenum);
        this.outputfiles.put(Long.valueOf(this.logSeqNum.get()), oldFile);
      }
    }
    return oldFile;
  }

  private void archiveLogFile(final Path p, final Long seqno) throws IOException {
    Path newPath = getHLogArchivePath(this.oldLogDir, p);
    LOG.info("moving old hlog file " + FSUtils.getPath(p) +
      " whose highest sequenceid is " + seqno + " to " +
      FSUtils.getPath(newPath));
    
    // Tell our listeners that a log is going to be archived.
    if (!this.listeners.isEmpty()) {
      for (WALActionsListener i : this.listeners) {
        i.preLogArchive(p, newPath);
      }
    }
    if (!HBaseFileSystem.renameAndSetModifyTime(this.fs, p, newPath)) {
      throw new IOException("Unable to rename " + p + " to " + newPath);
    }
    // Tell our listeners that a log has been archived.
    if (!this.listeners.isEmpty()) {
      for (WALActionsListener i : this.listeners) {
        i.postLogArchive(p, newPath);
      }
    }
  }

  /**
   * This is a convenience method that computes a new filename with a larger file-number
   * @return Path
   */
  public synchronized Path computeNewFilename() {
    long tmp = System.currentTimeMillis();
    if (tmp <= this.filenum) {
      this.filenum = this.filenum + 1;
    } else {
      this.filenum = tmp;
    }
    return computeFilename(this.filenum);
  }

  /**
   * This is a convenience method that computes a new filename with a given
   * using the current HLog file-number
   * @return Path
   */
  public synchronized Path computeFilename() {
    return computeFilename(this.filenum);
  }

  /**
   * This is a convenience method that computes a new filename with a given
   * file-number.
   * @param filenum to use
   * @return Path
   */
  protected Path computeFilename(long filenum) {
    if (filenum < 0) {
      throw new RuntimeException("hlog file number can't be < 0");
    }
    String child = prefix + "." + filenum;
    if (forMeta) {
      child += HLog.META_HLOG_FILE_EXTN;
    }
    return new Path(dir, child);
  }

  public static boolean isMetaFile(Path p) {
    if (p.getName().endsWith(HLog.META_HLOG_FILE_EXTN)) {
      return true;
    }
    return false;
  }

  /**
   * Shut down the log and delete the log directory
   *
   * @throws IOException
   */
  public void closeAndDelete() throws IOException {
    close();
    if (!fs.exists(this.dir)) return;
    FileStatus[] files = fs.listStatus(this.dir);
    for(FileStatus file : files) {

      Path p = getHLogArchivePath(this.oldLogDir, file.getPath());
      // Tell our listeners that a log is going to be archived.
      if (!this.listeners.isEmpty()) {
        for (WALActionsListener i : this.listeners) {
          i.preLogArchive(file.getPath(), p);
        }
      }
      if (!HBaseFileSystem.renameAndSetModifyTime(fs, file.getPath(), p)) {
        throw new IOException("Unable to rename " + file.getPath() + " to " + p);
      }
      // Tell our listeners that a log was archived.
      if (!this.listeners.isEmpty()) {
        for (WALActionsListener i : this.listeners) {
          i.postLogArchive(file.getPath(), p);
        }
      }
    }
    LOG.debug("Moved " + files.length + " log files to " +
      FSUtils.getPath(this.oldLogDir));
    if (!HBaseFileSystem.deleteDirFromFileSystem(fs, dir)) {
      LOG.info("Unable to delete " + dir);
    }
  }

  /**
   * Shut down the log.
   *
   * @throws IOException
   */
  public void close() throws IOException {
    if (this.closed) {
      return;
    }

    try {
      asyncNotifier.interrupt();
      asyncNotifier.join();
    } catch (InterruptedException e) {
      LOG.error("Exception while waiting for " + asyncNotifier.getName() +
          " threads to die", e);
    }

    for (int i = 0; i < asyncSyncers.length; ++i) {
      try {
        asyncSyncers[i].interrupt();
        asyncSyncers[i].join();
      } catch (InterruptedException e) {
        LOG.error("Exception while waiting for " + asyncSyncers[i].getName() +
            " threads to die", e);
      }
    }

    try {
      asyncWriter.interrupt();
      asyncWriter.join();
    } catch (InterruptedException e) {
      LOG.error("Exception while waiting for " + asyncWriter.getName() +
          " thread to die", e);
    }

    try {
      // Prevent all further flushing and rolling.
      closeBarrier.stopAndDrainOps();
    } catch (InterruptedException e) {
      LOG.error("Exception while waiting for cache flushes and log rolls", e);
      Thread.currentThread().interrupt();
    }

    // Tell our listeners that the log is closing
    if (!this.listeners.isEmpty()) {
      for (WALActionsListener i : this.listeners) {
        i.logCloseRequested();
      }
    }
    synchronized (updateLock) {
      this.closed = true;
      if (LOG.isDebugEnabled()) {
        LOG.debug("closing hlog writer in " + this.dir.toString());
      }
      if (this.writer != null) {
        this.writer.close();
      }
    }
  }

  /**
   * @param now
   * @param regionName
   * @param tableName
   * @param clusterId
   * @return New log key.
   */
  protected HLogKey makeKey(byte[] regionName, byte[] tableName, long seqnum,
      long now, UUID clusterId) {
    return new HLogKey(regionName, tableName, seqnum, now, clusterId);
  }


  /** Append an entry to the log.
   *
   * @param regionInfo
   * @param logEdit
   * @param logKey
   * @param doSync shall we sync after writing the transaction
   * @return The txid of this transaction
   * @throws IOException
   */
  public long append(HRegionInfo regionInfo, HLogKey logKey, WALEdit logEdit,
                     HTableDescriptor htd, boolean doSync)
  throws IOException {
    if (this.closed) {
      throw new IOException("Cannot append; log is closed");
    }
    long txid = 0;
    synchronized (updateLock) {
      long seqNum = obtainSeqNum();
      logKey.setLogSeqNum(seqNum);
      // The 'lastSeqWritten' map holds the sequence number of the oldest
      // write for each region (i.e. the first edit added to the particular
      // memstore). When the cache is flushed, the entry for the
      // region being flushed is removed. 
      this.oldestUnflushedSeqNums.putIfAbsent(regionInfo.getEncodedNameAsBytes(),
        Long.valueOf(seqNum));

      synchronized (pendingWritesLock) {
        doWrite(regionInfo, logKey, logEdit, htd);
        txid = this.unflushedEntries.incrementAndGet();
      }
      this.asyncWriter.setPendingTxid(txid);
      this.numEntries.incrementAndGet();
      if (htd.isDeferredLogFlush()) {
        lastDeferredTxid = txid;
      }
    }

    // Sync if catalog region, and if not then check if that table supports
    // deferred log flushing
    if (doSync &&
        (regionInfo.isMetaRegion() ||
        !htd.isDeferredLogFlush())) {
      // sync txn to file system
      this.sync(txid);
    }
    return txid;
  }

  /**
   * Only used in tests.
   *
   * @param info
   * @param tableName
   * @param edits
   * @param now
   * @param htd
   * @throws IOException
   */
  public void append(HRegionInfo info, byte [] tableName, WALEdit edits,
    final long now, HTableDescriptor htd)
  throws IOException {
    append(info, tableName, edits, HConstants.DEFAULT_CLUSTER_ID, now, htd);
  }

  /**
   * Append a set of edits to the log. Log edits are keyed by (encoded)
   * regionName, rowname, and log-sequence-id.
   *
   * Later, if we sort by these keys, we obtain all the relevant edits for a
   * given key-range of the HRegion (TODO). Any edits that do not have a
   * matching COMPLETE_CACHEFLUSH message can be discarded.
   *
   * <p>
   * Logs cannot be restarted once closed, or once the HLog process dies. Each
   * time the HLog starts, it must create a new log. This means that other
   * systems should process the log appropriately upon each startup (and prior
   * to initializing HLog).
   *
   * synchronized prevents appends during the completion of a cache flush or for
   * the duration of a log roll.
   *
   * @param info
   * @param tableName
   * @param edits
   * @param clusterId The originating clusterId for this edit (for replication)
   * @param now
   * @param doSync shall we sync?
   * @return txid of this transaction
   * @throws IOException
   */
  private long append(HRegionInfo info, byte [] tableName, WALEdit edits, UUID clusterId,
      final long now, HTableDescriptor htd, boolean doSync)
    throws IOException {
      if (edits.isEmpty()) return this.unflushedEntries.get();;
      if (this.closed) {
        throw new IOException("Cannot append; log is closed");
      }
      TracerUtils.addAnnotation("Start write wal logs");
      long txid = 0;
      synchronized (this.updateLock) {
        long seqNum = obtainSeqNum();
        // The 'lastSeqWritten' map holds the sequence number of the oldest
        // write for each region (i.e. the first edit added to the particular
        // memstore). . When the cache is flushed, the entry for the
        // region being flushed is removed.
        // Use encoded name.  Its shorter, guaranteed unique and a subset of
        // actual name.
        byte [] encodedRegionName = info.getEncodedNameAsBytes();
        this.oldestUnflushedSeqNums.putIfAbsent(encodedRegionName, seqNum);
        HLogKey logKey = makeKey(encodedRegionName, tableName, seqNum, now, clusterId);
        synchronized (pendingWritesLock) {
          doWrite(info, logKey, edits, htd);
          txid = this.unflushedEntries.incrementAndGet();
        }
        this.asyncWriter.setPendingTxid(txid);
        this.numEntries.incrementAndGet();
        if (htd.isDeferredLogFlush()) {
          lastDeferredTxid = txid;
        }
      }
      // Sync if catalog region, and if not then check if that table supports
      // deferred log flushing
      if (doSync && 
          (info.isMetaRegion() ||
          !htd.isDeferredLogFlush())) {
        TracerUtils.addAnnotation("Start sync wal logs");
        // sync txn to file system
        this.sync(txid);
        TracerUtils.addAnnotation("Finish write wal logs");
      }
      return txid;
    }

  /**
   * Append a set of edits to the log. Log edits are keyed by (encoded)
   * regionName, rowname, and log-sequence-id. The HLog is not flushed
   * after this transaction is written to the log.
   *
   * @param info
   * @param tableName
   * @param edits
   * @param clusterId The originating clusterId for this edit (for replication)
   * @param now
   * @return txid of this transaction
   * @throws IOException
   */
  public long appendNoSync(HRegionInfo info, byte [] tableName, WALEdit edits, 
    UUID clusterId, final long now, HTableDescriptor htd)
    throws IOException {
    return append(info, tableName, edits, clusterId, now, htd, false);
  }

  /**
   * Append a set of edits to the log. Log edits are keyed by (encoded)
   * regionName, rowname, and log-sequence-id. The HLog is flushed
   * after this transaction is written to the log.
   *
   * @param info
   * @param tableName
   * @param edits
   * @param clusterId The originating clusterId for this edit (for replication)
   * @param now
   * @return txid of this transaction
   * @throws IOException
   */
  public long append(HRegionInfo info, byte [] tableName, WALEdit edits, 
    UUID clusterId, final long now, HTableDescriptor htd)
    throws IOException {
    return append(info, tableName, edits, clusterId, now, htd, true);
  }

  /* The work of current write process of HLog goes as below:
   * 1). All write handler threads append edits to HLog's local pending buffer;
   *     (it notifies AsyncWriter thread that there is new edits in local buffer)
   * 2). All write handler threads wait in HLog.syncer() function for underlying threads to
   *     finish the sync that contains its txid;
   * 3). An AsyncWriter thread is responsible for retrieving all edits in HLog's
   *     local pending buffer and writing to the hdfs (hlog.writer.append);
   *     (it notifies AsyncSyncer threads that there is new writes to hdfs which needs a sync)
   * 4). AsyncSyncer threads are responsible for issuing sync request to hdfs to persist the
   *     writes by AsyncWriter; (they notify the AsyncNotifier thread that sync is done)
   * 5). An AsyncNotifier thread is responsible for notifying all pending write handler
   *     threads which are waiting in the HLog.syncer() function
   * 6). No LogSyncer thread any more (since there is always AsyncWriter/AsyncFlusher threads
   *     do the same job it does)
   * note: more than one AsyncSyncer threads are needed here to guarantee good enough performance
   *       when less concurrent write handler threads. since sync is the most time-consuming
   *       operation in the whole write process, multiple AsyncSyncer threads can provide better
   *       parallelism of sync to get better overall throughput
   */
  // thread to write locally buffered writes to HDFS
  private class AsyncWriter extends HasThread {
    private long pendingTxid = 0;
    private long txidToWrite = 0;
    private long lastWrittenTxid = 0;
    private Object writeLock = new Object();

    public AsyncWriter(String name) {
      super(name);
    }

    // wake up (called by (write) handler thread) AsyncWriter thread
    // to write buffered writes to HDFS
    public void setPendingTxid(long txid) {
      synchronized (this.writeLock) {
        if (txid <= this.pendingTxid)
          return;

        this.pendingTxid = txid;
        this.writeLock.notify();
      }
    }

    public void run() {
      try {
        while (!this.isInterrupted()) {
          // 1. wait until there is new writes in local buffer
          synchronized (this.writeLock) {
            while (this.pendingTxid <= this.lastWrittenTxid) {
              this.writeLock.wait();
            }
          }

          // 2. get all buffered writes and update 'real' pendingTxid
          //    since maybe newer writes enter buffer as AsyncWriter wakes
          //    up and holds the lock
          // NOTE! can't hold 'updateLock' here since rollWriter will pend
          // on 'sync()' with 'updateLock', but 'sync()' will wait for
          // AsyncWriter/AsyncSyncer/AsyncNotifier series. without updateLock
          // can leads to pendWrites more than pendingTxid, but not problem
          List<Entry> pendWrites = null;
          synchronized (pendingWritesLock) {
            this.txidToWrite = unflushedEntries.get();
            pendWrites = pendingWrites;
            pendingWrites = new LinkedList<Entry>();
          }

          // 3. write all buffered writes to HDFS(append, without sync)
          try {
            for (Entry e : pendWrites) {
              writer.append(e);
            }
          } catch(IOException e) {
            LOG.error("Error while AsyncWriter write, request close of hlog ", e);
            requestLogRoll(true);

            asyncIOE = e;
            failedTxid.set(this.txidToWrite);
          }

          // 4. update 'lastWrittenTxid' and notify AsyncSyncer to do 'sync'
          this.lastWrittenTxid = this.txidToWrite;
          boolean hasIdleSyncer = false;
          for (int i = 0; i < asyncSyncers.length; ++i) {
            if (!asyncSyncers[i].isSyncing()) {
              hasIdleSyncer = true;
              asyncSyncers[i].setWrittenTxid(this.lastWrittenTxid);
              break;
            }
          }
          if (!hasIdleSyncer) {
            int idx = (int)(this.lastWrittenTxid % asyncSyncers.length);
            asyncSyncers[idx].setWrittenTxid(this.lastWrittenTxid);
          }
        }
      } catch (InterruptedException e) {
        LOG.debug(getName() + " interrupted while waiting for " +
            "newer writes added to local buffer");
      } catch (Exception e) {
        LOG.error("UNEXPECTED", e);
      } finally {
        LOG.info(getName() + " exiting");
      }
    }
  }

  // thread to request HDFS to sync the WALEdits written by AsyncWriter
  // to make those WALEdits durable on HDFS side
  private class AsyncSyncer extends HasThread {
    private long writtenTxid = 0;
    private long txidToSync = 0;
    private long lastSyncedTxid = 0;
    private volatile boolean isSyncing = false;
    private Object syncLock = new Object();

    public AsyncSyncer(String name) {
      super(name);
    }

    public boolean isSyncing() {
      return this.isSyncing;
    }

    // wake up (called by AsyncWriter thread) AsyncSyncer thread
    // to sync(flush) writes written by AsyncWriter in HDFS
    public void setWrittenTxid(long txid) {
      synchronized (this.syncLock) {
        if (txid <= this.writtenTxid)
          return;

        this.writtenTxid = txid;
        this.syncLock.notify();
      }
    }

    public void run() {
      try {
        while (!this.isInterrupted()) {
          // 1. wait until AsyncWriter has written data to HDFS and
          //    called setWrittenTxid to wake up us
          synchronized (this.syncLock) {
            while (this.writtenTxid <= this.lastSyncedTxid) {
              this.syncLock.wait();
            }
            this.txidToSync = this.writtenTxid;
          }

          // if this syncer's writes have been synced by other syncer:
          // 1. just set lastSyncedTxid
          // 2. don't do real sync, don't notify AsyncNotifier, don't logroll check
          // regardless of whether the writer is null or not
          if (this.txidToSync <= syncedTillHere.get()) {
            this.lastSyncedTxid = this.txidToSync;
            continue;
          }

          // 2. do 'sync' to HDFS to provide durability
          try {
            if (writer == null) {
              // the only possible case where writer == null is as below:
              // 1. t1: AsyncWriter append writes to hdfs,
              //        envokes AsyncSyncer 1 with writtenTxid==100
              // 2. t2: AsyncWriter append writes to hdfs,
              //        envokes AsyncSyncer 2 with writtenTxid==200
              // 3. t3: rollWriter starts, it grabs the updateLock which
              //        prevents further writes entering pendingWrites and
              //        wait for all items(200) in pendingWrites to append/sync
              //        to hdfs
              // 4. t4: AsyncSyncer 2 finishes, now syncedTillHere==200
              // 5. t5: rollWriter close writer, set writer=null...
              // 6. t6: AsyncSyncer 1 starts to use writer to do sync... before
              //        rollWriter set writer to the newly created Writer
              //
              // Now writer == null and txidToSync > syncedTillHere here:
              // we need fail all the writes with txid <= txidToSync to avoid
              // 'data loss' where user get successful write response but can't
              // read the writes!
              LOG.fatal("should never happen: has unsynced writes but writer is null!");
              asyncIOE = new IOException("has unsynced writes but writer is null!");
              failedTxid.set(this.txidToSync);
            } else {
              this.isSyncing = true;
              long startTimeNs = System.nanoTime();
              writer.sync(syncLogMode == SyncLogMode.HSYNC_ALWAYS.ordinal());
              long syncNs = System.nanoTime() - startTimeNs;
              syncTime.inc(syncNs);
              int syncMs = (int) (syncNs / (1000 * 1000L));
              this.isSyncing = false;
              if (syncMs > slowSyncLogMs) {
                DatanodeInfo[] pipeline = getPipeLine();
                LOG.info("Slow sync cost: " + syncMs + "ms, Pipeline: "
                    + Arrays.toString(pipeline));
              }
              if (syncMs > slowSyncRequestRollMsThreshold) {
                long newCount = slowSyncRequestRollCounter.incrementAndGet();
                if (newCount >= slowSyncRequestRollCountThreshold) {
                  requestLogRoll(false);
                  // slowSyncRequestRollCounter.set(0);
                }
              }
            }
          } catch (IOException e) {
            LOG.fatal("Error while AsyncSyncer sync, request close of hlog ", e);
            requestLogRoll(true);

            asyncIOE = e;
            failedTxid.set(this.txidToSync);

            this.isSyncing = false;
          }
          // TODO: metrics for sync time?

          // 3. wake up AsyncNotifier to notify(wake-up) all pending 'put'
          // handler threads on 'sync()'
          this.lastSyncedTxid = this.txidToSync;
          asyncNotifier.setFlushedTxid(this.lastSyncedTxid);

          // 4. check and do logRoll if needed
          boolean logRollNeeded = false;
          if (rollWriterLock.tryLock()) {
            try {
              logRollNeeded = checkLowReplication();
            } finally {
              rollWriterLock.unlock();
            }
            try {
              if (logRollNeeded || writer != null && writer.getLength() > logrollsize) {
                requestLogRoll(true);
              }
            } catch (IOException e) {
              LOG.warn("writer.getLength() failed,this failure won't block here");
            }
          }
        }
      } catch (InterruptedException e) {
        LOG.debug(getName() + " interrupted while waiting for " +
            "notification from AsyncWriter thread");
      } catch (Exception e) {
        LOG.error("UNEXPECTED", e);
      } finally {
        LOG.info(getName() + " exiting");
      }
    }
  }

  // thread to notify all write handler threads which are pending on
  // their written WALEdits' durability(sync)
  // why an extra 'notifier' thread is needed rather than letting
  // AsyncSyncer thread itself notifies when sync is done is to let
  // AsyncSyncer thread do next sync as soon as possible since 'notify'
  // has heavy synchronization with all pending write handler threads
  private class AsyncNotifier extends HasThread {
    private long flushedTxid = 0;
    private long lastNotifiedTxid = 0;
    private Object notifyLock = new Object();

    public AsyncNotifier(String name) {
      super(name);
    }

    public void setFlushedTxid(long txid) {
      synchronized (this.notifyLock) {
        if (txid <= this.flushedTxid) {
          return;
        }

        this.flushedTxid = txid;
        this.notifyLock.notify();
      }
    }

    public void run() {
      try {
        while (!this.isInterrupted()) {
          synchronized (this.notifyLock) {
            while (this.flushedTxid <= this.lastNotifiedTxid) {
              this.notifyLock.wait();
            }
            this.lastNotifiedTxid = this.flushedTxid;
          }

          // notify(wake-up) all pending (write) handler thread
          // (or logroller thread which also may pend on sync())
          synchronized (syncedTillHere) {
            syncedTillHere.set(this.lastNotifiedTxid);
            syncedTillHere.notifyAll();
          }
        }
      } catch (InterruptedException e) {
        LOG.debug(getName() + " interrupted while waiting for " +
            " notification from AsyncSyncer thread");
      } catch (Exception e) {
        LOG.error("UNEXPECTED", e);
      } finally {
        LOG.info(getName() + " exiting");
      }
    }
  }

  // sync all known transactions
  private void syncer() throws IOException {
    syncer(this.unflushedEntries.get()); // sync all pending items
  }

  // sync all transactions upto the specified txid
  private void syncer(long txid) throws IOException {
    RpcCallContext rpcCall = HBaseServer.getCurrentCall();
    synchronized (this.syncedTillHere) {
      while (this.syncedTillHere.get() < txid) {
        try {
          this.syncedTillHere.wait();
          if (rpcCall != null) {
            try {
              rpcCall.throwExceptionIfCallerDisconnected();
            } catch (CallerDisconnectedException cde) {
              LOG.info("", cde);
              throw cde; 
            }
          }
        } catch (InterruptedException e) {
          LOG.debug("interrupted while waiting for notification from AsyncNotifier");
        }
      }
      if (txid <= this.failedTxid.get()) {
        assert asyncIOE != null :
          "current txid is among(under) failed txids, but asyncIOE is null!";
        throw asyncIOE;
      }
    }
  }

  /*
   * @return whether log roll should be requested
   */
  private boolean checkLowReplication() {
    boolean logRollNeeded = false;
    // if the number of replicas in HDFS has fallen below the configured
    // value, then roll logs.
    try {
      int numCurrentReplicas = getLogReplication();
      if (numCurrentReplicas != 0
          && numCurrentReplicas < this.minTolerableReplication) {
        if (this.lowReplicationRollEnabled) {
          if (this.consecutiveLogRolls.get() < this.lowReplicationRollLimit) {
            LOG.warn("HDFS pipeline error detected. " + "Found "
                + numCurrentReplicas + " replicas but expecting no less than "
                + this.minTolerableReplication + " replicas. "
                + " Requesting close of hlog.");
            logRollNeeded = true;
            // If rollWriter is requested, increase consecutiveLogRolls. Once it
            // is larger than lowReplicationRollLimit, disable the
            // LowReplication-Roller
            this.consecutiveLogRolls.getAndIncrement();
          } else {
            LOG.warn("Too many consecutive RollWriter requests, it's a sign of "
                + "the total number of live datanodes is lower than the tolerable replicas.");
            this.consecutiveLogRolls.set(0);
            this.lowReplicationRollEnabled = false;
          }
        }
      } else if (numCurrentReplicas >= this.minTolerableReplication) {

        if (!this.lowReplicationRollEnabled) {
          // The new writer's log replicas is always the default value.
          // So we should not enable LowReplication-Roller. If numEntries
          // is lower than or equals 1, we consider it as a new writer.
          if (this.numEntries.get() <= 1) {
            return logRollNeeded;
          }
          // Once the live datanode number and the replicas return to normal,
          // enable the LowReplication-Roller.
          this.lowReplicationRollEnabled = true;
          LOG.info("LowReplication-Roller was enabled.");
        }
      }
    } catch (Exception e) {
      LOG.warn("Unable to invoke DFSOutputStream.getNumCurrentReplicas" + e +
          " still proceeding ahead...");
    }
    return logRollNeeded;
  }

  /**
   * This method gets the datanode replication count for the current HLog.
   *
   * If the pipeline isn't started yet or is empty, you will get the default
   * replication factor.  Therefore, if this function returns 0, it means you
   * are not properly running with the HDFS-826 patch.
   * @throws InvocationTargetException
   * @throws IllegalAccessException
   * @throws IllegalArgumentException
   *
   * @throws Exception
   */
  int getLogReplication()
  throws IllegalArgumentException, IllegalAccessException, InvocationTargetException {
    if (this.getNumCurrentReplicas != null && this.hdfs_out != null) {
      Object repl = this.getNumCurrentReplicas.invoke(getOutputStream(), NO_ARGS);
      if (repl instanceof Integer) {
        return ((Integer)repl).intValue();
      }
    }
    return 0;
  }
  /**
   * This method gets the pipeline for the current HLog.
   * @return
   */
  DatanodeInfo[] getPipeLine() {
    if (this.getPipeLine != null && this.hdfs_out != null) {
      Object repl;
      try {
        repl = this.getPipeLine.invoke(getOutputStream(), NO_ARGS);
        if (repl instanceof DatanodeInfo[]) {
          return ((DatanodeInfo[]) repl);
        }
      } catch (Exception e) {
        LOG.info("Get pipeline failed", e);
      }
    }
    return new DatanodeInfo[0];
  }
  
  boolean canGetCurReplicas() {
    return this.getNumCurrentReplicas != null;
  }

  public void hsync() throws IOException {
    syncer();
  }

  public void hflush() throws IOException {
    syncer();
  }

  public void sync() throws IOException {
    syncer();
  }

  public void sync(long txid) throws IOException {
    syncer(txid);
  }

  private void requestLogRoll(boolean forceRoll) {
    if (!this.listeners.isEmpty()) {
      for (WALActionsListener i: this.listeners) {
        i.logRollRequested(forceRoll);
      }
    }
  }

  protected void doWrite(HRegionInfo info, HLogKey logKey, WALEdit logEdit,
                           HTableDescriptor htd)
  throws IOException {
    if (!this.enabled) {
      return;
    }
    if (!this.listeners.isEmpty()) {
      for (WALActionsListener i: this.listeners) {
        i.visitLogEntryBeforeWrite(htd, logKey, logEdit);
      }
    }
    try {
      long startTimeNs = System.nanoTime();
      // coprocessor hook:
      if (!coprocessorHost.preWALWrite(info, logKey, logEdit)) {
        // write to our buffer, must holding the pendingWritesLock
        this.pendingWrites.add(new HLog.Entry(logKey, logEdit));
      }
      long tookNs = System.nanoTime() - startTimeNs;
      coprocessorHost.postWALWrite(info, logKey, logEdit);
      writeTime.inc(tookNs);
      offerWriteLatency(tookNs);
      long len = 0;
      for (KeyValue kv : logEdit.getKeyValues()) {
        len += kv.getLength();
      }
      writeSize.inc(len);
      if (tookNs / (1000 * 1000L) > 1000) {
        LOG.warn(String.format("%s took %d ms appending an edit to hlog; editcount=%d, len~=%s",
          Thread.currentThread().getName(), tookNs / (1000 * 1000L), this.numEntries.get(),
          StringUtils.humanReadableInt(len)));
        slowHLogAppendCount.incrementAndGet();
        slowHLogAppendTime.inc(tookNs);
      }
    } catch (IOException e) {
      LOG.fatal("Could not append. Requesting close of hlog", e);
      requestLogRoll(true);
      throw e;
    }
  }


  /** @return How many items have been added to the log */
  int getNumEntries() {
    return numEntries.get();
  }

  /**
   * Obtain a log sequence number.
   */
  private long obtainSeqNum() {
    return this.logSeqNum.incrementAndGet();
  }

  /** @return the number of log files in use, including current one */
  public int getNumLogFiles() {
    return outputfiles.size() + 1;
  }
  
  public long getLogRollSize() {
    return this.logrollsize;
  }

  /** @return the total size of log files in use, including current one */
  public long getNumLogFileSize() {
    return totalLogSize.get() + curLogSize;
  }
  
  /** @return the total size of log files in use, excluding current one */
  public long getOutputLogFileSize() {
    return totalLogSize.get();
  }

  /**
   * WAL keeps track of the sequence numbers that were not yet flushed from memstores
   * in order to be able to do cleanup. This method tells WAL that some region is about
   * to flush memstore.
   *
   * We stash the oldest seqNum for the region, and let the the next edit inserted in this
   * region be recorded in {@link #append(HRegionInfo, byte[], WALEdit, long, HTableDescriptor)}
   * as new oldest seqnum. In case of flush being aborted, we put the stashed value back;
   * in case of flush succeeding, the seqNum of that first edit after start becomes the
   * valid oldest seqNum for this region.
   *
   * @return current seqNum, to pass on to flushers (who will put it into the metadata of
   *         the resulting file as an upper-bound seqNum for that file), or NULL if flush
   *         should not be started.
   */
  public Long startCacheFlush(final byte[] encodedRegionName) {
    Long oldRegionSeqNum = null;
    if (!closeBarrier.beginOp()) {
      return null;
    }
    synchronized (oldestSeqNumsLock) {
      oldRegionSeqNum = this.oldestUnflushedSeqNums.remove(encodedRegionName);
      if (oldRegionSeqNum != null) {
        Long oldValue =
            this.oldestFlushingSeqNums.put(encodedRegionName, oldRegionSeqNum);
        assert oldValue == null : "Flushing map not cleaned up for "
            + Bytes.toString(encodedRegionName);
      }
    }
    if (oldRegionSeqNum == null) {
      // TODO: if we have no oldRegionSeqNum, and WAL is not disabled, presumably either
      // the region is already flushing (which would make this call invalid), or there
      // were no appends after last flush, so why are we starting flush? Maybe we should
      // assert not null, and switch to "long" everywhere. Less rigorous, but safer,
      // alternative is telling the caller to stop. For now preserve old logic.
      LOG.warn("Couldn't find oldest seqNum for the region we are about to flush: ["
          + Bytes.toString(encodedRegionName) + "]");
    }
    return obtainSeqNum();
  }

  /**
   * Complete the cache flush.
   * @param encodedRegionName Encoded region name.
   */
  public void completeCacheFlush(final byte[] encodedRegionName) {
    synchronized (oldestSeqNumsLock) {
      this.oldestFlushingSeqNums.remove(encodedRegionName);
    }
    closeBarrier.endOp();
  }

  /**
   * Abort a cache flush.
   * Call if the flush fails. Note that the only recovery for an aborted flush
   * currently is a restart of the regionserver so the snapshot content dropped
   * by the failure gets restored to the memstore.
   * @param encodedRegionName Encoded region name.
   */
  public void abortCacheFlush(byte[] encodedRegionName) {
    Long currentSeqNum = null, seqNumBeforeFlushStarts = null;
    synchronized (oldestSeqNumsLock) {
      seqNumBeforeFlushStarts =
          this.oldestFlushingSeqNums.remove(encodedRegionName);
      if (seqNumBeforeFlushStarts != null) {
        currentSeqNum =
            this.oldestUnflushedSeqNums.put(encodedRegionName,
              seqNumBeforeFlushStarts);
      }
    }
    closeBarrier.endOp();
    if ((currentSeqNum != null)
        && (currentSeqNum.longValue() <= seqNumBeforeFlushStarts.longValue())) {
      String errorStr =
          "Region " + Bytes.toString(encodedRegionName)
              + "acquired edits out of order current memstore seq="
              + currentSeqNum + ", previous oldest unflushed id="
              + seqNumBeforeFlushStarts;
      LOG.error(errorStr);
      assert false : errorStr;
      Runtime.getRuntime().halt(1);
    }
  }

  public long getEarliestMemstoreSeqNum(byte[] encodedRegionName) {
    Long result = oldestUnflushedSeqNums.get(encodedRegionName);
    return result == null ? HConstants.NO_SEQNUM : result.longValue();
  }
  /**
   * @param family
   * @return true if the column is a meta column
   */
  public static boolean isMetaFamily(byte [] family) {
    return Bytes.equals(METAFAMILY, family);
  }

  /**
   * Get LowReplication-Roller status
   * 
   * @return lowReplicationRollEnabled
   */
  public boolean isLowReplicationRollEnabled() {
    return lowReplicationRollEnabled;
  }

  @SuppressWarnings("unchecked")
  public static Class<? extends HLogKey> getKeyClass(Configuration conf) {
     return (Class<? extends HLogKey>)
       conf.getClass("hbase.regionserver.hlog.keyclass", HLogKey.class);
  }

  public static HLogKey newKey(Configuration conf) throws IOException {
    Class<? extends HLogKey> keyClass = getKeyClass(conf);
    try {
      return keyClass.newInstance();
    } catch (InstantiationException e) {
      throw new IOException("cannot create hlog key");
    } catch (IllegalAccessException e) {
      throw new IOException("cannot create hlog key");
    }
  }

  /**
   * Utility class that lets us keep track of the edit with it's key
   * Only used when splitting logs
   */
  public static class Entry implements Writable {
    private WALEdit edit;
    private HLogKey key;

    public Entry() {
      edit = new WALEdit();
      key = new HLogKey();
    }

    /**
     * Constructor for both params
     * @param edit log's edit
     * @param key log's key
     */
    public Entry(HLogKey key, WALEdit edit) {
      super();
      this.key = key;
      this.edit = edit;
    }
    /**
     * Gets the edit
     * @return edit
     */
    public WALEdit getEdit() {
      return edit;
    }
    /**
     * Gets the key
     * @return key
     */
    public HLogKey getKey() {
      return key;
    }

    @Override
    public String toString() {
      return this.key + "=" + this.edit;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
      this.key.write(dataOutput);
      this.edit.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
      this.key.readFields(dataInput);
      this.edit.readFields(dataInput);
    }
  }

  /**
   * Construct the HLog directory name
   *
   * @param serverName Server name formatted as described in {@link ServerName}
   * @return the HLog directory name
   */
  public static String getHLogDirectoryName(final String serverName) {
    StringBuilder dirName = new StringBuilder(HConstants.HREGION_LOGDIR_NAME);
    dirName.append("/");
    dirName.append(serverName);
    return dirName.toString();
  }

  /**
   * Get the directory we are making logs in.
   * 
   * @return dir
   */
  protected Path getDir() {
    return dir;
  }
  
  /**
   * @param filename name of the file to validate
   * @return <tt>true</tt> if the filename matches an HLog, <tt>false</tt>
   *         otherwise
   */
  public static boolean validateHLogFilename(String filename) {
    return pattern.matcher(filename).matches();
  }

  static Path getHLogArchivePath(Path oldLogDir, Path p) {
    return new Path(oldLogDir, p.getName());
  }

  static String formatRecoveredEditsFileName(final long seqid) {
    return String.format("%019d", seqid);
  }

  /**
   * Returns sorted set of edit files made by wal-log splitter, excluding files
   * with '.temp' suffix.
   * @param fs
   * @param regiondir
   * @return Files in passed <code>regiondir</code> as a sorted set.
   * @throws IOException
   */
  public static NavigableSet<Path> getSplitEditFilesSorted(final FileSystem fs,
      final Path regiondir)
  throws IOException {
    NavigableSet<Path> filesSorted = new TreeSet<Path>();
    Path editsdir = getRegionDirRecoveredEditsDir(regiondir);
    if (!fs.exists(editsdir)) return filesSorted;
    FileStatus[] files = FSUtils.listStatus(fs, editsdir, new PathFilter() {
      @Override
      public boolean accept(Path p) {
        boolean result = false;
        try {
          // Return files and only files that match the editfile names pattern.
          // There can be other files in this directory other than edit files.
          // In particular, on error, we'll move aside the bad edit file giving
          // it a timestamp suffix.  See moveAsideBadEditsFile.
          Matcher m = EDITFILES_NAME_PATTERN.matcher(p.getName());
          result = fs.isFile(p) && m.matches();
          // Skip the file whose name ends with RECOVERED_LOG_TMPFILE_SUFFIX,
          // because it means splithlog thread is writting this file.
          if (p.getName().endsWith(RECOVERED_LOG_TMPFILE_SUFFIX)) {
            result = false;
          }
        } catch (IOException e) {
          LOG.warn("Failed isFile check on " + p);
        }
        return result;
      }
    });
    if (files == null) return filesSorted;
    for (FileStatus status: files) {
      filesSorted.add(status.getPath());
    }
    return filesSorted;
  }

  /**
   * Move aside a bad edits file.
   * @param fs
   * @param edits Edits file to move aside.
   * @return The name of the moved aside file.
   * @throws IOException
   */
  public static Path moveAsideBadEditsFile(final FileSystem fs,
      final Path edits)
  throws IOException {
    Path moveAsideName = new Path(edits.getParent(), edits.getName() + "." +
      System.currentTimeMillis());
    if (!HBaseFileSystem.renameDirForFileSystem(fs, edits, moveAsideName)) {
      LOG.warn("Rename failed from " + edits + " to " + moveAsideName);
    }
    return moveAsideName;
  }

  /**
   * @param regiondir This regions directory in the filesystem.
   * @return The directory that holds recovered edits files for the region
   * <code>regiondir</code>
   */
  public static Path getRegionDirRecoveredEditsDir(final Path regiondir) {
    return new Path(regiondir, RECOVERED_EDITS_DIR);
  }

  public static final long FIXED_OVERHEAD = ClassSize.align(
    ClassSize.OBJECT + (5 * ClassSize.REFERENCE) +
    ClassSize.ATOMIC_INTEGER + Bytes.SIZEOF_INT + (3 * Bytes.SIZEOF_LONG));

  private static void usage() {
    System.err.println("Usage: HLog <ARGS>");
    System.err.println("Arguments:");
    System.err.println(" --dump  Dump textual representation of passed one or more files");
    System.err.println("         For example: HLog --dump hdfs://example.com:9000/hbase/.logs/MACHINE/LOGFILE");
    System.err.println(" --split Split the passed directory of WAL logs");
    System.err.println("         For example: HLog --split hdfs://example.com:9000/hbase/.logs/DIR");
  }

  private static void split(final Configuration conf, final Path p)
  throws IOException {
    FileSystem fs = FileSystem.get(conf);
    if (!fs.exists(p)) {
      throw new FileNotFoundException(p.toString());
    }
    final Path baseDir = new Path(conf.get(HConstants.HBASE_DIR));
    final Path oldLogDir = new Path(baseDir, HConstants.HREGION_OLDLOGDIR_NAME);
    if (!fs.getFileStatus(p).isDir()) {
      throw new IOException(p + " is not a directory");
    }

    HLogSplitter logSplitter = HLogSplitter.createLogSplitter(
        conf, baseDir, p, oldLogDir, fs);
    logSplitter.splitLog();
  }

  /**
   * @return Coprocessor host.
   */
  public WALCoprocessorHost getCoprocessorHost() {
    return coprocessorHost;
  }

  /** Provide access to currently deferred sequence num for tests */
  boolean hasDeferredEntries() {
    return this.lastDeferredTxid > this.syncedTillHere.get();
  }

  public static final Collection<Long> getWriteLatenciesNanos() {
    final List<Long> latencies = 
        Lists.newArrayListWithCapacity(fsWriteLatenciesNanos.size());
    fsWriteLatenciesNanos.drainTo(latencies);
    return latencies;
  }

  private static final void offerWriteLatency(long latencyNanos) {
    // might be silently dropped, if the queue is full
    fsWriteLatenciesNanos.offer(latencyNanos); 
  }

  //A flag controls the balance between performance and data safety
  public enum SyncLogMode {
    //default value. lose data up to 30s once all dns crash or power outage
    HFLUSH_ALWAYS,
    //it will ensure every sucessful wal syncing goes into disk
    HSYNC_ALWAYS,
    //call hflush() in most of time, and call hsync() per second (not finished yet)
    HSYNC_PER_SECOND;
  }

  /**
   * Get the hlog files to compact
   * @return
   */
  public SortedMap<Long, Path> getCompactHLogFiles() {
    SortedMap<Long, Path> filesToCompact = new TreeMap<Long, Path>();
    this.outputfilesLock.lock();
    try {
      filesToCompact.putAll(outputfiles);
    } finally {
      this.outputfilesLock.unlock();
    }
    return filesToCompact;
  }

  /**
   * Complete the hlog compaction
   * @param newLogs
   * @throws IOException
   */
  public void completeHLogCompaction(Map<Long, Path> oldlogs,
      Map<Long, Path> newLogs) throws IOException {
    long totalOldSize = 0;
    for (Path log : oldlogs.values()) {
      if (fs.exists(log)) {
        try{
          totalOldSize += fs.getFileStatus(log).getLen();
        } catch (FileNotFoundException e) {
        }
      }
    }

    long totalNewSize = 0;
    for (Path log : newLogs.values()) {
      totalNewSize += fs.getFileStatus(log).getLen();
    }

    if (totalOldSize < totalNewSize) {
      throw new IOException(
          "Total size of old hlogs to be deleted is less than total size of all new hlogs: "
              + totalOldSize + " < " + totalNewSize
              + ". Abort this hlog compaction.");
    }

    //  move all new hlogs from tmp dir to region server's log dir
    for (Path log : newLogs.values()) {
      Path dstPath = new Path(this.dir, log.getName());
      HBaseFileSystem.renameDirForFileSystem(this.fs, log, dstPath);
    }

    Map<Long, Path> filesToArchive = new HashMap<Long, Path>();
    try {
      outputfilesLock.lock();
      // some old logs maybe have been removed
      for (Long seq : oldlogs.keySet()) {
        if (outputfiles.containsKey(seq)) {
          filesToArchive.put(seq, outputfiles.remove(seq));
        }
      }
      this.outputfiles.putAll(newLogs);
      newLogs.clear();
    } finally {
      outputfilesLock.unlock();
    }

    long removeHLogSize = 0;
    for (Long seq : filesToArchive.keySet()) {
      Path log = filesToArchive.get(seq);
      removeHLogSize += fs.getFileStatus(log).getLen();
      archiveLogFile(log, seq);
    }
    totalLogSize.addAndGet(totalNewSize - totalOldSize);
    LOG.info("Compact old hlogs with total size: " + removeHLogSize
        + "to new hlogs with size: " + totalNewSize
        + ". Current total hlog size: " + totalLogSize);
  }

  /**
   * Pass one or more log file names and it will either dump out a text version
   * on <code>stdout</code> or split the specified log files.
   *
   * @param args
   * @throws IOException
   */
  public static void main(String[] args) throws IOException {
    if (args.length < 2) {
      usage();
      System.exit(-1);
    }
    // either dump using the HLogPrettyPrinter or split, depending on args
    if (args[0].compareTo("--dump") == 0) {
      HLogPrettyPrinter.run(Arrays.copyOfRange(args, 1, args.length));
    } else if (args[0].compareTo("--split") == 0) {
      Configuration conf = HBaseConfiguration.create();
      for (int i = 1; i < args.length; i++) {
        try {
          conf.set("fs.default.name", args[i]);
          conf.set("fs.defaultFS", args[i]);
          Path logPath = new Path(args[i]);
          split(conf, logPath);
        } catch (Throwable t) {
          t.printStackTrace(System.err);
          System.exit(-1);
        }
      }
    } else {
      usage();
      System.exit(-1);
    }
  }
}
