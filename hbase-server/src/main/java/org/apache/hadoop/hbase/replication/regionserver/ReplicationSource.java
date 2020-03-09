/*
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
package org.apache.hadoop.hbase.replication.regionserver;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.catalog.MetaEditor;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.fs.HFileSystem;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.wal.HLogKey;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.replication.ChainWALEntryFilter;
import org.apache.hadoop.hbase.replication.ReplicationEndpoint;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationPeers;
import org.apache.hadoop.hbase.replication.ReplicationQueueInfo;
import org.apache.hadoop.hbase.replication.ReplicationQueues;
import org.apache.hadoop.hbase.replication.SystemTableWALEntryFilter;
import org.apache.hadoop.hbase.replication.WALEntryFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Threads;

/**
 * Class that handles the source of a replication stream.
 * Currently does not handle more than 1 slave
 * For each slave cluster it selects a random number of peers
 * using a replication ratio. For example, if replication ration = 0.1
 * and slave cluster has 100 region servers, 10 will be selected.
 * <p/>
 * A stream is considered down when we cannot contact a region server on the
 * peer cluster for more than 55 seconds by default.
 * <p/>
 *
 */
@InterfaceAudience.Private
public class ReplicationSource extends Thread
    implements ReplicationSourceInterface {

  public static final Log LOG = LogFactory.getLog(ReplicationSource.class);
  // Queue of logs to process
  protected PriorityBlockingQueue<Path> queue;
  protected ReplicationQueues replicationQueues;
  private ReplicationPeers replicationPeers;

  private Configuration conf;
  protected ReplicationQueueInfo replicationQueueInfo;
  // id of the peer cluster this source replicates to
  private String peerId;
  // The znode we currently play with
  // For normal replication source, it is same with peerId
  // For recovered replication source, it is in the form of peerId-servername-*
  protected String peerClusterZnode;
  // The manager of all sources to which we ping back our progress
  protected ReplicationSourceManager manager;
  // Should we stop everything?
  protected Stoppable stopper;
  // How long should we sleep for each retry
  private long sleepForRetries;
  // Max size in bytes of entriesArray
  private long replicationQueueSizeCapacity;
  // Max number of entries in entriesArray
  private int replicationQueueNbCapacity;
  // Max size in bytes of entries whose position in WAL have not been persistent
  private long logPositionSizeLimit;
  // Max number of entries whose position in WAL have not been persistent
  private int logPositionNbLimit;
  // Our reader for the current log
  protected HLog.Reader reader;
  // Last position in the log that we sent to ZooKeeper
  private long lastLoggedPosition = -1;
  private long unLoggedPositionEdits = 0;

  // Path of the current log
  protected volatile Path currentPath;
  protected FileSystem fs;
  // id of this cluster
  private UUID clusterId;
  // id of the other cluster
  private UUID peerClusterId;
  // total number of edits we replicated
  private long totalReplicatedEdits = 0;
  // total number of edits we replicated
  private long totalReplicatedOperations = 0;
  // Maximum number of retries before taking bold actions
  protected int maxRetriesMultiplier;
  // Current number of operations (Put/Delete) that we need to replicate
  private int currentNbOperations = 0;
  // Current size of data we need to replicate
  private int currentSize = 0;
  // Indicates if this particular source is running
  protected volatile boolean running = true;
  // Metrics for this source
  protected MetricsSource metrics;
  // Handle on the log reader helper
  protected ReplicationHLogReaderManager repLogReader;
  //WARN threshold for the number of queued logs, defaults to 2
  private int logQueueWarnThreshold;
  // ReplicationEndpoint which will handle the actual replication
  private ReplicationEndpoint replicationEndpoint;
  // A filter (or a chain of filters) for the WAL entries.
  private WALEntryFilter walEntryFilter;
  // Context for ReplicationEndpoint#replicate()
  private ReplicationEndpoint.ReplicateContext replicateContext;
  // throttler
  private ReplicationThrottler throttler;
  private long defaultBandwidth;
  private long currentBandwidth;

  protected int ioeSleepBeforeRetry = 0;

  Map<String, Long> lastPositionsForSerial = new HashMap<String, Long>();

  private AtomicLong totalBufferUsed;
  private long totalBufferQuota;
  List<HLog.Entry> entries;

  @VisibleForTesting
  public static boolean throwIOEWhenCloseReaderFirstTime = false;
  @VisibleForTesting
  public static boolean throwIOEWhenReadWALEntryFirstTime = false;

  /**
   * Instantiation method used by region servers
   *
   * @param conf configuration to use
   * @param fs file system to use
   * @param manager replication manager to ping to
   * @param stopper     the atomic boolean to use to stop the regionserver
   * @param peerClusterZnode the name of our znode
   * @param clusterId unique UUID for the cluster
   * @param replicationEndpoint the replication endpoint implementation
   * @param metrics metrics for replication source
   * @throws IOException
   */
  @Override
  public void init(final Configuration conf, final FileSystem fs,
      final ReplicationSourceManager manager, final ReplicationQueues replicationQueues,
      final ReplicationPeers replicationPeers, final Stoppable stopper,
      final String peerClusterZnode, final UUID clusterId, ReplicationEndpoint replicationEndpoint,
      final MetricsSource metrics)
          throws IOException {
    this.stopper = stopper;
    this.conf = conf;
    decorateConf();
    this.replicationQueueSizeCapacity =
        this.conf.getLong("replication.source.size.capacity", 1024*1024*64);
    this.replicationQueueNbCapacity =
        this.conf.getInt("replication.source.nb.capacity", 25000);
    this.logPositionSizeLimit =
        this.conf.getLong("replication.log.position.size.limit", 1024*1024*64);
    this.logPositionNbLimit =
        this.conf.getInt("replication.log.position.nb.limit", 25000);
    this.maxRetriesMultiplier = this.conf.getInt("replication.source.maxretriesmultiplier", 10);
    this.queue =
        new PriorityBlockingQueue<Path>(
            this.conf.getInt("hbase.regionserver.maxlogs", 32),
            new LogsComparator());
    this.replicationQueues = replicationQueues;
    this.replicationPeers = replicationPeers;
    this.manager = manager;
    this.sleepForRetries =
        this.conf.getLong("replication.source.sleepforretries", 1000);
    this.ioeSleepBeforeRetry = this.conf.getInt("replication.source.ioe.sleepbeforeretry", 0);
    this.fs = fs;
    this.metrics = metrics;
    this.repLogReader = new ReplicationHLogReaderManager(this.fs, this.conf);
    this.clusterId = clusterId;

    this.peerClusterZnode = peerClusterZnode;
    this.replicationQueueInfo = new ReplicationQueueInfo(peerClusterZnode);
    // ReplicationQueueInfo parses the peerId out of the znode for us
    this.peerId = this.replicationQueueInfo.getPeerId();
    this.logQueueWarnThreshold = this.conf.getInt("replication.source.log.queue.warn", 2);
    this.replicationEndpoint = replicationEndpoint;

    this.replicateContext = new ReplicationEndpoint.ReplicateContext();

    defaultBandwidth = this.conf.getLong("replication.source.per.peer.node.bandwidth", 0);
    currentBandwidth = getCurrentBandwidth(
      this.replicationPeers.getPeer(peerId).getPeerBandwidth(), defaultBandwidth);
    this.throttler = new ReplicationThrottler((double) currentBandwidth / 10.0);
    this.totalBufferUsed = manager.getTotalBufferUsed();
    this.totalBufferQuota = conf.getLong(HConstants.REPLICATION_SOURCE_TOTAL_BUFFER_KEY,
        HConstants.REPLICATION_SOURCE_TOTAL_BUFFER_DFAULT);
    this.entries = new ArrayList<HLog.Entry>();
    LOG.info("peerClusterZnode=" + peerClusterZnode + ", peerId=" + peerId
        + " inited, replicationQueueSizeCapacity=" + replicationQueueSizeCapacity
        + ", replicationQueueNbCapacity=" + replicationQueueNbCapacity + ", logPositionSizeLimit="
        + logPositionSizeLimit + ", logPositionNbLimit=" + logPositionNbLimit
        + ", curerntBandwidth=" + this.currentBandwidth);
  }

  // Use guava cache to set ttl for each key
  private LoadingCache<String, Boolean> canSkipWaitingSet = CacheBuilder.newBuilder()
      .expireAfterAccess(1, TimeUnit.DAYS).build(
          new CacheLoader<String, Boolean>() {
            @Override
            public Boolean load(String key) throws Exception {
              return false;
            }
          }
      );

  private void decorateConf() {
    String replicationCodec = this.conf.get(HConstants.REPLICATION_CODEC_CONF_KEY);
    if (StringUtils.isNotEmpty(replicationCodec)) {
      this.conf.set(HConstants.RPC_CODEC_CONF_KEY, replicationCodec);
    }
  }

  @Override
  public void enqueueLog(Path log) {
    this.queue.put(log);
    int queueSize = queue.size();
    this.metrics.setSizeOfLogQueue(queueSize);
    LOG.info("Enqueue a new log: " + log);
    // This will log a warning for each new log that gets created above the warn threshold
    if (queueSize > this.logQueueWarnThreshold) {
      LOG.warn("Queue size: " + queueSize +
        " exceeds value of replication.source.log.queue.warn: " + logQueueWarnThreshold);
    }
  }

  private void uninitialize() {
    LOG.debug("Source exiting " + this.peerId);
    metrics.clear();
    if (replicationEndpoint.state() == Service.State.STARTING
        || replicationEndpoint.state() == Service.State.RUNNING) {
      replicationEndpoint.stopAndWait();
    }
  }

  @Override
  public void run() {
    // We were stopped while looping to connect to sinks, just abort
    if (!this.isActive()) {
      uninitialize();
      return;
    }

    try {
      // start the endpoint, connect to the cluster
      Service.State state = replicationEndpoint.start().get();
      if (state != Service.State.RUNNING) {
        LOG.warn("ReplicationEndpoint was not started. Exiting");
        uninitialize();
        return;
      }
    } catch (Exception ex) {
      LOG.warn("Error starting ReplicationEndpoint, exiting", ex);
      throw new RuntimeException(ex);
    }

    // get the WALEntryFilter from ReplicationEndpoint and add it to default filters
    ArrayList<WALEntryFilter> filters = Lists.newArrayList(
      (WALEntryFilter)new SystemTableWALEntryFilter());
    WALEntryFilter filterFromEndpoint = this.replicationEndpoint.getWALEntryfilter();
    if (filterFromEndpoint != null) {
      filters.add(filterFromEndpoint);
    }
    this.walEntryFilter = new ChainWALEntryFilter(filters);

    int sleepMultiplier = 1;
    // delay this until we are in an asynchronous thread
    while (this.isActive() && this.peerClusterId == null) {
      this.peerClusterId = replicationEndpoint.getPeerUUID();
      if (this.isActive() && this.peerClusterId == null) {
        if (sleepForRetries("Cannot contact the peer's zk ensemble", sleepMultiplier)) {
          sleepMultiplier++;
        }
      }
    }
    // We were stopped while looping to contact peer's zk ensemble, just abort
    if (!this.isActive()) {
      uninitialize();
      return;
    }

    // In rare case, zookeeper setting may be messed up. That leads to the incorrect
    // peerClusterId value, which is the same as the source clusterId
    if (clusterId.equals(peerClusterId) && !replicationEndpoint.canReplicateToSameCluster()) {
      this.terminate("ClusterId " + clusterId + " is replicating to itself: peerClusterId "
          + peerClusterId + " which is not allowed by ReplicationEndpoint:"
          + replicationEndpoint.getClass().getName(), null, false);
    }
    LOG.info("Replicating "+clusterId + " -> " + peerClusterId);

    try {
      this.repLogReader.setPosition(getStartPosition());
    } catch (ReplicationException e) {
      this.terminate("Couldn't get the position of this recovered queue " + this.peerClusterZnode,
        e);
    }

    try {
      runLoop();
    } catch (Throwable e) {
      if (this.isActive()) {
        LOG.fatal("Replication source exited unexpectedly", e);
      }
    }
    uninitialize();
  }

  protected long getStartPosition() throws ReplicationException {
    return 0;
  }

  protected void runLoop() {
    int sleepMultiplier = 1;
    // Loop until we close down
    while (isActive()) {
      // Sleep until replication is enabled again
      if (!isPeerEnabled()) {
        if (sleepForRetries("Replication is disabled", sleepMultiplier)) {
          sleepMultiplier++;
        }
        continue;
      }
      Path oldPath = getCurrentPath(); // note that in the current scenario,
                                       // oldPath will be null when a log roll
                                       // happens.
      // Get a new path
      boolean hasCurrentPath = getNextPath();
      if (getCurrentPath() != null && oldPath == null) {
        sleepMultiplier = 1; // reset the sleepMultiplier on a path change
      }
      if (!hasCurrentPath) {
        if (sleepForRetries("No log to process", sleepMultiplier)) {
          sleepMultiplier++;
        }
        continue;
      }
      boolean currentWALisBeingWrittenTo = isCurrentWALisBeingWrittenTo();
      // Open a reader on it
      if (!openReader(sleepMultiplier)) {
        // Reset the sleep multiplier, else it'd be reused for the next file
        sleepMultiplier = 1;
        continue;
      }

      // If we got a null reader but didn't continue, then sleep and continue
      if (this.reader == null) {
        if (sleepForRetries("Unable to open a reader", sleepMultiplier)) {
          sleepMultiplier++;
        }
        continue;
      }

      try {
        updateCurrentLogMetrics();
        if (readAllEntriesToReplicateOrNextFile(currentWALisBeingWrittenTo, entries,
            lastPositionsForSerial)) {
          for (Map.Entry<String, Long> entry : lastPositionsForSerial.entrySet()) {
            waitingUntilCanPush(entry);
          }
          saveLastPositionsForSerial();
          continue;
        }
        if (throwIOEWhenReadWALEntryFirstTime) {
          throwIOEWhenReadWALEntryFirstTime = false;
          throw new IOException("Just for test. Throw IOException when read WAL entry!");
        }
      } catch (IOException ioe) {
        LOG.warn("Replication peer peerId=" + peerId + " peerClusterZnode=" + peerClusterZnode
            + " got IOException", ioe);
        if (ioe.getCause() instanceof EOFException && handleEOFException(sleepMultiplier)) {
          continue;
        }
        // Sleep and retry
        if (sleepForRetries("Failed to read WAL entries", sleepMultiplier)) {
          sleepMultiplier++;
        }
        continue;
      } finally {
        try {
          this.reader = null;
          this.repLogReader.closeReader();
          if (throwIOEWhenCloseReaderFirstTime) {
            throwIOEWhenCloseReaderFirstTime = false;
            throw new IOException("Just for test. Throw IOException when close log reader!");
          }
        } catch (IOException e) {
          LOG.warn("Unable to finalize the tailing of a file", e);
        }
      }

      for (Map.Entry<String, Long> entry : lastPositionsForSerial.entrySet()) {
        waitingUntilCanPush(entry);
      }

      // If we didn't get anything to replicate, wait a bit and retry.
      // But if we need to stop, don't bother sleeping
      if (this.isActive() && entries.isEmpty()) {
        // Save positions to meta table before zk.
        saveLastPositionsForSerial();
        logPosition(currentWALisBeingWrittenTo);

        // Reset the sleep multiplier if nothing has actually gone wrong
        sleepMultiplier = 1;
        // if there was nothing to ship and it's not an error
        // set "ageOfLastShippedOp" to <now> to indicate that we're current
        this.metrics.setAgeOfLastShippedOp(System.currentTimeMillis());

        if (sleepForRetries("Nothing to replicate", sleepMultiplier)) {
          sleepMultiplier++;
        }
        LOG.debug("Nothing to replicate for current log: " + this.currentPath + ", position: "
            + this.repLogReader.getPosition());
        continue;
      }
      sleepMultiplier = 1;
      shipEdits(currentWALisBeingWrittenTo, entries);
      // Only clear everything after successfully ship edits
      releaseBufferQuota();
    }
  }

  private void releaseBufferQuota() {
    totalBufferUsed.addAndGet(-currentSize);
    currentSize = 0;
    currentNbOperations = 0;
    entries.clear();
  }

  private void logPosition(boolean currentWALisBeingWrittenTo){
    if (shouldLogPosition(this.repLogReader.getPosition())) {
      this.manager.logPosition(this.currentPath, this.peerClusterZnode,
          this.repLogReader.getPosition());
      if (!currentWALisBeingWrittenTo) {
        tryCleanOldLogs();
      }
      this.lastLoggedPosition = this.repLogReader.getPosition();
      this.unLoggedPositionEdits = 0;
      this.metrics.setCurrentOffset(lastLoggedPosition);
    }
  }

  private void updateCurrentLogMetrics() throws IOException {
    Path prevLog = null;
    if (getSourceMetrics().getCurrentLog() != null) {
      prevLog = getSourceMetrics().getCurrentLog().getFirst();
    }
    getSourceMetrics().setCurrentReplicatingLog(
      new Pair<>(getCurrentPath(), fs.getContentSummary(getCurrentPath()).getLength()));
    if (prevLog != null && !prevLog.getName().equals(currentPath.getName())) {
      getSourceMetrics().setCurrentOffset(0);
    }
  }

  protected boolean handleEOFException(int sleepMultiplier) {
    return false;
  }

  // For WAL files we own (rather than recovered), take a snapshot of whether the
  // current WAL file (this.currentPath) is in use (for writing) NOW!
  // Since the new WAL paths are enqueued only after the prev WAL file
  // is 'closed', presence of an element in the queue means that
  // the previous WAL file was closed, else the file is in use (currentPath)
  // We take the snapshot now so that we are protected against races
  // where a new file gets enqueued while the current file is being processed
  // (and where we just finished reading the current file).
  protected boolean isCurrentWALisBeingWrittenTo() {
    return queue.size() == 0;
  }

  protected void waitingUntilCanPush(Map.Entry<String, Long> entry) {
    // Don't waiting if this peer is not serial
    if (!this.replicationPeers.getPeer(peerId).getPeerConfig().isSerial()) {
      return;
    }

    String key = entry.getKey();
    long seq = entry.getValue();
    boolean deleteKey = false;
    if (seq <= 0) {
      // There is a REGION_CLOSE marker, we can not continue skipping after this entry.
      deleteKey = true;
      seq = -seq;
    }

    if (!canSkipWaitingSet.getUnchecked(key)) {
      while (true) {
        try {
          manager.waitUntilCanBePushed(Bytes.toBytes(key), seq, peerId);
          break;
        } catch (IOException e) {
          LOG.error("waitUntilCanBePushed fail", e);
        } catch (InterruptedException e) {
          LOG.warn("thread interrupted, stop wating", e);
          Thread.currentThread().interrupt();
          break;
        }
        Threads.sleep(sleepForRetries);
      }
      canSkipWaitingSet.put(key, true);
    }
    if (deleteKey) {
      canSkipWaitingSet.invalidate(key);
    }
  }

  /**
   * Check if log position is needed to avoid too many write operations to zookeeper
   */
  protected boolean shouldLogPosition(long logPostion) {
    if ((logPostion - this.lastLoggedPosition) >= this.logPositionSizeLimit) {
      return true;
    }
    if (unLoggedPositionEdits >= this.logPositionNbLimit) {
      return true;
    }
    return false;
  }
  
  public static void recoverFileLease(FileSystem fs, Path filePath, Configuration conf) throws IOException {
    LOG.info("recoverFileLease fs: " + fs.getClass().getName() + " path: " + filePath.toString());
    if(fs instanceof HFileSystem) {
      HFileSystem hFileSystem = (HFileSystem) fs;
      FileSystem backingFs = hFileSystem.getBackingFs();
      LOG.info("recoverFileLease fs: " + fs.getClass().getName() + " backingFs: "
          + backingFs.getClass().getName() + " path: " + filePath.toString());
      FSUtils.getInstance(backingFs, conf).recoverFileLease(backingFs, filePath, conf, null);
    }
  }

  /**
   * Read all the entries from the current log files and retain those
   * that need to be replicated. Else, process the end of the current file.
   * @param currentWALisBeingWrittenTo is the current WAL being written to
   * @param entries resulting entries to be replicated
   * @return true if we got nothing and went to the next file, false if we got
   * entries
   * @throws IOException
   */
  protected boolean readAllEntriesToReplicateOrNextFile(boolean currentWALisBeingWrittenTo,
      List<HLog.Entry> entries, Map<String, Long> lastPosition) throws IOException {
    long seenEntries = 0;
    LOG.debug("Seeking in " + this.currentPath + " at position " + this.repLogReader.getPosition());
    this.repLogReader.seek();
    long positionBeforeRead = this.repLogReader.getPosition();
    HLog.Entry entry = null;
    try {
      entry = this.repLogReader.readNextAndSetPosition();
    } catch (EOFException e) {
      if (!currentWALisBeingWrittenTo) {
        recoverFileLease(this.fs, this.currentPath, this.conf);
        FileStatus stat = this.fs.getFileStatus(this.currentPath);
        LOG.info("Current log: " + this.currentPath + ", readerPosition: "
            + this.repLogReader.getReaderPosition() + ", len: " + stat.getLen());
        if (this.repLogReader.getReaderPosition() >= 0
            && this.repLogReader.getReaderPosition() == stat.getLen()) {
          return processEndOfFile();
        }
      }
      throw e;
    }
    
    while (entry != null) {
      this.metrics.incrLogEditsRead();
      seenEntries++;

      if (entry.hasSerialReplicationScope()) {
        String key = Bytes.toString(entry.getKey().getEncodedRegionName());
        lastPosition.put(key, entry.getKey().getLogSeqNum());
        if (entry.getEdit().getCells().size() > 0) {
          WALProtos.RegionEventDescriptor maybeEvent =
              WALEdit.getRegionEventDescriptor(entry.getEdit().getCells().get(0));
          if (maybeEvent != null && maybeEvent.getEventType()
              == WALProtos.RegionEventDescriptor.EventType.REGION_CLOSE) {
            // In serially replication, if we move a region to another RS and move it back, we may
            // read logs crossing two sections. We should break at REGION_CLOSE and push the first
            // section first in case of missing the middle section belonging to the other RS.
            // In a worker thread, if we can push the first log of a region, we can push all logs
            // in the same region without waiting until we read a close marker because next time
            // we read logs in this region, it must be a new section and not adjacent with this
            // region. Mark it negative.
            lastPosition.put(key, -entry.getKey().getLogSeqNum());
            break;
          }
        }
      }
      boolean totalBufferTooLarge = false;
      // don't replicate if the log entries have already been consumed by the cluster
      if (replicationEndpoint.canReplicateToSameCluster()
          || !entry.getKey().getClusterIds().contains(peerClusterId)) {
        // Remove all KVs that should not be replicated
        entry = walEntryFilter.filter(entry);
        WALEdit edit = null;
        HLogKey logKey = null;
        if (entry != null) {
          edit = entry.getEdit();
          logKey = entry.getKey();
        }

        if (edit != null && edit.size() != 0) {
          //Mark that the current cluster has the change
          logKey.addClusterId(clusterId);
          currentNbOperations += countDistinctRowKeys(edit);
          entries.add(entry);
          long delta = entry.getEdit().heapSize();
          currentSize += delta;
          totalBufferTooLarge = acquireBufferQuota(delta);
        } else {
          this.metrics.incrLogEditsFiltered();
        }
      }
      // Stop if too many entries or too big
      if (totalBufferTooLarge || currentSize >= this.replicationQueueSizeCapacity
          || entries.size() >= this.replicationQueueNbCapacity) {
        LOG.debug(
            "Current log: " + this.currentPath + ", read entries break as totalBufferTooLarge="
                + totalBufferTooLarge + ", currentSize=" + currentSize + ", entries.size()="
                + entries.size());
        break;
      }
      try {
        entry = this.repLogReader.readNextAndSetPosition();
      } catch (IOException ie) {
        LOG.warn("Current log: " + this.currentPath + ", read entries break on IOException", ie);
        break;
      }
    }
    metrics.incrLogReadInBytes(this.repLogReader.getPosition() - positionBeforeRead);
    if (currentWALisBeingWrittenTo) {
      return false;
    }
    // If we didn't get anything and the queue has an object, it means we
    // hit the end of the file for sure
    return seenEntries == 0 && processEndOfFile();
  }

  /**
   * Poll for the next path
   * @return true if a path was obtained, false if not
   */
  protected boolean getNextPath() {
    try {
      if (this.currentPath == null) {
        this.currentPath = queue.poll(this.sleepForRetries, TimeUnit.MILLISECONDS);
        this.metrics.setSizeOfLogQueue(queue.size());
        if (this.currentPath != null) {
          tryCleanOldLogs();
          LOG.info("Poll a new log from queue: " + this.currentPath);
        }
      }
    } catch (InterruptedException e) {
      LOG.warn("Interrupted while reading edits", e);
    }
    return this.currentPath != null;
  }

  protected void tryCleanOldLogs() {
    this.manager.cleanOldLogs(this.currentPath.getName(), this.peerId, false);
  }

  /**
   * Open a reader on the current path
   *
   * @param sleepMultiplier by how many times the default sleeping time is augmented
   * @return true if we should continue with that file, false if we are over with it
   */
  protected boolean openReader(int sleepMultiplier) {
    try {
      try {
        this.reader = repLogReader.openReader(this.currentPath);
      } catch (FileNotFoundException fnfe) {
        // If the log was archived, continue reading from there
        // Search in old archive directory
        Path archivedLogLocation = new Path(manager.getOldLogDir(), currentPath.getName());
        if (this.manager.getFs().exists(archivedLogLocation)) {
          currentPath = archivedLogLocation;
          LOG.info("Log " + this.currentPath + " was moved to " + archivedLogLocation);
          // Open the log at the new location
          this.openReader(sleepMultiplier);
          return true;
        }

        // Search in separate regionserver archive directory
        archivedLogLocation = new Path(manager.getOldLogDir(), manager.getServer().getServerName()
            + Path.SEPARATOR + currentPath.getName());
        if (this.manager.getFs().exists(archivedLogLocation)) {
          currentPath = archivedLogLocation;
          LOG.info("Log " + this.currentPath + " was moved to " + archivedLogLocation);
          // Open the log at the new location
          this.openReader(sleepMultiplier);
          return true;
        }
        // TODO What happens the log is missing in both places?
      }
    } catch (IOException ioe) {
      if (ioe instanceof EOFException) {
        if (isCurrentLogEmpty()) {
          return true;
        } else {
          boolean atTail = this.queue.size() == 0;
          LOG.warn("EOF in queue:" + this.peerClusterZnode + ", atTail=" + atTail
              + ", file path:" + this.currentPath, ioe);
          processEndOfFile();
          return false;
        }
      }

      LOG.warn(this.peerClusterZnode + " Got: ", ioe);
      this.reader = null;
      if (ioe.getCause() instanceof NullPointerException) {
        // Workaround for race condition in HDFS-4380
        // which throws a NPE if we open a file before any data node has the most recent block
        // Just sleep and retry. Will require re-reading compressed HLogs for compressionContext.
        LOG.warn("Got NPE opening reader, will retry.");
      }

      this.metrics.incrOpenReaderIOE();
      // Throttle the failure logs
      try {
        if (ioeSleepBeforeRetry > 0) {
          TimeUnit.MILLISECONDS.sleep(ioeSleepBeforeRetry);
        }
      } catch (Exception e) {
        // Ignore
      }
    }
    return true;
  }

  /*
   * Checks whether the current log file is empty, and it is not a recovered queue. This is to
   * handle scenario when in an idle cluster, there is no entry in the current log and we keep on
   * trying to read the log file and get EOFException. In case of a recovered queue the last log
   * file may be empty, and we don't want to retry that.
   */
  private boolean isCurrentLogEmpty() {
    return (this.repLogReader.getPosition() == 0 && queue.size() == 0);
  }

  /**
   * Do the sleeping logic
   * @param msg Why we sleep
   * @param sleepMultiplier by how many times the default sleeping time is augmented
   * @return True if <code>sleepMultiplier</code> is &lt; <code>maxRetriesMultiplier</code>
   */
  protected boolean sleepForRetries(String msg, int sleepMultiplier) {
    try {
      if (LOG.isTraceEnabled()) {
        LOG.trace(msg + ", sleeping " + sleepForRetries + " times " + sleepMultiplier);
      }
      Thread.sleep(this.sleepForRetries * sleepMultiplier);
    } catch (InterruptedException e) {
      LOG.debug("Interrupted while sleeping between retries");
      Thread.currentThread().interrupt();
    }
    return sleepMultiplier < maxRetriesMultiplier;
  }

  /**
   * Count the number of different row keys in the given edit because of
   * mini-batching. We assume that there's at least one KV in the WALEdit.
   * @param edit edit to count row keys from
   * @return number of different row keys
   */
  private int countDistinctRowKeys(WALEdit edit) {
    List<KeyValue> kvs = edit.getKeyValues();
    int distinctRowKeys = 1;
    KeyValue lastKV = kvs.get(0);
    for (int i = 0; i < edit.size(); i++) {
      if (!kvs.get(i).matchingRow(lastKV)) {
        distinctRowKeys++;
      }
    }
    return distinctRowKeys;
  }

  private long getCurrentBandwidth(long peerBandwidth, long defaultBandwidth) {
    return peerBandwidth != 0 ? peerBandwidth : defaultBandwidth;
  }

  private void checkBandwidthChangeAndResetThrottler() {
    long peerBandwidth = this.replicationPeers.getPeer(peerId).getPeerBandwidth();
    if (peerBandwidth != currentBandwidth) {
      // user can set peer bandwidth to 0 to use default bandwidth
      if (peerBandwidth != 0 || currentBandwidth != defaultBandwidth) {
        currentBandwidth = getCurrentBandwidth(peerBandwidth, defaultBandwidth);
        this.throttler.setBandwidth((double) currentBandwidth / 10.0);
        LOG.info("ReplicationSource : " + peerId
            + " bandwidth throttling changed, currentBandWidth=" + currentBandwidth);
      }
    }
  }

  /**
   * Do the shipping logic
   * @param currentWALisBeingWrittenTo was the current WAL being (seemingly)
   * written to when this method was called
   */
  protected void shipEdits(boolean currentWALisBeingWrittenTo, List<HLog.Entry> entries) {
    int sleepMultiplier = 1;
    if (entries.isEmpty()) {
      LOG.warn("Was given 0 edits to ship");
      return;
    }
    while (this.isActive()) {
      try {
        checkBandwidthChangeAndResetThrottler();
        if (this.throttler.isEnabled()) {
          long sleepTicks = this.throttler.getNextSleepInterval(currentSize);
          if (sleepTicks > 0) {
            try {
              LOG.debug("To sleep " + sleepTicks + "ms for throttling control");
              Thread.sleep(sleepTicks);
            } catch (InterruptedException e) {
              LOG.debug("Interrupted while sleeping for throttling control");
              Thread.currentThread().interrupt();
              // current thread might be interrupted to terminate
              // directly go back to while() for confirm this
              continue;
            }
            // reset throttler's cycle start tick when sleep for throttling occurs
            this.throttler.resetStartTick();
          }
        }
        replicateContext.setEntries(entries).setSize(currentSize);

        // send the edits to the endpoint. Will block until the edits are shipped and acknowledged
        boolean replicated = replicationEndpoint.replicate(replicateContext);

        if (!replicated) {
          continue;
        }

        this.unLoggedPositionEdits += entries.size();

        // Save positions to meta table before zk.
        saveLastPositionsForSerial();
        logPosition(currentWALisBeingWrittenTo);
        if (this.throttler.isEnabled()) {
          this.throttler.addPushSize(currentSize);
        }
        this.totalReplicatedEdits += entries.size();
        this.totalReplicatedOperations += currentNbOperations;
        this.metrics.shipBatch(this.currentNbOperations, this.currentSize/1024);
        this.metrics.setAgeOfLastShippedOp(entries.get(entries.size()-1).getKey().getWriteTime());
        LOG.debug("Replicated " + this.totalReplicatedEdits + " entries in total, or "
            + this.totalReplicatedOperations + " operations");
        break;
      } catch (Exception ex) {
        LOG.warn(replicationEndpoint.getClass().getName() + " threw unknown exception: ", ex);
        if (sleepForRetries("ReplicationEndpoint threw exception", sleepMultiplier)) {
          sleepMultiplier++;
        }
      }
    }
  }

  /**
   * check whether the peer is enabled or not
   *
   * @return true if the peer is enabled, otherwise false
   */
  protected boolean isPeerEnabled() {
    return this.replicationPeers.getStatusOfPeer(this.peerId);
  }

  /**
   * If the queue isn't empty, switch to the next one
   * Else if this is a recovered queue, it means we're done!
   * Else we'll just continue to try reading the log file
   * @return true if we're done with the current file, false if we should
   * continue trying to read from it
   */
  protected boolean processEndOfFile() {
    if (this.queue.size() != 0) {
      // at the end of the log
      saveLastPositionsForSerial();

      if (this.lastLoggedPosition != this.repLogReader.getPosition()) {
        this.manager.logPosition(this.currentPath, this.peerClusterZnode,
          this.repLogReader.getPosition());
        tryCleanOldLogs();
        this.lastLoggedPosition = 0;
        this.unLoggedPositionEdits = 0;
      }

      String filesize = "N/A";
      try {
        FileStatus stat = this.fs.getFileStatus(this.currentPath);
        filesize = stat.getLen() + "";
      } catch (IOException ex) {
      }
      LOG.info("Reached the end of a log, stats: " + getStats()
          + ", and the length of the file is " + filesize);

      this.currentPath = null;
      this.repLogReader.finishCurrentFile();
      this.reader = null;
      return true;
    }
    return false;
  }

  @Override
  public void startup() {
    String n = Thread.currentThread().getName();
    Thread.UncaughtExceptionHandler handler =
        new Thread.UncaughtExceptionHandler() {
          @Override
          public void uncaughtException(final Thread t, final Throwable e) {
            HRegionServer.exitIfOOME(e);
            LOG.error("Unexpected exception in ReplicationSource," +
              " currentPath=" + currentPath, e);
            stopper.stop("Unexpected exception in ReplicationSource");
          }
        };
    Threads.setDaemonThreadRunning(
        this, n + ".replicationSource," +
        this.peerClusterZnode, handler);
  }

  @Override
  public void terminate(String reason) {
    terminate(reason, null);
  }

  @Override
  public void terminate(String reason, Exception cause) {
    terminate(reason, cause, true);
  }

  public void terminate(String reason, Exception cause, boolean join) {
    if (cause == null) {
      LOG.info("Closing source "
          + this.peerClusterZnode + " because: " + reason);

    } else {
      LOG.error("Closing source " + this.peerClusterZnode
          + " because an error occurred: " + reason, cause);
    }
    this.running = false;
    this.interrupt();
    ListenableFuture<Service.State> future = null;
    if (this.replicationEndpoint != null) {
      future = this.replicationEndpoint.stop();
    }
    if (join) {
      Threads.shutdown(this, this.sleepForRetries);
      if (future != null) {
        try {
          future.get(sleepForRetries * maxRetriesMultiplier, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
          LOG.warn("Got exception while waiting for endpoint to shutdown for replication source :"
              + this.peerClusterZnode,
            e);
        }
      }
    }
  }

  private void saveLastPositionsForSerial() {
    // Only save last pushed seq id for serial replication peer
    if (this.replicationPeers.getPeer(peerId).getPeerConfig().isSerial()) {
      while (true) {
        try {
          MetaEditor
              .updateReplicationPositions(manager.getConnection(), peerId, lastPositionsForSerial);
          break;
        } catch (IOException e) {
          LOG.error("updateReplicationPositions fail, retry", e);
        }
        Threads.sleep(sleepForRetries);
      }
    }
    lastPositionsForSerial.clear();
  }

  @Override
  public String getPeerClusterZnode() {
    return this.peerClusterZnode;
  }

  @Override
  public String getPeerId() {
    return this.peerId;
  }

  @Override
  public Path getCurrentPath() {
    return this.currentPath;
  }

  protected boolean isActive() {
    return !this.stopper.isStopped() && this.running && !isInterrupted();
  }

  /**
   * Comparator used to compare logs together based on their start time
   */
  public static class LogsComparator implements Comparator<Path> {

    @Override
    public int compare(Path o1, Path o2) {
      return Long.valueOf(getTS(o1)).compareTo(getTS(o2));
    }

    /**
     * Split a path to get the start time
     * For example: 10.20.20.171%3A60020.1277499063250
     * @param p path to split
     * @return start time
     */
    private long getTS(Path p) {
      String[] parts = p.getName().split("\\.");
      return Long.parseLong(parts[parts.length-1]);
    }
  }

  @Override
  public String getStats() {
    long position = this.repLogReader.getPosition();
    return "Total replicated edits: " + totalReplicatedEdits +
      ", currently replicating from: " + this.currentPath +
      " at position: " + position;
  }

  /**
   * @param size delta size for grown buffer
   * @return true if we should clear buffer and push all
   */
  private boolean acquireBufferQuota(long size) {
    return totalBufferUsed.addAndGet(size) >= totalBufferQuota;
  }

  /**
   * Get Replication Source Metrics
   * @return sourceMetrics
   */
  public MetricsSource getSourceMetrics() {
    return this.metrics;
  }
}
