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

package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionContext;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionPolicy;
import org.apache.hadoop.hbase.regionserver.compactions.Compactor;
import org.apache.hadoop.hbase.regionserver.storefiletracker.StoreFileTracker;
import org.apache.hadoop.hbase.regionserver.storefiletracker.StoreFileTrackerFactory;
import org.apache.hadoop.hbase.util.ReflectionUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.Sets;
import org.apache.hbase.thirdparty.org.apache.commons.collections4.CollectionUtils;

/**
 * StoreEngine is a factory that can create the objects necessary for HStore to operate. Since not
 * all compaction policies, compactors and store file managers are compatible, they are tied
 * together and replaced together via StoreEngine-s.
 * <p/>
 * We expose read write lock methods to upper layer for store operations:<br/>
 * <ul>
 * <li>Locked in shared mode when the list of component stores is looked at:
 * <ul>
 * <li>all reads/writes to table data</li>
 * <li>checking for split</li>
 * </ul>
 * </li>
 * <li>Locked in exclusive mode when the list of component stores is modified:
 * <ul>
 * <li>closing</li>
 * <li>completing a compaction</li>
 * </ul>
 * </li>
 * </ul>
 * <p/>
 * It is a bit confusing that we have a StoreFileManager(SFM) and then a StoreFileTracker(SFT). As
 * its name says, SFT is used to track the store files list. The reason why we have a SFT beside SFM
 * is that, when introducing stripe compaction, we introduced the StoreEngine and also the SFM, but
 * actually, the SFM here is not a general 'Manager', it is only designed to manage the in memory
 * 'stripes', so we can select different store files when scanning or compacting. The 'tracking' of
 * store files is actually done in {@link org.apache.hadoop.hbase.regionserver.HRegionFileSystem}
 * and {@link HStore} before we have SFT. And since SFM is designed to only holds in memory states,
 * we will hold write lock when updating it, the lock is also used to protect the normal read/write
 * requests. This means we'd better not add IO operations to SFM. And also, no matter what the in
 * memory state is, stripe or not, it does not effect how we track the store files. So consider all
 * these facts, here we introduce a separated SFT to track the store files.
 */
@InterfaceAudience.Private
public abstract class StoreEngine<SF extends StoreFlusher,
    CP extends CompactionPolicy, C extends Compactor, SFM extends StoreFileManager> {

  private static final Logger LOG = LoggerFactory.getLogger(StoreEngine.class);

  protected SF storeFlusher;
  protected CP compactionPolicy;
  protected C compactor;
  protected SFM storeFileManager;
  private StoreContext ctx;
  private RegionCoprocessorHost coprocessorHost;
  private Function<String, ExecutorService> openStoreFileThreadPoolCreator;
  private StoreFileTracker storeFileTracker;

  private final ReadWriteLock storeLock = new ReentrantReadWriteLock();

  /**
   * The name of the configuration parameter that specifies the class of
   * a store engine that is used to manage and compact HBase store files.
   */
  public static final String STORE_ENGINE_CLASS_KEY = "hbase.hstore.engine.class";

  private static final Class<? extends StoreEngine<?, ?, ?, ?>>
    DEFAULT_STORE_ENGINE_CLASS = DefaultStoreEngine.class;

  /**
   * Acquire read lock of this store.
   */
  public void readLock() {
    storeLock.readLock().lock();
  }

  /**
   * Release read lock of this store.
   */
  public void readUnlock() {
    storeLock.readLock().unlock();
  }

  /**
   * Acquire write lock of this store.
   */
  public void writeLock() {
    storeLock.writeLock().lock();
  }

  /**
   * Release write lock of this store.
   */
  public void writeUnlock() {
    storeLock.writeLock().unlock();
  }

  /**
   * @return Compaction policy to use.
   */
  public CompactionPolicy getCompactionPolicy() {
    return this.compactionPolicy;
  }

  /**
   * @return Compactor to use.
   */
  public Compactor getCompactor() {
    return this.compactor;
  }

  /**
   * @return Store file manager to use.
   */
  public StoreFileManager getStoreFileManager() {
    return this.storeFileManager;
  }

  /**
   * @return Store flusher to use.
   */
  public StoreFlusher getStoreFlusher() {
    return this.storeFlusher;
  }

  /**
   * Returns the store file tracker to use
   */
  public StoreFileTracker getStoreFileTracker() {
    return storeFileTracker;
  }

  private StoreFileTracker createStoreFileTracker(HStore store) {
    return StoreFileTrackerFactory.create(store.conf, store.getRegionInfo().getTable(),
      store.isPrimaryReplicaStore(), store.getStoreContext());
  }

  /**
   * @param filesCompacting Files currently compacting
   * @return whether a compaction selection is possible
   */
  public abstract boolean needsCompaction(List<HStoreFile> filesCompacting);

  /**
   * Creates an instance of a compaction context specific to this engine.
   * Doesn't actually select or start a compaction. See CompactionContext class comment.
   * @return New CompactionContext object.
   */
  public abstract CompactionContext createCompaction() throws IOException;

  /**
   * Create the StoreEngine's components.
   */
  protected abstract void createComponents(
      Configuration conf, HStore store, CellComparator cellComparator) throws IOException;

  private void createComponentsOnce(Configuration conf, HStore store, CellComparator cellComparator)
    throws IOException {
    assert compactor == null && compactionPolicy == null && storeFileManager == null &&
      storeFlusher == null && storeFileTracker == null;
    createComponents(conf, store, cellComparator);
    this.ctx = store.getStoreContext();
    this.coprocessorHost = store.getHRegion().getCoprocessorHost();
    this.openStoreFileThreadPoolCreator = store.getHRegion()::getStoreFileOpenAndCloseThreadPool;
    this.storeFileTracker = createStoreFileTracker(store);
    assert compactor != null && compactionPolicy != null && storeFileManager != null &&
      storeFlusher != null && storeFileTracker != null;
  }

  @FunctionalInterface
  public interface StoreFileOpener {
    HStoreFile open(StoreFileInfo info) throws IOException;
  }

  private List<HStoreFile> openStoreFiles(Collection<StoreFileInfo> files, boolean warmup,
    StoreFileOpener opener, Function<String, ExecutorService> threadPoolCreator)
    throws IOException {
    if (CollectionUtils.isEmpty(files)) {
      return Collections.emptyList();
    }
    // initialize the thread pool for opening store files in parallel..
    ExecutorService storeFileOpenerThreadPool = threadPoolCreator.apply("StoreFileOpener-" +
      ctx.getRegionInfo().getEncodedName() + "-" + ctx.getFamily().getNameAsString());
    CompletionService<HStoreFile> completionService =
      new ExecutorCompletionService<>(storeFileOpenerThreadPool);

    int totalValidStoreFile = 0;
    for (StoreFileInfo storeFileInfo : files) {
      // open each store file in parallel
      completionService.submit(() -> opener.open(storeFileInfo));
      totalValidStoreFile++;
    }

    Set<String> compactedStoreFiles = new HashSet<>();
    ArrayList<HStoreFile> results = new ArrayList<>(files.size());
    IOException ioe = null;
    try {
      for (int i = 0; i < totalValidStoreFile; i++) {
        try {
          HStoreFile storeFile = completionService.take().get();
          if (storeFile != null) {
            LOG.debug("loaded {}", storeFile);
            results.add(storeFile);
            compactedStoreFiles.addAll(storeFile.getCompactedStoreFiles());
          }
        } catch (InterruptedException e) {
          if (ioe == null) {
            ioe = new InterruptedIOException(e.getMessage());
          }
        } catch (ExecutionException e) {
          if (ioe == null) {
            ioe = new IOException(e.getCause());
          }
        }
      }
    } finally {
      storeFileOpenerThreadPool.shutdownNow();
    }
    if (ioe != null) {
      // close StoreFile readers
      boolean evictOnClose =
        ctx.getCacheConf() != null ? ctx.getCacheConf().shouldEvictOnClose() : true;
      for (HStoreFile file : results) {
        try {
          if (file != null) {
            file.closeStoreFile(evictOnClose);
          }
        } catch (IOException e) {
          LOG.warn("Could not close store file {}", file, e);
        }
      }
      throw ioe;
    }

    // Should not archive the compacted store files when region warmup. See HBASE-22163.
    if (!warmup) {
      // Remove the compacted files from result
      List<HStoreFile> filesToRemove = new ArrayList<>(compactedStoreFiles.size());
      for (HStoreFile storeFile : results) {
        if (compactedStoreFiles.contains(storeFile.getPath().getName())) {
          LOG.warn("Clearing the compacted storefile {} from {}", storeFile, this);
          storeFile.getReader().close(
            storeFile.getCacheConf() != null ? storeFile.getCacheConf().shouldEvictOnClose() :
              true);
          filesToRemove.add(storeFile);
        }
      }
      results.removeAll(filesToRemove);
      if (!filesToRemove.isEmpty() && ctx.isPrimaryReplicaStore()) {
        LOG.debug("Moving the files {} to archive", filesToRemove);
        ctx.getRegionFileSystem().removeStoreFiles(ctx.getFamily().getNameAsString(),
          filesToRemove);
      }
    }

    return results;
  }

  public void initialize(boolean warmup, StoreFileOpener opener,
    Function<String, ExecutorService> threadPoolCreator) throws IOException {
    List<StoreFileInfo> fileInfos = storeFileTracker.load();
    List<HStoreFile> files = openStoreFiles(fileInfos, warmup, opener, threadPoolCreator);
    storeFileManager.insertNewFiles(files);
  }

  public void refreshStoreFiles(StoreFileOpener opener,
    Function<String, ExecutorService> threadPoolCreator) throws IOException {
    List<StoreFileInfo> fileInfos = storeFileTracker.load();
    refreshStoreFilesInternal(fileInfos, opener, threadPoolCreator);
  }

  public void refreshStoreFiles(Collection<String> newFiles, StoreFileOpener opener,
    Function<String, ExecutorService> threadPoolCreator) throws IOException {
    List<StoreFileInfo> storeFiles = new ArrayList<>(newFiles.size());
    for (String file : newFiles) {
      storeFiles
        .add(ctx.getRegionFileSystem().getStoreFileInfo(ctx.getFamily().getNameAsString(), file));
    }
    refreshStoreFilesInternal(storeFiles, opener, threadPoolCreator);
  }

  /**
   * Checks the underlying store files, and opens the files that have not been opened, and removes
   * the store file readers for store files no longer available. Mainly used by secondary region
   * replicas to keep up to date with the primary region files.
   */
  private void refreshStoreFilesInternal(Collection<StoreFileInfo> newFiles, StoreFileOpener opener,
    Function<String, ExecutorService> threadPoolCreator) throws IOException {
    Collection<HStoreFile> currentFiles = storeFileManager.getStorefiles();
    Collection<HStoreFile> compactedFiles = storeFileManager.getCompactedfiles();
    if (currentFiles == null) {
      currentFiles = Collections.emptySet();
    }
    if (newFiles == null) {
      newFiles = Collections.emptySet();
    }
    if (compactedFiles == null) {
      compactedFiles = Collections.emptySet();
    }

    HashMap<StoreFileInfo, HStoreFile> currentFilesSet = new HashMap<>(currentFiles.size());
    for (HStoreFile sf : currentFiles) {
      currentFilesSet.put(sf.getFileInfo(), sf);
    }
    HashMap<StoreFileInfo, HStoreFile> compactedFilesSet = new HashMap<>(compactedFiles.size());
    for (HStoreFile sf : compactedFiles) {
      compactedFilesSet.put(sf.getFileInfo(), sf);
    }

    Set<StoreFileInfo> newFilesSet = new HashSet<StoreFileInfo>(newFiles);
    // Exclude the files that have already been compacted
    newFilesSet = Sets.difference(newFilesSet, compactedFilesSet.keySet());
    Set<StoreFileInfo> toBeAddedFiles = Sets.difference(newFilesSet, currentFilesSet.keySet());
    Set<StoreFileInfo> toBeRemovedFiles = Sets.difference(currentFilesSet.keySet(), newFilesSet);

    if (toBeAddedFiles.isEmpty() && toBeRemovedFiles.isEmpty()) {
      return;
    }

    LOG.info("Refreshing store files for " + this + " files to add: " + toBeAddedFiles +
      " files to remove: " + toBeRemovedFiles);

    Set<HStoreFile> toBeRemovedStoreFiles = new HashSet<>(toBeRemovedFiles.size());
    for (StoreFileInfo sfi : toBeRemovedFiles) {
      toBeRemovedStoreFiles.add(currentFilesSet.get(sfi));
    }

    // try to open the files
    List<HStoreFile> openedFiles = openStoreFiles(toBeAddedFiles, false, opener, threadPoolCreator);

    // propogate the file changes to the underlying store file manager
    replaceStoreFiles(toBeRemovedStoreFiles, openedFiles); // won't throw an exception
  }
 
  public void addStoreFiles(Collection<HStoreFile> storeFiles)
    throws IOException {
    storeFileTracker.add(StoreUtils.toStoreFileInfo(storeFiles));
    writeLock();
    try {
      storeFileManager.insertNewFiles(storeFiles);
    } finally {
      // We need the lock, as long as we are updating the storeFiles
      // or changing the memstore. Let us release it before calling
      // notifyChangeReadersObservers. See HBASE-4485 for a possible
      // deadlock scenario that could have happened if continue to hold
      // the lock.
      writeUnlock();
    }
  }

  public void replaceStoreFiles(Collection<HStoreFile> compactedFiles,
    Collection<HStoreFile> newFiles) throws IOException {
    storeFileTracker.replace(StoreUtils.toStoreFileInfo(compactedFiles),
      StoreUtils.toStoreFileInfo(newFiles));
    writeLock();
    try {
      storeFileManager.addCompactionResults(compactedFiles, newFiles);
    } finally {
      writeUnlock();
    }
  }

  /**
   * Create the StoreEngine configured for the given Store.
   * @param store The store. An unfortunate dependency needed due to it
   *              being passed to coprocessors via the compactor.
   * @param conf Store configuration.
   * @param cellComparator CellComparator for storeFileManager.
   * @return StoreEngine to use.
   */
  public static StoreEngine<?, ?, ?, ?> create(HStore store, Configuration conf,
    CellComparator cellComparator) throws IOException {
    String className = conf.get(STORE_ENGINE_CLASS_KEY, DEFAULT_STORE_ENGINE_CLASS.getName());
    try {
      StoreEngine<?, ?, ?, ?> se =
        ReflectionUtils.instantiateWithCustomCtor(className, new Class[] {}, new Object[] {});
      se.createComponentsOnce(conf, store, cellComparator);
      return se;
    } catch (Exception e) {
      throw new IOException("Unable to load configured store engine '" + className + "'", e);
    }
  }
}
