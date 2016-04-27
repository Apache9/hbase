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
package org.apache.hadoop.hbase.regionserver.compactions;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.PeekingIterator;
import com.google.common.math.LongMath;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HDFSBlocksDistribution;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.regionserver.RSRpcServices;
import org.apache.hadoop.hbase.regionserver.StoreConfigInformation;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.StoreUtils;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.ReflectionUtils;

/**
 * HBASE-15181 This is a simple implementation of date-based tiered compaction similar to
 * Cassandra's for the following benefits:
 * <ol>
 * <li>Improve date-range-based scan by structuring store files in date-based tiered layout.</li>
 * <li>Reduce compaction overhead.</li>
 * <li>Improve TTL efficiency.</li>
 * </ol>
 * Perfect fit for the use cases that:
 * <ol>
 * <li>has mostly date-based data write and scan and a focus on the most recent data.</li>
 * </ol>
 * Out-of-order writes are handled gracefully. Time range overlapping among store files is tolerated
 * and the performance impact is minimized. Configuration can be set at hbase-site or overridden at
 * per-table or per-column-family level by hbase shell. Design spec is at
 * https://docs.google.com/document/d/1_AmlNb2N8Us1xICsTeGDLKIqL6T-oHoRLZ323MG_uy8/
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class DateTieredCompactionPolicy extends SortedCompactionPolicy {

  private static final Log LOG = LogFactory.getLog(DateTieredCompactionPolicy.class);

  private static DateTieredCompactionRequest EMPTY_REQUEST = new DateTieredCompactionRequest(
      Collections.<StoreFile> emptyList(), Collections.<Long> emptyList(), Long.MIN_VALUE);

  private final RatioBasedCompactionPolicy compactionPolicyPerWindow;

  private final CompactionWindowFactory windowFactory;

  public DateTieredCompactionPolicy(Configuration conf, StoreConfigInformation storeConfigInfo)
      throws IOException {
    super(conf, storeConfigInfo);
    try {
      compactionPolicyPerWindow = ReflectionUtils.instantiateWithCustomCtor(
        comConf.getCompactionPolicyForDateTieredWindow(),
        new Class[] { Configuration.class, StoreConfigInformation.class },
        new Object[] { conf, storeConfigInfo });
    } catch (Exception e) {
      throw new IOException("Unable to load configured compaction policy '"
          + comConf.getCompactionPolicyForDateTieredWindow() + "'", e);
    }
    try {
      windowFactory = ReflectionUtils.instantiateWithCustomCtor(
        comConf.getDateTieredCompactionWindowFactory(),
        new Class[] { CompactionConfiguration.class }, new Object[] { comConf });
    } catch (Exception e) {
      throw new IOException("Unable to load configured window factory '"
          + comConf.getDateTieredCompactionWindowFactory() + "'", e);
    }
  }

  /**
   * Heuristics for guessing whether we need minor compaction.
   */
  @Override
  @VisibleForTesting
  public boolean needsCompaction(final Collection<StoreFile> allFiles,
      final List<StoreFile> filesCompacting) {
    ArrayList<StoreFile> candidates = new ArrayList<StoreFile>(allFiles);
    try {
      return !selectMinorCompaction(allFiles, filesCompacting, candidates, false, true).getFiles()
          .isEmpty();
    } catch (Exception e) {
      LOG.error("Can not check for compaction: ", e);
      return false;
    }
  }

  public boolean shouldPerformMajorCompaction(final Collection<StoreFile> filesToCompact)
    throws IOException {
    long mcTime = getNextMajorCompactTime(filesToCompact);
    if (filesToCompact == null || mcTime == 0) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("filesToCompact: " + filesToCompact + " mcTime: " + mcTime);
      }
      return false;
    }

    // TODO: Use better method for determining stamp of last major (HBASE-2990)
    long lowTimestamp = StoreUtils.getLowestTimestamp(filesToCompact);
    long now = EnvironmentEdgeManager.currentTime();
    if (lowTimestamp <= 0L || lowTimestamp >= (now - mcTime)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("lowTimestamp: " + lowTimestamp + " lowTimestamp: " + lowTimestamp + " now: " +
            now + " mcTime: " + mcTime); 
      }
      return false;
    }

    long cfTTL = this.storeConfigInfo.getStoreFileTtl();
    List<Long> boundaries = getCompactBoundariesForMajor(filesToCompact, now);
    boolean[] filesInWindow = new boolean[boundaries.size()];
    HDFSBlocksDistribution hdfsBlocksDistribution = new HDFSBlocksDistribution();
    for (StoreFile file: filesToCompact) {
      Long minTimestamp = file.getMinimumTimestamp();
      long oldest = (minTimestamp == null) ? Long.MIN_VALUE : now - minTimestamp.longValue();
      if (cfTTL != Long.MAX_VALUE && oldest >= cfTTL) {
        LOG.debug("Major compaction triggered on store " + this
          + "; for TTL maintenance");
        return true;
      }
      if (!file.isMajorCompaction() || file.isBulkLoadResult()) {
        LOG.debug("Major compaction triggered on store " + this
          + ", because there are new files and time since last major compaction "
          + (now - lowTimestamp) + "ms");
        return true;
      }

      int lowerWindowIndex = Collections.binarySearch(boundaries,
        minTimestamp == null ? (Long) Long.MAX_VALUE : minTimestamp);
      int upperWindowIndex = Collections.binarySearch(boundaries,
        file.getMaximumTimestamp() == null ? (Long) Long.MAX_VALUE : file.getMaximumTimestamp());
      if (lowerWindowIndex != upperWindowIndex) {
        LOG.debug("Major compaction triggered on store " + this + "; because file "
          + file.getPath() + " has data with timestamps cross window boundaries");
        return true;
      } else if (filesInWindow[upperWindowIndex]) {
        LOG.debug("Major compaction triggered on store " + this +
          "; because there are more than one file in some windows");
        return true;
      } else {
        filesInWindow[upperWindowIndex] = true;
      }
      hdfsBlocksDistribution.add(file.getHDFSBlockDistribution());
    }

    float blockLocalityIndex = hdfsBlocksDistribution
        .getBlockLocalityIndex(RSRpcServices.getHostname(comConf.conf, false));
    if (blockLocalityIndex < comConf.getMinLocalityToForceCompact()) {
      LOG.debug("Major compaction triggered on store " + this
        + "; to make hdfs blocks local, current blockLocalityIndex is "
        + blockLocalityIndex + " (min " + comConf.getMinLocalityToForceCompact() + ")");
      return true;
    }

    LOG.debug("Skipping major compaction of " + this +
      ", because the files are already major compacted");
    return false;
  }

  @Override
  protected CompactionRequest createCompactionRequest(Collection<StoreFile> allFiles,
      List<StoreFile> filesCompacting, ArrayList<StoreFile> candidateSelection, boolean tryingMajor,
      boolean mayUseOffPeak, boolean mayBeStuck) throws IOException {
    CompactionRequest result = tryingMajor ? selectMajorCompaction(candidateSelection)
        : selectMinorCompaction(allFiles, filesCompacting, candidateSelection, mayUseOffPeak,
          mayBeStuck);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Generated compaction request: " + result);
    }
    return result;
  }

  public CompactionRequest selectMajorCompaction(ArrayList<StoreFile> candidateSelection) {
    long now = EnvironmentEdgeManager.currentTime();
    long oldestToCompact = getOldestToCompact(comConf.getDateTieredMaxStoreFileAgeMillis(), now);
    return new DateTieredCompactionRequest(candidateSelection,
        this.getCompactBoundariesForMajor(candidateSelection, now), oldestToCompact);
  }

  private boolean shouldIncludeForFreezing(long startMillis, long endMillis, StoreFile file) {
    if (file.getMinimumTimestamp() == null) {
      if (file.getMaximumTimestamp() == null) {
        return true;
      } else {
        return file.getMaximumTimestamp().longValue() >= startMillis;
      }
    } else {
      if (file.getMaximumTimestamp() == null) {
        return file.getMinimumTimestamp().longValue() < endMillis;
      } else {
        return file.getMinimumTimestamp().longValue() < endMillis
            && file.getMaximumTimestamp().longValue() >= startMillis;
      }
    }
  }

  private boolean canDropWholeFile(long now, long cfTTL, StoreFile file) {
    long expiredBefore = now - cfTTL;
    return cfTTL != Long.MAX_VALUE && file.getMaximumTimestamp() != null
        && file.getMaximumTimestamp().longValue() < expiredBefore;
  }

  private boolean fitsInWindow(long startMillis, long endMillis, StoreFile file) {
    return file.getMinimumTimestamp() != null && file.getMaximumTimestamp() != null
        && file.getMinimumTimestamp().longValue() >= startMillis
        && file.getMaximumTimestamp().longValue() < endMillis;
  }

  private CompactionRequest freezeOldWindows(Collection<StoreFile> allFiles,
      List<StoreFile> filesCompacting, CompactionWindow youngestFreezingWindow,
      long oldestToCompact, long now) {
    if (!comConf.freezeDateTieredWindowOlderThanMaxAge()) {
      // A non-null file list is expected by HStore
      return EMPTY_REQUEST;
    }
    long minTimestamp = Long.MAX_VALUE;
    for (StoreFile file : allFiles) {
      minTimestamp = Math.min(minTimestamp, file.getMinimumTimestamp() == null ? Long.MAX_VALUE
          : file.getMinimumTimestamp().longValue());
    }
    List<Long> freezingWindowBoundaries = Lists.newArrayList();
    freezingWindowBoundaries.add(youngestFreezingWindow.endMillis());
    for (CompactionWindow window = youngestFreezingWindow; window
        .compareToTimestamp(minTimestamp) >= 0; window = window.nextEarlierWindow()) {
      freezingWindowBoundaries.add(window.startMillis());
    }
    Collections.reverse(freezingWindowBoundaries);
    if (freezingWindowBoundaries.size() < 2) {
      return EMPTY_REQUEST;
    }
    List<StoreFile> candidates = Lists.newArrayList(allFiles);
    // from old to young
    for (int i = 0, n = freezingWindowBoundaries.size() - 1; i < n; i++) {
      long startMillis = freezingWindowBoundaries.get(i);
      long endMillis = freezingWindowBoundaries.get(i + 1);
      int first = 0, total = candidates.size();
      for (; first < total; first++) {
        if (shouldIncludeForFreezing(startMillis, endMillis, candidates.get(first))) {
          break;
        }
      }
      if (first == total) {
        continue;
      }
      int last = total - 1;
      for (; last > first; last--) {
        if (shouldIncludeForFreezing(startMillis, endMillis, candidates.get(last))) {
          break;
        }
      }
      if (last == first) {
        StoreFile file = candidates.get(first);
        // If we could drop the whole file due to TTL then we can create a compaction request.
        // And also check if the only file fits in the window. Otherwise we still need a compaction
        // to move the data that does not belong to this window to other windows.
        if (!canDropWholeFile(now, storeConfigInfo.getStoreFileTtl(), file)
            && fitsInWindow(startMillis, endMillis, file)) {
          continue;
        }
      }
      if (!filesCompacting.isEmpty()) {
        // check if we are overlapped with filesCompacting.
        int firstCompactingIdx = candidates.indexOf(filesCompacting.get(0));
        int lastCompactingIdx = candidates.indexOf(filesCompacting.get(filesCompacting.size() - 1));
        assert firstCompactingIdx >= 0;
        assert lastCompactingIdx >= 0;
        if (last >= firstCompactingIdx && first <= lastCompactingIdx) {
          continue;
        }
      }
      if (last - first + 1 > comConf.getMaxFilesToCompact()) {
        LOG.warn("Too many files(got " + (last - first + 1) + ", expected less than or equal to "
            + comConf.getMaxFilesToCompact() + ") to compact when freezing [" + startMillis + ", "
            + endMillis + "), give up");
        continue;
      }
      for (int j = first; j <= last; j++) {
        StoreFile file = candidates.get(j);
        if (file.excludeFromMinorCompaction()) {
          LOG.warn("Find bulk load file " + file.getPath()
              + " which is configured to be excluded from minor compaction when archiving ["
              + startMillis + ", " + endMillis + ") give up");
          continue;
        }
      }
      List<StoreFile> filesToCompact = Lists.newArrayList(candidates.subList(first, last + 1));
      return new DateTieredCompactionRequest(filesToCompact,
          getCompactBoundariesForMajor(filesToCompact, now), oldestToCompact);
    }
    return EMPTY_REQUEST;
  }

  /**
   * We receive store files sorted in ascending order by seqId then scan the list of files. If the
   * current file has a maxTimestamp older than last known maximum, treat this file as it carries
   * the last known maximum. This way both seqId and timestamp are in the same order. If files carry
   * the same maxTimestamps, they are ordered by seqId. We then reverse the list so they are ordered
   * by seqId and maxTimestamp in descending order and build the time windows. All the out-of-order
   * data into the same compaction windows, guaranteeing contiguous compaction based on sequence id.
   */
  public CompactionRequest selectMinorCompaction(Collection<StoreFile> allFiles,
      List<StoreFile> filesCompacting, ArrayList<StoreFile> candidateSelection,
      boolean mayUseOffPeak, boolean mayBeStuck) throws IOException {
    long now = EnvironmentEdgeManager.currentTime();
    long oldestToCompact = getOldestToCompact(comConf.getDateTieredMaxStoreFileAgeMillis(), now);

    List<Pair<StoreFile, Long>> storefileMaxTimestampPairs =
        Lists.newArrayListWithCapacity(candidateSelection.size());
    long maxTimestampSeen = Long.MIN_VALUE;
    for (StoreFile storeFile : candidateSelection) {
      // if there is out-of-order data,
      // we put them in the same window as the last file in increasing order
      maxTimestampSeen = Math.max(maxTimestampSeen,
        storeFile.getMaximumTimestamp() == null? Long.MIN_VALUE : storeFile.getMaximumTimestamp());
      storefileMaxTimestampPairs.add(new Pair<StoreFile, Long>(storeFile, maxTimestampSeen));
    }
    Collections.reverse(storefileMaxTimestampPairs);

    CompactionWindow window = getIncomingWindow(now);
    int minThreshold = comConf.getDateTieredIncomingWindowMin();
    PeekingIterator<Pair<StoreFile, Long>> it =
        Iterators.peekingIterator(storefileMaxTimestampPairs.iterator());
    while (it.hasNext()) {
      if (window.compareToTimestamp(oldestToCompact) < 0) {
        // the whole window lies before oldestToCompact
        break;
      }
      int compResult = window.compareToTimestamp(it.peek().getSecond());
      if (compResult > 0) {
        // If the file is too old for the window, switch to the next window
        window = window.nextEarlierWindow();
        minThreshold = comConf.getMinFilesToCompact();
      } else {
        // The file is within the target window
        ArrayList<StoreFile> fileList = Lists.newArrayList();
        // Add all files in the same window. For incoming window
        // we tolerate files with future data although it is sub-optimal
        while (it.hasNext() && window.compareToTimestamp(it.peek().getSecond()) <= 0) {
          fileList.add(it.next().getFirst());
        }
        if (fileList.size() >= minThreshold) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Processing files: " + fileList + " for window: " + window);
          }
          DateTieredCompactionRequest request = generateCompactionRequestForMinorCompaction(
            fileList, window, mayUseOffPeak, mayBeStuck, minThreshold);
          if (request != null) {
            return request;
          }
        }
      }
    }
    if (comConf.freezeDateTieredWindowOlderThanMaxAge()) {
      return freezeOldWindows(allFiles, filesCompacting, window, oldestToCompact, now);
    } else {
      // A non-null file list is expected by HStore
      return EMPTY_REQUEST;
    }

  }

  private DateTieredCompactionRequest generateCompactionRequestForMinorCompaction(
      ArrayList<StoreFile> storeFiles, CompactionWindow window, boolean mayUseOffPeak,
      boolean mayBeStuck, int minThreshold) throws IOException {
    // The files has to be in ascending order for ratio-based compaction to work right
    // and removeExcessFile to exclude youngest files.
    Collections.reverse(storeFiles);

    // Compact everything in the window if have more files than comConf.maxBlockingFiles
    compactionPolicyPerWindow.setMinThreshold(minThreshold);
    ArrayList<StoreFile> storeFileSelection = mayBeStuck ? storeFiles
      : compactionPolicyPerWindow.applyCompactionPolicy(storeFiles, mayUseOffPeak, false);
    if (storeFileSelection != null && !storeFileSelection.isEmpty()) {
      // If there is any file in the window excluded from compaction,
      // only one file will be output from compaction.
      boolean singleOutput = storeFiles.size() != storeFileSelection.size() ||
        comConf.useDateTieredSingleOutputForMinorCompaction();
      List<Long> boundaries = getCompactionBoundariesForMinor(window, singleOutput);
      // minor compaction will not compact window older than max age, so pass Long.MIN_VALUE is OK.
      DateTieredCompactionRequest result = new DateTieredCompactionRequest(storeFileSelection,
        boundaries, Long.MIN_VALUE);
      return result;
    }
    return null;
  }

  /**
   * Return a list of boundaries for multiple compaction output in ascending order.
   */
  private List<Long> getCompactBoundariesForMajor(Collection<StoreFile> filesToCompact, long now) {
    long minTimestamp = Long.MAX_VALUE;
    long maxTimestamp = Long.MIN_VALUE;
    for (StoreFile file : filesToCompact) {
      minTimestamp = Math.min(minTimestamp, file.getMinimumTimestamp() == null ? Long.MAX_VALUE
          : file.getMinimumTimestamp().longValue());
      maxTimestamp = Math.max(maxTimestamp, file.getMaximumTimestamp() == null ? Long.MIN_VALUE
          : file.getMaximumTimestamp().longValue());
    }

    List<Long> boundaries = Lists.newArrayList();
    CompactionWindow window = getIncomingWindow(now);
    // find the first window that covers the max timestamp.
    while (window.compareToTimestamp(maxTimestamp) > 0) {
      window = window.nextEarlierWindow();
    }
    boundaries.add(window.endMillis());

    // Add startMillis of all windows between overall max and min timestamp
    for (; window.compareToTimestamp(minTimestamp) > 0; window = window.nextEarlierWindow()) {
      boundaries.add(window.startMillis());
    }
    boundaries.add(minTimestamp);
    Collections.reverse(boundaries);
    return boundaries;
  }

  /**
   * @return a list of boundaries for multiple compaction output from minTimestamp to maxTimestamp.
   */
  private static List<Long> getCompactionBoundariesForMinor(CompactionWindow window,
      boolean singleOutput) {
    List<Long> boundaries = new ArrayList<Long>();
    boundaries.add(Long.MIN_VALUE);
    if (!singleOutput) {
      boundaries.add(window.startMillis());
    }
    boundaries.add(Long.MAX_VALUE);
    return boundaries;
  }

  private CompactionWindow getIncomingWindow(long now) {
    return windowFactory.newIncomingWindow(now);
  }

  private static long getOldestToCompact(long maxAgeMillis, long now) {
    try {
      return LongMath.checkedSubtract(now, maxAgeMillis);
    } catch (ArithmeticException ae) {
      LOG.warn("Value for " + CompactionConfiguration.DATE_TIERED_MAX_AGE_MILLIS_KEY + ": "
          + maxAgeMillis + ". All the files will be eligible for minor compaction.");
      return Long.MIN_VALUE;
    }
  }
}