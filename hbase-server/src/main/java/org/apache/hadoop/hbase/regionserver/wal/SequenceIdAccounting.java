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

import static org.apache.hadoop.hbase.HConstants.NO_SEQNUM;
import static org.apache.hadoop.hbase.util.CollectionUtils.computeIfAbsent;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import org.apache.commons.lang.mutable.MutableLong;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.HashedBytes;
import org.apache.hadoop.hbase.util.Pair;

/**
 * Accounting of sequence ids per region and then by column family. So we can our accounting
 * current, call startCacheFlush and then finishedCacheFlush or abortCacheFlush so this instance can
 * keep abreast of the state of sequence id persistence. Also call update per append.
 * <p>
 * For the implementation, we assume that all the {@code encodedRegionName} passed in is gotten by
 * {@link HRegionInfo#getEncodedNameAsBytes()}. So it is safe to use it as a hash key. And for
 * family name, we use {@link ImmutableByteArray} as key. This is because hash based map is much
 * faster than RBTree or CSLM and here we are on the critical write path. See HBASE-16278 for more
 * details.
 */
@InterfaceAudience.Private
class SequenceIdAccounting {

  private static final Log LOG = LogFactory.getLog(SequenceIdAccounting.class);

  /**
   * Map of encoded region names and family names to their OLDEST -- i.e. their first, the
   * longest-lived, their 'earliest', the 'lowest' -- sequence id.
   * <p>
   * This map will be updated at 4 places:
   * <ul>
   * <li>When region open, we will call {@code WAL.updateStore} which will add a record to the
   * map.</li>
   * <li>After flushing, we will call {@code WAL.completeCacheFlush} which will add several records
   * to the map.</li>
   * <li>After in-memory flush, we will call {@code WAL.updateStore} which will add a record to the
   * map.</li>
   * <li>When region close, we will call {@code WAL.closeRegion} which will remove the records
   * related to the region from the map.</li>
   * </ul>
   * The removal is necessary as we do not have other ways to clean up the records belong to regions
   * that have already been moved to other RSes.
   */
  private final ConcurrentMap<byte[], ConcurrentMap<HashedBytes, Long>> lowestUnflushedSequenceIds =
      new ConcurrentHashMap<>();

  /**
   * Map of region encoded names to the latest/highest region sequence id. Updated on each call to
   * append.
   * <p>
   * This map uses byte[] as the key, and uses reference equality. It works in our use case as we
   * use {@link HRegionInfo#getEncodedNameAsBytes()} as keys. For a given region, it always returns
   * the same array.
   */
  private Map<byte[], MutableLong> highestSequenceIds = new HashMap<>();

  /**
   * Returns the lowest unflushed sequence id for the region.
   * @param encodedRegionName
   * @return Lowest outstanding unflushed sequenceid for <code>encodedRegionName</code>. Will return
   *         {@link HConstants#NO_SEQNUM} when none.
   */
  long getLowestSequenceId(byte[] encodedRegionName) {
    ConcurrentMap<HashedBytes, Long> m = this.lowestUnflushedSequenceIds.get(encodedRegionName);
    return m != null ? getLowestSequenceId(m) : NO_SEQNUM;
  }

  /**
   * @param encodedRegionName
   * @param familyName
   * @return Lowest outstanding unflushed sequenceid for <code>encodedRegionname</code> and
   *         <code>familyName</code>. Returned sequenceid may be for an edit currently being
   *         flushed.
   */
  long getLowestSequenceId(byte[] encodedRegionName, byte[] familyName) {
    ConcurrentMap<HashedBytes, Long> m = this.lowestUnflushedSequenceIds.get(encodedRegionName);
    if (m == null) {
      return NO_SEQNUM;
    }
    Long lowest = m.get(new HashedBytes(familyName));
    return lowest != null ? lowest.longValue() : NO_SEQNUM;
  }

  /**
   * Reset the accounting of highest sequenceid by regionname.
   * @return Return the previous accounting Map of regions to the last sequence id written into
   *         each.
   */
  Map<byte[], Long> resetHighest() {
    Map<byte[], MutableLong> old = this.highestSequenceIds;
    this.highestSequenceIds = new HashMap<>();
    return old.entrySet().stream().map(e -> Pair.newPair(e.getKey(), e.getValue().longValue()))
        .collect(Collectors.toMap(Pair::getFirst, Pair::getSecond));
  }

  /**
   * We've been passed a new sequenceid for the region. Set it as highest seen for this region and
   * if we are to record oldest.
   * @param encodedRegionName
   * @param sequenceId
   */
  void update(byte[] encodedRegionName, long sequenceId) {
    this.highestSequenceIds.computeIfAbsent(encodedRegionName, k -> new MutableLong())
        .setValue(sequenceId);
  }

  private ConcurrentMap<HashedBytes, Long> getOrCreateLowestSequenceIds(byte[] encodedRegionName) {
    return computeIfAbsent(this.lowestUnflushedSequenceIds, encodedRegionName,
      ConcurrentHashMap::new);
  }

  /**
   * @param sequenceIds Map to search for lowest value.
   * @return Lowest value found in <code>sequenceIds</code>.
   */
  private static long getLowestSequenceId(Map<?, Long> sequenceIds) {
    return sequenceIds.values().stream().mapToLong(l -> l.longValue()).filter(l -> l != NO_SEQNUM)
        .min().orElse(NO_SEQNUM);
  }

  void updateLowestUnflushedSequenceIds(byte[] encodedRegionName,
      Map<byte[], Long> family2LowestUnflushedSequenceId) {
    ConcurrentMap<HashedBytes, Long> m = getOrCreateLowestSequenceIds(encodedRegionName);
    family2LowestUnflushedSequenceId
        .forEach((family, lowest) -> m.compute(new HashedBytes(family), (k, oldLowest) -> {
          if (oldLowest == null) {
            return lowest;
          }
          if (oldLowest.longValue() > lowest.longValue()) {
            LOG.error("lowest unflushed sequence id for region " +
                Bytes.toString(encodedRegionName) + " go backwards, " + oldLowest + "->" + lowest +
                ", there should be a critial bug that may cause data loss");
          }
          // Do not use math.max here as it requires extra boxing/unboxing
          return lowest.longValue() > oldLowest.longValue() ? lowest : oldLowest;
        }));
  }

  void remove(byte[] encodedRegionName) {
    this.lowestUnflushedSequenceIds.remove(encodedRegionName);
  }

  /**
   * See if passed <code>sequenceids</code> are lower -- i.e. earlier -- than any outstanding
   * sequenceids, sequenceids we are holding on to in this accounting instance.
   * @param sequenceIds Keyed by encoded region name. Cannot be null (doesn't make sense for it to
   *          be null).
   * @return true if all sequenceids are lower, older than, the old sequenceids in this instance.
   */
  boolean areAllLower(Map<byte[], Long> sequenceIds) {
    for (Map.Entry<byte[], Long> e : sequenceIds.entrySet()) {
      ConcurrentMap<HashedBytes, Long> m = this.lowestUnflushedSequenceIds.get(e.getKey());
      if (m == null) {
        continue;
      }
      if (m.values().stream().mapToLong(l -> l.longValue()).filter(l -> l != NO_SEQNUM)
          .anyMatch(l -> l <= e.getValue().longValue())) {
        return false;
      }
    }
    return true;
  }

  /**
   * Iterates over the given Map and compares sequence ids with corresponding entries in
   * {@link #lowestUnflushedSequenceIds}. If a region in {@link #lowestUnflushedSequenceIds} has a
   * sequence id less than that passed in <code>sequenceids</code> then return it.
   * @param sequenceids Sequenceids keyed by encoded region name.
   * @return regions found in this instance with sequence ids less than those passed in.
   */
  byte[][] findLower(Map<byte[], Long> sequenceIds) {
    List<byte[]> toFlush = new ArrayList<>();
    sequenceIds.forEach((encodedRegionName, sequenceId) -> {
      ConcurrentMap<HashedBytes, Long> m = this.lowestUnflushedSequenceIds.get(encodedRegionName);
      if (m == null) {
        return;
      }
      if (m.values().stream().mapToLong(l -> l.longValue()).filter(l -> l != NO_SEQNUM)
          .anyMatch(l -> l <= sequenceId)) {
        toFlush.add(encodedRegionName);
      }
    });
    return toFlush.isEmpty() ? null : toFlush.toArray(new byte[0][]);
  }
}