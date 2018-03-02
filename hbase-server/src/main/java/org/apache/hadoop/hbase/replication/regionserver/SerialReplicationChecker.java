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
package org.apache.hadoop.hbase.replication.regionserver;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.MetaTableAccessor.ReplicationBarrierResult;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationQueueStorage;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.cache.Cache;
import org.apache.hbase.thirdparty.com.google.common.cache.CacheBuilder;

/**
 * <p>
 * Helper class to determine whether we can push a given WAL entry without breaking the replication
 * order.
 * </p>
 * <p>
 * We record all the open sequence number for a region in a special family in meta, which is called
 * 'barrier', so there will be a sequence of open sequence number (b1, b2, b3, ...). We call [bn,
 * bn+1) a range, and it is obvious that a region will always be on the same RS within a range.
 * <p>
 * When split and merge, we will also record the parent for the generated region(s) in the special
 * family in meta. And also, we will write an extra 'open sequence number' for the parent region(s),
 * which is the max sequence id of the region plus one.
 * </p>
 * </p>
 * <p>
 * For each peer, we record the last pushed sequence id for each region. It is managed by the
 * replication storage.
 * </p>
 * <p>
 * The algorithm works like this:
 * <ol>
 * <li>Locate the sequence id we want to push in the barriers</li>
 * <li>If it is before the first barrier, we are safe to push. This usually because we enable serial
 * replication for this table after we create the table and write data into the table.</li>
 * <li>In general, if the previous range is finished, then we are safe to push. The way to determine
 * whether a range is finish is straight-forward: check whether the last pushed sequence id is equal
 * to the end barrier of the range minus 1. There are several exceptions:
 * <ul>
 * <li>If it is in the first range, we need to check whether there are parent regions. If so, we
 * need to make sure that the data for parent regions have all been pushed.</li>
 * <li>If it is in the last range, we need to check the region state. If state is OPENING, then we
 * are not safe to push. This is because that, before we call reportRIT to master which update the
 * open sequence number into meta table, we will write a open region event marker to WAL first, and
 * its sequence id is greater than the newest open sequence number(which has not been updated to
 * meta table yet so we do not know). For this scenario, the WAL entry for this open region event
 * marker actually belongs to the range after the 'last' range, so we are not safe to push it.
 * Otherwise the last pushed sequence id will be updated to this value and then we think the
 * previous range has already been finished, but this is not true.</li>
 * <li>Notice that the above two exceptions are not conflicts, since the first range can also be the
 * last range if we only have one range.</li>
 * </ul>
 * </li>
 * </ol>
 * </p>
 * <p>
 * And for performance reason, we do not want to check meta for every WAL entry, so here we have a
 * simple in memory cache. The idea is simple:
 * <ul>
 * <li>If a range can be pushed, then put its end barrier into the cache.</li>
 * <li>Before accessing meta, first check if the sequence id stored in cache. If the sequence id of
 * WAL entry is less the one stored in cache, then we are safe to push.</li>
 * </ul>
 * And for the last range, we do not have a end barrier, so we use the continuity of sequence id to
 * determine whether we can push. The rule is:
 * <ul>
 * <li>Check if the sequence id of WAL entry equals to the one stored in cache plus one. If so, we
 * are safe to push, and also update the cache to the sequence id of the WAL entry.</li>
 * </ul>
 * You will easily find that there maybe a problem. We will put the end barrier of a range to the
 * cache if it can be pushed, but this does no mean this range is finished. So if here comes a WAL
 * entry with a sequence id equals to the one in cache plus one, it is not safe to be pushed but in
 * the algorithm above we will think it can be pushed and update the cache.<br/>
 * The trick here is that, we will put the end barrier to the cache only if we want to push that
 * range. And this checker is per {@link ReplicationSourceWALReader}, not global, and it is single
 * threaded, so the scenario described above will not happen. We can not jump to the next range if
 * the current range has not been finished yet.
 * </p>
 */
@InterfaceAudience.Private
class SerialReplicationChecker {

  public static final String REPLICATION_SERIALLY_WAITING_KEY =
    "hbase.serial.replication.waiting.ms";
  public static final long REPLICATION_SERIALLY_WAITING_DEFAULT = 10000;

  private final String peerId;

  private final ReplicationQueueStorage storage;

  private final Connection conn;

  private final long waitTimeMs;

  // Use guava cache to set ttl for each key
  private final Cache<String, Long> canReplicateUnder =
    CacheBuilder.newBuilder().expireAfterAccess(1, TimeUnit.DAYS).build();

  public SerialReplicationChecker(Configuration conf, ReplicationSource source) {
    this.peerId = source.getPeerId();
    this.storage = source.queueStorage;
    this.conn = source.server.getConnection();
    this.waitTimeMs =
      conf.getLong(REPLICATION_SERIALLY_WAITING_KEY, REPLICATION_SERIALLY_WAITING_DEFAULT);
  }

  private boolean isRangeFinished(long endBarrier, String encodedRegionName) throws IOException {
    long pushedSeqId;
    try {
      pushedSeqId = storage.getLastSequenceId(encodedRegionName);
    } catch (ReplicationException e) {
      throw new IOException("Failed to get pushed sequence id for " + encodedRegionName);
    }
    // endBarrier is the open sequence number. When opening a region, the open sequence number will
    // be set to the old max sequence id plus one, so here we need to minus one.
    return pushedSeqId >= endBarrier - 1;
  }

  private boolean isParentFinished(byte[] regionName) throws IOException {
    long[] barriers = MetaTableAccessor.getReplicationBarrier(conn, regionName);
    if (barriers.length == 0) {
      return true;
    }
    return isRangeFinished(barriers[barriers.length - 1], RegionInfo.encodeRegionName(regionName));
  }

  // We may write a open region marker to WAL before we write the open sequence number to meta, so
  // if a region is in OPENING state and we are in the last range, it is not safe to say we can push
  // even if the previous range is finished.
  private boolean isLastRangeAndOpening(ReplicationBarrierResult barrierResult, int index) {
    return index == barrierResult.getBarriers().length &&
      barrierResult.getState() == RegionState.State.OPENING;
  }

  private void recordCanPushUnder(String encodedNameAsString, long seqId, long[] barriers,
      int index) {
    if (barriers.length > index) {
      canReplicateUnder.put(encodedNameAsString, barriers[index]);
    } else {
      canReplicateUnder.put(encodedNameAsString, seqId);
    }
  }

  private boolean canPush(Entry entry, byte[] row) throws IOException {
    String encodedNameAsString = Bytes.toString(entry.getKey().getEncodedRegionName());
    long seqId = entry.getKey().getSequenceId();
    ReplicationBarrierResult barrierResult = MetaTableAccessor.getReplicationBarrierResult(conn,
      entry.getKey().getTableName(), row, entry.getKey().getEncodedRegionName());
    long[] barriers = barrierResult.getBarriers();
    int index = Arrays.binarySearch(barriers, seqId);
    if (index == -1) {
      // This means we are in the range before the first record openSeqNum, this usually because the
      // wal is written before we enable serial replication for this table, just return true since
      // we can not guarantee the order.
      if (barriers.length > 0) {
        canReplicateUnder.put(encodedNameAsString, seqId);
      }
      return true;
    }
    // The sequence id range is left closed and right open, so either we decrease the missed insert
    // point to make the index start from 0, or increase the hit insert point to make the index
    // start from 1. Here we choose the latter one.
    if (index < 0) {
      index = -index - 1;
    } else {
      index++;
    }
    if (index == 1) {
      // we are in the first range, check whether we have parents
      for (byte[] regionName : barrierResult.getParentRegionNames()) {
        if (!isParentFinished(regionName)) {
          return false;
        }
      }
      if (isLastRangeAndOpening(barrierResult, index)) {
        return false;
      }
      recordCanPushUnder(encodedNameAsString, seqId, barriers, 1);
      return true;
    }
    // check whether the previous range is finished
    if (!isRangeFinished(barriers[index - 1], encodedNameAsString)) {
      return false;
    }
    if (isLastRangeAndOpening(barrierResult, index)) {
      return false;
    }
    recordCanPushUnder(encodedNameAsString, seqId, barriers, index);
    return true;
  }

  public boolean canPush(Entry entry, Cell firstCellInEdit) throws IOException {
    String encodedNameAsString = Bytes.toString(entry.getKey().getEncodedRegionName());
    long seqId = entry.getKey().getSequenceId();
    Long canReplicateUnderSeqId = canReplicateUnder.getIfPresent(encodedNameAsString);
    if (canReplicateUnderSeqId != null) {
      if (seqId <= canReplicateUnderSeqId.longValue()) {
        return true;
      }
      // This is for the case where the region is currently opened on us, if the sequence id is
      // continuous then we are safe to replicate. If there is a breakpoint, then maybe the region
      // has been moved to another RS and then back, so we need to check the barrier.
      if (seqId == canReplicateUnderSeqId.longValue() + 1) {
        canReplicateUnder.put(encodedNameAsString, entry.getKey().getSequenceId());
        return true;
      }
      // we are already beyond the last safe point, remove
      canReplicateUnder.invalidate(encodedNameAsString);
    }
    return canPush(entry, CellUtil.cloneRow(firstCellInEdit));
  }

  public void waitUntilCanPush(Entry entry, Cell firstCellInEdit)
      throws IOException, InterruptedException {
    byte[] row = CellUtil.cloneRow(firstCellInEdit);
    while (!canPush(entry, row)) {
      Thread.sleep(waitTimeMs);
    }
  }
}
