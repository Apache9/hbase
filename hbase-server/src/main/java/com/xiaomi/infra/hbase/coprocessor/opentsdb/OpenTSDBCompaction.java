package com.xiaomi.infra.hbase.coprocessor.opentsdb;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.PriorityQueue;

import static com.xiaomi.infra.hbase.coprocessor.opentsdb.OpenTSDBUtil.MAX_TIMESPAN;
import static com.xiaomi.infra.hbase.coprocessor.opentsdb.OpenTSDBUtil.MS_MIXED_COMPACT;
import static com.xiaomi.infra.hbase.coprocessor.opentsdb.OpenTSDBUtil.USE_OTSDB_TIMESTAMP;

/**
 * rewrite of opentsdb compaction
 */
@InterfaceAudience.Private
public class OpenTSDBCompaction {
  private static final Logger LOG = LoggerFactory.getLogger(OpenTSDBCompaction.class);
  private final List<Cell> row;
  private long compactedKVTimestamp;
  private List<Cell> skipCompactCells;

  private final int nkvs;

  // true if any ms-resolution datapoints have been seen in the column
  private boolean ms_in_row;
  // true if any s-resolution datapoints have been seen in the column
  private boolean s_in_row;

  // heap of columns, ordered by increasing timestamp
  private PriorityQueue<ColumnDatapointIterator> heap;

  // KeyValue containing the longest qualifier for the datapoint, used to optimize
  // checking if the compacted qualifier already exists.
  private Cell longest;

  // the latest append column. If set then we don't want to re-write the row
  // and if we only had a single column with a single value, we return this.
  private Cell last_append_column;

  /**
   * Constructor for Compaction
   * @param row row cells to compact
   * @param skipCells out list
   */
  public OpenTSDBCompaction(List<Cell> row, List<Cell> skipCells) {
    nkvs = row.size();
    this.row = row;
    compactedKVTimestamp = Long.MIN_VALUE;
    this.skipCompactCells = skipCells;
  }

  /**
   * do compact for a row, return compacted Cell.
   * @return if null, no thing to do
   */
  public Cell compact() {
    // no columns in row, nothing to do
    if (nkvs == 0) {
      return null;
    }
    compactedKVTimestamp = Long.MIN_VALUE;
    // go through all the columns
    // ignore process annotations, and histograms
    heap = new PriorityQueue<ColumnDatapointIterator>(nkvs);
    int tot_values = buildHeapProcessAnnotations();

    // if there are no datapoints or only one that needs no fixup, we are done
    if (noMergesOrFixups()) {
      // return the single non-annotation entry if requested
      if (heap.size() == 1) {
        return findFirstDatapointColumn();
      }
      return null;
    }

    // merge the datapoints, ordered by timestamp and removing duplicates
    final ByteBufferList compacted_qual = new ByteBufferList(tot_values);
    final ByteBufferList compacted_val = new ByteBufferList(tot_values);
    //compaction_count.incrementAndGet();
    mergeDatapoints(compacted_qual, compacted_val);

    // if we wound up with no data in the compacted column, we are done
    if (compacted_qual.segmentCount() == 0) {
      return null;
    }

    // build the compacted columns
    final Cell compact = buildCompactedColumn(compacted_qual, compacted_val);
    final long base_time = OpenTSDBUtil.getRowKeyBaseTime(compact);
    final long cut_off = System.currentTimeMillis() / 1000 - MAX_TIMESPAN - 1;
    if (base_time > cut_off) {  // If row is too recent...
      return null;              // ... Don't write back compacted.
    }
    return compact;
  }

  /**
   * Build a heap of columns containing datapoints.  Assumes that non-datapoint columns are
   * never merged.  Adds datapoint columns to the list of rows to be deleted.
   *
   * @return an estimate of the number of total values present, which may be high
   */
  private int buildHeapProcessAnnotations() {
    // skip Annotation and HistogramDataPoint and AppendDataPoints
    byte ANNOTATION_PREFIX = 0x01;
    byte HISTOGRAMDATAPOIN_PREFIX = 0x6;
    byte APPEND_COLUMN_PREFIX = 0x05;

    int tot_values = 0;
    for (final Cell kv : row) {
      byte[] qual = CellUtil.cloneQualifier(kv);
      int len = qual.length;
      if ((len & 1) != 0) {
        // process annotations and other extended formats
        if (qual[0] == ANNOTATION_PREFIX) {
          // TODO: skip Annotation now
        } else if (qual[0] == HISTOGRAMDATAPOIN_PREFIX) {
          // skip HistogramDataPoint
        } else if (qual[0] == APPEND_COLUMN_PREFIX) {
          // skip AppendDataPoints
        } else {
          LOG.warn("Ignoring unexpected extended format type " + qual[0]);
        }
        skipCompactCells.add(kv);
        continue;
      }
      // estimate number of points based on the size of the first entry
      // in the column; if ms/sec datapoints are mixed, this will be
      // incorrect, which will cost a reallocation/copy
      final int entry_size = OpenTSDBUtil.inMilliseconds(qual) ? 4 : 2;
      tot_values += (len + entry_size - 1) / entry_size;
      if (longest == null || longest.getQualifierLength() < kv.getQualifierLength()) {
        longest = kv;
      }
      ColumnDatapointIterator col = new ColumnDatapointIterator(kv);
      compactedKVTimestamp = Math.max(compactedKVTimestamp, kv.getTimestamp());
      if (col.hasMoreData()) {
        heap.add(col);
      }
      // don't need to delete kvs
      // to_delete.add(kv);
    }
    return tot_values;
  }

  /**
   * Check if there are no fixups or merges required.  This will be the case when:
   * <ul>
   *  <li>there are no columns in the heap</li>
   *  <li>there is only one single-valued column needing no fixups</li>
   * </ul>
   *
   * @return true if we know no additional work is required
   */
  private boolean noMergesOrFixups() {
    switch (heap.size()) {
      case 0:
        // no data points, nothing to do
        return true;
      case 1:
        // only one column, check to see if it needs fixups
        ColumnDatapointIterator col = heap.peek();
        // either a 2-byte qualifier or one 4-byte ms qualifier, and no fixups required
        return (col.qualifier.length == 2 || (col.qualifier.length == 4
            && OpenTSDBUtil.inMilliseconds(col.qualifier))) && !col.needsFixup();
      default:
        // more than one column, need to merge
        return false;
    }
  }

  /**
   * Find the first datapoint column in a row. It may be an appended column
   *
   * @return the first found datapoint column in the row, or null if none
   */
  private Cell findFirstDatapointColumn() {
    if (last_append_column != null) {
      return last_append_column;
    }
    for (final Cell kv : row) {
      if (isDatapoint(kv)) {
        return kv;
      }
    }
    return null;
  }

  /**
   * Check if a particular column is a datapoint column (as opposed to annotation or other
   * extended formats).
   *
   * @param kv column to check
   * @return true if the column represents one or more datapoint
   */
  public boolean isDatapoint(Cell kv) {
    return (kv.getQualifierLength() & 1) == 0;
  }

  /**
   * Process datapoints from the heap in order, merging into a sorted list.  Handles duplicates
   * by keeping the most recent (based on HBase column timestamps; if duplicates in the )
   *
   * @param compacted_qual qualifiers for sorted datapoints
   * @param compacted_val values for sorted datapoints
   */
  private void mergeDatapoints(ByteBufferList compacted_qual,
                               ByteBufferList compacted_val) {
    // defalut tsdb.getConfig().use_otsdb_timestamp() = false
    boolean use_otsdb_timestamp = USE_OTSDB_TIMESTAMP;
    if (use_otsdb_timestamp) {
      dtcsMergeDataPoints(compacted_qual, compacted_val);
    } else {
      defaultMergeDataPoints(compacted_qual, compacted_val);
    }
  }

  private void defaultMergeDataPoints(ByteBufferList compacted_qual,
                                      ByteBufferList compacted_val) {
    int prevTs = -1;
    while (!heap.isEmpty()) {
      final ColumnDatapointIterator col = heap.remove();
      final int ts = col.getTimestampOffsetMs();
      if (ts == prevTs) {
        // check to see if it is a complete duplicate, or if the value changed
        final byte[] existingVal = compacted_val.getLastSegment();
        final byte[] discardedVal = col.getCopyOfCurrentValue();
        if (!Arrays.equals(existingVal, discardedVal)) {
          // TODO: skip Duplicate cells now
          LOG.warn("Duplicate timestamp for key=" + Arrays.toString(CellUtil.cloneRow(row.get(0)))
              + ", ms_offset=" + ts + ", kept=" + Arrays.toString(existingVal) + ", discarded="
              + Arrays.toString(discardedVal));
        } else {
          // two datapoint has same timestamp and value.
          //duplicates_same.incrementAndGet();
        }
      } else {
        prevTs = ts;
        col.writeToBuffers(compacted_qual, compacted_val);
        ms_in_row |= col.isMilliseconds();
        s_in_row |= !col.isMilliseconds();
      }
      if (col.advance()) {
        // there is still more data in this column, so add it back to the heap
        heap.add(col);
      }
    }
  }

  private void dtcsMergeDataPoints(ByteBufferList compacted_qual, ByteBufferList compacted_val) {
    // Compare timestamps for two KeyValues at the same time, if they are same compare their values
    // Return maximum or minimum value depending upon tsd.storage.use_max_value parameter
    // This function is called once for every RowKey, so we only care about comparing offsets, which
    // are a part of column qualifier
    // default tsdb.config.use_max_value() = true
    boolean use_max_value = true;
    ColumnDatapointIterator col1 = null;
    ColumnDatapointIterator col2 = null;
    while (!heap.isEmpty()) {
      col1 = heap.remove();
      Pair<Integer, Integer> offsets = col1.getOffsets();
      Pair<Integer, Integer> offsetLengths = col1.getOffsetLengths();
      int ts1 = col1.getTimestampOffsetMs();
      double val1 = col1.getCellValueAsDouble();
      if (col1.advance()) {
        heap.add(col1);
      }
      int ts2 = ts1;
      while (ts1 == ts2) {
        col2 = heap.peek();
        ts2 = col2 != null ? col2.getTimestampOffsetMs() : ts2;
        if (col2 == null || ts1 != ts2)
          break;
        double val2 = col2.getCellValueAsDouble();
        if ((use_max_value && val2 > val1) || (!use_max_value && val1 > val2)) {
          // Reduce copying of byte arrays by just using col1 variable to reference to either max or min KeyValue
          col1 = col2;
          val1 = val2;
          offsets = col2.getOffsets();
          offsetLengths = col2.getOffsetLengths();
        }
        heap.remove();
        if (col2.advance()) {
          heap.add(col2);
        }
      }
      col1.writeToBuffersFromOffset(compacted_qual, compacted_val, offsets, offsetLengths);
      ms_in_row |= col1.isMilliseconds();
      s_in_row |= !col1.isMilliseconds();
    }
  }

  /**
   * Build the compacted column from the list of byte buffers that were
   * merged together.
   *
   * @param compacted_qual list of merged qualifiers
   * @param compacted_val list of merged values
   *
   * @return instance for the compacted column
   */
  private Cell buildCompactedColumn(ByteBufferList compacted_qual, ByteBufferList compacted_val) {
    // metadata is a single byte for a multi-value column, otherwise nothing
    final int metadata_length = compacted_val.segmentCount() > 1 ? 1 : 0;
    final byte[] cq = compacted_qual.toBytes(0);
    final byte[] cv = compacted_val.toBytes(metadata_length);

    // add the metadata flag, which right now only includes whether we mix s/ms datapoints
    if (metadata_length > 0) {
      byte metadata_flag = 0;
      if (ms_in_row && s_in_row) {
        metadata_flag |= MS_MIXED_COMPACT;
      }
      cv[cv.length - 1] = metadata_flag;
    }

    final Cell first = row.get(0);
    // tsdb.getConfig().getBoolean("tsd.storage.use_otsdb_timestamp")
    boolean use_otsdb_timestamp = USE_OTSDB_TIMESTAMP;
    if (use_otsdb_timestamp) {
      // discard Cell: mvcc tags.
      return CellUtil.createCell(CellUtil.cloneRow(first), CellUtil.cloneFamily(first),
          cq, compactedKVTimestamp, first.getTypeByte(), cv);
    } else {
      long ts = EnvironmentEdgeManager.currentTime();
      return CellUtil.createCell(CellUtil.cloneRow(first), CellUtil.cloneFamily(first),
          cq, ts, first.getTypeByte(), cv);
    }
  }
}
