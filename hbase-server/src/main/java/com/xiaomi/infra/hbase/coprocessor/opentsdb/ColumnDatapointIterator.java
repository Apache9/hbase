package com.xiaomi.infra.hbase.coprocessor.opentsdb;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.yetus.audience.InterfaceAudience;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * OpenTSDBUtil implementation detail for {@link net.opentsdb.core.CompactionQueue}.  This
 * allows iterating over the datapoints in a column without creating objects for each
 * datapoint.
 */
@InterfaceAudience.Private
public class ColumnDatapointIterator implements Comparable<ColumnDatapointIterator> {

  /**
   * @return true if this column needs one or more fixups applied.
   */
  public boolean needsFixup() {
    return needs_fixup;
  }

  private final long column_timestamp;

  // immutable once the constructor returns, but may need to be adjusted for fixups
  public byte[] qualifier; // referenced by CompactionQueue
  private byte[] value;
  private boolean needs_fixup;

  // pointers into the qualifier/value buffers
  private int qualifier_offset;
  private int value_offset;

  // data from the current point
  private int current_timestamp_offset;
  private int current_qual_length;
  private int current_val_length;
  private boolean is_ms;

  /**
   * Create an entry for a column, which will be able to iterate over the individual values
   * contained in it.
   *
   * NOTE: This currently does not expect to be handed a column containing non-datapoint values.
   *
   * @param kv
   */
  public ColumnDatapointIterator(final Cell kv) {
    this.column_timestamp = kv.getTimestamp();
    this.qualifier = CellUtil.cloneQualifier(kv);
    this.value = CellUtil.cloneValue(kv);
    qualifier_offset = 0;
    value_offset = 0;
    checkForFixup();
    update();
  }

  private void checkForFixup() {
    // fixups predate compaction and ms-resolution timestamps, so are all exactly 2 bytes
    if (qualifier.length == 2) {
      final byte qual1 = qualifier[1];
      if (OpenTSDBUtil.floatingPointValueToFix(qual1, value)) {
        value = OpenTSDBUtil.fixFloatingPointValue(qual1, value);
        needs_fixup = true;
      }
      final byte lenByte = OpenTSDBUtil.fixQualifierFlags(qual1, value.length);
      if (lenByte != qual1) {
        qualifier = new byte[] { qualifier[0], lenByte };
        needs_fixup = true;
      }
    }
  }

  /**
   * @return true if there are datapoints in this column.
   */
  public boolean hasMoreData() {
    return qualifier_offset < qualifier.length;
  }

  /**
   * @return the offset of the current datapoint from the column timestamp, in milliseconds
   * (regardless of the stored precision).
   */
  public int getTimestampOffsetMs() {
    return current_timestamp_offset;
  }

  /**
   * @return true if the current datapoint's timestamp is in milliseconds.
   */
  public boolean isMilliseconds() {
    return is_ms;
  }

  /**
   * Copy this value to the output and advance to the next one.
   *
   * @param compQualifier
   * @param compValue
   * @return true if there is more data left in this column
   */
  public void writeToBuffers(ByteBufferList compQualifier, ByteBufferList compValue) {
    compQualifier.add(qualifier, qualifier_offset, current_qual_length);
    compValue.add(value, value_offset, current_val_length);
  }

  public void writeToBuffersFromOffset(ByteBufferList compQualifier, ByteBufferList compValue, Pair<Integer, Integer> offsets, Pair<Integer, Integer> offsetLengths) {
    compQualifier.add(qualifier, offsets.getFirst(), offsetLengths.getFirst());
    compValue.add(value, offsets.getSecond(), offsetLengths.getSecond());
  }

  public Pair<Integer, Integer> getOffsets() {
    return new Pair<Integer, Integer>(qualifier_offset, value_offset);
  }

  public Pair<Integer, Integer> getOffsetLengths() {
    return new Pair<Integer, Integer>(current_qual_length, current_val_length);
  }

  /**
   * @return the length of the qualifier for the current datapoint.
   */
  public int getCurrentQualiferLength() {
    return current_qual_length;
  }

  /**
   * @return a copy of the value of the current datapoint, after any fixups.
   */
  public byte[] getCopyOfCurrentValue() {
    if (needs_fixup) {
      assert value_offset == 0; // fixups should only be in single-value columns
      return OpenTSDBUtil.fixFloatingPointValue(qualifier[qualifier_offset + 1], value);
    } else {
      return Arrays.copyOfRange(value, value_offset, value_offset + current_val_length);
    }
  }

  /**
   * @return a copy of the Qualifier of the current datapoint, after any fixups.
   */
  public byte[] getCopyOfCurrentQualifier() {
    return Arrays.copyOfRange(qualifier, qualifier_offset, qualifier_offset + current_qual_length);
  }

  /**
   * Advance to the next datapoint.
   *
   * @return true if there is at least one more datapoint after advancing
   */
  public boolean advance() {
    qualifier_offset += current_qual_length;
    value_offset += current_val_length;
    return update();
  }

  private boolean update() {
    if (qualifier_offset >= qualifier.length || value_offset >= value.length) {
      return false;
    }
    if (OpenTSDBUtil.inMilliseconds(qualifier[qualifier_offset])) {
      current_qual_length = 4;
      is_ms = true;
    } else {
      current_qual_length = 2;
      is_ms = false;
    }
    current_timestamp_offset = OpenTSDBUtil.getOffsetFromQualifier(qualifier, qualifier_offset);
    current_val_length = OpenTSDBUtil.getValueLengthFromQualifier(qualifier, qualifier_offset);
    return true;
  }

  // order in ascending order by timestamp, descending order by row timestamp (so we find the
  // entry we are going to keep first, and don't have to copy over it)
  @Override
  public int compareTo(ColumnDatapointIterator o) {
    int c = current_timestamp_offset - o.current_timestamp_offset;
    if (c == 0) {
      // note inverse order of comparison!
      c = Long.signum(o.column_timestamp - column_timestamp);
    }
    return c;
  }

  public double getCellValueAsDouble() {
    byte[] copy = this.getCopyOfCurrentValue();
    byte[] qual = this.getCopyOfCurrentQualifier();
    ByteBuffer bb = ByteBuffer.wrap(copy);
    if (OpenTSDBUtil.isFloat(qual)) {
      return copy.length == 4 ? bb.getFloat() : bb.getDouble();
    } else {
      return ((copy.length == 1) ? bb.get()
              : ((copy.length == 2) ? bb.getShort() : ((copy.length == 4) ? bb.getInt() : bb.getLong())));
    }
  }

  @Override
  public String toString() {
    return "q=" + Arrays.toString(qualifier) + " [ofs=" + qualifier_offset + "], v="
            + Arrays.toString(value) + " [ofs=" + value_offset + "], ts=" + current_timestamp_offset;
  }

}
