package com.xiaomi.infra.hbase.coprocessor.opentsdb;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * Accumulates byte buffers, analogous to a StringBuffer.
 *
 * @since 2.1
 */
public class ByteBufferList {

  private static class BufferSegment {
    public final byte[] buf;
    public final int offset;
    public final int len;

    public BufferSegment(final byte[] buf, final int offset, final int len) {
      this.buf = buf;
      this.offset = offset;
      this.len = len;
    }
  }

  private final ArrayList<BufferSegment> segments;
  private int total_length;

  /**
   * Create a ByteBufferList.
   *
   * @param n estimated number of segments (an overestimate is better than an underestimate)
   */
  public ByteBufferList(final int n) {
    segments = new ArrayList<BufferSegment>(n);
  }

  /**
   * Add a segment to the buffer list.
   *
   * @param buf byte array
   * @param offset offset into buf
   * @param len length of segment, starting at offset
   */
  public void add(final byte[] buf, final int offset, final int len) {
    segments.add(new BufferSegment(buf, offset, len));
    total_length += len;
  }

  /**
   * Get the most recently added segment.
   *
   * @return byte array, a copy of the most recently added segment
   */
  public byte[] getLastSegment() {
    if (segments.isEmpty()) {
      return null;
    }
    BufferSegment seg = segments.get(segments.size() - 1);
    return Arrays.copyOfRange(seg.buf, seg.offset, seg.offset + seg.len);
  }

  /**
   * Get the number of segments that have added to this buffer list.
   *
   * @return the segment count
   */
  public int segmentCount() {
    return segments.size();
  }

  /**
   * Get the accumulated bytes as a single byte array (may be a zero-byte array if empty).
   *
   * @param padding the number of additional bytes to include at the end
   * @return the accumulated bytes
   */
  public byte[] toBytes(final int padding) {
    // special case a single entry
    if (padding == 0 && segments.size() == 1) {
      BufferSegment seg = segments.get(0);
      if (seg.offset == 0 && seg.len == seg.buf.length) {
        return seg.buf;
      }
      return Arrays.copyOfRange(seg.buf, seg.offset, seg.offset + seg.len);
    }
    byte[] result = new byte[total_length + padding];
    int ofs = 0;
    for (BufferSegment seg : segments) {
      System.arraycopy(seg.buf, seg.offset, result, ofs, seg.len);
      ofs += seg.len;
    }
    return result;
  }
}
