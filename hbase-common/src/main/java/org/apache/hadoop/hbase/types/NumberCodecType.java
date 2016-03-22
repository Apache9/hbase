package org.apache.hadoop.hbase.types;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.PositionedByteRange;
import org.apache.hadoop.hbase.util.SimplePositionedByteRange;

public enum NumberCodecType {
  // the id is used for serialization, don't change or remove the existing value
  RAW_BYTE((byte) 0x00),
  RAW_SHORT((byte) 0x01),
  RAW_INTEGER((byte) 0x02),
  RAW_LONG((byte) 0x03),
  RAW_FLOAT((byte) 0x04),
  RAW_DOUBLE((byte) 0x05),

  ORDERED_INT8_ASC((byte) 0x10),
  ORDERED_INT16_ASC((byte) 0x11),
  ORDERED_INT32_ASC((byte) 0x12),
  ORDERED_INT64_ASC((byte) 0x13),
  ORDERED_FLOAT32_ASC((byte) 0x14),
  ORDERED_FLOAT64_ASC((byte) 0x15);

  static Map<Byte, NumberCodecType> lookupMap;
  static {
    lookupMap = new HashMap<Byte, NumberCodecType>();
    for (NumberCodecType e : NumberCodecType.values()) {
      lookupMap.put(e.id, e);
    }
  }

  private final byte id;

  NumberCodecType(byte id) {
    this.id = id;
  }

  public byte getTypeId() {
    return id;
  }

  public static NumberCodecType fromTypeId(byte id) {
    NumberCodecType type = lookupMap.get(id);
    if (type == null) {
      throw new IllegalArgumentException("invalid enum type: " + id);
    }
    return type;
  }

  /**
   * Encode the value, the caller is responsible for the conversion overflow.
   */
  public byte[] encode(Number value) {
    switch (this) {
    case RAW_BYTE: {
      return Bytes.toBytes(value.byteValue());
    }
    case RAW_SHORT: {
      return Bytes.toBytes(value.shortValue());
    }
    case RAW_INTEGER: {
      return Bytes.toBytes(value.intValue());
    }
    case RAW_LONG: {
      return Bytes.toBytes(value.longValue());
    }
    case RAW_FLOAT: {
      return Bytes.toBytes(value.floatValue());
    }
    case RAW_DOUBLE: {
      return Bytes.toBytes(value.doubleValue());
    }
    case ORDERED_INT8_ASC: {
      OrderedInt8 codec = OrderedInt8.ASCENDING;
      byte val = value.byteValue();
      PositionedByteRange dst = new SimplePositionedByteRange(codec.encodedLength(val));
      codec.encode(dst, val);
      return dst.getBytes();
    }
    case ORDERED_INT16_ASC: {
      OrderedInt16 codec = OrderedInt16.ASCENDING;
      short val = value.shortValue();
      PositionedByteRange dst = new SimplePositionedByteRange(codec.encodedLength(val));
      codec.encode(dst, val);
      return dst.getBytes();
    }
    case ORDERED_INT32_ASC: {
      OrderedInt32 codec = OrderedInt32.ASCENDING;
      int val = value.intValue();
      PositionedByteRange dst = new SimplePositionedByteRange(codec.encodedLength(val));
      codec.encode(dst, val);
      return dst.getBytes();
    }
    case ORDERED_INT64_ASC: {
      OrderedInt64 codec = OrderedInt64.ASCENDING;
      long val = value.longValue();
      PositionedByteRange dst = new SimplePositionedByteRange(codec.encodedLength(val));
      codec.encode(dst, val);
      return dst.getBytes();
    }
    case ORDERED_FLOAT32_ASC: {
      OrderedFloat32 codec = OrderedFloat32.ASCENDING;
      float val = value.floatValue();
      PositionedByteRange dst = new SimplePositionedByteRange(codec.encodedLength(val));
      codec.encode(dst, val);
      return dst.getBytes();
    }
    case ORDERED_FLOAT64_ASC: {
      OrderedFloat64 codec = OrderedFloat64.ASCENDING;
      double val = value.doubleValue();
      PositionedByteRange dst = new SimplePositionedByteRange(codec.encodedLength(val));
      codec.encode(dst, val);
      return dst.getBytes();
    }
    default:
      throw new IllegalArgumentException("invalid type: " + this);
    }
  }

  /**
   * Decode the byte array.
   */
  public Number decode(byte[] bytes) {
    return decode(bytes, 0, bytes.length);
  }

  /**
   * Decode the byte array.
   */
  public Number decode(byte[] bytes, int offset, int length) {
    PositionedByteRange src = new SimplePositionedByteRange(bytes, offset, length);
    switch (this) {
    case RAW_BYTE:
      return bytes[0];
    case RAW_SHORT:
      return Bytes.toShort(bytes, offset, length);
    case RAW_INTEGER:
      return Bytes.toInt(bytes, offset, length);
    case RAW_LONG:
      return Bytes.toLong(bytes, offset, length);
    case RAW_FLOAT:
      return Bytes.toFloat(bytes, offset);
    case RAW_DOUBLE:
      return Bytes.toDouble(bytes, offset);
    case ORDERED_INT8_ASC:
      return OrderedInt8.ASCENDING.decode(src);
    case ORDERED_INT16_ASC:
      return OrderedInt16.ASCENDING.decode(src);
    case ORDERED_INT32_ASC:
      return OrderedInt32.ASCENDING.decode(src);
    case ORDERED_INT64_ASC:
      return OrderedInt64.ASCENDING.decode(src);
    case ORDERED_FLOAT32_ASC:
      return OrderedFloat32.ASCENDING.decode(src);
    case ORDERED_FLOAT64_ASC:
      return OrderedFloat64.ASCENDING.decode(src);
    default:
      throw new IllegalArgumentException("invalid type: " + this);
    }
  }
}
