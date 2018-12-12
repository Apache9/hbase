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
package org.apache.hadoop.hbase.types;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.PositionedByteRange;
import org.apache.hadoop.hbase.util.SimplePositionedByteRange;
import org.apache.hadoop.hbase.util.SimplePositionedMutableByteRange;

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

  private static final NumberCodecType[] VALUES;
  static {
    VALUES = new NumberCodecType[256];
    for (NumberCodecType type: values()) {
      VALUES[type.getTypeId() & 0xFF] = type;
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
    NumberCodecType type = VALUES[id & 0xFF];
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
        return new byte[] { value.byteValue() };
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
        PositionedByteRange dst = new SimplePositionedMutableByteRange(codec.encodedLength(val));
        codec.encode(dst, val);
        return dst.getBytes();
      }
      case ORDERED_INT16_ASC: {
        OrderedInt16 codec = OrderedInt16.ASCENDING;
        short val = value.shortValue();
        PositionedByteRange dst = new SimplePositionedMutableByteRange(codec.encodedLength(val));
        codec.encode(dst, val);
        return dst.getBytes();
      }
      case ORDERED_INT32_ASC: {
        OrderedInt32 codec = OrderedInt32.ASCENDING;
        int val = value.intValue();
        PositionedByteRange dst = new SimplePositionedMutableByteRange(codec.encodedLength(val));
        codec.encode(dst, val);
        return dst.getBytes();
      }
      case ORDERED_INT64_ASC: {
        OrderedInt64 codec = OrderedInt64.ASCENDING;
        long val = value.longValue();
        PositionedByteRange dst = new SimplePositionedMutableByteRange(codec.encodedLength(val));
        codec.encode(dst, val);
        return dst.getBytes();
      }
      case ORDERED_FLOAT32_ASC: {
        OrderedFloat32 codec = OrderedFloat32.ASCENDING;
        float val = value.floatValue();
        PositionedByteRange dst = new SimplePositionedMutableByteRange(codec.encodedLength(val));
        codec.encode(dst, val);
        return dst.getBytes();
      }
      case ORDERED_FLOAT64_ASC: {
        OrderedFloat64 codec = OrderedFloat64.ASCENDING;
        double val = value.doubleValue();
        PositionedByteRange dst = new SimplePositionedMutableByteRange(codec.encodedLength(val));
        codec.encode(dst, val);
        return dst.getBytes();
      }
      default:
        throw new IllegalArgumentException("invalid type: " + this);
    }
  }

  /**
   * Decode the given bytes, add the amount, and then encode the result.
   */
  public byte[] add(byte[] bytes, int offset, int length, Number amount) {
    switch (this) {
      case RAW_BYTE: {
        int v = bytes[offset] & 0xFF;
        int r = v + amount.intValue();
        return new byte[] { ((byte) r) };
      }
      case RAW_SHORT: {
        int v = Bytes.toShort(bytes, offset, length) & 0xFFFF;
        int r = v + amount.intValue();
        return Bytes.toBytes((short) r);
      }
      case RAW_INTEGER: {
        int v = Bytes.toInt(bytes, offset, length);
        int r = v + amount.intValue();
        return Bytes.toBytes(r);
      }
      case RAW_LONG: {
        long v = Bytes.toLong(bytes, offset, length);
        long r = v + amount.longValue();
        return Bytes.toBytes(r);
      }
      case RAW_FLOAT: {
        float v = Bytes.toFloat(bytes, offset);
        float r = v + amount.floatValue();
        return Bytes.toBytes(r);
      }
      case RAW_DOUBLE: {
        double v = Bytes.toDouble(bytes, offset);
        double r = v + amount.doubleValue();
        return Bytes.toBytes(r);
      }
      case ORDERED_INT8_ASC: {
        OrderedInt8 codec = OrderedInt8.ASCENDING;
        int v =
          codec.decode(new SimplePositionedByteRange(bytes, offset, length)).byteValue() & 0xFF;
        int r = v + amount.intValue();
        PositionedByteRange dst =
          new SimplePositionedMutableByteRange(codec.encodedLength((byte) r));
        codec.encode(dst, (byte) r);
        return dst.getBytes();
      }
      case ORDERED_INT16_ASC: {
        OrderedInt16 codec = OrderedInt16.ASCENDING;
        int v =
          codec.decode(new SimplePositionedByteRange(bytes, offset, length)).shortValue() & 0xFFFF;
        int r = v + amount.intValue();
        PositionedByteRange dst =
          new SimplePositionedMutableByteRange(codec.encodedLength((short) r));
        codec.encode(dst, (short) r);
        return dst.getBytes();
      }
      case ORDERED_INT32_ASC: {
        OrderedInt32 codec = OrderedInt32.ASCENDING;
        int v = codec.decode(new SimplePositionedByteRange(bytes, offset, length)).intValue();
        int r = v + amount.intValue();
        PositionedByteRange dst = new SimplePositionedMutableByteRange(codec.encodedLength(r));
        codec.encode(dst, r);
        return dst.getBytes();
      }
      case ORDERED_INT64_ASC: {
        OrderedInt64 codec = OrderedInt64.ASCENDING;
        long v = codec.decode(new SimplePositionedByteRange(bytes, offset, length)).longValue();
        long r = v + amount.longValue();
        PositionedByteRange dst = new SimplePositionedMutableByteRange(codec.encodedLength(r));
        codec.encode(dst, r);
        return dst.getBytes();
      }
      case ORDERED_FLOAT32_ASC: {
        OrderedFloat32 codec = OrderedFloat32.ASCENDING;
        float v = codec.decode(new SimplePositionedByteRange(bytes, offset, length)).floatValue();
        float r = v + amount.floatValue();
        PositionedByteRange dst = new SimplePositionedMutableByteRange(codec.encodedLength(r));
        codec.encode(dst, r);
        return dst.getBytes();
      }
      case ORDERED_FLOAT64_ASC: {
        OrderedFloat64 codec = OrderedFloat64.ASCENDING;
        double v = codec.decode(new SimplePositionedByteRange(bytes, offset, length)).doubleValue();
        double r = v + amount.doubleValue();
        PositionedByteRange dst = new SimplePositionedMutableByteRange(codec.encodedLength(r));
        codec.encode(dst, r);
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
