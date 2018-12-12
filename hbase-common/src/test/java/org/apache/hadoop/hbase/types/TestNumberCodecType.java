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

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MiscTests.class, SmallTests.class })
public class TestNumberCodecType {

  private Number encodeAndDecode(NumberCodecType type, Number value) {
    return type.decode(type.encode(value));
  }

  @Test
  public void testEncodeAndDecode() {
    assertEquals((byte) 123, encodeAndDecode(NumberCodecType.RAW_BYTE, 123));
    assertEquals((byte) -123, encodeAndDecode(NumberCodecType.ORDERED_INT8_ASC, -123));

    assertEquals((short) 23456, encodeAndDecode(NumberCodecType.RAW_SHORT, 23456));
    assertEquals((short) -23456, encodeAndDecode(NumberCodecType.ORDERED_INT16_ASC, -23456));

    assertEquals(2345678, encodeAndDecode(NumberCodecType.RAW_INTEGER, 2345678));
    assertEquals(-2345678, encodeAndDecode(NumberCodecType.ORDERED_INT32_ASC, -2345678));

    assertEquals(1234567890123L, encodeAndDecode(NumberCodecType.RAW_LONG, 1234567890123L));
    assertEquals(-1234567890123L,
      encodeAndDecode(NumberCodecType.ORDERED_INT64_ASC, -1234567890123L));

    assertEquals(12.34f, encodeAndDecode(NumberCodecType.RAW_FLOAT, 12.34f).floatValue(), 1e-4);
    assertEquals(-12.34f,
      encodeAndDecode(NumberCodecType.ORDERED_FLOAT32_ASC, -12.34f).floatValue(), 1e-4);

    assertEquals(23.456789, encodeAndDecode(NumberCodecType.RAW_DOUBLE, 23.456789).doubleValue(),
      1e-8);
    assertEquals(-23.456789,
      encodeAndDecode(NumberCodecType.ORDERED_FLOAT64_ASC, -23.456789).doubleValue(), 1e-8);
  }

  private Number add(NumberCodecType type, Number base, Number amount) {
    byte[] b = type.encode(base);
    return type.decode(type.add(b, 0, b.length, amount));
  }

  @Test
  public void testAdd() {
    assertEquals((byte) 123, add(NumberCodecType.RAW_BYTE, 100, 23));
    assertEquals((byte) -123, add(NumberCodecType.RAW_BYTE, -100, -23));

    assertEquals((short) 23456, add(NumberCodecType.RAW_SHORT, 23000, 456));
    assertEquals((short) -23456, add(NumberCodecType.ORDERED_INT16_ASC, -23000, -456));

    assertEquals(2345678, add(NumberCodecType.RAW_INTEGER, 2340000, 5678));
    assertEquals(-2345678, add(NumberCodecType.ORDERED_INT32_ASC, -2340000, -5678));

    assertEquals(1234567890123L, add(NumberCodecType.RAW_LONG, 1234567890000L, 123L));
    assertEquals(-1234567890123L, add(NumberCodecType.ORDERED_INT64_ASC, -1234567890000L, -123L));

    assertEquals(12.34f, add(NumberCodecType.RAW_FLOAT, 10.04f, 2.3f).floatValue(), 1e-4);
    assertEquals(-12.34f, add(NumberCodecType.ORDERED_FLOAT32_ASC, -10.04f, -2.3f).floatValue(),
      1e-4);

    assertEquals(23.456789, add(NumberCodecType.RAW_DOUBLE, 20.056789, 3.4).doubleValue(), 1e-8);
    assertEquals(-23.456789,
      add(NumberCodecType.ORDERED_FLOAT64_ASC, -20.056789, -3.4).doubleValue(), 1e-8);
  }
}
