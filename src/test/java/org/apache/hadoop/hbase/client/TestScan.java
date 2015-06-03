/**
 * Copyright 2009 The Apache Software Foundation
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

package org.apache.hadoop.hbase.client;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;
import java.util.NavigableSet;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

// TODO: cover more test cases
@Category(SmallTests.class)
public class TestScan {
  @Test
  public void testAttributesSerialization() throws IOException {
    Scan scan = new Scan();
    scan.setAttribute("attribute1", Bytes.toBytes("value1"));
    scan.setAttribute("attribute2", Bytes.toBytes("value2"));
    scan.setAttribute("attribute3", Bytes.toBytes("value3"));

    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutput out = new DataOutputStream(byteArrayOutputStream);
    scan.write(out);

    Scan scan2 = new Scan();
    Assert.assertTrue(scan2.getAttributesMap().isEmpty());

    scan2.readFields(new DataInputStream(new ByteArrayInputStream(byteArrayOutputStream.toByteArray())));

    Assert.assertNull(scan2.getAttribute("absent"));
    Assert.assertTrue(Arrays.equals(Bytes.toBytes("value1"), scan2.getAttribute("attribute1")));
    Assert.assertTrue(Arrays.equals(Bytes.toBytes("value2"), scan2.getAttribute("attribute2")));
    Assert.assertTrue(Arrays.equals(Bytes.toBytes("value3"), scan2.getAttribute("attribute3")));
    Assert.assertEquals(3, scan2.getAttributesMap().size());
  }

  @Test
  public void testScanAttributes() {
    Scan scan = new Scan();
    Assert.assertTrue(scan.getAttributesMap().isEmpty());
    Assert.assertNull(scan.getAttribute("absent"));

    scan.setAttribute("absent", null);
    Assert.assertTrue(scan.getAttributesMap().isEmpty());
    Assert.assertNull(scan.getAttribute("absent"));

    // adding attribute
    scan.setAttribute("attribute1", Bytes.toBytes("value1"));
    Assert.assertTrue(Arrays.equals(Bytes.toBytes("value1"), scan.getAttribute("attribute1")));
    Assert.assertEquals(1, scan.getAttributesMap().size());
    Assert.assertTrue(Arrays.equals(Bytes.toBytes("value1"), scan.getAttributesMap().get("attribute1")));

    // overriding attribute value
    scan.setAttribute("attribute1", Bytes.toBytes("value12"));
    Assert.assertTrue(Arrays.equals(Bytes.toBytes("value12"), scan.getAttribute("attribute1")));
    Assert.assertEquals(1, scan.getAttributesMap().size());
    Assert.assertTrue(Arrays.equals(Bytes.toBytes("value12"), scan.getAttributesMap().get("attribute1")));

    // adding another attribute
    scan.setAttribute("attribute2", Bytes.toBytes("value2"));
    Assert.assertTrue(Arrays.equals(Bytes.toBytes("value2"), scan.getAttribute("attribute2")));
    Assert.assertEquals(2, scan.getAttributesMap().size());
    Assert.assertTrue(Arrays.equals(Bytes.toBytes("value2"), scan.getAttributesMap().get("attribute2")));

    // removing attribute
    scan.setAttribute("attribute2", null);
    Assert.assertNull(scan.getAttribute("attribute2"));
    Assert.assertEquals(1, scan.getAttributesMap().size());
    Assert.assertNull(scan.getAttributesMap().get("attribute2"));

    // removing non-existed attribute
    scan.setAttribute("attribute2", null);
    Assert.assertNull(scan.getAttribute("attribute2"));
    Assert.assertEquals(1, scan.getAttributesMap().size());
    Assert.assertNull(scan.getAttributesMap().get("attribute2"));

    // removing another attribute
    scan.setAttribute("attribute1", null);
    Assert.assertNull(scan.getAttribute("attribute1"));
    Assert.assertTrue(scan.getAttributesMap().isEmpty());
    Assert.assertNull(scan.getAttributesMap().get("attribute1"));
  }
  
  @Test
  public void testSetNullStartStopRow() {
    Scan scan = new Scan();
    scan.setStartRow(null);
    Assert.assertTrue(Bytes.equals(scan.getStartRow(), HConstants.EMPTY_START_ROW));
    
    scan.setStopRow(null);
    Assert.assertTrue(Bytes.equals(scan.getStartRow(), HConstants.EMPTY_END_ROW));
  }
  
  @Test
  public void testReverseScanCompatibility() throws IOException {
    class MockScan extends Scan {
      private boolean reverse = false;

      @Override
      public void setReversed(boolean reversed) {
        this.reverse = reversed;
      }

      // old reverse scan serialization
      @Override
      public void write(final DataOutput out) throws IOException {
        if (reverse) {
          out.writeByte((byte) 3);
        } else {
          out.writeByte((byte) 2);
        }
        Bytes.writeByteArray(out, null);
        Bytes.writeByteArray(out, null);
        out.writeInt(1);
        out.writeInt(-1);
        out.writeInt(-1);
        out.writeBoolean(true);
        if (reverse) {
          out.writeBoolean(this.reverse);
        }
        out.writeBoolean(false);
        TimeRange tr = new TimeRange();
        tr.write(out);
        out.writeInt(0);
        writeAttributes(out);
      }
    }

    MockScan mockScan = new MockScan();
    Scan scan = getSerializedScan(mockScan);
    Assert.assertFalse(scan.isReversed());
    Assert.assertNull(scan.getAttribute("_reversed_"));
    
    mockScan.setReversed(true);
    scan = getSerializedScan(mockScan);
    Assert.assertTrue(scan.isReversed());
    Assert.assertTrue(Bytes.toBoolean(scan.getAttribute("_reversed_")));
  }

  @Test
  public void testRawLimit() throws IOException {
    Scan scan = new Scan();
    Assert.assertEquals(-1, scan.getRawLimit());
    scan = getSerializedScan(scan);
    Assert.assertEquals(-1, scan.getRawLimit());
    scan.setRawLimit(100);
    scan = getSerializedScan(scan);
    Assert.assertEquals(100, scan.getRawLimit());
  }

  private Scan getSerializedScan(Scan scan) throws IOException {
    DataOutputBuffer out = new DataOutputBuffer();
    scan.write(out);
    out.close();

    DataInputBuffer in = new DataInputBuffer();
    in.reset(out.getData(), out.getLength());
    Scan newScan = new Scan();
    newScan.readFields(in);
    in.close();
    return newScan;
  }
  
  @org.junit.Rule
  public org.apache.hadoop.hbase.ResourceCheckerJUnitRule cu =
    new org.apache.hadoop.hbase.ResourceCheckerJUnitRule();
}

