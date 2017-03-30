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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestSequenceIdAccounting {
  private static final byte[] ENCODED_REGION_NAME = Bytes.toBytes("r");
  private static final byte[] FAMILY1 = Bytes.toBytes("cf1");
  private static final byte[] FAMILY2 = Bytes.toBytes("cf2");
  private static final Set<byte[]> FAMILIES = ImmutableSet.of(FAMILY1, FAMILY2);

  @Test
  public void testUpdateLowestUnflushedSequenceIds() {
    SequenceIdAccounting sida = new SequenceIdAccounting();
    assertEquals(NO_SEQNUM, sida.getLowestSequenceId(ENCODED_REGION_NAME));
    FAMILIES.forEach(
      family -> assertEquals(NO_SEQNUM, sida.getLowestSequenceId(ENCODED_REGION_NAME, family)));

    sida.updateLowestUnflushedSequenceIds(ENCODED_REGION_NAME, ImmutableMap.of(FAMILY1, 1L));
    assertEquals(1L, sida.getLowestSequenceId(ENCODED_REGION_NAME));
    assertEquals(1L, sida.getLowestSequenceId(ENCODED_REGION_NAME, FAMILY1));
    assertEquals(NO_SEQNUM, sida.getLowestSequenceId(ENCODED_REGION_NAME, FAMILY2));

    sida.updateLowestUnflushedSequenceIds(ENCODED_REGION_NAME,
      ImmutableMap.of(FAMILY1, 3L, FAMILY2, 2L));
    assertEquals(2L, sida.getLowestSequenceId(ENCODED_REGION_NAME));
    assertEquals(3L, sida.getLowestSequenceId(ENCODED_REGION_NAME, FAMILY1));
    assertEquals(2L, sida.getLowestSequenceId(ENCODED_REGION_NAME, FAMILY2));
  }

  @Test
  public void testAreAllLower() {
    SequenceIdAccounting sida = new SequenceIdAccounting();
    Map<byte[], Long> m = new HashMap<byte[], Long>();
    m.put(ENCODED_REGION_NAME, NO_SEQNUM);
    assertTrue(sida.areAllLower(m));
    long sequenceId = 1L;
    sida.updateLowestUnflushedSequenceIds(ENCODED_REGION_NAME,
      ImmutableMap.of(FAMILY1, sequenceId, FAMILY2, sequenceId));
    assertTrue(sida.areAllLower(m));
    m.put(ENCODED_REGION_NAME, sequenceId);
    assertFalse(sida.areAllLower(m));
  }

  @Test
  public void testFindLower() {
    SequenceIdAccounting sida = new SequenceIdAccounting();
    Map<byte[], Long> m = new HashMap<byte[], Long>();
    m.put(ENCODED_REGION_NAME, NO_SEQNUM);
    long sequenceId = 2;
    sida.updateLowestUnflushedSequenceIds(ENCODED_REGION_NAME,
      ImmutableMap.of(FAMILY1, sequenceId, FAMILY2, sequenceId));
    assertNull(sida.findLower(m));
    m.put(ENCODED_REGION_NAME, sequenceId);
    assertEquals(1, sida.findLower(m).length);
    m.put(ENCODED_REGION_NAME, sequenceId - 1);
    assertNull(sida.findLower(m));
  }
}