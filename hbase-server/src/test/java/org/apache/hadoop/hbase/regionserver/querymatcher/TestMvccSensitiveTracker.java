/*
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
package org.apache.hadoop.hbase.regionserver.querymatcher;

import java.io.IOException;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.regionserver.querymatcher.DeleteTracker.DeleteResult;
import org.apache.hadoop.hbase.regionserver.querymatcher.ScanQueryMatcher.MatchCode;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;

@Category(SmallTests.class )
public class TestMvccSensitiveTracker {

  private final byte[] col1 = Bytes.toBytes("col1");
  private final byte[] col2 = Bytes.toBytes("col2");
  private final byte[] row = Bytes.toBytes("row");
  private final byte[] family = Bytes.toBytes("family");
  private final byte[] value = Bytes.toBytes("value");

  @Test
  public void testMaxVersionMask() {
    CombinedTracker tracker = new CombinedTracker(null, 1, 3, 3, 10000);

    KeyValue keyValue = new KeyValue(row, family, col1, 20000, KeyValue.Type.Put, value);
    keyValue.setMvccVersion(1000);
    assertEquals(DeleteResult.NOT_DELETED, tracker.isDeleted(keyValue));
    keyValue = new KeyValue(row, family, col1, 19999, KeyValue.Type.Put, value);
    keyValue.setMvccVersion(999);
    assertEquals(DeleteResult.NOT_DELETED, tracker.isDeleted(keyValue));
    keyValue.setMvccVersion(998);
    assertEquals(DeleteResult.VERSION_MASKED, tracker.isDeleted(keyValue));
    keyValue = new KeyValue(row, family, col1, 19998, KeyValue.Type.Put, value);
    keyValue.setMvccVersion(997);
    assertEquals(DeleteResult.NOT_DELETED, tracker.isDeleted(keyValue));
    keyValue = new KeyValue(row, family, col1, 19997, KeyValue.Type.Put, value);
    keyValue.setMvccVersion(996);
    assertEquals(DeleteResult.VERSION_MASKED, tracker.isDeleted(keyValue));

    keyValue = new KeyValue(row, family, col2, 20000, KeyValue.Type.Put, value);
    keyValue.setMvccVersion(1000);
    assertEquals(DeleteResult.NOT_DELETED, tracker.isDeleted(keyValue));
    keyValue = new KeyValue(row, family, col2, 19999, KeyValue.Type.Put, value);
    keyValue.setMvccVersion(1002);
    assertEquals(DeleteResult.NOT_DELETED, tracker.isDeleted(keyValue));
    keyValue = new KeyValue(row, family, col2, 19999, KeyValue.Type.Put, value);
    keyValue.setMvccVersion(1001);
    assertEquals(DeleteResult.VERSION_MASKED, tracker.isDeleted(keyValue));
    keyValue = new KeyValue(row, family, col2, 19998, KeyValue.Type.Put, value);
    keyValue.setMvccVersion(1003);
    assertEquals(DeleteResult.NOT_DELETED, tracker.isDeleted(keyValue));
    keyValue = new KeyValue(row, family, col2, 19997, KeyValue.Type.Put, value);
    keyValue.setMvccVersion(1004);
    assertEquals(DeleteResult.VERSION_MASKED, tracker.isDeleted(keyValue));
  }

  @Test
  public void testVersionsDelete() {
    CombinedTracker tracker = new CombinedTracker(null, 1, 3, 3, 10000);
    KeyValue put = new KeyValue(row, family, col1, 20000, KeyValue.Type.Put, value);
    KeyValue delete = new KeyValue(row, family, col1, 20000, KeyValue.Type.DeleteColumn, value);
    delete.setMvccVersion(1000);
    tracker.add(delete);
    put = new KeyValue(row, family, col1, 19999, KeyValue.Type.Put, value);
    put.setMvccVersion(1001);
    assertEquals(DeleteResult.NOT_DELETED, tracker.isDeleted(put));
    put = new KeyValue(row, family, col1, 19998, KeyValue.Type.Put, value);
    put.setMvccVersion(999);
    assertEquals(DeleteResult.COLUMN_DELETED, tracker.isDeleted(put));

    delete = new KeyValue(row, family, col2, 20000, KeyValue.Type.DeleteColumn, value);
    delete.setMvccVersion(1002);
    tracker.add(delete);
    put = new KeyValue(row, family, col2, 19999, KeyValue.Type.Put, value);
    put.setMvccVersion(1001);
    assertEquals(DeleteResult.COLUMN_DELETED, tracker.isDeleted(put));
    put = new KeyValue(row, family, col2, 19998, KeyValue.Type.Put, value);
    put.setMvccVersion(999);
    assertEquals(DeleteResult.COLUMN_DELETED, tracker.isDeleted(put));
  }

  @Test
  public void testVersionDelete() {
    CombinedTracker tracker = new CombinedTracker(null, 1, 3, 3, 10000);
    KeyValue put = new KeyValue(row, family, col1, 20000, KeyValue.Type.Put, value);
    KeyValue delete = new KeyValue(row, family, col1, 20000, KeyValue.Type.Delete, value);
    delete.setMvccVersion(1000);
    tracker.add(delete);
    put.setMvccVersion(1001);
    assertEquals(DeleteResult.NOT_DELETED, tracker.isDeleted(put));
    put.setMvccVersion(999);
    assertEquals(DeleteResult.VERSION_DELETED, tracker.isDeleted(put));

    delete = new KeyValue(row, family, col2, 20000, KeyValue.Type.Delete, value);
    delete.setMvccVersion(1002);
    tracker.add(delete);
    put = new KeyValue(row, family, col2, 20000, KeyValue.Type.Put, value);
    put.setMvccVersion(1001);
    assertEquals(DeleteResult.VERSION_DELETED, tracker.isDeleted(put));
    put.setMvccVersion(999);
    assertEquals(DeleteResult.VERSION_DELETED, tracker.isDeleted(put));
    put = new KeyValue(row, family, col2, 19999, KeyValue.Type.Put, value);
    put.setMvccVersion(1002);
    assertEquals(DeleteResult.NOT_DELETED, tracker.isDeleted(put));
    put.setMvccVersion(998);
    assertEquals(DeleteResult.VERSION_MASKED, tracker.isDeleted(put));
  }

  @Test
  public void testFamilyVersionsDelete() {
    CombinedTracker tracker = new CombinedTracker(null, 1, 3, 3, 10000);

    KeyValue delete = new KeyValue(row, family, null, 20000, KeyValue.Type.DeleteFamily, value);
    delete.setMvccVersion(1000);

    KeyValue put = new KeyValue(row, family, col1, 20000, KeyValue.Type.Put, value);
    tracker.add(delete);
    put.setMvccVersion(1001);
    assertEquals(DeleteResult.NOT_DELETED, tracker.isDeleted(put));
    put = new KeyValue(row, family, col1, 19998, KeyValue.Type.Put, value);
    put.setMvccVersion(999);
    assertEquals(DeleteResult.COLUMN_DELETED, tracker.isDeleted(put));

    put = new KeyValue(row, family, col2, 19999, KeyValue.Type.Put, value);
    put.setMvccVersion(998);
    assertEquals(DeleteResult.COLUMN_DELETED, tracker.isDeleted(put));
    put = new KeyValue(row, family, col2, 19998, KeyValue.Type.Put, value);
    put.setMvccVersion(999);
    assertEquals(DeleteResult.COLUMN_DELETED, tracker.isDeleted(put));
  }

  @Test
  public void testFamilyVersionDelete() {
    CombinedTracker tracker = new CombinedTracker(null, 1, 3, 3, 10000);

    KeyValue delete = new KeyValue(row, family, null, 20000, KeyValue.Type.DeleteFamilyVersion,
        value);
    delete.setMvccVersion(1000);
    tracker.add(delete);

    KeyValue put = new KeyValue(row, family, col1, 20000, KeyValue.Type.Put, value);
    put.setMvccVersion(1001);
    assertEquals(DeleteResult.NOT_DELETED, tracker.isDeleted(put));
    put.setMvccVersion(999);
    assertEquals(DeleteResult.VERSION_DELETED, tracker.isDeleted(put));

    put = new KeyValue(row, family, col2, 20000, KeyValue.Type.Put, value);
    put.setMvccVersion(1001);
    assertEquals(DeleteResult.NOT_DELETED, tracker.isDeleted(put));
    put.setMvccVersion(999);
    assertEquals(DeleteResult.VERSION_DELETED, tracker.isDeleted(put));
    put = new KeyValue(row, family, col2, 19999, KeyValue.Type.Put, value);
    put.setMvccVersion(1002);
    assertEquals(DeleteResult.NOT_DELETED, tracker.isDeleted(put));
    put.setMvccVersion(998);
    assertEquals(DeleteResult.VERSION_MASKED, tracker.isDeleted(put));
  }

  @Test
  public void testMinVersionsAndTTL() throws IOException {
    CombinedTracker tracker = new CombinedTracker(null, 1, 3, 3, 30000);

    KeyValue keyValue = new KeyValue(row, family, col1, 20000, KeyValue.Type.Put, value);
    keyValue.setMvccVersion(1000);
    assertEquals(DeleteResult.NOT_DELETED, tracker.isDeleted(keyValue));
    assertEquals(MatchCode.INCLUDE_AND_SEEK_NEXT_COL, tracker.checkVersions(
        keyValue.getQualifierArray(), keyValue.getQualifierOffset(), keyValue.getQualifierLength(),
        keyValue.getTimestamp(), keyValue.getTypeByte(), false));
    keyValue = new KeyValue(row, family, col1, 19999, KeyValue.Type.Put, value);
    keyValue.setMvccVersion(999);
    assertEquals(DeleteResult.NOT_DELETED, tracker.isDeleted(keyValue));
    assertEquals(
        MatchCode.SEEK_NEXT_COL,
        tracker.checkVersions(
            keyValue.getQualifierArray(), keyValue.getQualifierOffset(), keyValue.getQualifierLength(),
            keyValue.getTimestamp(), keyValue.getTypeByte(), false));
    keyValue.setMvccVersion(998);
    assertEquals(DeleteResult.VERSION_MASKED, tracker.isDeleted(keyValue));
    assertEquals(MatchCode.SEEK_NEXT_COL,
        tracker.checkVersions(
            keyValue.getQualifierArray(), keyValue.getQualifierOffset(), keyValue.getQualifierLength(),
            keyValue.getTimestamp(), keyValue.getTypeByte(), false));
    keyValue = new KeyValue(row, family, col1, 19998, KeyValue.Type.Put, value);
    keyValue.setMvccVersion(997);
    assertEquals(DeleteResult.NOT_DELETED, tracker.isDeleted(keyValue));
    assertEquals(MatchCode.SEEK_NEXT_COL,
        tracker.checkVersions(
            keyValue.getQualifierArray(), keyValue.getQualifierOffset(), keyValue.getQualifierLength(),
            keyValue.getTimestamp(), keyValue.getTypeByte(), false));
    keyValue = new KeyValue(row, family, col1, 19997, KeyValue.Type.Put, value);
    keyValue.setMvccVersion(996);
    assertEquals(DeleteResult.VERSION_MASKED, tracker.isDeleted(keyValue));
    assertEquals(MatchCode.SEEK_NEXT_COL,
        tracker.checkVersions(
            keyValue.getQualifierArray(), keyValue.getQualifierOffset(), keyValue.getQualifierLength(),
            keyValue.getTimestamp(), keyValue.getTypeByte(), false));

    keyValue = new KeyValue(row, family, col2, 20000, KeyValue.Type.Put, value);
    keyValue.setMvccVersion(1000);
    assertEquals(DeleteResult.NOT_DELETED, tracker.isDeleted(keyValue));
    assertEquals(MatchCode.INCLUDE_AND_SEEK_NEXT_COL,
        tracker.checkVersions(
            keyValue.getQualifierArray(), keyValue.getQualifierOffset(), keyValue.getQualifierLength(),
            keyValue.getTimestamp(), keyValue.getTypeByte(), false));
    keyValue = new KeyValue(row, family, col2, 19999, KeyValue.Type.Put, value);
    keyValue.setMvccVersion(1002);
    assertEquals(DeleteResult.NOT_DELETED, tracker.isDeleted(keyValue));
    assertEquals(MatchCode.SEEK_NEXT_COL,
        tracker.checkVersions(
            keyValue.getQualifierArray(), keyValue.getQualifierOffset(), keyValue.getQualifierLength(),
            keyValue.getTimestamp(), keyValue.getTypeByte(), false));
    keyValue.setMvccVersion(1001);
    assertEquals(DeleteResult.VERSION_MASKED, tracker.isDeleted(keyValue));
    assertEquals(MatchCode.SEEK_NEXT_COL,
        tracker.checkVersions(
            keyValue.getQualifierArray(), keyValue.getQualifierOffset(), keyValue.getQualifierLength(),
            keyValue.getTimestamp(), keyValue.getTypeByte(), false));
    keyValue = new KeyValue(row, family, col2, 19998, KeyValue.Type.Put, value);
    keyValue.setMvccVersion(1003);
    assertEquals(DeleteResult.NOT_DELETED, tracker.isDeleted(keyValue));
    assertEquals(MatchCode.SEEK_NEXT_COL,
        tracker.checkVersions(
            keyValue.getQualifierArray(), keyValue.getQualifierOffset(), keyValue.getQualifierLength(),
            keyValue.getTimestamp(), keyValue.getTypeByte(), false));
    keyValue = new KeyValue(row, family, col2, 19997, KeyValue.Type.Put, value);
    keyValue.setMvccVersion(1004);
    assertEquals(DeleteResult.VERSION_MASKED, tracker.isDeleted(keyValue));
    assertEquals(MatchCode.SEEK_NEXT_COL,
        tracker.checkVersions(
            keyValue.getQualifierArray(), keyValue.getQualifierOffset(), keyValue.getQualifierLength(),
            keyValue.getTimestamp(), keyValue.getTypeByte(), false));
  }
}
