package org.apache.hadoop.hbase.mapreduce;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestProgressEstimator {
  private static final double DELTA = 1e-15;

  @Test
  public void testGetProgress() {
    byte[] startRow = Bytes.toBytesBinary("\\x01");
    byte[] endRow = Bytes.toBytesBinary("\\x02");
    ProgressEstimator estimator = new ProgressEstimator(startRow, endRow);

    ImmutableBytesWritable key;
    key = new ImmutableBytesWritable(Bytes.toBytesBinary("\\x01"));
    assertEquals(0, estimator.getProgress(key), DELTA);

    key = new ImmutableBytesWritable(Bytes.toBytesBinary("\\x01\\xAB"));
    assertEquals(0.66796875, estimator.getProgress(key), DELTA);
    float progress1 = estimator.getProgress(key);

    // ImmutableBytesWritable with share bytes
    key = new ImmutableBytesWritable(Bytes.toBytesBinary("\\xCD\\x01\\xFF\\xFF"), 1, 1);
    assertEquals(0, estimator.getProgress(key), DELTA);

    key = new ImmutableBytesWritable(Bytes.toBytesBinary("\\xCD\\xEF\\x01\\xAB\\xFF\\xFF"), 2, 2);
    assertEquals(0.66796875, estimator.getProgress(key), DELTA);

    key = new ImmutableBytesWritable(Bytes.toBytesBinary("\\x01\\xFF"));
    float progress2 = estimator.getProgress(key);
    assertTrue(progress2 > progress1);

    key = new ImmutableBytesWritable(Bytes.toBytesBinary("\\x01\\xFF\\xFF\\xFF"));
    float progress3 = estimator.getProgress(key);
    assertTrue(progress3 > progress2);

    key = new ImmutableBytesWritable(Bytes.toBytesBinary("\\x01\\xFF\\xFF\\xFF\\x01"));
    float progress4 = estimator.getProgress(key);
    assertEquals(progress4, progress3, DELTA);

    key = new ImmutableBytesWritable(Bytes.toBytesBinary("\\x02"));
    assertEquals(1, estimator.getProgress(key), DELTA);
  }

  @Test
  public void testGetProgressForEndRowKey() {
    byte[] startRow = Bytes.toBytesBinary("\\x01");
    byte[] endRow = null;
    ProgressEstimator estimator = new ProgressEstimator(startRow, endRow);

    ImmutableBytesWritable key = new ImmutableBytesWritable(Bytes.toBytesBinary("\\x01"));
    assertEquals(0, estimator.getProgress(key), DELTA);

    key = new ImmutableBytesWritable(Bytes.toBytesBinary("\\xFF\\xFF\\xFF\\xFF"));
    assertEquals(1, estimator.getProgress(key), DELTA);
  }

  @Test
  public void testGetProgressForEqualEmptyRange() {
    byte[] startRow = Bytes.toBytesBinary("\\x01");
    byte[] endRow = Bytes.toBytesBinary("\\x01");
    ProgressEstimator estimator = new ProgressEstimator(startRow, endRow);
    ImmutableBytesWritable key = new ImmutableBytesWritable(Bytes.toBytesBinary("\\x01"));
    assertEquals(0, estimator.getProgress(key), DELTA);
  }

  @Test
  public void testGetProgressForNullStartRow() {
    byte[] startRow = null;
    byte[] endRow = Bytes.toBytesBinary("\\x01");
    ProgressEstimator estimator = new ProgressEstimator(startRow, endRow);
    ImmutableBytesWritable key = new ImmutableBytesWritable(Bytes.toBytesBinary("\\x00"));
    assertTrue(1 > estimator.getProgress(key));

    key = new ImmutableBytesWritable(Bytes.toBytesBinary("\\x01"));
    assertEquals(1, estimator.getProgress(key), DELTA);
  }
}