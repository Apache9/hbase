package org.apache.hadoop.hbase.quotas;

import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;

@Category({ SmallTests.class })
public class TestQuotaUtil {
  @Test
  public void testCalculateReadCapacityUnitNum() {
    assertEquals(0, QuotaUtil.calculateReadCapacityUnitNum(0));

    assertEquals(1, QuotaUtil.calculateReadCapacityUnitNum(1));
    assertEquals(1, QuotaUtil.calculateReadCapacityUnitNum(QuotaUtil.READ_CAPACITY_UNIT / 4));
    assertEquals(1, QuotaUtil.calculateReadCapacityUnitNum(QuotaUtil.READ_CAPACITY_UNIT / 2));
    assertEquals(1, QuotaUtil.calculateReadCapacityUnitNum(QuotaUtil.READ_CAPACITY_UNIT));

    assertEquals(2, QuotaUtil.calculateReadCapacityUnitNum(QuotaUtil.READ_CAPACITY_UNIT + 1));
    assertEquals(2, QuotaUtil.calculateReadCapacityUnitNum(QuotaUtil.READ_CAPACITY_UNIT * 3 / 2));
    assertEquals(2, QuotaUtil.calculateReadCapacityUnitNum(QuotaUtil.READ_CAPACITY_UNIT * 2));

    assertEquals(3, QuotaUtil.calculateReadCapacityUnitNum(QuotaUtil.READ_CAPACITY_UNIT * 2 + 1));
    assertEquals(3, QuotaUtil.calculateReadCapacityUnitNum(QuotaUtil.READ_CAPACITY_UNIT * 3));

    assertEquals(5, QuotaUtil.calculateReadCapacityUnitNum(QuotaUtil.READ_CAPACITY_UNIT * 5));
  }

  @Test
  public void testCalculateWriteCapacityUnitNum() {
    assertEquals(0, QuotaUtil.calculateWriteCapacityUnitNum(0));

    assertEquals(1, QuotaUtil.calculateWriteCapacityUnitNum(1));
    assertEquals(1, QuotaUtil.calculateWriteCapacityUnitNum(QuotaUtil.WRITE_CAPACITY_UNIT / 4));
    assertEquals(1, QuotaUtil.calculateWriteCapacityUnitNum(QuotaUtil.WRITE_CAPACITY_UNIT / 2));
    assertEquals(1, QuotaUtil.calculateWriteCapacityUnitNum(QuotaUtil.WRITE_CAPACITY_UNIT));

    assertEquals(2, QuotaUtil.calculateWriteCapacityUnitNum(QuotaUtil.WRITE_CAPACITY_UNIT + 1));
    assertEquals(2, QuotaUtil.calculateWriteCapacityUnitNum(QuotaUtil.WRITE_CAPACITY_UNIT * 3 / 2));
    assertEquals(2, QuotaUtil.calculateWriteCapacityUnitNum(QuotaUtil.WRITE_CAPACITY_UNIT * 2));

    assertEquals(3, QuotaUtil.calculateWriteCapacityUnitNum(QuotaUtil.WRITE_CAPACITY_UNIT * 2 + 1));
    assertEquals(3, QuotaUtil.calculateWriteCapacityUnitNum(QuotaUtil.WRITE_CAPACITY_UNIT * 3));

    assertEquals(5, QuotaUtil.calculateWriteCapacityUnitNum(QuotaUtil.WRITE_CAPACITY_UNIT * 5));
  }
}
