package com.xiaomi.infra.hbase.salted;

import org.junit.Assert;
import org.junit.Test;

public class TestOneBytePrefixKeySalter {
  @Test
  public void testLastNextSalt() {
    int slotsCount = 10;
    OneBytePrefixKeySalter salter = new OneBytePrefixKeySalter(10);
    byte[][] allSalts = salter.getAllSalts();
    
    for (int i = 0; i < slotsCount; ++i) {
      if (i == 0) {
        Assert.assertNull(salter.lastSalt(allSalts[i]));
      } else {
        Assert.assertArrayEquals(allSalts[i - 1], salter.lastSalt(allSalts[i]));
      }
    }
    
    for (int i = 0; i < slotsCount; ++i) {
      if (i == slotsCount - 1) {
        Assert.assertNull(salter.nextSalt(allSalts[i]));
      } else {
        Assert.assertArrayEquals(allSalts[i + 1], salter.nextSalt(allSalts[i]));
      }
    }
  }
}
