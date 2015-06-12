package com.xiaomi.infra.hbase.salted;

import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;

public class TestNBytePrefixKeySalter {
  @Test
  public void testConstruction() {
    try {
      new NBytePrefixKeySalter(Integer.MAX_VALUE);
      Assert.fail();
    } catch (RuntimeException e) {
      Assert.assertTrue(e.getMessage().indexOf("slots count illegal") >= 0);
    }
    
    try {
      new NBytePrefixKeySalter(0);
      Assert.fail();
    } catch (RuntimeException e) {
      Assert.assertTrue(e.getMessage().indexOf("slots count illegal") >= 0);
    }
  }
  
  @Test
  public void testGetPrefixLength() {
    Assert.assertEquals(1, NBytePrefixKeySalter.getPrefixLength(256));
    Assert.assertEquals(2, NBytePrefixKeySalter.getPrefixLength(256 * 256));
    Assert.assertEquals(3, NBytePrefixKeySalter.getPrefixLength(256 * 256 * 256));
    Assert.assertEquals(3, NBytePrefixKeySalter.getPrefixLength(Integer.MAX_VALUE));
  }
  
  @Test
  public void testToSalt() {
    for (int i = 0; i < 256; ++i) {
      byte[] salt = NBytePrefixKeySalter.hashValueToSalt(i, 2);
      Assert.assertEquals((byte)i, salt[0]);
    }
    
    for (int i = 256; i < 1000; ++i) {
      byte[] salt = NBytePrefixKeySalter.hashValueToSalt(i, 2);
      Assert.assertEquals((byte)(i % 256), salt[0]);
      Assert.assertEquals((byte)(i / 256), salt[1]);
    }
  }
  
  @Test
  public void testGetAllSalts() {
    NBytePrefixKeySalter salter = new NBytePrefixKeySalter(1000);
    Assert.assertEquals(2, salter.getSaltLength());
    Assert.assertEquals(1000, salter.getAllSalts().length);    
  }
  
  @Test
  public void testSalt() {
    int base = 1000000;
    int slotsCount = 1000;
    NBytePrefixKeySalter salter = new NBytePrefixKeySalter(slotsCount);
    TreeMap<byte[], Integer> slotsValueCount = new TreeMap<byte[], Integer>(Bytes.BYTES_COMPARATOR);
    for (byte[] salt : salter.getAllSalts()) {
      slotsValueCount.put(salt, 0);
    }
    int testCount = 100000;
    for (int i = 0; i < testCount; ++i) {
      byte[] rowkey = Bytes.toBytes(String.valueOf(base + i));
      byte[] salt = salter.getSalt(rowkey);
      slotsValueCount.put(salt, slotsValueCount.get(salt) + 1);
      byte[] saltedRowkey = salter.salt(rowkey);
      Assert.assertEquals(rowkey.length + 2, saltedRowkey.length);
      Assert.assertTrue(Bytes.equals(salt, 0, salt.length, saltedRowkey, 0, salt.length));
      Assert.assertTrue(Bytes.equals(rowkey, 0, rowkey.length, saltedRowkey, 2, rowkey.length));
    }
    
    for (Entry<byte[], Integer> entry : slotsValueCount.entrySet()) {
      int minValue = (int)((testCount / slotsCount) * 0.9);
      Assert.assertTrue(entry.getValue() > minValue);
    }
  }
  
  @Test
  public void testLastNextSalt() {
    int slotsCount = 10;
    NBytePrefixKeySalter salter = new NBytePrefixKeySalter(10);
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
