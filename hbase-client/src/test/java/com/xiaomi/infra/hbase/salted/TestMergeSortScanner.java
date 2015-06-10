package com.xiaomi.infra.hbase.salted;

import junit.framework.Assert;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import com.xiaomi.infra.hbase.salted.MergeSortScanner.IgnorePrefixComparator;

public class TestMergeSortScanner {
  
  protected Result getResult(String rowkey) {
    KeyValue kv = new KeyValue(Bytes.toBytes(rowkey), HConstants.LATEST_TIMESTAMP);
    return new Result(new KeyValue[]{kv});
  }
  
  @Test
  public void testIgnorePrefixComparator() {
    int prefixLenght = 2;
    IgnorePrefixComparator comparator = new IgnorePrefixComparator(prefixLenght);
    try {
      comparator.compare(getResult("a"), getResult("b"));
      Assert.fail();
    } catch (RuntimeException e) {
      Assert.assertTrue(e.getMessage().indexOf("row length is shorter than prefixLength") >= 0);
    }
    try {
      comparator.compare(getResult("a"), getResult("ab"));
      Assert.fail();
    } catch (RuntimeException e) {
      Assert.assertTrue(e.getMessage().indexOf("row length is shorter than prefixLength") >= 0);
    }
    try {
      comparator.compare(getResult("ab"), getResult("a"));
      Assert.fail();
    } catch (RuntimeException e) {
      Assert.assertTrue(e.getMessage().indexOf("row length is shorter than prefixLength") >= 0);
    }
    
    Assert.assertEquals(0, comparator.compare(getResult("ab"), getResult("aa")));
    Assert.assertEquals(1, comparator.compare(getResult("abc"), getResult("aa")));
    Assert.assertEquals(-1, comparator.compare(getResult("ab"), getResult("aaa")));
    Assert.assertEquals(0, comparator.compare(getResult("aab"), getResult("aab")));
    Assert.assertEquals(-1, comparator.compare(getResult("aaa"), getResult("aab")));
    Assert.assertEquals(1, comparator.compare(getResult("aac"), getResult("aab")));
  }
}
