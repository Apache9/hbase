package org.apache.hadoop.hbase.util;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import junit.framework.Assert;

import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.client.Action;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.MultiAction;
import org.apache.hadoop.hbase.client.Row;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestObjects {
  @Test
  public void testAddOperationDetail() {
    Map<String, Object> details = new HashMap<String, Object>();
    // Operations such as Get/Delete
    Get get = new Get(Bytes.toBytes("row"));
    get.addColumn(Bytes.toBytes("F"), Bytes.toBytes("q"));
    Objects.addOperationDetail(details, get.getClass().getSimpleName(), get);
    Assert.assertEquals(1, details.size());
    Entry<String, Object> entry = details.entrySet().iterator().next();
    Assert.assertEquals("Get", entry.getKey());
    
    details.clear();
    Increment incr = new Increment(Bytes.toBytes("row"));
    incr.addColumn(Bytes.toBytes("F"), Bytes.toBytes("q"), 1);
    Objects.addOperationDetail(details, incr.getClass().getSimpleName(), incr);
    Assert.assertEquals(1, details.size());
    entry = details.entrySet().iterator().next();
    Assert.assertEquals("Increment", entry.getKey());
    Assert.assertEquals("row", entry.getValue());
  }
  
  @Test
  public void testGetOperationDetails() throws IOException {
    byte[] fakeRegion = "abc".getBytes();
    Object[] params = null;
    
    // null request
    Assert.assertEquals("", Objects.getOperationDetails(null));
    
    // get request
    Get get = new Get(Bytes.toBytes("row"));
    get.addColumn(Bytes.toBytes("F"), Bytes.toBytes("q"));
    params = new Object[]{fakeRegion, get};
    Assert.assertTrue(Objects.getOperationDetails(params).indexOf("row") >= 0);
    
    // multi request
    MultiAction<Row> multi = new MultiAction<Row>();
    Action<Row> action = new Action<Row>(get, 0);
    multi.add(fakeRegion, action);
    get = new Get(Bytes.toBytes("row1"));
    get.addColumn(Bytes.toBytes("F"), Bytes.toBytes("q"));
    action = new Action<Row>(get, 1);
    multi.add(fakeRegion, action);
    
    Increment incr = new Increment(Bytes.toBytes("row"));
    incr.addColumn(Bytes.toBytes("F"), Bytes.toBytes("q"), 1);
    fakeRegion = "abcd".getBytes();
    action = new Action<Row>(incr, 2);
    multi.add(fakeRegion, action);
    params = new Object[]{multi};
    String result = Objects.getOperationDetails(params);
    Assert.assertTrue(result.indexOf("Get_0") >= 0);
    Assert.assertTrue(result.indexOf("Get_1") >= 0);
    Assert.assertTrue(result.indexOf("Increment_2") >= 0);
    
    // operation not recognized
    Assert.assertEquals(
      "",
      Objects.getOperationDetails(new Object[] { Bytes.toBytes("regionName"), Bytes.toBytes("row"),
          Bytes.toBytes("f"), Bytes.toBytes("q") }));
  }
}
