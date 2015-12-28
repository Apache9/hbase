package org.apache.hadoop.hbase.replication.thrift;

import java.io.IOException;

import junit.framework.Assert;

import org.apache.hadoop.hbase.replication.thrift.generated.TBatchEdit;
import org.apache.hadoop.hbase.replication.thrift.generated.TEdit;
import org.apache.hadoop.hbase.replication.thrift.generated.THLogKey;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ SmallTests.class })
public class TestThriftClient {
  
  @Test
  public void testLoadTableNameMap() throws IOException {
    ThriftClient.loadTableNameMap(null);
    Assert.assertNull(ThriftClient.tableNameMap);
    
    try {
      ThriftClient.loadTableNameMap("a,b");
      Assert.fail();
    } catch (IOException e) {
    }
    ThriftClient.tableNameMap = null;
    
    ThriftClient.loadTableNameMap("ns_a=>ns:a,ns_b=>ns:b");
    Assert.assertEquals(2, ThriftClient.tableNameMap.size());
    Assert.assertEquals("ns:a", ThriftClient.tableNameMap.get("ns_a"));
    Assert.assertEquals("ns:b", ThriftClient.tableNameMap.get("ns_b"));
    ThriftClient.tableNameMap = null;
  }
  
  @Test
  public void testTransferTableNames() throws IOException {
    ThriftClient.loadTableNameMap("ns_a=>ns:a,ns_b=>ns:b");
    TBatchEdit batchEdit = new TBatchEdit();
    TEdit edit = new TEdit();
    THLogKey hlogKey = new THLogKey();
    hlogKey.setTableName("ns_a".getBytes());
    edit.setHLogKey(hlogKey);
    batchEdit.addToEdits(edit);
    ThriftClient.transferTableNames(batchEdit, ThriftClient.tableNameMap);
    Assert.assertEquals("ns:a", new String(batchEdit.getEdits().get(0).getHLogKey().getTableName()));
    hlogKey.setTableName("ns_c".getBytes());
    Assert.assertEquals("ns_c", new String(batchEdit.getEdits().get(0).getHLogKey().getTableName()));
    ThriftClient.tableNameMap = null;
  }
}
