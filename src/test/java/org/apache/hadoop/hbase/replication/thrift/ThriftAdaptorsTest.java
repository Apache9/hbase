package org.apache.hadoop.hbase.replication.thrift;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

import junit.framework.Assert;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.wal.HLogKey;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.replication.thrift.generated.TBatchEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Lists;

@Category(SmallTests.class)
public class ThriftAdaptorsTest {

  @Test
  public void testSerializingHLogEntryAndBack() throws Exception {
    long now = System.currentTimeMillis();
    UUID uuid = UUID.randomUUID();

    List<KeyValue> keyValues = makeKeyValues(1, 10, "CF", "CQ", KeyValue.Type.Put, now);

    HLogKey key = createKey("region", "table", now, uuid);
    WALEdit walEdit = createWALEdit(Lists.<UUID>newArrayList(), keyValues);
    HLog.Entry entry = new HLog.Entry(
        key,
        walEdit
    );

    TBatchEdit result =
        ThriftAdaptors.REPLICATION_BATCH_ADAPTOR.toThrift(new HLog.Entry[] { entry });

    assertEquals(1, result.getEditsSize());

    // change this back to key values and compare each one to ensure they match
    HLog.Entry[] entries = ThriftAdaptors.REPLICATION_BATCH_ADAPTOR.fromThrift(result);
    HLog.Entry onlyEntry = entries[0];
    assertTrue(customHLogKeyEquality(onlyEntry.getKey(), key).isEquals());
    List<KeyValue> transformedKeyValues = onlyEntry.getEdit().getKeyValues();
    Assert.assertEquals(keyValues.size(), transformedKeyValues.size());
    Collections.sort(keyValues, KeyValue.COMPARATOR);
    Collections.sort(transformedKeyValues, KeyValue.COMPARATOR);
    for (int i = 0; i < keyValues.size(); ++i) {
      Assert.assertTrue(keyValues.get(i).equals(transformedKeyValues.get(i)));
      Assert.assertTrue(Bytes.equals(keyValues.get(i).getValue(), transformedKeyValues.get(i)
          .getValue()));
    }
  }

  private HLogKey createKey(String region, String table, long timestamp, UUID uuid) {
    return new HLogKey(Bytes.toBytes(region), Bytes.toBytes(table), -1, timestamp, uuid);
  }

  private WALEdit createWALEdit(List<UUID> clusterIds, List<KeyValue> keyValues) {
    WALEdit edit = new WALEdit();
    edit.addClusterIds(clusterIds);
    for (KeyValue keyValue : keyValues) {
      edit.add(keyValue);
    }
    return edit;
  }

  private List<KeyValue> makeKeyValues(int from, int to, String family, String cq,
      KeyValue.Type type, long ts) {
    List<KeyValue> result = Lists.newArrayList();
    while (from < to) {
      byte[] rowkey = Bytes.toBytes("row-" + from);
      result.add(
          new KeyValue(rowkey, Bytes.toBytes(family), Bytes.toBytes(cq), ts, type, rowkey));
      from++;
    }
    return result;
  }

  // have to write custom equality because all we nee for replication is
  // clusterId and tableName and writeTime
  private EqualsBuilder customHLogKeyEquality(HLogKey left, HLogKey right) {
    return new EqualsBuilder()
        .append(left.getClusterId(), right.getClusterId())
        .append(left.getTablename(), right.getTablename());
  }

}
