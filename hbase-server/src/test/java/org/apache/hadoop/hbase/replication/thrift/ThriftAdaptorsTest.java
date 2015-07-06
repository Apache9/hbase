package org.apache.hadoop.hbase.replication.thrift;

import com.google.common.collect.Lists;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.wal.HLogKey;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.replication.thrift.generated.TBatchEdit;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.hamcrest.CoreMatchers;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.apache.hadoop.hbase.replication.thrift.ThriftAdaptors.REPLICATION_BATCH_ADAPTOR;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category(SmallTests.class)
public class ThriftAdaptorsTest {

  @Test
  public void testSerializingHLogEntryAndBack() throws Exception {
    long now = System.currentTimeMillis();
    UUID uuid = UUID.randomUUID();

    List<KeyValue> keyValues = makeKeyValues(1, 10, "CF", "CQ", KeyValue.Type.Put, now);

    List<UUID> clusterIds = Arrays.asList(uuid);
    HLogKey key = createKey("region", "table", now, clusterIds);
    WALEdit walEdit = createWALEdit(Lists.<UUID>newArrayList(), keyValues);
    HLog.Entry entry = new HLog.Entry(
        key,
        walEdit
    );

    TBatchEdit result =
        REPLICATION_BATCH_ADAPTOR.toThrift(Arrays.asList(entry));

    assertEquals(1, result.getEditsSize());

    // change this back to key values and compare each one to ensure they match
    List<HLog.Entry> entries = REPLICATION_BATCH_ADAPTOR.fromThrift(result);
    HLog.Entry onlyEntry = entries.get(0);
    assertTrue(customHLogKeyEquality(onlyEntry.getKey(), key).isEquals());
    List<KeyValue> transformedKeyValues = onlyEntry.getEdit().getKeyValues();
    assertThat(transformedKeyValues, CoreMatchers.hasItems(
        keyValues.toArray(new KeyValue[keyValues.size()])));
  }

  private HLogKey createKey(String region, String table, long timestamp, List<UUID> clusterIds) {
    return new HLogKey(
        Bytes.toBytes(region),
        TableName.valueOf(table),
        -1,
        timestamp,
        clusterIds,
        HConstants.NO_NONCE,
        HConstants.NO_NONCE
    );
  }

  private WALEdit createWALEdit(List<UUID> clusterIds, List<KeyValue> keyValues) {
    WALEdit edit = new WALEdit();
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
        .append(left.getClusterIds(), right.getClusterIds())
        .append(left.getTablename(), right.getTablename());
  }
}
