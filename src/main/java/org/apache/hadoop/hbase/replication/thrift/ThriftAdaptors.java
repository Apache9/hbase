package org.apache.hadoop.hbase.replication.thrift;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.wal.HLogKey;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.replication.thrift.generated.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.List;
import java.util.UUID;

public class ThriftAdaptors {

  interface Adaptor<K, V> {

    K fromThrift(V vee);

    V toThrift(K kay);

  }

  public static final ReplicationBatchAdaptor REPLICATION_BATCH_ADAPTOR = new ReplicationBatchAdaptor();
  static final HLogKeyAdaptor H_LOG_KEY_ADAPTOR = new HLogKeyAdaptor();
  static final ClusterIdAdaptor CLUSTER_ID_ADAPTOR = new ClusterIdAdaptor();
  static final ColumnValueAdaptor COLUMN_VALUE_ADAPTOR = new ColumnValueAdaptor();
  static final WALEntryAdaptor WAL_ENTRY_ADAPTOR = new WALEntryAdaptor();

  public static class ReplicationBatchAdaptor implements Adaptor<HLog.Entry[], TBatchEdit> {

    @Override public HLog.Entry[] fromThrift(TBatchEdit thriftBatch) {
      HLog.Entry[] entries = new HLog.Entry[thriftBatch.getEditsSize()];
      int i = 0;
      for (TEdit edit : thriftBatch.getEdits()) {
        List<UUID> uuids = CLUSTER_ID_ADAPTOR.fromThrift(edit.getClusterIds());
        entries[i] = new HLog.Entry(
            H_LOG_KEY_ADAPTOR.fromThrift(new EntryWrapper<THLogKey>(edit.getHLogKey(), uuids)),
            WAL_ENTRY_ADAPTOR.fromThrift(new EntryWrapper<TWalLEdit>(edit.getWalEdit(), uuids))
        );
        i++;
      }
      return entries;
    }

    @Override public TBatchEdit toThrift(HLog.Entry[] hLogEntries) {
      TBatchEdit batch = new TBatchEdit();

      for (HLog.Entry hLogEntry : hLogEntries) {
        List<UUID> clusterIds = Lists.newArrayList();
        clusterIds.add(hLogEntry.getKey().getClusterId());
        clusterIds.addAll(hLogEntry.getEdit().getClusterIds());
        batch.addToEdits(
            new TEdit(
                H_LOG_KEY_ADAPTOR.toThrift(hLogEntry.getKey()).getEntryPart(),
                WAL_ENTRY_ADAPTOR.toThrift(hLogEntry.getEdit()).getEntryPart(),
                CLUSTER_ID_ADAPTOR.toThrift(clusterIds)
            ));
      }
      return batch;
    }
  }

  // Maps HLogKey to Thrift: THLogKey
  static class HLogKeyAdaptor implements Adaptor<HLogKey, EntryWrapper<THLogKey>> {

    @Override public HLogKey fromThrift(EntryWrapper<THLogKey> wrapper) {
      return new HLogKey(
          null,
          wrapper.getEntryPart().getTableName(),
          System.currentTimeMillis(),
          wrapper.getEntryPart().getWriteTime(),
          wrapper.getClusterIds().get(0)
      );
    }


    @Override public EntryWrapper<THLogKey> toThrift(HLogKey hLogKey) {
      THLogKey key = new THLogKey();
      key.setTableName(hLogKey.getTablename());
      key.setWriteTime(hLogKey.getWriteTime());
      return new EntryWrapper<THLogKey>(key);
    }
  }

  // Maps WALEdit to Thrift: TWalEdit
  static class WALEntryAdaptor implements Adaptor<WALEdit, EntryWrapper<TWalLEdit>> {

    @Override public WALEdit fromThrift(EntryWrapper<TWalLEdit> thriftEdit) {
      WALEdit walEdit = new WALEdit();
      for (TColumnValue mutation : thriftEdit.getEntryPart().getMutations()) {
        walEdit.add(COLUMN_VALUE_ADAPTOR.fromThrift(mutation));
      }
      List<UUID> clusterIds = thriftEdit.getClusterIds();
      if (clusterIds.size() > 1) {
        walEdit.addClusterIds(clusterIds.subList(1, clusterIds.size()));
      }
      return walEdit;
    }

    @Override public EntryWrapper<TWalLEdit> toThrift(WALEdit walEdit) {
      TWalLEdit thriftEdit = new TWalLEdit();
      thriftEdit.setMutations(
          Lists.transform(walEdit.getKeyValues(), new Function<KeyValue, TColumnValue>() {
            @Override public TColumnValue apply(KeyValue keyValue) {
              return COLUMN_VALUE_ADAPTOR.toThrift(keyValue);
            }
          })
      );
      return new EntryWrapper<TWalLEdit>(thriftEdit);
    }
  }

  // Maps Cluster's UUID to Thrift: TClusterId
  static class ClusterIdAdaptor implements Adaptor<List<UUID>, List<TClusterId>> {

    @Override public List<UUID> fromThrift(List<TClusterId> clusterIds) {
      return Lists.transform(clusterIds, new Function<TClusterId, UUID>() {
        @Override public UUID apply(TClusterId clusterId) {
          return new UUID(clusterId.getUb(), clusterId.getLb());
        }
      });
    }

    @Override public List<TClusterId> toThrift(List<UUID> uuids) {
      return Lists.transform(uuids, new Function<UUID, TClusterId>() {
        @Override public TClusterId apply(UUID uuid) {
          return new TClusterId(uuid.getLeastSignificantBits(), uuid.getMostSignificantBits());
        }
      });
    }
  }
  // Maps KeyValue to Thrift: TColumnValue
  static class ColumnValueAdaptor implements Adaptor<KeyValue, TColumnValue> {

    @Override public KeyValue fromThrift(TColumnValue tColumnValue) {
      return new KeyValue(
          tColumnValue.getRow(),
          tColumnValue.getFamily(),
          tColumnValue.getQualifier(),
          tColumnValue.getTimestamp(),
          ThriftEditType.codeToType(tColumnValue.getType()).getKvType(),
          tColumnValue.getValue()
      );
    }

    @Override public TColumnValue toThrift(KeyValue kv) {
      TColumnValue col = new TColumnValue();
      col.setRow(kv.getRow());
      col.setFamily(kv.getFamily());
      col.setQualifier(kv.getQualifier());
      col.setValue(kv.getValue());
      col.setTimestamp(kv.getTimestamp());
      col.setType(ThriftEditType.keyValueToType(KeyValue.Type.codeToType(kv.getType())).getCode());
      return col;
    }
  }

  private static class EntryWrapper<T> {
    private final T EntryPart;
    private final List<UUID> clusterIds;

    private EntryWrapper(T entryPart) {
      EntryPart = entryPart;
      clusterIds = null;
    }

    private EntryWrapper(T entryPart, List<UUID> clusterIds) {
      EntryPart = entryPart;
      this.clusterIds = clusterIds;
    }

    public T getEntryPart() {
      return EntryPart;
    }

    public List<UUID> getClusterIds() {
      return clusterIds;
    }
  }
}
