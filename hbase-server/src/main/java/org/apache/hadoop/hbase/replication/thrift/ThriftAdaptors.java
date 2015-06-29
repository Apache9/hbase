package org.apache.hadoop.hbase.replication.thrift;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.wal.HLogKey;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.replication.thrift.generated.*;
import java.util.List;
import java.util.UUID;

public class ThriftAdaptors {

  interface Adaptor<K, V> {

    K fromThrift(V vee);

    V toThrift(K kay);

  }

  public static final ReplicationBatchAdaptor REPLICATION_BATCH_ADAPTOR = new ReplicationBatchAdaptor();
  static final ClusterIdAdaptor CLUSTER_ID_ADAPTOR = new ClusterIdAdaptor();
  static final ColumnValueAdaptor COLUMN_VALUE_ADAPTOR = new ColumnValueAdaptor();
  static final WALEntryAdaptor WAL_ENTRY_ADAPTOR = new WALEntryAdaptor();

  public static class ReplicationBatchAdaptor implements Adaptor<List<HLog.Entry>, TBatchEdit> {

    @Override public List<HLog.Entry> fromThrift(TBatchEdit thriftBatch) {
      return Lists.transform(thriftBatch.getEdits(), new Function<TEdit, HLog.Entry>() {
        @Override public HLog.Entry apply(TEdit input) {
          return new HLog.Entry(
              new HLogKeyAdaptor(
                  CLUSTER_ID_ADAPTOR.fromThrift(input.getClusterIds()))
                  .fromThrift(input.getHLogKey()),
              WAL_ENTRY_ADAPTOR.fromThrift(input.getWalEdit())
          );
        }
      });
    }

    @Override public TBatchEdit toThrift(List<HLog.Entry> hLogEntries) {
      TBatchEdit batch = new TBatchEdit();
      for (HLog.Entry hLogEntry : hLogEntries) {
        List<UUID> clusterIds = hLogEntry.getKey().getClusterIds();
        HLogKeyAdaptor adaptor = new HLogKeyAdaptor(clusterIds);
        batch.addToEdits(
            new TEdit(
                adaptor.toThrift(hLogEntry.getKey()),
                WAL_ENTRY_ADAPTOR.toThrift(hLogEntry.getEdit()),
                CLUSTER_ID_ADAPTOR.toThrift(clusterIds)
            ));
      }
      return batch;
    }
  }

  // Maps HLogKey to Thrift: THLogKey
  static class HLogKeyAdaptor implements Adaptor<HLogKey, THLogKey> {

    private final List<UUID> clusterIds;

    HLogKeyAdaptor(List<UUID> clusterIds) {
      this.clusterIds = clusterIds;
    }

    @Override public HLogKey fromThrift(THLogKey key) {
      return new HLogKey(
          null,
          TableName.valueOf(key.getTableName()),
          key.getSeqNum(),
          System.currentTimeMillis(),
          clusterIds,
          HConstants.NO_NONCE,
          HConstants.NO_NONCE
      );
    }

    @Override public THLogKey toThrift(HLogKey hLogKey) {
      THLogKey key = new THLogKey();
      key.setTableName(hLogKey.getTablename().getName());
      key.setWriteTime(hLogKey.getWriteTime());
      return key;
    }
  }

  // Maps WALEdit to Thrift: TWalEdit
  static class WALEntryAdaptor implements Adaptor<WALEdit, TWalLEdit> {

    @Override public WALEdit fromThrift(TWalLEdit thriftEdit) {
      WALEdit walEdit = new WALEdit();
      for (TColumnValue mutation : thriftEdit.getMutations()) {
        walEdit.add(COLUMN_VALUE_ADAPTOR.fromThrift(mutation));
      }
      return walEdit;
    }

    @Override public TWalLEdit toThrift(WALEdit walEdit) {
      TWalLEdit thriftEdit = new TWalLEdit();
      thriftEdit.setMutations(
          Lists.transform(walEdit.getKeyValues(), new Function<KeyValue, TColumnValue>() {
            @Override public TColumnValue apply(KeyValue keyValue) {
              return COLUMN_VALUE_ADAPTOR.toThrift(keyValue);
            }
          })
      );
      return thriftEdit;
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
  static class ColumnValueAdaptor implements Adaptor<Cell, TColumnValue> {

    @Override public Cell fromThrift(TColumnValue tColumnValue) {
      return new KeyValue(
          tColumnValue.getRow(),
          tColumnValue.getFamily(),
          tColumnValue.getQualifier(),
          tColumnValue.getTimestamp(),
          ThriftEditType.codeToType(tColumnValue.getType()).getKvType(),
          tColumnValue.getValue()
      );
    }

    @Override public TColumnValue toThrift(Cell kv) {
      TColumnValue col = new TColumnValue();
      col.setRow(kv.getRow());
      col.setFamily(kv.getFamily());
      col.setQualifier(kv.getQualifier());
      col.setValue(kv.getValue());
      col.setTimestamp(kv.getTimestamp());
      col.setType(
          ThriftEditType.keyValueToType(KeyValue.Type.codeToType(kv.getTypeByte())).getCode());
      return col;
    }
  }
}
