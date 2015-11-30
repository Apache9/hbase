package org.apache.hadoop.hbase.replication.thrift;

import com.google.common.base.Function;
import com.google.common.collect.Maps;
import org.apache.hadoop.hbase.KeyValue;

import java.util.Arrays;
import java.util.Map;

public enum ThriftEditType {
  PUT((byte)1, KeyValue.Type.Put),
  DELCF((byte)2, KeyValue.Type.DeleteFamily),
  DELCOLS((byte)3, KeyValue.Type.DeleteColumn),
  DEL((byte)4, KeyValue.Type.Delete);

  private static Map<Byte, ThriftEditType> toEditType =
      Maps.uniqueIndex(Arrays.asList(ThriftEditType.values()),
          new Function<ThriftEditType, Byte>() {
            @Override public Byte apply(ThriftEditType editType) {
              return editType.getCode();
            }
          }
      );

  private static Map<KeyValue.Type, ThriftEditType> keyValueToThrift =
      Maps.uniqueIndex(Arrays.asList(ThriftEditType.values()),
          new Function<ThriftEditType, KeyValue.Type>() {
            @Override public KeyValue.Type apply(ThriftEditType editType) {
              return editType.getKvType();
            }
          }
      );

  private byte val;
  private KeyValue.Type kvType;

  ThriftEditType(byte i, KeyValue.Type kvType) {
    val = i;
    this.kvType = kvType;
  }

  public byte getCode() {
    return val;
  }

  public KeyValue.Type getKvType() {
    return kvType;
  }

  public static ThriftEditType codeToType(byte someByte) {
    return toEditType.get(someByte);
  }

  public static ThriftEditType keyValueToType(KeyValue.Type keyValueType) {
    return keyValueToThrift.get(keyValueType);
  }

}
