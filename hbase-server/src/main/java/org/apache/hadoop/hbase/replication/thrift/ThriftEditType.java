/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
