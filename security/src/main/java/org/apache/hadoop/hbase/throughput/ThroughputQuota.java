/*
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

package org.apache.hadoop.hbase.throughput;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VersionedWritable;

/**
 * Table throughput quota.
 */
public class ThroughputQuota extends VersionedWritable {
  private static final byte VERSION = 0;
  private String tableName;
  private Map<String, EnumMap<RequestType, Double>> limits;

  public static boolean isValidLimit(Double limit) {
    return limit != null && limit >= 0;
  }

  // for Writable
  public ThroughputQuota() {
    super();
  }

  public ThroughputQuota(String tableName, Map<String, EnumMap<RequestType, Double>> limits) {
    super();
    this.tableName = tableName;
    this.limits = limits;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public Map<String, EnumMap<RequestType, Double>> getLimits() {
    return limits;
  }

  public void setLimits(Map<String, EnumMap<RequestType, Double>> limits) {
    this.limits = limits;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    super.write(out);
    Text.writeString(out, this.tableName);
    if (limits == null || limits.isEmpty()) {
      out.writeInt(0);
    } else {
      out.writeInt(limits.size());
      for (Entry<String, EnumMap<RequestType, Double>> entry : limits.entrySet()) {
        Text.writeString(out, entry.getKey());
        writeRequestLimits(out, entry.getValue());
      }
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    this.tableName = Text.readString(in);
    this.limits = new HashMap<String, EnumMap<RequestType, Double>>();
    int count = in.readInt();
    while (count-- > 0) {
      String userName = Text.readString(in);
      EnumMap<RequestType, Double> reqLimits = readRequestLimits(in);
      this.limits.put(userName, reqLimits);
    }
  }

  @Override
  public byte getVersion() {
    return VERSION;
  }

  @Override
  public String toString() {
    return "ThroughputQuota [tableName=" + tableName + ", limits=" + limits + "]";
  }

  private void writeRequestLimits(DataOutput out, EnumMap<RequestType, Double> limits)
      throws IOException {
    if (limits == null || limits.isEmpty()) {
      out.write(0);
    } else {
      out.write(limits.size());
      for (Entry<RequestType, Double> entry : limits.entrySet()) {
        out.writeByte(entry.getKey().code());
        out.writeDouble(entry.getValue());
      }
    }
  }

  private EnumMap<RequestType, Double> readRequestLimits(DataInput in) throws IOException {
    EnumMap<RequestType, Double> limits = new EnumMap<RequestType, Double>(RequestType.class);
    int count = in.readByte();
    while (count-- > 0) {
      RequestType requestType = RequestType.fromCode(in.readByte());
      Double limit = in.readDouble();
      limits.put(requestType, limit);
    }
    return limits;
  }

  public static byte[] toBytes(ThroughputQuota quota) throws IOException {
    if (quota != null) {
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      DataOutputStream out = new DataOutputStream(bos);
      quota.write(out);
      return bos.toByteArray();
    }
    return null;
  }

  public static ThroughputQuota fromBytes(byte[] data) throws IOException {
    if (data != null && data.length > 0) {
      DataInput in = new DataInputStream(new ByteArrayInputStream(data));
      ThroughputQuota quota = new ThroughputQuota();
      quota.readFields(in);
      return quota;
    }
    return null;
  }
}
