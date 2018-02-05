/**
 * Copyright The Apache Software Foundation
 *
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

package org.apache.hadoop.hbase;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.protobuf.generated.ClusterStatusProtos;
import org.apache.hadoop.hbase.util.Strings;

/**
 * Encapsulates per-family load metrics.
 */
@InterfaceAudience.Private
public class FamilyLoad {

  private String name;

  private long rowCount;

  private long keyValueCount;

  private long deleteFamilyCount;

  private long deleteKeyValueCount;

  FamilyLoad(ClusterStatusProtos.FamilyInfo familyInfo) {
    this.name = familyInfo.getFamilyname();
    this.rowCount = familyInfo.getRowCount();
    this.keyValueCount = familyInfo.getKvCount();
    this.deleteFamilyCount = familyInfo.getDelFamilyCount();
    this.deleteKeyValueCount = familyInfo.getDelKvCount();
  }

  public String getName() {
    return this.name;
  }

  public long getRowCount() {
    return this.rowCount;
  }

  public long getKeyValueCount() {
    return this.keyValueCount;
  }

  public long getDeleteFamilyCount() {
    return this.deleteFamilyCount;
  }

  public long getDeleteKeyValueCount() {
    return this.deleteKeyValueCount;
  }

  public void incrementRowCount(long rowCount) {
    this.rowCount += rowCount;
  }

  public void incrementKeyValueCount(long keyValueCount) {
    this.keyValueCount += keyValueCount;
  }

  public void incrementDeleteFamilyCount(long deleteFamilyCount) {
    this.deleteFamilyCount += deleteFamilyCount;
  }

  public void incrementDeleteKeyValueCount(long deleteKeyValueCount) {
    this.deleteKeyValueCount += deleteKeyValueCount;
  }

  @Override
  public String toString() {
    StringBuilder sb = Strings.appendKeyValue(new StringBuilder(), "Family name:", name);
    Strings.appendKeyValue(sb, "rowCount", rowCount);
    Strings.appendKeyValue(sb, "kvCount", keyValueCount);
    Strings.appendKeyValue(sb, "deleteFamilyCount", deleteFamilyCount);
    Strings.appendKeyValue(sb, "deleteKvCount", deleteKeyValueCount);
    return sb.toString();
  }
}
