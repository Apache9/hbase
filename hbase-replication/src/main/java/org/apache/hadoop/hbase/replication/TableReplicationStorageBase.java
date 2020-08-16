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
package org.apache.hadoop.hbase.replication;

import java.io.IOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class TableReplicationStorageBase {

  protected static final String PREFIX = HConstants.SPECIAL_META_ROW_PREFIX + "rep:";

  protected final Connection conn;

  protected TableReplicationStorageBase(Connection conn) {
    this.conn = conn;
  }

  protected byte[] appendPrefix(String row) {
    return Bytes.toBytes(PREFIX + row);
  }

  protected String removePrefix(byte[] row) {
    return Bytes.toString(row).substring(PREFIX.length());
  }

  protected Table getTable() throws IOException {
    return conn.getTable(TableName.META_TABLE_NAME);
  }
}
