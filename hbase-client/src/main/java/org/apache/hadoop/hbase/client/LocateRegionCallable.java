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
package org.apache.hadoop.hbase.client;

import java.util.concurrent.Callable;

import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;

/**
 * The based callable for calling {@link HConnection#relocateRegion(TableName, byte[])} and {@link HConnection#locateRegion(TableName, byte[])}
 */
@InterfaceAudience.Private
abstract class LocateRegionCallable implements Callable<HRegionLocation> {

  protected final TableName tableName;

  protected final byte[] row;

  protected LocateRegionCallable(TableName tableName, byte[] row) {
    this.tableName = tableName;
    this.row = row;
  }

  public TableName getTableName() {
    return tableName;
  }

  public byte[] getRow() {
    return row;
  }
}
