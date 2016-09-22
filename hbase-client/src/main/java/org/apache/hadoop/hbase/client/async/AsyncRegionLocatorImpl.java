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
package org.apache.hadoop.hbase.client.async;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.util.Pair;

/**
 *
 */
@InterfaceAudience.Private
public class AsyncRegionLocatorImpl implements AsyncRegionLocator {

  private final TableName tableName;

  private final RegionLocatorImpl locator;

  public AsyncRegionLocatorImpl(TableName tableName, RegionLocatorImpl locator) {
    this.tableName = tableName;
    this.locator = locator;
  }

  @Override
  public TableName getName() {
    return tableName;
  }

  @Override
  public CompletableFuture<HRegionLocation> getRegionLocation(byte[] row, boolean reload) {
    return locator.getRegionLocation(tableName, row, reload);
  }

  @Override
  public CompletableFuture<List<HRegionLocation>> getAllRegionLocations() {
    throw new UnsupportedOperationException();
  }

  @Override
  public CompletableFuture<byte[][]> getStartKeys() {
    throw new UnsupportedOperationException();
  }

  @Override
  public CompletableFuture<byte[][]> getEndKeys() {
    throw new UnsupportedOperationException();
  }

  @Override
  public CompletableFuture<Pair<byte[][], byte[][]>> getStartEndKeys() {
    throw new UnsupportedOperationException();
  }

}
