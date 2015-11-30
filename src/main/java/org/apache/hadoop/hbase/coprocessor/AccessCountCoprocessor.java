/**
 * Copyright 2010 The Apache Software Foundation
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
package org.apache.hadoop.hbase.coprocessor;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Check;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.WritableByteArrayComparable;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.ipc.RequestContext;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.security.User;

public class AccessCountCoprocessor extends BaseRegionObserver {
 
  @Override
  public void postGet(final ObserverContext<RegionCoprocessorEnvironment> c,
      final Get get, final List<KeyValue> results) throws IOException {
    RegionCoprocessorEnvironment e = c.getEnvironment();
    User user = getActiveUser();
    byte[] table = getTableName(e);
    HRegion region = e.getRegion();
    if (region != null) {
      for (KeyValue kv : results) {
        region.updateReadCount(user, table, kv.getFamily(), kv.getQualifier());
      }
    }
  }
  
  @Override
  public boolean postScannerNext(final ObserverContext<RegionCoprocessorEnvironment> c,
      final InternalScanner s, final List<Result> results, final int limit,
      final boolean hasMore) throws IOException {
    RegionCoprocessorEnvironment e = c.getEnvironment();
    User user = getActiveUser();
    byte[] table = getTableName(e);
    HRegion region = e.getRegion();
    if (region != null) {
      for (Result result : results) {
        for (KeyValue kv : result.list()) {
          region.updateReadCount(user, table, kv.getFamily(), kv.getQualifier());
        }
      }
    }
    return hasMore;
  }
  
  @Override
  public void postGetClosestRowBefore(final ObserverContext<RegionCoprocessorEnvironment> c,
      final byte [] row, final byte [] family, final Result result)
      throws IOException {
    RegionCoprocessorEnvironment e = c.getEnvironment();
    User user = getActiveUser();
    byte[] table = getTableName(e);
    HRegion region = e.getRegion();
    if (region != null) {
      for (KeyValue kv : result.list()) {
        region.updateReadCount(user, table, kv.getFamily(), kv.getQualifier());
      }
    }
  }
  
  @Override
  public void postPut(final ObserverContext<RegionCoprocessorEnvironment> c, 
      final Put put, final WALEdit edit, final boolean writeToWAL) throws IOException {
    RegionCoprocessorEnvironment e = c.getEnvironment();
    User user = getActiveUser();
    byte[] table = getTableName(e);
    HRegion region = e.getRegion();
    if (region != null) {
      for (KeyValue kv : edit.getKeyValues()) {
        region.updateWriteCount(user, table, kv.getFamily(), kv.getQualifier());
      }
    }
  }

  @Override
  public void postDelete(final ObserverContext<RegionCoprocessorEnvironment> c,
      final Delete delete, final WALEdit edit, final boolean writeToWAL) throws IOException {
    RegionCoprocessorEnvironment e = c.getEnvironment();
    User user = getActiveUser();
    byte[] table = getTableName(e);
    HRegion region = e.getRegion();
    if (region != null) {
      for (KeyValue kv : edit.getKeyValues()) {
        region.updateWriteCount(user, table, kv.getFamily(), kv.getQualifier());
      }
    }
  }

  @Override
  public Result postIncrement(final ObserverContext<RegionCoprocessorEnvironment> c,
      final Increment increment, final Result result) throws IOException {
    RegionCoprocessorEnvironment e = c.getEnvironment();
    User user = getActiveUser();
    byte[] table = getTableName(e);
    HRegion region = e.getRegion();
    if (region != null) {
      for (KeyValue kv : result.list()) {
        region.updateWriteCount(user, table, kv.getFamily(), kv.getQualifier());
      }
    }
    return result;
  }
  
  @Override
  public long postIncrementColumnValue(final ObserverContext<RegionCoprocessorEnvironment> c,
      final byte [] row, final byte [] family, final byte [] qualifier,
      final long amount, final boolean writeToWAL, long result)
      throws IOException {
    RegionCoprocessorEnvironment e = c.getEnvironment();
    User user = getActiveUser();
    byte[] table = getTableName(e);
    HRegion region = e.getRegion();
    if (region != null) {
      region.updateWriteCount(user, table, family, qualifier);
    }
    return result;
  }

  @Override
  public boolean postCheckAndPut(final ObserverContext<RegionCoprocessorEnvironment> c,
      final byte [] row, final byte [] family, final byte [] qualifier,
      final CompareOp compareOp, final WritableByteArrayComparable comparator,
      final Put put, final boolean result) throws IOException {
    RegionCoprocessorEnvironment e = c.getEnvironment();
    User user = getActiveUser();
    byte[] table = getTableName(e);
    HRegion region = e.getRegion();
    if (region != null && result) {
      region.updateWriteCount(user, table, family, qualifier);
    }
    return result;
  }

  @Override
  public boolean postCheckAndDelete(final ObserverContext<RegionCoprocessorEnvironment> c,
      final byte [] row, final byte [] family, final byte [] qualifier,
      final CompareOp compareOp, final WritableByteArrayComparable comparator,
      final Delete delete, final boolean result) throws IOException {
    RegionCoprocessorEnvironment e = c.getEnvironment();
    User user = getActiveUser();
    byte[] table = getTableName(e);
    HRegion region = e.getRegion();
    if (region != null && result) {
      region.updateWriteCount(user, table, family, qualifier);
    }
    return result;
  }

  @Override
  public boolean postCheckAndMutate(final ObserverContext<RegionCoprocessorEnvironment> c,
      final Check check, final Mutation mutate, final boolean result)
    throws IOException{
    RegionCoprocessorEnvironment e = c.getEnvironment();
    User user = getActiveUser();
    byte[] table = getTableName(e);
    HRegion region = e.getRegion();
    if (region != null && result) {
      for(List<KeyValue> kvList : mutate.getFamilyMap().values()) {
        for (KeyValue kv : kvList) {
          region.updateWriteCount(user, table, kv.getFamily(), kv.getQualifier());
        }
      }
    }
    return result;
  }

  @Override
  public Result postAppend(final ObserverContext<RegionCoprocessorEnvironment> c,
      final Append append, final Result result) throws IOException {
    RegionCoprocessorEnvironment e = c.getEnvironment();
    User user = getActiveUser();
    byte[] table = getTableName(e);
    HRegion region = e.getRegion();
    if (region != null) {
      for (KeyValue kv : result.list()) {
        region.updateWriteCount(user, table, kv.getFamily(), kv.getQualifier());
      }
    }
    return result;
  }

  /**
   * Returns the active user to which authorization checks should be applied.
   * If we are in the context of an RPC call, the remote user is used,
   * otherwise the currently logged in user is used.
   */
  private User getActiveUser() throws IOException {
    User user = RequestContext.getRequestUser();
    if (!RequestContext.isInRequestContext()) {
      // for non-rpc handling, fallback to system user
      user = User.getCurrent();
    }

    return user;
  }
  
  private byte[] getTableName(RegionCoprocessorEnvironment e) {
    HRegion region = e.getRegion();
    byte[] tableName = null;

    if (region != null) {
      HRegionInfo regionInfo = region.getRegionInfo();
      if (regionInfo != null) {
        tableName = regionInfo.getTableName();
      }
    }
    return tableName;
  }
}
