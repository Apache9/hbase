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

import static org.apache.hadoop.hbase.HConstants.EMPTY_END_ROW;
import static org.apache.hadoop.hbase.HConstants.EMPTY_START_ROW;

import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.ClientService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.ScanResponse;

/**
 * 
 */
@InterfaceAudience.Private
class AsyncClientScanner {

  private final Scan scan;

  private final ScanObserver scanObserver;

  private final TableName tableName;

  private final AsyncConnectionImpl conn;

  private final long scanTimeoutNs;

  private final long rpcTimeoutNs;

  private HRegionInfo currentRegion;

  private long scannerId = -1L;

  private long nextCallSeq = 0L;

  public AsyncClientScanner(Scan scan, ScanObserver scanObserver, TableName tableName,
      AsyncConnectionImpl conn, long scanTimeoutNs, long rpcTimeoutNs) {
    this.scan = scan;
    if (scan.getStartRow() == null) {
      scan.setStartRow(EMPTY_START_ROW);
    }
    if (scan.getStopRow() == null) {
      scan.setStopRow(EMPTY_END_ROW);
    }
    this.scanObserver = scanObserver;
    this.tableName = tableName;
    this.conn = conn;
    this.scanTimeoutNs = scanTimeoutNs;
    this.rpcTimeoutNs = rpcTimeoutNs;
  }

  private void scan(HBaseRpcController controller, HRegionLocation loc,
      ClientService.Interface stub) {
    if (scannerId == -1L) {
      
    }
  }

  private void scan() {
    conn.callerFactory.<ScanResponse> single().rpcTimeout(rpcTimeoutNs, TimeUnit.NANOSECONDS)
        .operationTimeout(scanTimeoutNs, TimeUnit.NANOSECONDS).action((controller, loc, stub) -> {
          return null;
        }).call().whenComplete((resp, error) -> {

        });
  }

  public void start() {
    scan();
  }
}
