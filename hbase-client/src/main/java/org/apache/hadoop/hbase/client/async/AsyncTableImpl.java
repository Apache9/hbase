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

import com.google.protobuf.RpcCallback;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.RequestConverter;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.ClientService;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.GetRequest;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.GetResponse;

/**
 *
 */
@InterfaceAudience.Private
public class AsyncTableImpl implements AsyncTable {

  private static final Log LOG = LogFactory.getLog(AsyncTableImpl.class);

  private final AsyncConnectionImpl conn;

  private final TableName tableName;

  public AsyncTableImpl(AsyncConnectionImpl conn, TableName tableName) {
    this.conn = conn;
    this.tableName = tableName;
  }

  @Override
  public TableName getName() {
    return tableName;
  }

  @Override
  public Configuration getConfiguration() {
    return conn.getConfiguration();
  }

  private void get(CompletableFuture<Result> future, Get get, int retryCount) {
    conn.locator.getRegionLocation(tableName, get.getRow(), false).whenComplete((loc, error) -> {
      if (error != null) {
        LOG.warn("Get region location failed", error);
        // inject backoff calculation here, and quit if we are timed out.
        conn.retryAfter(100 * (retryCount + 1), TimeUnit.MILLISECONDS,
          () -> get(future, get, retryCount + 1));
        return;
      }

      try {
        ClientService.Interface stub = conn.getRegionServerStub(loc.getServerName());
        GetRequest req = RequestConverter.buildGetRequest(loc.getRegionInfo().getRegionName(), get);
        HBaseRpcController controller = conn.rpcControllerFactory.newController();
        stub.get(controller, req, new RpcCallback<GetResponse>() {

          @Override
          public void run(GetResponse resp) {
            if (controller.failed()) {
              LOG.warn("request failed", controller.getFailed());
              // inject backoff calculation here, and quit if we are timed out.
              conn.retryAfter(100 * (retryCount + 1), TimeUnit.MILLISECONDS,
                () -> get(future, get, retryCount + 1));
              return;
            }
            try {
              future.complete(ProtobufUtil.toResult(resp.getResult(), controller.cellScanner()));
            } catch (IOException e) {
              LOG.warn("convert result failed", e);
              // inject backoff calculation here, and quit if we are timed out.
              conn.retryAfter(100 * (retryCount + 1), TimeUnit.MILLISECONDS,
                () -> get(future, get, retryCount + 1));
            }
          }
        });
      } catch (IOException e) {
        LOG.warn("send request failed", e);
        // inject backoff calculation here, and quit if we are timed out.
        conn.retryAfter(100 * (retryCount + 1), TimeUnit.MILLISECONDS,
          () -> get(future, get, retryCount + 1));
        return;
      }
    });
  }

  @Override
  public CompletableFuture<Result> get(Get get) {
    CompletableFuture<Result> future = new CompletableFuture<>();
    get(future, get, 0);
    return future;
  }
}
