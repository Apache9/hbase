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
package org.apache.hadoop.hbase.util;

import static org.apache.hadoop.hbase.HConstants.TALOS_ACCESS_ENDPOINT;
import static org.apache.hadoop.hbase.HConstants.TALOS_ACCESS_KEY;
import static org.apache.hadoop.hbase.HConstants.TALOS_ACCESS_SECRET;
import static org.apache.hadoop.hbase.regionserver.wal.WALEdit.METAFAMILY;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.protobuf.generated.CellProtos;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.xiaomi.infra.thirdparty.galaxy.rpc.thrift.Credential;
import com.xiaomi.infra.thirdparty.galaxy.rpc.thrift.UserType;
import com.xiaomi.infra.thirdparty.galaxy.talos.admin.TalosAdmin;
import com.xiaomi.infra.thirdparty.galaxy.talos.client.TalosClientConfig;
import com.xiaomi.infra.thirdparty.galaxy.talos.thrift.Message;
import libthrift091.TException;

@InterfaceAudience.Private
public class TalosUtil {
  private static final String COLON = ":";
  private static final String ESCAPE_COLON = "---";
  private static final String ESCAPE_DASH = "--";
  private static final String DASH = "-";

  private TalosUtil(){}

  public static List<Result> convertMessageToResult(Message message,
      TimeRange timeRange) throws IOException {
    AdminProtos.WALEntry walEntry;
    try {
      List<Result> results = new ArrayList<>();
      walEntry = AdminProtos.WALEntry.parseFrom(message.getMessage());
      if (!timeRange.withinTimeRange(walEntry.getKey().getWriteTime())) {
        return results;
      }
      List<ByteString> kvs = walEntry.getKeyValueBytesList();
      long mvcc = walEntry.getKey().getLogSequenceNumber();
      List<Cell> cells = new ArrayList<>();

      Cell prevCell = null;
      for (ByteString kv : kvs) {
        Cell cell = ProtobufUtil.toCell(CellProtos.Cell.parseFrom(kv), mvcc);
        if (CellUtil.matchingFamily(cell, METAFAMILY)) {
          continue;
        }
        if (prevCell == null) {
          prevCell = cell;
        } else if (CellComparator.compareRows(prevCell, cell) != 0) {
          results.add(Result.create(cells));
          cells = new ArrayList<>();
          prevCell = cell;
        }
        cells.add(cell);
      }
      if (!cells.isEmpty()) {
        results.add(Result.create(cells));
      }
      return results;
    } catch (InvalidProtocolBufferException e) {
      throw new IOException("convert ByteString to WalEntry or Cell failed", e);
    }
  }

  public static Message constructSingleMessage(HLog.Entry entry) throws IOException {
    Message message = new Message();
    WALProtos.WALKey.Builder keyBuilder = entry.getKey().getBuilder(null);
    AdminProtos.WALEntry.Builder entryBuilder = AdminProtos.WALEntry.newBuilder();
    entryBuilder.setKey(keyBuilder.build());
    ArrayList<Cell> cells = entry.getEdit().getCells();
    for (Cell cell : cells) {
      CellProtos.Cell protoCell = ProtobufUtil.toCell(cell);
      entryBuilder.addKeyValueBytes(protoCell.toByteString());
    }
    message.setMessage(entryBuilder.build().toByteArray());
    return message;
  }

  public static String encodeTableName(String tableName) {
    return tableName.replaceAll(DASH, ESCAPE_DASH)
        .replaceAll(COLON, ESCAPE_COLON);
  }

  public static String parseFromTopicName(String topicName) {
    return topicName.replaceAll(ESCAPE_COLON, COLON)
        .replaceAll(ESCAPE_DASH, DASH);
  }


  public static void checkConfig(ReplicationPeerConfig peerConfig) throws DoNotRetryIOException {
    Map<String, String> configuration = peerConfig.getConfiguration();
    // check if missing any required config
    String endpoint = configuration.get(TALOS_ACCESS_ENDPOINT);
    String appKeyId = configuration.get(TALOS_ACCESS_KEY);
    String appKeySecret = configuration.get(TALOS_ACCESS_SECRET);
    if (endpoint == null || appKeySecret == null || appKeyId == null) {
      throw new DoNotRetryIOException(
          "endpoint, appKeyId and appKeySecret must be set when create a talos replication");
    }
    // check if talos is available
    Properties properties = new Properties();
    properties.setProperty(TALOS_ACCESS_ENDPOINT, endpoint);
    TalosClientConfig clientConfig = new TalosClientConfig(properties);
    Credential credential = new Credential();
    credential.setSecretKeyId(appKeyId).setSecretKey(appKeySecret).setType(UserType.APP_SECRET);
    TalosAdmin talosAdmin = new TalosAdmin(clientConfig, credential);
    try {
      talosAdmin.listTopic();
    } catch (TException e) {
      throw new DoNotRetryIOException("send request to talos failed, please check your config");
    }
  }
}
