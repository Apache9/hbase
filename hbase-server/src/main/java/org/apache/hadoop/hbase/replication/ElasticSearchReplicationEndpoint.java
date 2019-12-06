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
package org.apache.hadoop.hbase.replication;

import com.xiaomi.infra.thirdparty.com.google.common.annotations.VisibleForTesting;
import com.google.gson.JsonParser;
import com.xiaomi.infra.thirdparty.galaxy.talos.thrift.Message;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.yetus.audience.InterfaceAudience;


import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.util.TalosUtil;

import com.google.gson.JsonObject;
import com.google.gson.JsonArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import static org.apache.hadoop.hbase.HConstants.TALOS_UPDATE_ES_TABLE;


@InterfaceAudience.Private
public class ElasticSearchReplicationEndpoint extends TalosReplicationEndpoint {
  private static final Logger LOG = LoggerFactory.getLogger(ElasticSearchReplicationEndpoint.class);
  private Map<TableName, Set<String>> esIndexPropertiesCache;
  private Connection localConn;
  private Admin localAdmin;
  //last modified table@timestamp (replicate to ES column change)
  private String lastUpdateTable;

  @Override
  public void init(Context context) throws IOException {
    super.init(context);
    Configuration conf = HBaseConfiguration.create(ctx.getConfiguration());
    this.localConn = ConnectionFactory.createConnection(ctx.getLocalConfiguration());
    this.localAdmin = localConn.getAdmin();
    this.esIndexPropertiesCache = new HashMap<>();
    this.lastUpdateTable = conf.get(TALOS_UPDATE_ES_TABLE);
  }

  @Override
  public synchronized void peerConfigUpdated(ReplicationPeerConfig rpc) {
    super.peerConfigUpdated(rpc);
    Map<String, String> newConfig = rpc.getConfiguration();
    /**
     * change ES index replication column via
     * update peer config with key "galaxy.talos.update.es.table"
     * value is tableName@timestamp E.g. mynamespace:mytable@1574951108000
     * the timestamp suffix only identify twice modify to same table
     *
     * when update other config E.g.galaxy.talos.access.key, the
     * esIndexPropertiesCache will not be cleared
     */
    String tableUpdated = newConfig.get(TALOS_UPDATE_ES_TABLE);
    if (tableUpdated != null && !tableUpdated.equals(lastUpdateTable)) {
      String[] sp = tableUpdated.split("@");
      if (sp.length == 2) {
        lastUpdateTable = tableUpdated;
        esIndexPropertiesCache.remove(TableName.valueOf(sp[0]));
        LOG.info("peerConfigUpdated clear table cache success, table: " + tableUpdated);
      } else {
        LOG.error("peerConfigUpdated invalid update table :" + tableUpdated);
      }
    }
  }

  @Override
  protected void doStart() {
    try {
      notifyStarted();
    } catch (Throwable e) {
      notifyFailed(e);
    }
  }

  @Override
  protected void doStop() {
    IOUtils.closeQuietly(this.localAdmin);
    IOUtils.closeQuietly(this.localConn);
    notifyStopped();
  }


  @Override
  public UUID getPeerUUID() {
    return UUID.randomUUID();
  }


  String getEncodeTableName(TableName tableName){
    return TalosUtil.encodeESTableName(tableName.getNameAsString());
  }

  @VisibleForTesting
  public void setEsIndexPropertiesCache(TableName tableName, Set<String> propertiesSet ){
    if(esIndexPropertiesCache == null){
      esIndexPropertiesCache = new HashMap<>();
    }
    esIndexPropertiesCache.put(tableName, propertiesSet);
  }

  protected Map<TableName, Map<String, List<Message>>> constructTalosMessages(
      List<WAL.Entry> entries) throws IOException {
    Map<TableName, Map<String, List<Message>>> messages = new HashMap<>();
    for (WAL.Entry entry : entries) {
      TableName tableName = entry.getKey().getTableName();
      if (!esIndexPropertiesCache.containsKey(tableName)) {
        getEsIndexProperties(tableName);
      }
      Set<String> properties = esIndexPropertiesCache.get(tableName);
      if (properties != null && !properties.isEmpty()) {
        List<Message> constructedMessages = TalosUtil.constructJsonMessages(entry, properties);
        assembleMessage(messages, entry, constructedMessages);
      } else {
        LOG.error("constructTalosMessages no need replication table: " + tableName);
      }
    }
    return messages;
  }

  /** in HTableDescriptor
   *  key = "es"
   *  value = "
   * {
   *　   "properties": {
   *    　"A:c1": {
   *         "type": "keyword"
   *       },
   *      "A:c2": {
   *           "type": "text",
   *           "analyzer": "standard"
   *       }
   *    }
   *  }"
   *
   */
  private void getEsIndexProperties(TableName tableName) throws IOException{
    String esTablePropertyJsonStr = null;
    try {
      HTableDescriptor desc = localAdmin.getTableDescriptor(tableName);
      Set<String> propertiesSet = new HashSet<>();
      esTablePropertyJsonStr = desc.getValue("es");
      if (esTablePropertyJsonStr != null) {
        JsonObject esIndexJson = new JsonParser().parse(esTablePropertyJsonStr).getAsJsonObject();
        JsonArray esPropertyArray = (JsonArray) esIndexJson.get("properties");
        if(!esPropertyArray.isJsonNull()) {
          esPropertyArray.forEach(jsonElement -> {
            propertiesSet.add(((JsonObject)jsonElement).get("column").getAsString());
          });
          esIndexPropertiesCache.put(tableName, propertiesSet);
          LOG.info("getEsIndexProperties success ,tableName = " + tableName + " properties = "
            + propertiesSet.toString());
        }
      }

    } catch (IOException e) {
      throw new IOException("Failed to get table schema for table " + tableName, e);
    }
  }
}
