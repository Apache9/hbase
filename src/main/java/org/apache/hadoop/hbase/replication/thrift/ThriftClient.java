/**
 * Copyright 2011 The Apache Software Foundation
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
package org.apache.hadoop.hbase.replication.thrift;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListMap;

import javax.security.sasl.Sasl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.replication.thrift.generated.TBatchEdit;
import org.apache.hadoop.hbase.replication.thrift.generated.TEdit;
import org.apache.hadoop.hbase.replication.thrift.generated.THBaseService;
import org.apache.hadoop.hbase.replication.thrift.generated.TIOError;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.SaslRpcServer;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSaslClientTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

public class ThriftClient {
  private static final Log LOG = LogFactory.getLog(ThriftClient.class);
  // 0.98 support namespace to manager tables of the same business. We may need to
  // change the tablename when migrate from 0.94 to 0.98. For example, the tablename
  // may be businessName_tablename, it could be transfered to businessName:tablename.
  // This option allow us to define the table name mapping for replication. The format
  // of the value should be 'sourceTable1=>destTable1,sourceTable2=>destTable2'
  public static final String HBASE_REPLICATION_THRIFT_TABLE_NAME_MAP = "hbase.replication.thrift.tablename.map";
  private static Object tableNameMapLock = new Object();
  protected static Map<String, String> tableNameMap = null;
  private Configuration conf;
  private final String peerId;
  private boolean isSecure;
  private ConcurrentSkipListMap<String, THBaseService.Client> clients =
      new ConcurrentSkipListMap<String, THBaseService.Client>();


  public ThriftClient(Configuration conf, String peerId) throws IOException {
    this.conf = conf;
    this.peerId = peerId;
    this.isSecure = User.isHBaseSecurityEnabled(conf);
    loadTableNameMap(conf.get(HBASE_REPLICATION_THRIFT_TABLE_NAME_MAP));
  }
  
  protected static void loadTableNameMap(String mappingString) throws IOException {
    if (mappingString == null) {
      return;
    }
    if (tableNameMap == null) {
      synchronized(tableNameMapLock) {
        if (tableNameMap == null) {
          tableNameMap = new HashMap<String, String>();
          loadTableName(mappingString, tableNameMap);
        }
      }
    }
  }
  
  public static void loadTableName(String mappingString, Map<String, String> tableNameMap)
      throws IOException {
    // The format of the value should be 'sourceTable1=>destTable1,sourceTable2=>destTable2'
    String[] mappingItems = mappingString.split(",");
    for (String mappingItem : mappingItems) {
      String[] names = mappingItem.split("=>");
      if (names.length != 2) {
        throw new IOException("table name mapping string is error formatted, mappingString="
            + mappingItem);
      }
      tableNameMap.put(names[0], names[1]);
    }
  }

  private THBaseService.Client createClient(String host, int port) throws IOException,
      TTransportException {
    boolean isCompact =
        conf.getBoolean("hbase.replication.thrift.compact", true);

    String serverProtocol = UserGroupInformation.getCurrentUser().getUserName();
    String serverAddress = null;
    if(User.isHBaseSecurityEnabled(conf)) {
      String kerberosName = UserGroupInformation.getCurrentUser().getUserName();
      final String names[] = SaslRpcServer.splitKerberosName(kerberosName);
      if (names.length != 3) {
        throw new TTransportException("Kerberos principal should have 3 parts: " + kerberosName);
      }
      serverProtocol = names[0];
      serverAddress = names[1];
    }

    TTransport transport = new TSocket(host, port);
    if(isSecure) {
      Map<String, String> saslProps = new HashMap<String, String>();
      saslProps.put(Sasl.QOP, ThriftUtilities.getQOP(conf).getSaslQop());
      transport = new TUGIAssumingTransport(
          new TSaslClientTransport(
              ThriftUtilities.getAuthType(conf).getMechanismName(),
              null,
              serverProtocol, serverAddress,
              saslProps, null,
              transport),
              User.getCurrent().getUGI());
    }
    try {
      transport.open();
      LOG.debug("Connected to "+host+":"+port);
    } catch (TTransportException e) {
      throw new IOException("Failed to open transport connection to : "+host+":"+port, e);
    }

    TProtocol protocol;
    if(isCompact) {
      protocol = new TCompactProtocol(transport);
    } else {
      protocol = new TBinaryProtocol(transport);
    }

    return new THBaseService.Client(protocol);
  }

  public THBaseService.Client getClient(String host, int port) throws IOException, TTransportException {
    String key = host+":"+port;
    if(clients.containsKey(key)) {
      return clients.get(key);
    }
    THBaseService.Client client = createClient(host, port);
    clients.put(key, client);
    return clients.get(key);
  }

  public void removeClient(String host, int port) {
    clients.remove(host+":"+port);
  }

  public void ping(ServerName serverName) throws IOException{
    THBaseService.Client client = null;
    try {
      client = getClientFromServerName(serverName);
      client.ping();
    } catch (TException e) {
      removeClient(serverName.getHostname(),
        ThriftUtilities.getDestinationPeerPort(conf, peerId, serverName.getPort()));
      try {
        client.getOutputProtocol().getTransport().close();
      } catch(Exception e2) {
        LOG.debug("Failed to gracefully close broken transport.", e2);
      }
      throw new IOException("Failed to ping replication client", e);
    }

  }

  public static void transferTableNames(TBatchEdit batchEdit, Map<String, String> tableNameMap) {
    if (batchEdit.isSetEdits()) {
      for (TEdit edit : batchEdit.getEdits()) {
        if (edit.isSetHLogKey() && edit.getHLogKey().isSetTableName()) {
          String sourceTable = Bytes.toString(edit.getHLogKey().getTableName());
          String destTable = tableNameMap.get(sourceTable);
          if (destTable != null) {
            edit.getHLogKey().setTableName(Bytes.toBytes(destTable));
          }
        }
      }
    }
  }
  
  public void shipEdits(ServerName serverName, HLog.Entry[] entries) throws IOException {
    THBaseService.Client client;
    String host = serverName.getHostname();
    int port = ThriftUtilities.getDestinationPeerPort(conf, peerId, serverName.getPort());
    
    try {
      client = getClient(host, port);
    } catch (TTransportException e) {
      throw new IOException("Failed to create replication client", e);
    }
    try {
      TBatchEdit batchEdit = ThriftAdaptors.REPLICATION_BATCH_ADAPTOR.toThrift(entries);
      if (tableNameMap != null) {
        transferTableNames(batchEdit, tableNameMap);
      }
      client.replicate(batchEdit);
    } catch (TTransportException e) {
      removeClient(host, port);
      try {
        client.getOutputProtocol().getTransport().close();
      } catch(Exception e2) {
        LOG.debug("Failed to gracefully close broken transport.", e2);
      }
      throw new IOException("Failed to ship edits", e);
    } catch (TException e) {
      throw new IOException("Failed to ship edits", e);
    } catch (TIOError e) {
      throw new IOException("Failed to ship edits", e);
    }
  }

  public UUID getPeerClusterUUID(ServerName serverName) {
    THBaseService.Client client;
    String host = serverName.getHostname();
    int port = ThriftUtilities.getDestinationPeerPort(conf, peerId, serverName.getPort());
    try {
      client = getClient(host, port);
      return UUID.fromString(client.getClusterUUID());
    } catch (Exception e) {
      LOG.error("Error getting UUID from remote cluster", e);
      return null;
    }
  }

  private THBaseService.Client getClientFromServerName(ServerName serverName) throws IOException {
    THBaseService.Client client;
    String host = serverName.getHostname();
    int port = ThriftUtilities.getDestinationPeerPort(conf, peerId, serverName.getPort());
    try {
      client = getClient(host, port);
    } catch (TTransportException e) {
      throw new IOException("Failed to create replication client", e);
    }
    return client;
  }
}
