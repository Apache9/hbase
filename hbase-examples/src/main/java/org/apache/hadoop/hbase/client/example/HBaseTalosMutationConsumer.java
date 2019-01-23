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
package org.apache.hadoop.hbase.client.example;


import com.xiaomi.infra.base.nameservice.NameService;
import libthrift091.TException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.MutationConsumer;
import org.apache.hadoop.hbase.client.MutationConsumerFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TalosTopicConsumer;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * used to consume HBase WAL from Talos and write to another HBase table.
 * {@link HBaseTalosMutationConsumerFactory} will help create a HBaseTalosMutationConsumer
 * for a specified table.
 */
@InterfaceAudience.Public
public class HBaseTalosMutationConsumer implements MutationConsumer {
  private String uri;
  private Table tableInterface;
  private String tableName;

  public HBaseTalosMutationConsumer(String uri, String tableName) {
    this.uri = uri;
    this.tableName = tableName;
  }

  public void init() {
    Configuration configuration = HBaseConfiguration.create();
    try {
      Connection connection = ConnectionFactory
          .createConnection(NameService.createConfigurationByClusterKey(uri, configuration), uri);
      tableInterface = connection.getTable(TableName.valueOf(tableName));
    } catch (IOException ioe) {
      throw new RuntimeException("Cannot connect to HBase table " + tableName + " of " + uri);
    }
  }

  @Override
  public void consume(List<TalosTopicConsumer.SimpleMutation> mutationList) throws IOException {
    List<Row> mutations = new ArrayList<>();
    for (TalosTopicConsumer.SimpleMutation mutation : mutationList) {
      switch (mutation.getType()) {
      case Put:
        Put put = new Put(mutation.getRow());
        for (Cell cell : mutation.getCells()) {
          put.add(cell);
        }
        mutations.add(put);
        break;
      case DeleteColumn:
      case Delete:
      case DeleteFamily:
      case DeleteFamilyVersion:
        Delete delete = new Delete(mutation.getRow());
        for (Cell cell : mutation.getCells()) {
          delete.addDeleteMarker(cell);
        }
        mutations.add(delete);
        break;
      default:
        throw new IOException("UnExpected type: " + mutation.getType().getCode());
      }
    }
    if (mutations.size() > 0) {
      try {
        tableInterface.batch(mutations, new Result[mutations.size()]);
      } catch (InterruptedException e) {
        throw new IOException("Interrupted...", e);
      } finally {
        mutations.clear();
      }
    }
  }

  public static class HBaseTalosMutationConsumerFactory implements MutationConsumerFactory {
    private String uri;
    private String tableName;

    public HBaseTalosMutationConsumerFactory(String uri, String tableName) {
      this.uri = uri;
      this.tableName = tableName;
    }

    @Override
    public MutationConsumer createMutationConsumer() {
      return new HBaseTalosMutationConsumer(uri, tableName);
    }
  }

  public static void main(String[] args) throws IOException, TException {
    if (args.length != 7) {
      throw new IllegalArgumentException("usage: ./hbase " + TalosTopicConsumer.class.getName()
          + " access_key acess_secret endpoint topic cluster tableName");
    }
    String APP_ACCESS_KEY = args[0];
    String APP_ACCESS_SECRET = args[1];
    String endpoint = args[2];
    String topicName = args[3];
    String cluster = args[4];
    String tableName = args[5];
    String consumerGroup = args[6];
    Properties properties = new Properties();
    properties.setProperty("galaxy.talos.consumer.max.fetch.records", "1000");
    TalosTopicConsumer.TalosTopicConsumerBuilder builder = TalosTopicConsumer.newBuilder();
    TalosTopicConsumer topicConsumer =
        builder.withAccessKey(APP_ACCESS_KEY).withAccessSecret(APP_ACCESS_SECRET)
            .withEndpoint(endpoint).withTopicName(topicName).withConsumerGroup(consumerGroup)
            .withMutationConsumerFactory(new HBaseTalosMutationConsumerFactory(cluster, tableName))
            .withTalosProperties(properties).build();
    topicConsumer.start();
  }
}
