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
package org.apache.hadoop.hbase.mapreduce;

import static org.apache.hadoop.hbase.HConstants.TALOS_ACCESS_ENDPOINT;
import static org.apache.hadoop.hbase.mapreduce.HBaseStreamCopyTableToTalos.*;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.xiaomi.infra.thirdparty.galaxy.rpc.thrift.Credential;
import com.xiaomi.infra.thirdparty.galaxy.rpc.thrift.UserType;
import com.xiaomi.infra.thirdparty.galaxy.talos.admin.TalosAdmin;
import com.xiaomi.infra.thirdparty.galaxy.talos.client.TalosClientConfig;
import com.xiaomi.infra.thirdparty.galaxy.talos.producer.SimpleProducer;
import com.xiaomi.infra.thirdparty.galaxy.talos.producer.TalosProducerConfig;
import com.xiaomi.infra.thirdparty.galaxy.talos.thrift.DescribeTopicRequest;
import com.xiaomi.infra.thirdparty.galaxy.talos.thrift.Message;
import com.xiaomi.infra.thirdparty.galaxy.talos.thrift.Topic;
import com.xiaomi.infra.thirdparty.galaxy.talos.thrift.TopicAndPartition;
import com.xiaomi.infra.thirdparty.galaxy.talos.thrift.TopicTalosResourceName;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import libthrift091.TException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.HBaseStreamUtil;
import org.apache.hadoop.io.Writable;

/**
 * reducer for {@link HBaseStreamCopyTableToTalos}
 */
public class HBaseStreamCopyTableToTalosReducer extends TableReducer<ImmutableBytesWritable, Mutation, Writable> {

  private static final Log LOG = LogFactory.getLog(HBaseStreamCopyTableToTalosReducer.class);

  private SimpleProducer[] producers;

  private String fieldsControl;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    try {
      initTalos(context);
    } catch (TException e) {
      LOG.error("init Talos producers exception", e);
      throw new IOException(e);
    }
  }

  @Override
  protected void reduce(ImmutableBytesWritable key, Iterable<Mutation> values, Context context)
      throws IOException, InterruptedException {
    byte[] rowkey = key.get();
    for (Mutation mutation : values) {
      CellScanner cellScanner = mutation.cellScanner();
      while (cellScanner.advance()) {
        Cell cell = cellScanner.current();
        JsonObject jsonObject = cellToJson(cell);
        JsonArray array = new JsonArray();
        array.add(jsonObject);
        List<Message> messages = HBaseStreamUtil.toMessage(array);
        try {
          producers[Bytes.hashCode(rowkey) % producers.length].putMessageList(messages);
        } catch (TException e) {
          LOG.error("Talos producer putMessageList exceptionally", e);
          throw new IOException(e);
        }
      }
    }
  }

  private JsonObject cellToJson(Cell cell) {
    String valueInBase64 = null;
    if (!"KEYS_ONLY".equals(fieldsControl)) {
      valueInBase64 = HBaseStreamUtil.toBase64String(CellUtil.cloneValue(cell));
    }
    return HBaseStreamUtil.cellToJson(cell, valueInBase64);
  }

  private void initTalos(Context context) throws TException {
    fieldsControl = context.getConfiguration().get(FIELDS_CONTROL);
    String endpoint = context.getConfiguration().get(TALOS_ENDPOINT);
    String accesskey = context.getConfiguration().get(TALOS_ACCESSKEY);
    String accesssecret = context.getConfiguration().get(TALOS_ACCESSSECRET);
    String topicName = context.getConfiguration().get(TALOS_TOPIC_NAME);

    Properties properties = new Properties();
    properties.setProperty(TALOS_ACCESS_ENDPOINT, endpoint);
    TalosClientConfig clientConfig = new TalosClientConfig(properties);
    TalosProducerConfig producerConfig = new TalosProducerConfig(properties);
    Credential credential = new Credential().setSecretKeyId(accesskey).setSecretKey(accesssecret)
        .setType(UserType.APP_SECRET);
    TalosAdmin talosAdmin = new TalosAdmin(clientConfig, credential);

    Topic topic = talosAdmin.describeTopic(new DescribeTopicRequest(topicName));
    int partitionNum = topic.getTopicAttribute().getPartitionNumber();
    producers = new SimpleProducer[partitionNum];
    TopicTalosResourceName topicTalosResourceName = topic.getTopicInfo().getTopicTalosResourceName();
    for (int i = 0; i < partitionNum; i++) {
      TopicAndPartition topicAndPartition =
          new TopicAndPartition(topic.getTopicInfo().getTopicName(), topicTalosResourceName, i);
      producers[i] = new SimpleProducer(producerConfig, topicAndPartition, credential);
    }
  }
}