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
package org.apache.hadoop.hbase.client;

import static org.apache.hadoop.hbase.HConstants.TALOS_ACCESS_ENDPOINT;

import libthrift091.TException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.util.TalosUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xiaomi.infra.thirdparty.galaxy.rpc.thrift.Credential;
import com.xiaomi.infra.thirdparty.galaxy.rpc.thrift.UserType;
import com.xiaomi.infra.thirdparty.galaxy.talos.admin.TalosAdmin;
import com.xiaomi.infra.thirdparty.galaxy.talos.client.SimpleTopicAbnormalCallback;
import com.xiaomi.infra.thirdparty.galaxy.talos.client.TalosClientConfig;
import com.xiaomi.infra.thirdparty.galaxy.talos.consumer.MessageCheckpointer;
import com.xiaomi.infra.thirdparty.galaxy.talos.consumer.MessageProcessor;
import com.xiaomi.infra.thirdparty.galaxy.talos.consumer.TalosConsumer;
import com.xiaomi.infra.thirdparty.galaxy.talos.consumer.TalosConsumerConfig;
import com.xiaomi.infra.thirdparty.galaxy.talos.thrift.DescribeTopicRequest;
import com.xiaomi.infra.thirdparty.galaxy.talos.thrift.MessageAndOffset;
import com.xiaomi.infra.thirdparty.galaxy.talos.thrift.Topic;
import com.xiaomi.infra.thirdparty.galaxy.talos.thrift.TopicAndPartition;
import com.xiaomi.infra.thirdparty.galaxy.talos.thrift.TopicTalosResourceName;

/**
 * used to consume data from Talos.
 * will new a {@link TalosConsumer} to help fetch data from Talos.
 * {@link HBaseMessageProcessor} implements MessageProcessor to consume these messages.
 * messages will be merged to SimpleMutations and passed to {@link MutationConsumer}
 * to process again.
 */
@InterfaceAudience.Public
public class TalosTopicConsumer {
  private static final Logger LOG = LoggerFactory.getLogger(TalosTopicConsumer.class);

  private Credential credential;
  private String topicName;
  private TalosConsumerConfig consumerConfig;
  private TalosClientConfig clientConfig;
  private TalosConsumer talosConsumer = null;
  private MutationConsumerFactory consumerFactory;
  private String consumerGroup;

  TalosTopicConsumer(TalosTopicConsumerBuilder builder) {
    credential = new Credential();
    credential.setSecretKeyId(builder.accessKey).setSecretKey(builder.accessSecret)
        .setType(UserType.APP_SECRET);
    Properties properties = new Properties(builder.properties);
    properties.setProperty(TALOS_ACCESS_ENDPOINT, builder.endpoint);
    properties.setProperty("galaxy.talos.consumer.checkpoint.auto.commit", "false");
    consumerConfig = new TalosConsumerConfig(properties);
    clientConfig = new TalosClientConfig(properties);
    topicName = builder.topicName;
    consumerFactory = builder.consumerFactory;
    consumerGroup = builder.consumerGroup;
  }

  public void start() throws TException {
    TalosAdmin talosAdmin = new TalosAdmin(clientConfig, credential);
    Topic topic = talosAdmin.describeTopic(new DescribeTopicRequest(topicName));
    TopicTalosResourceName topicTalosResourceName =
        topic.getTopicInfo().getTopicTalosResourceName();
    talosConsumer =
        new TalosConsumer(consumerGroup, consumerConfig, credential, topicTalosResourceName,
            () -> new HBaseMessageProcessor(consumerFactory), new SimpleTopicAbnormalCallback());
    LOG.info("Talos WAL Consumer starts...");
  }

  public void stop() {
    talosConsumer.shutDown();
    LOG.info("Talos WAL Consumer stops...");
  }

  public static class SimpleMutation {

    private KeyValue.Type keyType;
    private byte[] row;
    private List<Cell> cells;

    public SimpleMutation(byte[] row, byte type) {
      this.row = row;
      this.keyType = KeyValue.Type.codeToType(type);
      this.cells = new ArrayList<>();
    }

    public byte[] getRow() {
      return row;
    }

    public KeyValue.Type getType() {
      return keyType;
    }

    public void setRow(byte[] row) {
      this.row = row;
    }

    public void addCell(Cell cell) {
      cells.add(cell);
    }

    public List<Cell> getCells() {
      return cells;
    }

  }

  private static class HBaseMessageProcessor implements MessageProcessor {
    private LinkedList<TalosUtil.MessageChunk> chunks;
    private MutationConsumerFactory consumerFactory;
    private MutationConsumer consumer;

    public HBaseMessageProcessor(MutationConsumerFactory consumerFactory) {
      this.consumerFactory = consumerFactory;
      this.chunks = new LinkedList<>();
    }

    @Override
    public void init(TopicAndPartition topicAndPartition, long startMessageOffset) {
      this.consumer = consumerFactory.createMutationConsumer();
      consumer.init();
    }

    @Override
    public void process(List<MessageAndOffset> messages, MessageCheckpointer messageCheckpointer) {
      long lastSuccessOffset = -1;
      List<SimpleMutation> mutations = new ArrayList<>();
      for (MessageAndOffset message : messages) {
        try {
          TalosUtil.MessageChunk messageChunk =
              TalosUtil.convertMessageSliceToMessageChunk(message.getMessage());
          if (shouldClearChunkList(messageChunk)) {
            chunks.clear();
          }
          if (isValidChunk(messageChunk)) {
            chunks.add(messageChunk);
          } else {
            // if the chunk is not valid, error will be detected when merge chunks. Thus here we
            // just skip
            LOG.warn("Invalid message chunk, skipped...");
          }
          if (messageChunk.isLastChunk()) {
            // add simpleMutations to our mutation list, we will process the list at last.
            mutations.addAll(mergeChunksToMutations());
            lastSuccessOffset = message.getMessageOffset();
          }
        } catch (Throwable e) {
          LOG.warn("convert message to result failed, data is corrupt, ignore...", e);
        }
      }

      if (!mutations.isEmpty()) {
        try {
          LOG.debug("start to process {} mutations", mutations.size());
          consumer.consume(mutations);
        } catch (IOException ioe) {
          LOG.warn(
            "Failed to process mutations, will not advance checkpoint. Thus next time it will retry",
            ioe);
          lastSuccessOffset = -1;
        } finally {
          mutations.clear();
        }
      }
      // commit checkpoint
      if (lastSuccessOffset != -1) {
        messageCheckpointer.checkpoint(lastSuccessOffset);
      }
    }

    private List<SimpleMutation> mergeChunksToMutations() throws IOException {
      try {
        if (!checkChunkListValid()) {
          throw new IOException("message chunks are not complete");
        }
        return TalosUtil.convertMessageToMutation(TalosUtil.mergeChunksToMessage(chunks));
      } finally {
        // should clean chunks list no matter it succeeds or not
        chunks.clear();
      }
    }

    private boolean isValidChunk(TalosUtil.MessageChunk messageChunk) {
      // a valid chunk is the first chunk when chunk list is empty, or has a incrementing index
      // compare to last chunk
      return (chunks.isEmpty() && messageChunk.isFirstChunk())
          || messageChunk.getIndex() == chunks.getLast().getIndex() + 1;
    }

    private boolean checkChunkListValid() {
      return chunks.size() == chunks.getLast().getTotalSlices();
    }

    private boolean shouldClearChunkList(TalosUtil.MessageChunk messageChunk) {
      // if chunk list is not empty, but the chunk we read has different seqNum with last Chunk, that
      // means the list is corrupt, should be cleared.
      return !chunks.isEmpty()
          && messageChunk.getSeqNum() != chunks.getLast().getSeqNum();
    }

    @Override
    public void shutdown(MessageCheckpointer messageCheckpointer) {
      LOG.info("Talos Consumer shut down...");
    }
  }

  public static TalosTopicConsumerBuilder newBuilder() {
    return new TalosTopicConsumerBuilder();
  }

  public static class TalosTopicConsumerBuilder {
    private String accessKey;
    private String accessSecret;
    private String endpoint;
    private String topicName;
    private MutationConsumerFactory consumerFactory;
    private String consumerGroup;
    private Properties properties;

    public TalosTopicConsumerBuilder withAccessKey(String accessKey) {
      this.accessKey = accessKey;
      return this;
    }

    public TalosTopicConsumerBuilder withAccessSecret(String accessSecret) {
      this.accessSecret = accessSecret;
      return this;
    }

    public TalosTopicConsumerBuilder withEndpoint(String endpoint) {
      this.endpoint = endpoint;
      return this;
    }

    public TalosTopicConsumerBuilder withTopicName(String topicName) {
      this.topicName = topicName;
      return this;
    }

    public TalosTopicConsumerBuilder
        withMutationConsumerFactory(MutationConsumerFactory consumerFactory) {
      this.consumerFactory = consumerFactory;
      return this;
    }

    public TalosTopicConsumerBuilder withConsumerGroup(String consumerGroup) {
      this.consumerGroup = consumerGroup;
      return this;
    }

    public TalosTopicConsumerBuilder withTalosProperties(Properties properties) {
      this.properties = properties;
      return this;
    }

    public TalosTopicConsumer build() throws IOException {
      if (accessKey == null || accessSecret == null || topicName == null || endpoint == null
          || consumerFactory == null || consumerGroup == null) {
        throw new IOException(
            "endpoint, accessKey, accessSecret, topicName, consumerGroup and consumerFactory must be set");
      }
      return new TalosTopicConsumer(this);
    }
  }

  public static void main(String[] args) throws IOException, TException {
    if (args.length != 4) {
      throw new IllegalArgumentException("usage: ./hbase " + TalosTopicConsumer.class.getName()
          + " access_key acess_secret endpoint topic");
    }
    String APP_ACCESS_KEY = args[0];
    String APP_ACCESS_SECRET = args[1];
    String endpoint = args[2];
    String topicName = args[3];
    TalosTopicConsumerBuilder builder = TalosTopicConsumer.newBuilder();
    TalosTopicConsumer consumer = builder.withAccessKey(APP_ACCESS_KEY)
        .withAccessSecret(APP_ACCESS_SECRET).withEndpoint(endpoint).withTopicName(topicName)
        .withConsumerGroup("test").withMutationConsumerFactory(null).build();
    consumer.start();
  }
}
