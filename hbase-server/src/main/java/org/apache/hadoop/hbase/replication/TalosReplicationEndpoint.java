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

import static com.xiaomi.infra.thirdparty.galaxy.talos.thrift.ErrorCode.THROTTLE_REJECT_ERROR;
import static com.xiaomi.infra.thirdparty.galaxy.talos.thrift.ErrorCode.TOPIC_NOT_EXIST;
import static org.apache.hadoop.hbase.HConstants.TALOS_ACCESS_ENDPOINT;
import static org.apache.hadoop.hbase.HConstants.TALOS_ACCESS_KEY;
import static org.apache.hadoop.hbase.HConstants.TALOS_ACCESS_SECRET;

import com.xiaomi.infra.thirdparty.galaxy.rpc.thrift.Credential;
import com.xiaomi.infra.thirdparty.galaxy.rpc.thrift.UserType;
import com.xiaomi.infra.thirdparty.galaxy.talos.admin.TalosAdmin;
import com.xiaomi.infra.thirdparty.galaxy.talos.client.TalosClientConfig;
import com.xiaomi.infra.thirdparty.galaxy.talos.producer.SimpleProducer;
import com.xiaomi.infra.thirdparty.galaxy.talos.producer.TalosProducerConfig;
import com.xiaomi.infra.thirdparty.galaxy.talos.thrift.DescribeTopicRequest;
import com.xiaomi.infra.thirdparty.galaxy.talos.thrift.GalaxyTalosException;
import com.xiaomi.infra.thirdparty.galaxy.talos.thrift.Message;
import com.xiaomi.infra.thirdparty.galaxy.talos.thrift.Topic;
import com.xiaomi.infra.thirdparty.galaxy.talos.thrift.TopicAndPartition;
import com.xiaomi.infra.thirdparty.galaxy.talos.thrift.TopicTalosResourceName;
import com.xiaomi.infra.thirdparty.libthrift091.TException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.ByteArrayHashKey;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Hash;
import org.apache.hadoop.hbase.util.MurmurHash3;
import org.apache.hadoop.hbase.util.TalosUtil;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private
public class TalosReplicationEndpoint extends BaseReplicationEndpoint {
  private static final Logger LOG = LoggerFactory.getLogger(TalosReplicationEndpoint.class);
  private String accessKey;
  private String accessSecret;
  private String endpoint;
  private Credential credential;
  private TalosProducerConfig producerConfig;
  private TalosClientConfig clientConfig;
  private Map<String, SimpleProducer> producers = new HashMap<>();
  private Map<TableName, Topic> topics = new HashMap<>();
  private TalosAdmin talosAdmin;
  private final Hash hashTool = MurmurHash3.getInstance();
  private final int maxTries = 10;
  private long batchSizeLimit = 20971520L;
  // How long should we sleep for each retry
  protected long sleepForRetriesCount;
  // Maximum number of retries before taking bold actions
  protected int maxRetriesMultiplier;


  /**
   * Do the sleeping logic
   * @param msg Why we sleep
   * @param sleepMultiplier by how many times the default sleeping time is augmented
   * @return True if <code>sleepMultiplier</code> is &lt; <code>maxRetriesMultiplier</code>
   */
  protected boolean sleepForRetries(String msg, int sleepMultiplier) {
    try {
      if (LOG.isTraceEnabled()) {
        LOG.trace(msg + ", sleeping " + sleepForRetriesCount + " times " + sleepMultiplier);
      }
      Thread.sleep(this.sleepForRetriesCount * sleepMultiplier);
    } catch (InterruptedException e) {
      LOG.debug("Interrupted while sleeping between retries");
    }
    return sleepMultiplier < maxRetriesMultiplier;
  }

  @Override
  public void init(Context context) throws IOException {
    super.init(context);
    Configuration conf = HBaseConfiguration.create(ctx.getConfiguration());
    endpoint = conf.get(TALOS_ACCESS_ENDPOINT);
    accessKey = conf.get(TALOS_ACCESS_KEY);
    accessSecret = conf.get(TALOS_ACCESS_SECRET);
    if (endpoint == null || accessKey == null || accessSecret == null) {
      throw new IOException("endpoint, accessKey and accessSecret must be set to talos replication");
    }
    initialTalosConfigs();
    this.maxRetriesMultiplier = conf.getInt("replication.source.maxretriesmultiplier", 10);
    this.sleepForRetriesCount = conf.getLong("replication.source.sleepforretries", 1000);
    this.batchSizeLimit = conf.getLong("talos.replication.batchsize.limit",20971520L);
  }

  private void initialTalosConfigs() {
    Properties properties = new Properties();
    properties.setProperty(TALOS_ACCESS_ENDPOINT, endpoint);
    clientConfig = new TalosClientConfig(properties);
    producerConfig = new TalosProducerConfig(properties);
    credential = new Credential();
    credential.setSecretKeyId(accessKey).setSecretKey(accessSecret).setType(UserType.APP_SECRET);
    talosAdmin = new TalosAdmin(clientConfig, credential);
  }

  @Override
  public synchronized void peerConfigUpdated(ReplicationPeerConfig rpc) {
    Map<String, String> newConfig = rpc.getConfiguration();
    if (!(accessKey.equals(newConfig.get(TALOS_ACCESS_KEY))
        && accessSecret.equals(newConfig.get(TALOS_ACCESS_SECRET))
        && endpoint.equals(newConfig.get(TALOS_ACCESS_ENDPOINT)))) {
      LOG.info("talos config change detected, reinitialize endpoint...");
      endpoint = newConfig.get(TALOS_ACCESS_ENDPOINT);
      accessKey = newConfig.get(TALOS_ACCESS_KEY);
      accessSecret = newConfig.get(TALOS_ACCESS_SECRET);
      initialTalosConfigs();
      cleanTopicsAndProducers();
      LOG.info("talos replication endpoint reinitialize finished");
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
    try {
      notifyStopped();
    } catch (Throwable e) {
      notifyFailed(e);
    }
  }

  String getEncodeTableName(TableName tableName){
    return TalosUtil.encodeTableName(tableName.getNameAsString());
  }

  protected synchronized SimpleProducer getSimpleProducer(TableName tableName,
      String encodedRegionName) throws TException, IOException {
    SimpleProducer producer = producers.get(encodedRegionName);
    if (producer != null) {
      return producer;
    }
    String topicName = getEncodeTableName(tableName);
    Topic topic = getOrCreateTopic(tableName, topicName);
    producer = createSimpleProducer(topic, topicName, encodedRegionName);
    producers.put(encodedRegionName, producer);
    return producer;
  }

  private Topic getOrCreateTopic(TableName tableName, String topicName)
      throws TException, IOException {
    if (topics.containsKey(tableName)) {
      return topics.get(tableName);
    }
    int tries = 0;
    Topic topic = null;
    while (topic == null && tries <= maxTries) {
      try {
        topic = talosAdmin.describeTopic(new DescribeTopicRequest(topicName));
        topics.put(tableName, topic);
      } catch (TException e) {
        if (tries == maxTries) {
          throw new IOException("Times of describing topic reach to the maximum number");
        }
        if (e instanceof GalaxyTalosException
            && ((GalaxyTalosException) e).getErrorCode() == THROTTLE_REJECT_ERROR) {
          LOG.warn("Get throttled by talos when we call describe topic {}, tries={}", topicName,
              tries);
          sleepBackoff(tries++);
        } else {
          throw e;
        }
      }
    }
    return topic;
  }

  private SimpleProducer createSimpleProducer(Topic topic, String topicName,
      String encodedRegionName) {
    int totalPartition = topic.getTopicAttribute().getPartitionNumber();
    byte[] regionNameBytes = Bytes.toBytes(encodedRegionName);
    int hash = (hashTool.hash(new ByteArrayHashKey(regionNameBytes, 0, regionNameBytes.length), 0)
        & Integer.MAX_VALUE) % totalPartition;
    LOG.info("talos partition: topic =>" + topicName + "; Get the partition of "
        + encodedRegionName + " => " + hash);
    TopicTalosResourceName topicTalosResourceName =
        topic.getTopicInfo().getTopicTalosResourceName();
    TopicAndPartition topicAndPartition =
        new TopicAndPartition(topicName, topicTalosResourceName, hash);
    return new SimpleProducer(producerConfig, topicAndPartition, credential);
  }

  public Map<TableName, Topic> getTopics() {
    return this.topics;
  }

  public Map<String, SimpleProducer> getProducers() {
    return this.producers;
  }

  @Override
  public UUID getPeerUUID() {
    return UUID.randomUUID();
  }

  @Override
  public boolean replicate(ReplicateContext replicateContext) {
    List<WAL.Entry> entries = replicateContext.getEntries();
    int sleepMultiplier = 1;
    while (this.isRunning()) {
      if (!isPeerEnabled()) {
        if (sleepForRetries("Replication is disabled", sleepMultiplier)) {
          sleepMultiplier++;
        }
        continue;
      }
      try {
        Map<TableName, Map<String, List<Message>>> messages = constructTalosMessages(entries);
        sendMessagesToTalos(messages);
        ctx.getMetrics()
            .setAgeOfLastShippedOp(entries.get(entries.size() - 1).getKey().getWriteTime(), replicateContext.getWalGroupId());
        return true;
      } catch (IOException | TException e) {
        if (e instanceof TException) {
          LOG.warn("TException may due to change of topic, so we clean the cache of topics");
          cleanTopicsAndProducers();
        }
        LOG.warn("Replicate edites to talos failed.", e);
        // Didn't ship anything, but must still age the last time we did
        ctx.getMetrics().refreshAgeOfLastShippedOp(replicateContext.getWalGroupId());
        if (sleepForRetries("Failed to replicate", sleepMultiplier)) {
          sleepMultiplier++;
        }
      }
    }
    return false;
  }

  @Override
  public void start() {
    startAsync();
  }

  @Override
  public void stop() {
    stopAsync();
  }

  private void sendMessagesToTalos(Map<TableName, Map<String, List<Message>>> messages)
      throws TException, IOException {
    TableName tableName;
    for (Map.Entry<TableName, Map<String, List<Message>>> messageEntry : messages.entrySet()) {
      tableName = messageEntry.getKey();
      for (Map.Entry<String, List<Message>> regionMessages : messageEntry.getValue().entrySet()) {
        try {
          SimpleProducer simpleProducer =
              this.getSimpleProducer(tableName, regionMessages.getKey());
          batchSendMessages(simpleProducer, regionMessages.getValue());
        } catch (TException | IOException e) {
          if (e instanceof GalaxyTalosException
              && ((GalaxyTalosException) e).getErrorCode() == TOPIC_NOT_EXIST) {
            if (this.ctx.getPeerConfig().needToReplicate(tableName)) {
              throw e;
            } else {
              LOG.warn("This table " + tableName.getNameAsString()
                  + " is not in the replication peer " + this.ctx.getPeerId() + " anymore, we can skip its entries");
            }
          } else {
            throw e;
          }
        }
      }
    }
  }

  private void batchSendMessages(SimpleProducer simpleProducer, List<Message> messages)
      throws IOException, TException {
    int fromIndex = 0;
    int toIndex = 0;
    while (toIndex < messages.size()) {
      long batchSize = 0;
      for (toIndex = fromIndex; toIndex < messages.size()
          && batchSize < batchSizeLimit; toIndex++) {
        batchSize += messages.get(toIndex).getMessage().length;
      }
      simpleProducer.putMessageList(messages.subList(fromIndex, toIndex));
      fromIndex = toIndex;
    }
  }

  protected void assembleMessage(Map<TableName, Map<String, List<Message>>> messages , WAL.Entry entry,
      List<Message> constructedMessages){
    TableName tableName = entry.getKey().getTableName();
    Map<String, List<Message>> regionMessages =
        messages.computeIfAbsent(tableName, key -> new HashMap<>());
    String encodedRegionName = Bytes.toStringBinary(entry.getKey().getEncodedRegionName());
    List<Message> messageList =
        regionMessages.computeIfAbsent(encodedRegionName, key -> new ArrayList<>());
    messageList.addAll(constructedMessages);

  }


  protected Map<TableName, Map<String, List<Message>>>
  constructTalosMessages(List<WAL.Entry> entries) throws IOException {
    Map<TableName, Map<String, List<Message>>> messages = new HashMap<>();
    for (WAL.Entry entry : entries) {
      List<Message> constructedMessages = TalosUtil.constructMessages(entry);
      assembleMessage(messages, entry, constructedMessages);
    }
    return messages;
  }

  private void cleanTopicsAndProducers() {
    this.producers = new HashMap<>();
    this.topics = new HashMap<>();
  }

  private static void sleepBackoff(int tries) {
    if (tries >= HConstants.RETRY_BACKOFF.length) {
      tries = HConstants.RETRY_BACKOFF.length - 1;
    }
    long sleepTime = 1000 * HConstants.RETRY_BACKOFF[tries];
    long jitter = (long) (1000 * ThreadLocalRandom.current().nextFloat());
    long totalTime = sleepTime + jitter;
    LOG.warn("gonna sleep for {} ms to avoid describe topic throttle", totalTime);
    try {
      Thread.currentThread().sleep(totalTime);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }
}
