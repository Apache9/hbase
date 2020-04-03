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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
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
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import libthrift091.TException;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CollectionUtils;
import org.apache.hadoop.hbase.util.HBaseStreamUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link BaseReplicationEndpoint} for replication endpoints whose target is Talos.
 */
@InterfaceAudience.Private
public class HBaseStreamReplicationEndpoint extends BaseReplicationEndpoint {

  private static final Logger LOG = LoggerFactory.getLogger(HBaseStreamReplicationEndpoint.class);

  private HConnection localConn;
  private HBaseAdmin localAdmin;

  private String accessKey;
  private String accessSecret;
  private String endpoint;

  private long batchSizeLimit = 20971520L;

  private TalosClientConfig clientConfig;
  private TalosAdmin talosAdmin;
  private TalosProducerConfig producerConfig;
  private Credential credential;

  private final int maxTries = 10;

  private LoadingCache<TableName, Topic> topicCache = CacheBuilder.newBuilder()
      // expires per 10 mins, used to sense changes in the num of partition
      .expireAfterWrite(10, TimeUnit.MINUTES)
      .build(new CacheLoader<TableName, Topic>() {
        @Override
        public Topic load(TableName key) throws Exception {
          return loadTopicCache(key);
        }
      });

  private LoadingCache<TableName, TableStreamConfig> tableStreamConfigCache = CacheBuilder.newBuilder()
      // expires per 10 mins, used to sense changes in TableStreamConfig of table
      .expireAfterWrite(10, TimeUnit.MINUTES)
      .build(new CacheLoader<TableName, TableStreamConfig>() {
        @Override
        public TableStreamConfig load(TableName key) throws Exception {
          return loadTableStreamConfigCache(key);
        }
      });

  private LoadingCache<TableName, TalosProducers> tableProducersCache = CacheBuilder.newBuilder()
      // expires after 1 hour without access. cache of producers needs to be cleaned up after table or stream is deleted
      .expireAfterAccess(1, TimeUnit.HOURS)
      .build(new CacheLoader<TableName, TalosProducers>() {
        @Override
        public TalosProducers load(TableName key) throws Exception {
          return loadTalosProducersCache(key);
        }
      });

  @Override
  public void init(Context context) throws IOException {
    super.init(context);
    ReplicationPeerConfig peerConfig = context.getPeerConfig();
    try {
      Configuration conf = HBaseConfiguration.create(ctx.getConfiguration());
      this.maxRetriesMultiplier = conf.getInt("replication.source.maxretriesmultiplier", 10);
      this.sleepForRetriesCount = conf.getLong("replication.source.sleepforretries", 1000);
      this.batchSizeLimit = conf.getLong("talos.replication.batchsize.limit",20971520L);

      this.localConn = HConnectionManager.createConnection(ctx.getLocalConfiguration());
      this.localAdmin = new HBaseAdmin(this.localConn);

      endpoint = peerConfig.getConfiguration().get(TALOS_ACCESS_ENDPOINT);
      accessKey = peerConfig.getConfiguration().get(TALOS_ACCESS_KEY);
      accessSecret = peerConfig.getConfiguration().get(TALOS_ACCESS_SECRET);
      if (endpoint == null || accessKey == null || accessSecret == null) {
        // Don't throw exception, let replicationSource thread start
        LOG.warn("endpoint, accessKey or accessSecret is null");
      }
      initialTalosConfigs();
    } catch (Exception e) {
      LOG.warn("Failed to init endpoint of peer {}", context.getPeerId(), e);
    }
  }

  private void initialTalosConfigs() {
    Properties properties = new Properties();
    properties.setProperty(TALOS_ACCESS_ENDPOINT, endpoint);
    clientConfig = new TalosClientConfig(properties);
    producerConfig = new TalosProducerConfig(properties);
    credential = new Credential().setSecretKeyId(accessKey).setSecretKey(accessSecret).setType(UserType.APP_SECRET);
    talosAdmin = new TalosAdmin(clientConfig, credential);
  }

  @Override
  public synchronized void peerConfigUpdated(ReplicationPeerConfig peerConfig) {
    super.peerConfigUpdated(peerConfig);
    String newEndpoint = peerConfig.getConfiguration().get(TALOS_ACCESS_ENDPOINT);
    String newAccessKey = peerConfig.getConfiguration().get(TALOS_ACCESS_KEY);
    String newAccessSecret = peerConfig.getConfiguration().get(TALOS_ACCESS_SECRET);
    if (newEndpoint == null || newAccessKey == null || newAccessSecret == null) {
      throw new IllegalArgumentException(
          "endpoint, accessKey and accessSecret must be set to talos replication");
    }
    if (!Objects.equals(endpoint, newEndpoint) || !Objects.equals(accessKey, newAccessKey)
        || !Objects.equals(accessSecret, newAccessSecret)) {
      endpoint = newEndpoint;
      accessKey = newAccessKey;
      accessSecret = newAccessSecret;
      initialTalosConfigs();
      cleanTopicsAndProducers();
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
    super.doStop();
  }

  @Override
  public UUID getPeerUUID() {
    return UUID.randomUUID();
  }

  @Override
  public boolean replicate(ReplicateContext replicateContext) {
    List<HLog.Entry> entries = replicateContext.getEntries();
    int sleepMultiplier = 1;
    while (this.isRunning()) {
      if (!isPeerEnabled()) {
        if (sleepForRetries("Replication is disabled", sleepMultiplier)) {
          sleepMultiplier++;
        }
        continue;
      }
      if (talosAdmin == null) {
        LOG.warn("talosAdmin is null, please check talos config");
        if (sleepForRetries("talosAdmin is null", sleepMultiplier)) {
          sleepMultiplier++;
        }
        continue;
      }
      try {
        // group HLog. table => encodedRegionName => messages
        Map<TableName, Map<byte[], List<Message>>> messages = groupHLog(entries);
        // send
        sendMessagesToTalos(messages);
        ctx.getMetrics().setAgeOfLastShippedOp(entries.get(entries.size() - 1).getKey().getWriteTime());
        return true;
      } catch (IOException | TException e) {
        if (e instanceof TException) {
          LOG.warn("peer: {}. TException may due to change of topic, so we clean the cache of topics.",
              this.ctx.getPeerId());
          cleanTopicsAndProducers();
        }
        LOG.warn("peer: {}. Replicate edits to talos failed.", this.ctx.getPeerId(), e);
        // Didn't ship anything, but must still age the last time we did
        ctx.getMetrics().refreshAgeOfLastShippedOp();
        if (sleepForRetries("Failed to replicate", sleepMultiplier)) {
          sleepMultiplier++;
        }
      }
    }
    return false;
  }

  private Map<TableName, Map<byte[], List<Message>>> groupHLog(List<HLog.Entry> entries) throws IOException {
    Map<TableName, Map<byte[], List<Message>>> messages = new HashMap<>();
    for (HLog.Entry entry : entries) {
      TableName tablename = entry.getKey().getTablename();
      TableStreamConfig tableStreamConfig;
      try {
        tableStreamConfig = this.tableStreamConfigCache.get(tablename);
      } catch (ExecutionException e) {
        Throwable cause = e.getCause();
        if (cause instanceof IOException) {
          throw (IOException) cause;
        }
        throw new IOException(cause);
      }
      List<Message> entryMessages = constructJsonMessages(entry, tableStreamConfig);
      // entryMessages will be empty if all cells of entry are filtered.
      if (!CollectionUtils.isEmpty(entryMessages)) {
        messages.computeIfAbsent(tablename, t -> new TreeMap<>(Bytes.BYTES_COMPARATOR))
            .computeIfAbsent(entry.getKey().getEncodedRegionName(), e -> new ArrayList<>())
            .addAll(entryMessages);
      }
    }
    return messages;
  }

  private void sendMessagesToTalos(Map<TableName, Map<byte[], List<Message>>> messages)
      throws IOException, TException {
    for (Map.Entry<TableName, Map<byte[], List<Message>>> tableEntry : messages.entrySet()) {
      TableName tableName = tableEntry.getKey();
      try {
        for (Map.Entry<byte[], List<Message>> regionAndMessages : tableEntry.getValue().entrySet()) {
          SimpleProducer producer = getProducer(tableName, regionAndMessages.getKey());
          batchSendMessages(producer, regionAndMessages.getValue());
        }
      } catch (TException | IOException e) {
        if (e instanceof GalaxyTalosException && ((GalaxyTalosException) e).getErrorCode() == TOPIC_NOT_EXIST) {
          if (this.ctx.getPeerConfig().needToReplicate(tableName)) {
            throw e;
          } else {
            LOG.warn("This table {} is not in the replication peer {} anymore, we can skip its entries",
                tableName, this.ctx.getPeerId());
          }
        } else {
          throw e;
        }
      }
    }
  }

  private SimpleProducer getProducer(TableName tableName, byte[] encodeRegionName) throws IOException, TException {
    try {
      Topic topic = topicCache.get(tableName);
      TalosProducers talosProducers = tableProducersCache.get(tableName);
      talosProducers.update(topic);
      return talosProducers.get(encodeRegionName);
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof IOException) {
        throw (IOException) cause;
      }
      if (cause instanceof TException) {
        throw (TException) cause;
      }
      throw new IOException(e.getCause());
    }
  }

  private void batchSendMessages(SimpleProducer simpleProducer, List<Message> messages)
      throws IOException, TException {
    for (int from = 0, to = 0; to < messages.size(); from = to) {
      for (long batch = 0; to < messages.size() && batch < batchSizeLimit; to ++) {
        batch += messages.get(to).getMessage().length;
      }
      simpleProducer.putMessageList(messages.subList(from, to));
    }
  }

  private void cleanTopicsAndProducers() {
    this.topicCache.invalidateAll();
    this.tableProducersCache.invalidateAll();
  }

  private Topic loadTopicCache(TableName tableName) throws IOException, TException {
    int tries = 0;
    Topic topic = null;
    // topic name: hbase-stream-ns:table, after encode: hbase-stream-ns---table
    String topicName = HBaseStreamUtil.encodeTopicName(tableName.getNameAsString());
    while (topic == null && tries <= maxTries) {
      try {
        topic = talosAdmin.describeTopic(new DescribeTopicRequest(topicName));
      } catch (TException e) {
        if (tries == maxTries) {
          throw new IOException("Times of describing topic reach to the maximum number");
        }
        if (e instanceof GalaxyTalosException
            && ((GalaxyTalosException) e).getErrorCode() == THROTTLE_REJECT_ERROR) {
          LOG.warn("Get throttled by talos when we call describe topic {}, tries={}", topicName, tries);
          sleepBackoff(tries++);
        } else {
          throw e;
        }
      }
    }
    LOG.debug("load topic {}, table: {}", topicName, tableName);
    return topic;
  }

  private TableStreamConfig loadTableStreamConfigCache(TableName tableName) throws IOException {
    int tries = 0;
    TableStreamConfig streamConfig = null;
    while (streamConfig == null && tries <= maxTries) {
      try {
        HTableDescriptor desc = localAdmin.getTableDescriptor(tableName);
        streamConfig = new TableStreamConfig(desc.getValue("stream_config"));
      } catch (IOException e) {
        if (tries == maxTries) {
          throw new IOException("Times of describing table reach to the maximum number");
        }
        LOG.warn("Get throttled by admin when we call getTableDescriptor, table {}, tries={}", tableName, tries);
        throw e;
      }
    }
    LOG.debug("load TalosStreamConfig, table: {}, fieldsControl: {}", tableName, streamConfig.getFieldsControl());
    return streamConfig;
  }

  private TalosProducers loadTalosProducersCache(TableName tableName) throws Exception {
    Topic topic;
    try {
      topic = topicCache.get(tableName);
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof IOException) {
        throw (IOException) cause;
      }
      if (cause instanceof TException) {
        throw (TException) cause;
      }
      throw new IOException(e.getCause());
    }
    return new TalosProducers(topic);
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

  public List<Message> constructJsonMessages(HLog.Entry entry, TableStreamConfig tableStreamConfig) {
    ArrayList<Cell> cells = entry.getEdit().getCells();
    JsonArray jsonObjs = new JsonArray();
    cells.stream()
        .map(cell -> cellToJson(cell, tableStreamConfig))
        .filter(Objects::nonNull)
        .forEach(jsonObjs::add);
    return jsonObjs.size() == 0 ? Collections.emptyList() : HBaseStreamUtil.toMessage(jsonObjs);
  }

  private JsonObject cellToJson(Cell cell, TableStreamConfig tableStreamConfig) {
    String valueInBase64 = null;
    if (FieldsControl.MUTATE_LOG.equals(tableStreamConfig.getFieldsControl())) {
      valueInBase64 = HBaseStreamUtil.toBase64String(CellUtil.cloneValue(cell));
    }
    KeyValue.Type opType = KeyValue.Type.codeToType(cell.getTypeByte());
    if (!tableStreamConfig.canAcceptOp(opType)) {
      return null;
    }
    JsonObject cellJson = null;
    switch (opType) {
      case Put:
        cellJson = HBaseStreamUtil.cellToJson(cell, valueInBase64);
        break;
      case Delete:
      case DeleteFamily:
      case DeleteColumn:
        cellJson = HBaseStreamUtil.cellToJson(cell, null);
        break;
    }
    return cellJson;
  }

  class TalosProducers {
    Topic topic;
    SimpleProducer[] producers;
    AtomicInteger index;

    public TalosProducers(Topic topic) {
      this.topic = topic;
      producers = new SimpleProducer[0];
      index = new AtomicInteger(0);
      update(topic);
    }

    private void update(Topic topic) {
      this.topic = topic;
      int partitionNum = topic.getTopicAttribute().getPartitionNumber();
      if (partitionNum == producers.length) {
        return;
      }
      SimpleProducer[] newProducers = new SimpleProducer[partitionNum];
      System.arraycopy(producers, 0, newProducers, 0, producers.length);
      TopicTalosResourceName topicTalosResourceName = topic.getTopicInfo().getTopicTalosResourceName();
      for (int i = producers.length; i < partitionNum; i++) {
        TopicAndPartition topicAndPartition =
            new TopicAndPartition(topic.getTopicInfo().getTopicName(), topicTalosResourceName, i);
        newProducers[i] = new SimpleProducer(producerConfig, topicAndPartition, credential);
      }
      this.producers = newProducers;
    }

    SimpleProducer get(byte[] rowkey) {
      return producers[(Bytes.hashCode(rowkey) & Integer.MAX_VALUE) % producers.length];
    }
  }

  static enum FieldsControl {
    MUTATE_LOG,
    KEYS_ONLY
  }

  static class TableStreamConfig {
    // fields_control
    private FieldsControl fieldsControl;
    // ops
    private Set<KeyValue.Type> ops;

    public TableStreamConfig(String configJsonStr) {
      if (configJsonStr == null || "".equals(configJsonStr)) {
        return;
      }
      JsonObject configJson = new JsonParser().parse(configJsonStr).getAsJsonObject();
      if (configJson.isJsonNull()) {
        return;
      }
      JsonElement fieldsControlJson = configJson.get("fields_control");
      if(fieldsControlJson != null && !fieldsControlJson.isJsonNull()) {
        fieldsControl = FieldsControl.valueOf(fieldsControlJson.getAsString());
      }
      JsonArray opsJsonArray = configJson.getAsJsonArray("ops");
      if (opsJsonArray != null && !opsJsonArray.isJsonNull()) {
        ops = new HashSet<>();
        for (JsonElement jsonElement : opsJsonArray) {
          ops.add(KeyValue.Type.valueOf(jsonElement.getAsString()));
        }
      }
    }

    public FieldsControl getFieldsControl() {
      return fieldsControl == null ? FieldsControl.MUTATE_LOG : fieldsControl;
    }

    public boolean canAcceptOp(KeyValue.Type op) {
      return ops == null || ops.isEmpty() || ops.contains(op);
    }
  }
}
