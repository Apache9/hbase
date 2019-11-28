/**
 * Copyright The Apache Software Foundation
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.replication;

import com.xiaomi.infra.thirdparty.galaxy.talos.consumer.SimpleConsumer;
import com.xiaomi.infra.thirdparty.galaxy.talos.producer.SimpleProducer;
import com.xiaomi.infra.thirdparty.galaxy.talos.thrift.Message;
import com.xiaomi.infra.thirdparty.galaxy.talos.thrift.MessageAndOffset;
import com.xiaomi.infra.thirdparty.galaxy.talos.thrift.Topic;
import com.xiaomi.infra.thirdparty.galaxy.talos.thrift.TopicAttribute;

import java.util.Random;
import java.util.HashSet;
import java.util.Set;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.ArrayList;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.wal.HLogKey;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.replication.regionserver.MetricsSource;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

@Category(SmallTests.class) public class TestElasticSearchReplicationEndpoint {
  private static ElasticSearchReplicationEndpoint endpoint;
  private static ReplicationEndpoint.ReplicateContext context;
  private static final TableName TEST_TABLE = TableName.valueOf("test:es");
  private static final TableName TEST_TABLE_2 = TableName.valueOf("test:es2");
  private static final String TEST_REGION = "testRegion";
  private static final String TEST_REGION_2 = "testRegion2";
  private static final String TEST_REGION_3 = "testRegion3";
  private static final String TEST_REGION_4 = "testRegion4";
  private static final int CHUNK_SIZE = 4194304; // 4MB
  private static final int MAX_STRFING_LEN = CHUNK_SIZE / 3;
  private static TalosQueueDummy dummyTalosQueue;
  private static TalosQueueDummy dummyTalosQueue2;
  private static TalosQueueDummy dummyTalosQueue3;
  private static TalosQueueDummy dummyTalosQueue4;
  private static int putCount, deleteCount;

  @BeforeClass public static void setUpBeforeClass() throws Exception {
    endpoint = new ElasticSearchReplicationEndpoint();
    context = new ReplicationEndpoint.ReplicateContext();
    byte[] encodedRegionName = Bytes.toBytes(TEST_REGION);
    byte[] encodedRegionName2 = Bytes.toBytes(TEST_REGION_2);
    byte[] encodedRegionName3 = Bytes.toBytes(TEST_REGION_3);
    byte[] encodedRegionName4 = Bytes.toBytes(TEST_REGION_4);
    long now = System.currentTimeMillis();
    UUID clusterId = UUID.randomUUID();
    List<HLog.Entry> entries = new ArrayList<>();
    //empty kv should be ignore
    for (int i = 0; i < 10; i++) {
      KeyValue kv = new KeyValue();
      HLog.Entry entry =
          new HLog.Entry(new HLogKey(encodedRegionName, TEST_TABLE, i, now, clusterId),
              new WALEdit().add(kv));
      entries.add(entry);
    }

    for (int i = 0; i < 10; i++) {
      KeyValue kv = new KeyValue(new byte[10]);
      HLog.Entry entry =
          new HLog.Entry(new HLogKey(encodedRegionName, TEST_TABLE, i, now, clusterId),
              new WALEdit().add(kv));
      entries.add(entry);
    }

    putCount = deleteCount = 0;
    //test Put
    for (int i = 0; i < 10; i++) {
      Random r = new Random();
      int num = r.nextInt(10);
      KeyValue kv =
          new KeyValue("row1".getBytes(), "A".getBytes(), String.format("c%d", num).getBytes(), now,
              KeyValue.Type.Put, String.format("value_%d", putCount).getBytes());

      HLog.Entry entry =
          new HLog.Entry(new HLogKey(encodedRegionName, TEST_TABLE, i, now, clusterId),
              new WALEdit().add(kv));
      entries.add(entry);
      putCount += num >= 1 && num <= 3 ? 1 : 0;
    }
    //test DeleteColumn
    for (int i = 10; i < 20; i++) {
      Random r = new Random();
      int num = r.nextInt(10);
      KeyValue kv =
          new KeyValue("row1".getBytes(), "A".getBytes(), String.format("c%d", num).getBytes(), now,
              KeyValue.Type.DeleteColumn, String.format("value_%d", putCount).getBytes());

      HLog.Entry entry =
          new HLog.Entry(new HLogKey(encodedRegionName, TEST_TABLE, i, now, clusterId),
              new WALEdit().add(kv));
      entries.add(entry);
      deleteCount += num >= 1 && num <= 3 ? 1 : 0;
    }

    //test Delete
    WALEdit deleteWalEdit = new WALEdit();
    for (int i = 0; i < 10; i++) {
      KeyValue kv = new KeyValue(String.format("row_%d", i).getBytes(), now, KeyValue.Type.Delete);
      deleteWalEdit.add(kv);
    }

    for (int i = 10; i < 20; i++) {
      KeyValue kv = new KeyValue("row_11".getBytes(), now, KeyValue.Type.Delete);
      deleteWalEdit.add(kv);
    }

    HLog.Entry entry1 =
        new HLog.Entry(new HLogKey(encodedRegionName2, TEST_TABLE, 0, now, clusterId),
            deleteWalEdit);
    entries.add(entry1);

    //test DeleteFamily
    for (int i = 0; i < 10; i++) {
      KeyValue kv = new KeyValue("row1".getBytes(), "A".getBytes(), "c1".getBytes(), now,
          KeyValue.Type.DeleteFamily);
      HLog.Entry entry2 =
          new HLog.Entry(new HLogKey(encodedRegionName3, TEST_TABLE_2, i, now, clusterId),
              new WALEdit().add(kv));
      entries.add(entry2);
    }

    //test entry biger than chunk size
    WALEdit bigEntryWalEdit = new WALEdit();
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < MAX_STRFING_LEN - 20; i++) {
      if (i % 2 == 0) {
        sb.append("a");
      } else {
        sb.append("我");
      }
    }
    for (int i = 0; i < 100; i++) {
      KeyValue bigKv =
          new KeyValue("row0".getBytes(), "A".getBytes(), "c1".getBytes(), now, KeyValue.Type.Put,
              sb.toString().getBytes());
      bigEntryWalEdit.add(bigKv);
    }

    HLog.Entry entry3 =
        new HLog.Entry(new HLogKey(encodedRegionName4, TEST_TABLE_2, 1, now, clusterId),
            bigEntryWalEdit);
    entries.add(entry3);

    //kv biger than chunk size
    WALEdit bigKVWalEdit = new WALEdit();
    StringBuilder sb2 = new StringBuilder();
    for (int i = 0; i < MAX_STRFING_LEN + 1; i++) {
      if (i % 2 == 0) {
        sb2.append("a");
      } else {
        sb2.append("我");
      }
    }
    for (int i = 0; i < 10; i++) {
      KeyValue bigKv =
          new KeyValue("row0".getBytes(), "A".getBytes(), "c1".getBytes(), now, KeyValue.Type.Put,
              sb2.toString().getBytes());
      bigKVWalEdit.add(bigKv);
    }
    HLog.Entry entry4 =
        new HLog.Entry(new HLogKey(encodedRegionName4, TEST_TABLE_2, 2, now, clusterId),
            bigKVWalEdit);
    entries.add(entry4);

    context.setEntries(entries);
    dummyTalosQueue = new TalosQueueDummy();
    dummyTalosQueue2 = new TalosQueueDummy();
    dummyTalosQueue3 = new TalosQueueDummy();
    dummyTalosQueue4 = new TalosQueueDummy();
  }

  @Test public void testReplicate() throws Exception {
    Map<TableName, Topic> topics = endpoint.getTopics();
    TopicAttribute topicAttribute = new TopicAttribute();
    topicAttribute.setPartitionNumber(5);
    Topic topic = new Topic();
    topic.setTopicAttribute(topicAttribute);
    topics.put(TEST_TABLE, topic);
    topics.put(TEST_TABLE_2, topic);

    SimpleProducer producer = dummyTalosQueue.getSimpleProducer();
    SimpleProducer producer2 = dummyTalosQueue2.getSimpleProducer();
    SimpleProducer producer3 = dummyTalosQueue3.getSimpleProducer();
    SimpleProducer producer4 = dummyTalosQueue4.getSimpleProducer();

    endpoint.getProducers().put(TEST_REGION, producer);
    endpoint.getProducers().put(TEST_REGION_2, producer2);
    endpoint.getProducers().put(TEST_REGION_3, producer3);
    endpoint.getProducers().put(TEST_REGION_4, producer4);

    ReplicationPeer peer = Mockito.mock(ReplicationPeer.class);
    Mockito.when(peer.getPeerState()).thenReturn(ReplicationPeer.PeerState.ENABLED);
    MetricsSource ms = Mockito.mock(MetricsSource.class);
    Mockito.doNothing().when(ms).setAgeOfLastShippedOp(Mockito.anyLong());
    endpoint.ctx =
        new ReplicationEndpoint.Context(null, null, null, null, UUID.randomUUID(), peer, ms);

    Set<String> properties = new HashSet<>();
    properties.add("A:c1");
    properties.add("A:c2");
    properties.add("A:c3");
    endpoint.setEsIndexPropertiesCache(TEST_TABLE, properties);
    endpoint.setEsIndexPropertiesCache(TEST_TABLE_2, properties);
    endpoint.start();
    endpoint.replicate(context);

    Assert.assertEquals("size of queue1 = " + (putCount + deleteCount), putCount + deleteCount,
        dummyTalosQueue.getQueue().size());

    SimpleConsumer consumer1 = dummyTalosQueue.getConsumer();
    List<MessageAndOffset> messageAndOffsets1 = consumer1.fetchMessage(0, putCount + deleteCount);
    for (int i = 0; i < putCount; i++) {
      Message message = messageAndOffsets1.get(i).getMessage();
      String jsonStr = new String(message.getMessage());
      JSONObject json = new JSONObject(jsonStr);
      String value = json.getJSONArray("kv").getJSONObject(0).getString("v");
      Assert.assertEquals(value, String.format("value_%d", i));
      Assert.assertEquals("PUT", json.getJSONArray("kv").getJSONObject(0).getString("op"));
    }
    for (int i = putCount; i < putCount + deleteCount; i++) {
      Message message = messageAndOffsets1.get(i).getMessage();
      String jsonStr = new String(message.getMessage());
      JSONObject json = new JSONObject(jsonStr);
      String value = json.getJSONArray("kv").getJSONObject(0).getString("v");
      Assert.assertEquals(value, "");
      Assert.assertEquals("PUT", json.getJSONArray("kv").getJSONObject(0).getString("op"));
    }

    Assert.assertEquals("size of queue2 = 11", 11, dummyTalosQueue2.getQueue().size());
    SimpleConsumer consumer2 = dummyTalosQueue2.getConsumer();
    List<MessageAndOffset> messageAndOffsets2 = consumer2.fetchMessage(0, 11);
    for (int i = 0; i < 11; i++) {
      Message message = messageAndOffsets2.get(i).getMessage();
      String jsonStr = new String(message.getMessage());
      JSONObject json = new JSONObject(jsonStr);
      String rowkey = json.getString("r");
      Assert.assertEquals("DELETE", json.getJSONArray("kv").getJSONObject(0).getString("op"));
      if (rowkey.equals("row_11")) {
        Assert.assertEquals(10, json.getJSONArray("kv").length());
      } else {
        Assert.assertEquals(1, json.getJSONArray("kv").length());
      }
    }

    Assert.assertEquals("size of queue3 = 10", 10, dummyTalosQueue3.getQueue().size());
    SimpleConsumer consumer3 = dummyTalosQueue3.getConsumer();
    List<MessageAndOffset> messageAndOffsets3 = consumer3.fetchMessage(0, 10);
    for (int i = 0; i < 10; i++) {
      Message message = messageAndOffsets3.get(i).getMessage();
      String jsonStr = new String(message.getMessage());
      JSONObject json = new JSONObject(jsonStr);
      Assert.assertEquals(properties.size(), json.getJSONArray("kv").length());
      for (int j = 0; j < properties.size(); j++) {
        Assert.assertEquals("PUT", json.getJSONArray("kv").getJSONObject(j).getString("op"));
        Assert.assertEquals("", json.getJSONArray("kv").getJSONObject(j).getString("v"));
      }
    }

    Assert.assertEquals("size of queue4 = 10", 100, dummyTalosQueue4.getQueue().size());
    SimpleConsumer consumer4 = dummyTalosQueue4.getConsumer();
    List<MessageAndOffset> messageAndOffsets4 = consumer4.fetchMessage(0, 100);
    for (int i = 0; i < 100; i++) {
      Message message = messageAndOffsets4.get(i).getMessage();
      String jsonStr = new String(message.getMessage());
      JSONObject json = new JSONObject(jsonStr);
      Assert.assertEquals(MAX_STRFING_LEN - 20,
          json.getJSONArray("kv").getJSONObject(0).getString("v").length());
    }
  }
}
