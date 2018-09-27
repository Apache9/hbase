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

import com.xiaomi.infra.thirdparty.galaxy.talos.consumer.SimpleConsumer;
import com.xiaomi.infra.thirdparty.galaxy.talos.producer.SimpleProducer;
import com.xiaomi.infra.thirdparty.galaxy.talos.thrift.Message;
import com.xiaomi.infra.thirdparty.galaxy.talos.thrift.MessageAndOffset;
import com.xiaomi.infra.thirdparty.galaxy.talos.thrift.Topic;
import com.xiaomi.infra.thirdparty.galaxy.talos.thrift.TopicAttribute;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.regionserver.wal.HLogKey;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.replication.regionserver.MetricsSource;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import static org.apache.hadoop.hbase.util.TalosUtil.HEADER_SIZE;

@Category(SmallTests.class)
public class TestTalosReplicationEndpoint {
  private static TalosReplicationEndpoint endpoint;
  private static ReplicationEndpoint.ReplicateContext context;
  private static final TableName TEST_TABLE = TableName.valueOf("test:talos");
  private static final TableName TEST_TABLE_2 = TableName.valueOf("test:talos2");
  private static final String TEST_REGION = "testRegion";
  private static final String TEST_REGION_2 = "testRegion2";
  private static final String TEST_REGION_3 = "testRegion3";

  private static TalosQueueDummy dummyTalosQueue;
  private static TalosQueueDummy dummyTalosQueue2;
  private static TalosQueueDummy dummyTalosQueue3;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    endpoint = new TalosReplicationEndpoint();
    context = new ReplicationEndpoint.ReplicateContext();
    byte[] encodedRegionName = Bytes.toBytes(TEST_REGION);
    byte[] encodedRegionName2 = Bytes.toBytes(TEST_REGION_2);
    byte[] encodedRegionName3 = Bytes.toBytes(TEST_REGION_3);
    long now = System.currentTimeMillis();
    UUID clusterId = UUID.randomUUID();
    List<HLog.Entry> entries = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      HLog.Entry entry = new HLog.Entry(
          new HLogKey(encodedRegionName, TEST_TABLE, i, now, clusterId), new WALEdit());
      entries.add(entry);
      HLog.Entry entry1 = new HLog.Entry(
          new HLogKey(encodedRegionName2, TEST_TABLE, i, now, clusterId), new WALEdit());
      entries.add(entry1);
      HLog.Entry entry2 = new HLog.Entry(
          new HLogKey(encodedRegionName3, TEST_TABLE_2, i, now, clusterId), new WALEdit());
      entries.add(entry2);
    }
    context.setEntries(entries);
    dummyTalosQueue = new TalosQueueDummy();
    dummyTalosQueue2 = new TalosQueueDummy();
    dummyTalosQueue3 = new TalosQueueDummy();
  }

  @Test
  public void testReplicate() throws Exception {
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

    endpoint.getProducers().put(TEST_REGION, producer);
    endpoint.getProducers().put(TEST_REGION_2, producer2);
    endpoint.getProducers().put(TEST_REGION_3, producer3);

    ReplicationPeer peer = Mockito.mock(ReplicationPeer.class);
    Mockito.when(peer.getPeerState()).thenReturn(ReplicationPeer.PeerState.ENABLED);
    MetricsSource ms = Mockito.mock(MetricsSource.class);
    Mockito.doNothing().when(ms).setAgeOfLastShippedOp(Mockito.anyLong());
    endpoint.ctx =
        new ReplicationEndpoint.Context(null, null, null, null, UUID.randomUUID(), peer, ms);
    endpoint.start();
    endpoint.replicate(context);

    Assert.assertEquals("size of queue1 = 100", 100, dummyTalosQueue.getQueue().size());
    Assert.assertEquals("size of queue2 = 100", 100, dummyTalosQueue2.getQueue().size());
    Assert.assertEquals("size of queue3 = 100", 100, dummyTalosQueue3.getQueue().size());

    SimpleConsumer consumer = dummyTalosQueue.getConsumer();
    List<MessageAndOffset> messageAndOffsets = consumer.fetchMessage(0, 100);
    for(int i = 0; i < 100; i++){
      Message message = messageAndOffsets.get(i).getMessage();
      AdminProtos.WALEntry walEntry = AdminProtos.WALEntry.parseFrom(
        Bytes.copy(message.getMessage(), HEADER_SIZE, message.getMessage().length - HEADER_SIZE));
      Assert.assertEquals(walEntry.getKey().getLogSequenceNumber(), i);
    }
  }

}
