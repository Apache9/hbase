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
import libthrift091.TException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.mockito.Mockito;

@InterfaceAudience.Private
public class TalosQueueDummy {
  private List<MessageAndOffset> queue =  new ArrayList<>();
  private SimpleConsumer fakeConsumer = Mockito.mock(SimpleConsumer.class);
  private SimpleProducer fakeProducer = Mockito.mock(SimpleProducer.class);

  public SimpleConsumer getConsumer() throws TException, IOException {
    Mockito.doAnswer(invocation -> {
      long offset = invocation.<Long>getArgument(0);
      int cache = invocation.<Integer>getArgument(1);
      return queue.subList((int)offset, (int)offset+cache);
    }).when(fakeConsumer).fetchMessage(Mockito.anyLong(), Mockito.anyInt());
    return fakeConsumer;
  }

  public SimpleProducer getSimpleProducer() throws IOException, TException {
    Mockito.doAnswer( invocation -> {
      List<Message> messageList = invocation.<List>getArgument(0);
      List<MessageAndOffset> messageAndOffsets = new ArrayList<>();
      long offset = queue.size();
      for(Message message : messageList){
        messageAndOffsets.add(new MessageAndOffset(message, offset));
        offset++;
      }
      queue.addAll(messageAndOffsets);
      return null;
    }).when(fakeProducer).putMessageList(Mockito.anyListOf(Message.class));
    return fakeProducer;
  }

  public List<MessageAndOffset> getQueue(){
    return queue;
  }
}
