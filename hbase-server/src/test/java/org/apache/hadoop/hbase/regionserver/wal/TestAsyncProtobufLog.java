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
package org.apache.hadoop.hbase.regionserver.wal;

import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.wal.AsyncFSWALProvider;
import org.apache.hadoop.hbase.wal.WALProvider.Writer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.experimental.categories.Category;

import com.xiaomi.infra.thirdparty.io.netty.channel.Channel;
import com.xiaomi.infra.thirdparty.io.netty.channel.EventLoopGroup;
import com.xiaomi.infra.thirdparty.io.netty.channel.nio.NioEventLoopGroup;
import com.xiaomi.infra.thirdparty.io.netty.channel.socket.nio.NioSocketChannel;

@Category({ RegionServerTests.class, SmallTests.class })
public class TestAsyncProtobufLog extends AbstractTestProtobufLog {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestAsyncProtobufLog.class);

  private static EventLoopGroup EVENT_LOOP_GROUP;

  private static Class<? extends Channel> CHANNEL_CLASS;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    EVENT_LOOP_GROUP = new NioEventLoopGroup();
    CHANNEL_CLASS = NioSocketChannel.class;
    AbstractTestProtobufLog.setUpBeforeClass();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    AbstractTestProtobufLog.tearDownAfterClass();
    EVENT_LOOP_GROUP.shutdownGracefully().syncUninterruptibly();
  }

  @Override
  protected Writer createWriter(Path path) throws IOException {
    return new WriterOverAsyncWriter(AsyncFSWALProvider.createAsyncWriter(
      TEST_UTIL.getConfiguration(), fs, path, false, EVENT_LOOP_GROUP.next(), CHANNEL_CLASS));
  }
}
