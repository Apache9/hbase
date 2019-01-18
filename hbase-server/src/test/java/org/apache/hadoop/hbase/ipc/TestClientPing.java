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
package org.apache.hadoop.hbase.ipc;

import static org.apache.hadoop.hbase.ipc.RpcClient.PING_INTERVAL_NAME;
import static org.apache.hadoop.hbase.ipc.RpcClient.PING_TIMEOUT;
import static org.apache.hadoop.hbase.ipc.TestProtobufRpcServiceImpl.SERVICE;
import static org.apache.hadoop.hbase.ipc.TestProtobufRpcServiceImpl.newBlockingStub;
import static org.apache.hadoop.hbase.ipc.TestProtobufRpcServiceImpl.newStub;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.Server;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RPCTests;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xiaomi.infra.thirdparty.com.google.common.collect.Lists;
import com.xiaomi.infra.thirdparty.com.google.common.io.Closeables;
import com.xiaomi.infra.thirdparty.com.google.protobuf.RpcCallback;
import com.xiaomi.infra.thirdparty.com.google.protobuf.ServiceException;
import com.xiaomi.infra.thirdparty.io.netty.channel.ChannelHandlerContext;
import com.xiaomi.infra.thirdparty.io.netty.channel.ChannelOutboundHandlerAdapter;
import com.xiaomi.infra.thirdparty.io.netty.channel.ChannelPipeline;
import com.xiaomi.infra.thirdparty.io.netty.channel.ChannelPromise;

import org.apache.hadoop.hbase.shaded.ipc.protobuf.generated.TestProtos.EchoRequestProto;
import org.apache.hadoop.hbase.shaded.ipc.protobuf.generated.TestProtos.EmptyResponseProto;
import org.apache.hadoop.hbase.shaded.ipc.protobuf.generated.TestProtos.PauseRequestProto;
import org.apache.hadoop.hbase.shaded.ipc.protobuf.generated.TestRpcServiceProtos.TestProtobufRpcProto.BlockingInterface;
import org.apache.hadoop.hbase.shaded.ipc.protobuf.generated.TestRpcServiceProtos.TestProtobufRpcProto.Interface;

@Category({ RPCTests.class, MediumTests.class })
public class TestClientPing {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestClientPing.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestClientPing.class);

  private static final Configuration CONF = HBaseConfiguration.create();

  private static TestPingRpcServer SERVER;

  private static boolean STOP_PING = false;

  static class TestPingRpcServer extends NettyRpcServer {

    public TestPingRpcServer(Server server, String name, List<BlockingServiceAndInterface> services,
        InetSocketAddress bindAddress, Configuration conf, RpcScheduler scheduler,
        boolean reservoirEnabled) throws IOException {
      super(server, name, services, bindAddress, conf, scheduler, reservoirEnabled);
    }

    @Override
    protected void initPipeline(ChannelPipeline pipeline) {
      super.initPipeline(pipeline);
      pipeline.addLast(new ChannelOutboundHandlerAdapter() {

        @Override
        public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise)
            throws Exception {
          if (STOP_PING && msg instanceof NettyServerCall) {
            NettyServerCall call = (NettyServerCall) msg;
            if (call.id == RpcClient.PING_CALL_ID) {
              TestClientPing.LOG.info("Skip responding ping {}", call);
              // stop ping response
              return;
            }
          }
          super.write(ctx, msg, promise);
        }
      });
    }
  }

  private RpcClient client;

  @BeforeClass
  public static void setUpBeforeClass() throws IOException {
    CONF.setInt(PING_INTERVAL_NAME, 2000);
    CONF.setInt(PING_TIMEOUT, 500);
    CONF.setInt("hbase.ipc.client.connection.maxidletime", 60000);
    SERVER = new TestPingRpcServer(null, "testRpcServer",
        Lists.newArrayList(new RpcServer.BlockingServiceAndInterface(SERVICE, null)),
        new InetSocketAddress("localhost", 0), CONF, new FifoRpcScheduler(CONF, 2), true);
    SERVER.start();
  }

  @AfterClass
  public static void tearDownAfterClass() {
    if (SERVER != null) {
      SERVER.stop();
    }
  }

  @Before
  public void setUp() {
    CONF.setClass(RpcClientFactory.CUSTOM_RPC_CLIENT_IMPL_CONF_KEY, NettyRpcClient.class,
      RpcClient.class);
    client = RpcClientFactory.createClient(CONF, "test");
  }

  @After
  public void tearDown() throws IOException {
    Closeables.close(client, true);
  }

  @Test
  public void test() throws IOException, ServiceException, InterruptedException {
    STOP_PING = false;
    InetSocketAddress addr = SERVER.getListenerAddress();
    BlockingInterface stub = newBlockingStub(client, addr);
    String msg = "haha";
    assertEquals(msg,
      stub.echo(null, EchoRequestProto.newBuilder().setMessage(msg).build()).getMessage());
    assertEquals(1, SERVER.getNumOpenConnections());

    Interface asyncStub = newStub(client, addr);
    asyncStub.pause(new HBaseRpcControllerImpl(),
      PauseRequestProto.newBuilder().setMs(60000).build(), new RpcCallback<EmptyResponseProto>() {

        @Override
        public void run(EmptyResponseProto parameter) {
        }
      });

    Thread.sleep(5000);
    // we have ping so the connection should be still alive.
    assertEquals(1, SERVER.getNumOpenConnections());
    // confirm that the connection is still usable
    assertEquals(msg,
      stub.echo(null, EchoRequestProto.newBuilder().setMessage(msg).build()).getMessage());

    STOP_PING = true;
    Thread.sleep(5000);
    // The connection should be closed by client as we do not send ping to client and client think
    // we are dead.
    assertEquals(0, SERVER.getNumOpenConnections());
    assertEquals(msg,
      stub.echo(null, EchoRequestProto.newBuilder().setMessage(msg).build()).getMessage());
  }
}
