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
import static org.apache.hadoop.hbase.ipc.RpcClient.SOCKET_TIMEOUT_READ;
import static org.apache.hadoop.hbase.ipc.RpcClientFactory.CUSTOM_RPC_CLIENT_IMPL_CONF_KEY;
import static org.apache.hadoop.hbase.ipc.protobuf.generated.TestRpcServiceProtos.TestProtobufRpcProto.newBlockingStub;
import static org.apache.hadoop.hbase.ipc.protobuf.generated.TestRpcServiceProtos.TestProtobufRpcProto.newStub;
import static org.junit.Assert.assertEquals;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.ServiceException;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.ipc.AbstractTestIPC.TestRpcServer;
import org.apache.hadoop.hbase.ipc.protobuf.generated.TestProtos.EchoRequestProto;
import org.apache.hadoop.hbase.ipc.protobuf.generated.TestProtos.EmptyResponseProto;
import org.apache.hadoop.hbase.ipc.protobuf.generated.TestProtos.PauseRequestProto;
import org.apache.hadoop.hbase.ipc.protobuf.generated.TestRpcServiceProtos.TestProtobufRpcProto.BlockingInterface;
import org.apache.hadoop.hbase.ipc.protobuf.generated.TestRpcServiceProtos.TestProtobufRpcProto.Interface;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
@Category(MediumTests.class)
public class TestClientPing {

  private static final Configuration CONF = new Configuration();

  private static TestPingRpcServer SERVER;

  private static boolean STOP_PING = false;

  static class TestPingRpcServer extends TestRpcServer {

    class TestPingConnection extends RpcServer.Connection {

      public TestPingConnection(SocketChannel channel, long lastContact) {
        super(channel, lastContact);
      }

      @Override
      protected void respondPing() throws IOException {
        if (STOP_PING) {
          return;
        }
        super.respondPing();
      }
    }

    TestPingRpcServer() throws IOException {
      super(new FifoRpcScheduler(CONF, 5), CONF);
    }

    @Override
    protected Connection getConnection(SocketChannel channel, long time) {
      return new TestPingConnection(channel, time);
    }
  }

  @Parameter
  public Class<?> rpcClientImpl;

  private RpcClient client;

  @Parameters
  public static List<Object[]> params() {
    return Arrays.asList(new Object[] { BlockingRpcClient.class },
      new Object[] { NettyRpcClient.class });
  }

  @BeforeClass
  public static void setUpBeforeClass() throws IOException {
    CONF.setInt(SOCKET_TIMEOUT_READ, 2000);
    CONF.setInt(PING_INTERVAL_NAME, 2000);
    CONF.setInt(PING_TIMEOUT, 500);
    CONF.setInt("hbase.ipc.client.connection.maxidletime", 60000);
    SERVER = new TestPingRpcServer();
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
    CONF.setClass(CUSTOM_RPC_CLIENT_IMPL_CONF_KEY, rpcClientImpl, RpcClient.class);
    client = RpcClientFactory.createClient(CONF, "test");
  }

  @After
  public void tearDown() {
    IOUtils.closeQuietly(client);
  }

  @Test
  public void test() throws IOException, ServiceException, InterruptedException {
    STOP_PING = false;
    InetSocketAddress addr = SERVER.getListenerAddress();
    BlockingInterface stub = newBlockingStub(client.createBlockingRpcChannel(
      ServerName.valueOf(addr.getHostName(), addr.getPort(), System.currentTimeMillis()),
      User.getCurrent(), 0));
    String msg = "haha";
    assertEquals(msg,
      stub.echo(null, EchoRequestProto.newBuilder().setMessage(msg).build()).getMessage());
    assertEquals(1, SERVER.connectionList.size());

    Interface asyncStub = newStub(client.createRpcChannel(
      ServerName.valueOf(addr.getHostName(), addr.getPort(), System.currentTimeMillis()),
      User.getCurrent(), 0));
    asyncStub.pause(new HBaseRpcControllerImpl(),
      PauseRequestProto.newBuilder().setMs(60000).build(), new RpcCallback<EmptyResponseProto>() {

        @Override
        public void run(EmptyResponseProto parameter) {
        }
      });

    Thread.sleep(5000);
    // we have ping so the connection should be still alive.
    assertEquals(1, SERVER.connectionList.size());

    STOP_PING = true;
    Thread.sleep(5000);
    // The connection should be closed by client as we do not send ping to client and client think
    // we are dead.
    assertEquals(0, SERVER.connectionList.size());
    assertEquals(msg,
      stub.echo(null, EchoRequestProto.newBuilder().setMessage(msg).build()).getMessage());
  }
}
