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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.internal.verification.VerificationModeFactory.times;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.protobuf.BlockingService;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.ipc.protobuf.generated.TestProtos.EchoRequestProto;
import org.apache.hadoop.hbase.ipc.protobuf.generated.TestProtos.EchoResponseProto;
import org.apache.hadoop.hbase.ipc.protobuf.generated.TestProtos.EmptyRequestProto;
import org.apache.hadoop.hbase.ipc.protobuf.generated.TestProtos.EmptyResponseProto;
import org.apache.hadoop.hbase.ipc.protobuf.generated.TestProtos.PauseRequestProto;
import org.apache.hadoop.hbase.ipc.protobuf.generated.TestProtos.PauseResponseProto;
import org.apache.hadoop.hbase.ipc.protobuf.generated.TestRpcServiceProtos;
import org.apache.hadoop.hbase.monitoring.MonitoredRPCHandler;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.util.StringUtils;
import org.junit.Test;

/**
 * Some basic ipc tests.
 */
public abstract class AbstractTestIPC {

  private static final Log LOG = LogFactory.getLog(AbstractTestIPC.class);

  protected static byte[] CELL_BYTES = Bytes.toBytes("xyz");
  protected static Cell CELL = new KeyValue(CELL_BYTES, CELL_BYTES, CELL_BYTES, CELL_BYTES);
  protected static byte[] BIG_CELL_BYTES = new byte[10 * 1024];
  protected static Cell BIG_CELL = new KeyValue(CELL_BYTES, CELL_BYTES, CELL_BYTES, BIG_CELL_BYTES);
  protected final static Configuration CONF = HBaseConfiguration.create();
  // We are using the test TestRpcServiceProtos generated classes and Service because they are
  // available and basic with methods like 'echo', and ping. Below we make a blocking service
  // by passing in implementation of blocking interface. We use this service in all tests that
  // follow.
  public static final BlockingService SERVICE = TestRpcServiceProtos.TestProtobufRpcProto
      .newReflectiveBlockingService(
        new TestRpcServiceProtos.TestProtobufRpcProto.BlockingInterface() {

          @Override
          public EmptyResponseProto ping(RpcController controller, EmptyRequestProto request)
              throws ServiceException {
            return EmptyResponseProto.getDefaultInstance();
          }

          @Override
          public EmptyResponseProto error(RpcController controller, EmptyRequestProto request)
              throws ServiceException {
            throw new ServiceException(new IOException("remote error"));
          }

          @Override
          public EchoResponseProto echo(RpcController controller, EchoRequestProto request)
              throws ServiceException {
            if (controller instanceof PayloadCarryingRpcController) {
              PayloadCarryingRpcController pcrc = (PayloadCarryingRpcController) controller;
              // If cells, scan them to check we are able to iterate what we were given and since
              // this is
              // an echo, just put them back on the controller creating a new block. Tests our block
              // building.
              CellScanner cellScanner = pcrc.cellScanner();
              List<Cell> list = null;
              if (cellScanner != null) {
                list = new ArrayList<Cell>();
                try {
                  while (cellScanner.advance()) {
                    list.add(cellScanner.current());
                  }
                } catch (IOException e) {
                  throw new ServiceException(e);
                }
              }
              cellScanner = CellUtil.createCellScanner(list);
              ((PayloadCarryingRpcController) controller).setCellScanner(cellScanner);
            }
            return EchoResponseProto.newBuilder().setMessage(request.getMessage()).build();
          }

          @Override
          public PauseResponseProto pause(RpcController controller, PauseRequestProto request)
              throws ServiceException {
            Threads.sleepWithoutInterrupt(request.getMs());
            return PauseResponseProto.getDefaultInstance();
          }
        });

  /**
   * Instance of server. We actually don't do anything speical in here so could just use
   * HBaseRpcServer directly.
   */
  static class TestRpcServer extends RpcServer {

    TestRpcServer() throws IOException {
      this(new FifoRpcScheduler(CONF, 1));
    }

    TestRpcServer(RpcScheduler scheduler) throws IOException {
      super(null, "testRpcServer",
          Lists.newArrayList(new BlockingServiceAndInterface(SERVICE, null)),
          new InetSocketAddress("localhost", 0), CONF, scheduler);
    }

    @Override
    public Pair<Message, CellScanner> call(BlockingService service, MethodDescriptor md,
        Message param, CellScanner cellScanner, long receiveTime, MonitoredRPCHandler status)
        throws IOException {
      return super.call(service, md, param, cellScanner, receiveTime, status);
    }
  }

  protected abstract AbstractRpcClient<?> createRpcClientNoCodec(Configuration conf);

  /**
   * Ensure we do not HAVE TO HAVE a codec.
   * @throws IOException
   * @throws ServiceException
   */
  @Test
  public void testNoCodec() throws IOException, ServiceException {
    AbstractRpcClient<?> client = createRpcClientNoCodec(CONF);
    TestRpcServer rpcServer = new TestRpcServer();
    try {
      rpcServer.start();
      InetSocketAddress address = rpcServer.getListenerAddress();
      MethodDescriptor md = SERVICE.getDescriptorForType().findMethodByName("echo");
      final String message = "hello";
      EchoRequestProto param = EchoRequestProto.newBuilder().setMessage(message).build();
      PayloadCarryingRpcController pcrc = new PayloadCarryingRpcController();
      Message r = client.callBlockingMethod(md, pcrc, param, EchoResponseProto.getDefaultInstance(),
        User.getCurrent(), address, 0);
      assertNull(pcrc.cellScanner());
      // Silly assertion that the message is in the returned pb.
      assertTrue(r.toString().contains(message));
    } finally {
      client.close();
      rpcServer.stop();
    }
  }

  protected abstract AbstractRpcClient<?> createRpcClient(Configuration conf);

  @Test
  public void testCompressCellBlock()
      throws IOException, SecurityException, NoSuchMethodException, ServiceException {
    Configuration conf = new Configuration(CONF);
    conf.set("hbase.client.rpc.compressor", GzipCodec.class.getCanonicalName());
    AbstractRpcClient<?> client = createRpcClient(conf);
    TestRpcServer rpcServer = new TestRpcServer();
    List<Cell> cells = new ArrayList<Cell>();
    int count = 3;
    for (int i = 0; i < count; i++) {
      cells.add(CELL);
    }
    try {
      rpcServer.start();
      InetSocketAddress address = rpcServer.getListenerAddress();
      MethodDescriptor md = SERVICE.getDescriptorForType().findMethodByName("echo");
      EchoRequestProto param = EchoRequestProto.newBuilder().setMessage("hello").build();
      PayloadCarryingRpcController pcrc = new PayloadCarryingRpcController();
      pcrc.setCellScanner(CellUtil.createCellScanner(cells));
      client.callBlockingMethod(md, pcrc, param, EchoResponseProto.getDefaultInstance(),
        User.getCurrent(), address, 0);
      int index = 0;
      while (pcrc.cellScanner().advance()) {
        assertEquals(CELL, pcrc.cellScanner().current());
        index++;
      }
      assertEquals(count, index);
    } finally {
      client.close();
      rpcServer.stop();
    }
  }

  /**
   * Tests that the rpc scheduler is called when requests arrive.
   */
  @Test
  public void testRpcScheduler() throws IOException, InterruptedException, ServiceException {
    RpcScheduler scheduler = spy(new FifoRpcScheduler(CONF, 1));
    RpcServer rpcServer = new TestRpcServer(scheduler);
    verify(scheduler).init((RpcScheduler.Context) anyObject());
    AbstractRpcClient<?> client = createRpcClient(CONF);
    try {
      rpcServer.start();
      verify(scheduler).start();
      MethodDescriptor md = SERVICE.getDescriptorForType().findMethodByName("echo");
      EchoRequestProto param = EchoRequestProto.newBuilder().setMessage("hello").build();
      PayloadCarryingRpcController pcrc = new PayloadCarryingRpcController();
      for (int i = 0; i < 10; i++) {
        pcrc.reset();
        pcrc.setCellScanner(CellUtil.createCellScanner(ImmutableList.of(CELL)));
        client.callBlockingMethod(md, pcrc, param, EchoResponseProto.getDefaultInstance(),
          User.getCurrent(), rpcServer.getListenerAddress(), 0);
      }
      verify(scheduler, times(10)).dispatch((CallRunner) anyObject());
    } finally {
      client.close();
      rpcServer.stop();
      verify(scheduler).stop();
    }
  }

  @Test
  public void testEcho() throws IOException, ServiceException {
    AbstractRpcClient<?> client = createRpcClient(CONF);
    TestRpcServer rpcServer = new TestRpcServer();
    try {
      rpcServer.start();
      InetSocketAddress address = rpcServer.getListenerAddress();
      MethodDescriptor md = SERVICE.getDescriptorForType().findMethodByName("echo");
      PayloadCarryingRpcController pcrc = new PayloadCarryingRpcController();
      for (int i = 0; i < 100; i++) {
        final String message = "hello-" + i;
        EchoRequestProto param = EchoRequestProto.newBuilder().setMessage(message).build();
        pcrc.reset();
        Message r = client.callBlockingMethod(md, pcrc, param,
          EchoResponseProto.getDefaultInstance(), User.getCurrent(), address, 0);
        assertNull(pcrc.cellScanner());
        assertEquals(message, ((EchoResponseProto) r).getMessage());
      }
    } finally {
      client.close();
      rpcServer.stop();
    }
  }

  @Test
  public void testRemoteError() throws IOException, ServiceException {
    AbstractRpcClient<?> client = createRpcClient(CONF);
    TestRpcServer rpcServer = new TestRpcServer();
    try {
      rpcServer.start();
      InetSocketAddress address = rpcServer.getListenerAddress();
      MethodDescriptor md = SERVICE.getDescriptorForType().findMethodByName("error");
      client.callBlockingMethod(md, new PayloadCarryingRpcController(),
        EmptyRequestProto.getDefaultInstance(), EmptyResponseProto.getDefaultInstance(),
        User.getCurrent(), address, 0);
      fail("Expected an exception to have been thrown!");
    } catch (ServiceException e) {
      LOG.info("Caught expected exception: ", e);
      assertTrue(e.getCause() instanceof RemoteException);
      assertTrue(StringUtils.stringifyException(e).contains("remote error"));
    } finally {
      client.close();
      rpcServer.stop();
    }
  }

  @Test
  public void testTimeout() throws IOException {
    AbstractRpcClient<?> client = createRpcClient(CONF);
    TestRpcServer rpcServer = new TestRpcServer();
    try {
      rpcServer.start();
      InetSocketAddress address = rpcServer.getListenerAddress();
      MethodDescriptor md = SERVICE.getDescriptorForType().findMethodByName("pause");
      PayloadCarryingRpcController pcrc = new PayloadCarryingRpcController();
      int ms = 2000;
      int timeout = 100;
      for (int i = 0; i < 10; i++) {
        pcrc.reset();
        long startTime = System.nanoTime();
        try {
          client.callBlockingMethod(md, pcrc, PauseRequestProto.newBuilder().setMs(ms).build(),
            EchoResponseProto.getDefaultInstance(), User.getCurrent(), address, timeout);
        } catch (ServiceException e) {
          long waitTime = (System.nanoTime() - startTime) / 1000000;
          // expected
          LOG.warn("", e);
          // confirm that we got exception before the actual pause.
          assertTrue(waitTime < ms);
        }
      }
    } finally {
      client.close();
      rpcServer.stop();
    }
  }

  @Test
  public void testCleanupIdleConnections()
      throws IOException, ServiceException, InterruptedException {
    Configuration conf = new Configuration(CONF);
    // +1s
    conf.setInt("hbase.ipc.client.connection.maxidletime", 1000);
    AbstractRpcClient<?> client = createRpcClient(conf);
    TestRpcServer rpcServer = new TestRpcServer();
    try {
      rpcServer.start();
      InetSocketAddress address = rpcServer.getListenerAddress();
      MethodDescriptor md = SERVICE.getDescriptorForType().findMethodByName("ping");
      client.callBlockingMethod(md, new PayloadCarryingRpcController(),
        EmptyRequestProto.getDefaultInstance(), EmptyResponseProto.getDefaultInstance(),
        User.getCurrent(), address, 0);
      Collection<? extends Connection> conns = client.connections.values();
      assertEquals(1, conns.size());
      Connection conn = conns.iterator().next();
      Thread.sleep(5000);
      assertEquals(0, client.connections.values().size());
      client.callBlockingMethod(md, new PayloadCarryingRpcController(),
        EmptyRequestProto.getDefaultInstance(), EmptyResponseProto.getDefaultInstance(),
        User.getCurrent(), address, 0);
      conns = client.connections.values();
      assertEquals(1, conns.size());
      Connection conn1 = conns.iterator().next();
      assertNotSame(conn, conn1);
    } finally {
      client.close();
      rpcServer.stop();
    }
  }

  @Test
  public void testRTEDuringConnectionSetup() throws Exception {
    Configuration conf = new Configuration(CONF) {

      @Override
      public int getInt(String name, int defaultValue) {
        if (name.equals(RpcClient.SOCKET_TIMEOUT)) {
          throw new RuntimeException("Injected fault");
        }
        return super.getInt(name, defaultValue);
      }

    };

    TestRpcServer rpcServer = new TestRpcServer();
    AbstractRpcClient<?> client = createRpcClient(conf);
    try {
      rpcServer.start();
      InetSocketAddress address = rpcServer.getListenerAddress();
      MethodDescriptor md = SERVICE.getDescriptorForType().findMethodByName("echo");
      EchoRequestProto param = EchoRequestProto.newBuilder().setMessage("hello").build();
      client.callBlockingMethod(md, new PayloadCarryingRpcController(), param,
        EchoResponseProto.getDefaultInstance(), User.getCurrent(), address, 0);
      fail("Expected an exception to have been thrown!");
    } catch (Exception e) {
      LOG.info("Caught expected exception: ", e);
      assertTrue(StringUtils.stringifyException(e).contains("Injected fault"));
    } finally {
      client.close();
      rpcServer.stop();
    }
  }

  @Test
  public void testAsyncEcho() throws IOException {
    AbstractRpcClient<?> client = createRpcClient(CONF);
    TestRpcServer rpcServer = new TestRpcServer();
    try {
      rpcServer.start();
      InetSocketAddress address = rpcServer.getListenerAddress();
      MethodDescriptor md = SERVICE.getDescriptorForType().findMethodByName("echo");
      List<BlockingRpcCallback<Message>> callbackList = new ArrayList<BlockingRpcCallback<Message>>();
      List<PayloadCarryingRpcController> pcrcList = new ArrayList<PayloadCarryingRpcController>();
      for (int i = 0; i < 100; i++) {
        final String message = "hello-" + i;
        EchoRequestProto param = EchoRequestProto.newBuilder().setMessage(message).build();
        PayloadCarryingRpcController pcrc = new PayloadCarryingRpcController();
        BlockingRpcCallback<Message> callback = new BlockingRpcCallback<Message>();
        client.callMethod(md, pcrc, param, EchoResponseProto.getDefaultInstance(),
          User.getCurrent(), address, 0, callback);
        callbackList.add(callback);
        pcrcList.add(pcrc);
      }
      for (int i = 0; i < 100; i++) {
        Message r = callbackList.get(i).get();
        PayloadCarryingRpcController pcrc = pcrcList.get(i);
        assertFalse(pcrc.failed());
        assertEquals("hello-" + i, ((EchoResponseProto) r).getMessage());
        assertNull(pcrc.cellScanner());
      }
    } finally {
      client.close();
      rpcServer.stop();
    }
  }

  @Test
  public void testAsyncRemoteError() throws IOException {
    AbstractRpcClient<?> client = createRpcClient(CONF);
    TestRpcServer rpcServer = new TestRpcServer();
    try {
      rpcServer.start();
      InetSocketAddress address = rpcServer.getListenerAddress();
      MethodDescriptor md = SERVICE.getDescriptorForType().findMethodByName("error");
      BlockingRpcCallback<Message> callback = new BlockingRpcCallback<Message>();
      PayloadCarryingRpcController pcrc = new PayloadCarryingRpcController();
      client.callMethod(md, pcrc, EmptyRequestProto.getDefaultInstance(),
        EmptyResponseProto.getDefaultInstance(), User.getCurrent(), address, 0, callback);
      callback.get();
      assertTrue(pcrc.failed());
      LOG.info("Caught expected exception: ", pcrc.getError());
      assertTrue(pcrc.getError() instanceof RemoteException);
      assertTrue(StringUtils.stringifyException(pcrc.getError()).contains("remote error"));
    } finally {
      client.close();
      rpcServer.stop();
    }
  }
}
