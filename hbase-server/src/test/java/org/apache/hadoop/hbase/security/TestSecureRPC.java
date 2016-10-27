/**
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
package org.apache.hadoop.hbase.security;

import static org.apache.hadoop.hbase.security.HBaseKerberosUtils.getKeytabFileForTesting;
import static org.apache.hadoop.hbase.security.HBaseKerberosUtils.getPrincipalForTesting;
import static org.apache.hadoop.hbase.security.HBaseKerberosUtils.getSecuredConfiguration;
import static org.apache.hadoop.hbase.security.HBaseKerberosUtils.isKerberosPropertySetted;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assume.assumeTrue;

import com.google.common.collect.Lists;
import com.google.protobuf.BlockingRpcChannel;
import com.google.protobuf.BlockingService;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

import io.netty.util.internal.ThreadLocalRandom;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.ipc.FifoRpcScheduler;
import org.apache.hadoop.hbase.ipc.PayloadCarryingRpcController;
import org.apache.hadoop.hbase.ipc.RpcClient;
import org.apache.hadoop.hbase.ipc.RpcClientImpl;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.ipc.RpcServerInterface;
import org.apache.hadoop.hbase.ipc.protobuf.generated.TestProtos;
import org.apache.hadoop.hbase.ipc.protobuf.generated.TestProtos.EchoRequestProto;
import org.apache.hadoop.hbase.ipc.protobuf.generated.TestProtos.EchoResponseProto;
import org.apache.hadoop.hbase.ipc.protobuf.generated.TestProtos.EmptyRequestProto;
import org.apache.hadoop.hbase.ipc.protobuf.generated.TestProtos.EmptyResponseProto;
import org.apache.hadoop.hbase.ipc.protobuf.generated.TestProtos.PauseRequestProto;
import org.apache.hadoop.hbase.ipc.protobuf.generated.TestProtos.PauseResponseProto;
import org.apache.hadoop.hbase.ipc.protobuf.generated.TestRpcServiceProtos;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

@Category(SmallTests.class)
public class TestSecureRPC {
  public static RpcServerInterface rpcServer;

  static final BlockingService SERVICE = TestRpcServiceProtos.TestProtobufRpcProto
      .newReflectiveBlockingService(
        new TestRpcServiceProtos.TestProtobufRpcProto.BlockingInterface() {

        @Override
        public EmptyResponseProto ping(RpcController controller, EmptyRequestProto request)
            throws ServiceException {
          return null;
        }

        @Override
        public EchoResponseProto echo(RpcController controller, EchoRequestProto request)
            throws ServiceException {
          if (controller instanceof PayloadCarryingRpcController) {
            PayloadCarryingRpcController pcrc = (PayloadCarryingRpcController) controller;
            // If cells, scan them to check we are able to iterate what we were given and since this
            // is an echo, just put them back on the controller creating a new block.
            // Tests our block building.
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
            pcrc.setCellScanner(cellScanner);
          }
          return EchoResponseProto.newBuilder().setMessage(request.getMessage()).build();
        }

        @Override
        public EmptyResponseProto error(RpcController controller, EmptyRequestProto request)
            throws ServiceException {
          return null;
        }

        @Override
        public PauseResponseProto pause(RpcController controller, PauseRequestProto request)
            throws ServiceException {
          return null;
        }

      });

  /**
   * To run this test, we must specify the following system properties:
   *<p>
   * <b> hbase.regionserver.kerberos.principal </b>
   * <p>
   * <b> hbase.regionserver.keytab.file </b>
   */
  @Test
  public void testRpcCallWithEnabledKerberosSaslAuth() throws Exception {
    assumeTrue(isKerberosPropertySetted());
    String krbKeytab = getKeytabFileForTesting();
    String krbPrincipal = getPrincipalForTesting();

    Configuration cnf = new Configuration();
    cnf.set(CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION, "kerberos");
    UserGroupInformation.setConfiguration(cnf);
    UserGroupInformation.loginUserFromKeytab(krbPrincipal, krbKeytab);
    UserGroupInformation ugi = UserGroupInformation.getLoginUser();
    UserGroupInformation ugi2 = UserGroupInformation.getCurrentUser();

    // check that the login user is okay:
    assertSame(ugi, ugi2);
    assertEquals(AuthenticationMethod.KERBEROS, ugi.getAuthenticationMethod());
    assertEquals(krbPrincipal, ugi.getUserName());

    Configuration conf = getSecuredConfiguration();

    SecurityInfo securityInfoMock = Mockito.mock(SecurityInfo.class);
    Mockito.when(securityInfoMock.getServerPrincipal())
      .thenReturn(HBaseKerberosUtils.KRB_PRINCIPAL);
    SecurityInfo.addInfo("TestProtobufRpcProto", securityInfoMock);

    InetSocketAddress isa = new InetSocketAddress("localhost", 0);

    rpcServer = new RpcServer(null, "AbstractTestSecureIPC",
        Lists.newArrayList(new RpcServer.BlockingServiceAndInterface(SERVICE, null)), isa, conf,
        new FifoRpcScheduler(conf, 1));
    rpcServer.start();
    RpcClient rpcClient = new RpcClientImpl(conf, HConstants.DEFAULT_CLUSTER_ID.toString());
    try {
      BlockingRpcChannel channel = rpcClient.createBlockingRpcChannel(ServerName.valueOf(rpcServer
          .getListenerAddress().getHostName(), rpcServer.getListenerAddress().getPort(), System
          .currentTimeMillis()), User.getCurrent(), 1000);
      TestRpcServiceProtos.TestProtobufRpcProto.BlockingInterface stub = 
          TestRpcServiceProtos.TestProtobufRpcProto.newBlockingStub(channel);
      List<String> results = new ArrayList<String>();
      TestThread th1 = new TestThread(stub, results);
      th1.start();
      Thread.sleep(100);
      th1.join();

    } finally {
      rpcClient.close();
    }
  }

  public static class TestThread extends Thread {
    private final TestRpcServiceProtos.TestProtobufRpcProto.BlockingInterface stub;

    private final List<String> results;

    public TestThread(TestRpcServiceProtos.TestProtobufRpcProto.BlockingInterface stub,
        List<String> results) {
      this.stub = stub;
      this.results = results;
    }

    @Override
    public void run() {
      String result;
      try {
        result = stub.echo(
          null,
          TestProtos.EchoRequestProto.newBuilder()
              .setMessage(String.valueOf(ThreadLocalRandom.current().nextInt())).build())
            .getMessage();
      } catch (ServiceException e) {
        throw new RuntimeException(e);
      }
      if (results != null) {
        synchronized (results) {
          results.add(result);
        }
      }
    }
  }
}