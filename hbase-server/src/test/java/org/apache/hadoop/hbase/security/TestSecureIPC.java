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
package org.apache.hadoop.hbase.security;

import static org.apache.hadoop.hbase.security.HBaseKerberosUtils.getKeytabFileForTesting;
import static org.apache.hadoop.hbase.security.HBaseKerberosUtils.getPrincipalForTesting;
import static org.apache.hadoop.hbase.security.HBaseKerberosUtils.getSecuredConfiguration;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

import com.google.common.collect.Lists;
import com.google.protobuf.BlockingRpcChannel;
import com.google.protobuf.BlockingService;
import com.google.protobuf.ServiceException;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import javax.security.sasl.SaslException;

import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.ipc.AbstractTestIPC;
import org.apache.hadoop.hbase.ipc.FifoRpcScheduler;
import org.apache.hadoop.hbase.ipc.NettyRpcClient;
import org.apache.hadoop.hbase.ipc.RpcClient;
import org.apache.hadoop.hbase.ipc.RpcClientFactory;
import org.apache.hadoop.hbase.ipc.RpcClientImpl;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.ipc.RpcServerInterface;
import org.apache.hadoop.hbase.ipc.protobuf.generated.TestProtos;
import org.apache.hadoop.hbase.ipc.protobuf.generated.TestRpcServiceProtos;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.mockito.Mockito;

@RunWith(Parameterized.class)
@Category(SmallTests.class)
public class TestSecureIPC {

  private static final Log LOG = LogFactory.getLog(TestSecureIPC.class);

  public static final BlockingService SERVICE = AbstractTestIPC.SERVICE;

  private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  private static final File KEYTAB_FILE = new File(
      TEST_UTIL.getDataTestDir("keytab").toUri().getPath());

  private static Method GET_LOGIN;

  private static MiniKdc KDC;
  private static String HOST = "localhost";
  private static String PRINCIPAL;

  String krbKeytab;
  String krbPrincipal;
  UserGroupInformation ugi;
  Configuration clientConf;
  Configuration serverConf;

  @Parameterized.Parameters(name = "{index}: RpcClientClass={0}")
  public static Iterable<Object[]> data() {
    return Arrays.asList(new Object[] { RpcClientImpl.class.getName() },
      new Object[] { NettyRpcClient.class.getName() });
  }

  @Parameter
  public String rpcClientClass;

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    Properties conf = MiniKdc.createConf();
    conf.put(MiniKdc.DEBUG, true);
    KDC = new MiniKdc(conf, new File(TEST_UTIL.getDataTestDir("kdc").toUri().getPath()));
    KDC.start();
    PRINCIPAL = "hbase/" + HOST;
    KDC.createPrincipal(KEYTAB_FILE, PRINCIPAL);
    HBaseKerberosUtils.setKeytabFileForTesting(KEYTAB_FILE.getAbsolutePath());
    HBaseKerberosUtils.setPrincipalForTesting(PRINCIPAL + "@" + KDC.getRealm());
    GET_LOGIN = UserGroupInformation.class.getDeclaredMethod("getLogin");
    GET_LOGIN.setAccessible(true);
  }

  @AfterClass
  public static void tearDownAfterClass() throws IOException {
    if (KDC != null) {
      KDC.stop();
    }
    TEST_UTIL.cleanupTestDir();
  }

  private UserGroupInformation loginKerberosPrincipal(String krbKeytab, String krbPrincipal)
      throws Exception {
    Configuration cnf = new Configuration();
    cnf.set(CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION, "kerberos");
    UserGroupInformation.setConfiguration(cnf);
    UserGroupInformation.loginUserFromKeytab(krbPrincipal, krbKeytab);
    return UserGroupInformation.getLoginUser();
  }

  private void logout(UserGroupInformation ugi)
      throws IllegalAccessException, InvocationTargetException, LoginException {
    LoginContext ctx = (LoginContext) GET_LOGIN.invoke(ugi);
    ctx.logout();
  }

  @Before
  public void setUp() throws Exception {
    krbKeytab = getKeytabFileForTesting();
    krbPrincipal = getPrincipalForTesting();
    ugi = loginKerberosPrincipal(krbKeytab, krbPrincipal);
    clientConf = getSecuredConfiguration();
    clientConf.set(RpcClientFactory.CUSTOM_RPC_CLIENT_IMPL_CONF_KEY, rpcClientClass);
    serverConf = getSecuredConfiguration();
  }

  @After
  public void tearDown() throws Exception {
    logout(ugi);
  }

  @Test
  public void testRpcCallWithEnabledKerberosSaslAuth() throws Exception {
    UserGroupInformation ugi2 = UserGroupInformation.getCurrentUser();

    // check that the login user is okay:
    assertSame(ugi, ugi2);
    assertEquals(AuthenticationMethod.KERBEROS, ugi.getAuthenticationMethod());
    assertEquals(krbPrincipal, ugi.getUserName());

    callRpcService(User.create(ugi2));
  }

  @Test
  public void testRpcFallbackToSimpleAuth() throws Exception {
    serverConf.set(User.HBASE_SECURITY_CONF_KEY, "simple");
    clientConf.setBoolean(RpcClient.IPC_CLIENT_FALLBACK_TO_SIMPLE_AUTH_ALLOWED_KEY, true);
    callRpcService(User.create(ugi));
  }

  void setRpcProtection(String clientProtection, String serverProtection) {
    clientConf.set("hbase.rpc.protection", clientProtection);
    serverConf.set("hbase.rpc.protection", serverProtection);
  }

  /**
   * Test various combinations of Server and Client qops.
   * @throws Exception
   */
  @Test
  public void testSaslWithCommonQop() throws Exception {
    setRpcProtection("privacy,authentication", "authentication");
    callRpcService(User.create(ugi));

    setRpcProtection("authentication", "privacy,authentication");
    callRpcService(User.create(ugi));

    setRpcProtection("integrity,authentication", "privacy,authentication");
    callRpcService(User.create(ugi));

    setRpcProtection("integrity,authentication", "integrity,authentication");
    callRpcService(User.create(ugi));

    setRpcProtection("privacy,authentication", "privacy,authentication");
    callRpcService(User.create(ugi));
  }

  @Test
  public void testSaslNoCommonQop() throws Exception {
    exception.expect(SaslException.class);
    exception.expectMessage("No common protection layer between client and server");
    setRpcProtection("integrity", "privacy");
    callRpcService(User.create(ugi));
  }

  @Test
  public void testRecoverFromSaslError() throws Exception {
    logout(ugi);
    try {
      callRpcService(User.create(ugi));
    } catch (Exception e) {
      // This may fail if the rpc client implementation does not retry on connect error.
      LOG.warn("", e);
    }
    Thread.sleep(5000);
    // This time should success
    callRpcService(User.create(UserGroupInformation.getLoginUser()));
  }

  /**
   * Sets up a RPC Server and a Client. Does a RPC checks the result. If an exception is thrown from
   * the stub, this function will throw root cause of that exception.
   */
  private void callRpcService(User clientUser) throws Exception {
    SecurityInfo securityInfoMock = Mockito.mock(SecurityInfo.class);
    Mockito.when(securityInfoMock.getServerPrincipal())
        .thenReturn(HBaseKerberosUtils.KRB_PRINCIPAL);
    SecurityInfo.addInfo("TestProtobufRpcProto", securityInfoMock);

    InetSocketAddress isa = new InetSocketAddress(HOST, 0);

    RpcServerInterface rpcServer = new RpcServer(null, "AbstractTestSecureIPC",
        Lists.newArrayList(new RpcServer.BlockingServiceAndInterface(SERVICE, null)), isa,
        serverConf, new FifoRpcScheduler(serverConf, 1));
    rpcServer.start();
    RpcClient rpcClient = RpcClientFactory.createClient(clientConf,
      HConstants.DEFAULT_CLUSTER_ID.toString());
    try {
      InetSocketAddress address = rpcServer.getListenerAddress();
      if (address == null) {
        throw new IOException("Listener channel is closed");
      }
      BlockingRpcChannel channel = rpcClient.createBlockingRpcChannel(
        ServerName.valueOf(address.getHostName(), address.getPort(), System.currentTimeMillis()),
        clientUser, 0);
      TestRpcServiceProtos.TestProtobufRpcProto.BlockingInterface stub = TestRpcServiceProtos.TestProtobufRpcProto
          .newBlockingStub(channel);
      TestThread th1 = new TestThread(stub);
      final Throwable exception[] = new Throwable[1];
      Collections.synchronizedList(new ArrayList<Throwable>());
      Thread.UncaughtExceptionHandler exceptionHandler = new Thread.UncaughtExceptionHandler() {
        public void uncaughtException(Thread th, Throwable ex) {
          exception[0] = ex;
        }
      };
      th1.setUncaughtExceptionHandler(exceptionHandler);
      th1.start();
      th1.join();
      if (exception[0] != null) {
        // throw root cause.
        while (exception[0].getCause() != null) {
          exception[0] = exception[0].getCause();
        }
        throw (Exception) exception[0];
      }
    } finally {
      rpcServer.stop();
      rpcClient.close();
    }
  }

  public static class TestThread extends Thread {
    private final TestRpcServiceProtos.TestProtobufRpcProto.BlockingInterface stub;

    public TestThread(TestRpcServiceProtos.TestProtobufRpcProto.BlockingInterface stub) {
      this.stub = stub;
    }

    @Override
    public void run() {
      try {
        int[] messageSize = new int[] { 100, 1000, 10000 };
        for (int i = 0; i < messageSize.length; i++) {
          String input = RandomStringUtils.random(messageSize[i]);
          String result = stub
              .echo(null, TestProtos.EchoRequestProto.newBuilder().setMessage(input).build())
              .getMessage();
          assertEquals(input, result);
        }
      } catch (ServiceException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
