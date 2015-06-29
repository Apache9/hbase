/**
 * Copyright 2011 The Apache Software Foundation
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
package org.apache.hadoop.hbase.replication.thrift;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.replication.thrift.generated.THBaseService;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.security.SaslRpcServer;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.*;

import javax.security.sasl.Sasl;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

public class ThriftServer extends Thread {
  private static final Log LOG = LogFactory.getLog(ThriftServer.class);
  private TServer server;
  private Configuration conf;
  private THBaseService.Iface sinkInterface;
  private boolean useSecure;

  public ThriftServer(Configuration conf, THBaseService.Iface sinkInterface) throws IOException {
    this.conf = conf;
    this.sinkInterface = sinkInterface;
    this.useSecure = User.isHBaseSecurityEnabled(conf);
    try {
      init();
    } catch (TTransportException e) {
      throw new IOException(e);
    }
  }

  private static TProtocolFactory getTProtocolFactory(boolean isCompact) {
    if (isCompact) {
      LOG.debug("Using compact protocol");
      return new TCompactProtocol.Factory();
    } else {
      LOG.debug("Using binary protocol");
      return new TBinaryProtocol.Factory();
    }
  }

  private static TTransportFactory getTTransportFactory(boolean framed) {
    if (framed) {
      LOG.debug("Using framed transport");
      return new TFramedTransport.Factory();
    } else {
      return new TTransportFactory();
    }
  }

  /*
   * If bindValue is null, we don't bind.
   */
  private static InetSocketAddress bindToPort(String bindValue, int listenPort)
      throws UnknownHostException {
    try {
      if (bindValue == null) {
        return new InetSocketAddress(listenPort);
      } else {
        return new InetSocketAddress(InetAddress.getByName(bindValue), listenPort);
      }
    } catch (UnknownHostException e) {
      throw new RuntimeException("Could not bind to provided ip address", e);
    }
  }

  private static TServer getTThreadPoolServer(TProtocolFactory protocolFactory, THBaseService.Processor processor,
      TTransportFactory transportFactory, InetSocketAddress inetSocketAddress) throws TTransportException {
    TServerTransport serverTransport = new TServerSocket(inetSocketAddress);
    LOG.info("starting HBase ThreadPool Thrift server on " + inetSocketAddress.toString());
    TThreadPoolServer.Args serverArgs = new TThreadPoolServer.Args(serverTransport);
    serverArgs.maxWorkerThreads(100);
    serverArgs.processor(processor);
    serverArgs.transportFactory(transportFactory);
    serverArgs.protocolFactory(protocolFactory);
    return new TThreadPoolServer(serverArgs);
  }

  public void init() throws TTransportException, IOException {
    int listenPort = ThriftUtilities.getThriftServerPort(conf);

    ThriftMetrics metrics = new ThriftMetrics(conf);

    boolean isCompact =
        conf.getBoolean("hbase.replication.thrift.compact", true);

    // Construct correct ProtocolFactory
    TProtocolFactory protocolFactory = getTProtocolFactory(isCompact);
    THBaseService.Iface handler =
        ThriftHBaseServiceHandler.newInstance(conf, sinkInterface, metrics);
    THBaseService.Processor processor = new THBaseService.Processor(handler);


    boolean isFramed =
        conf.getBoolean("hbase.replication.thrift.framed", false);
    TTransportFactory transportFactory = getTTransportFactory(isFramed);


    String serverProtocol = UserGroupInformation.getCurrentUser().getUserName();
    String serverAddress = null;
    if(User.isHBaseSecurityEnabled(conf)) {
      String kerberosName = UserGroupInformation.getCurrentUser().getUserName();
      final String names[] = SaslRpcServer.splitKerberosName(kerberosName);
      if (names.length != 3) {
        throw new TTransportException("Kerberos principal should have 3 parts: " + kerberosName);
      }
      serverProtocol = names[0];
      serverAddress = names[1];
    }

    if(useSecure) {
      Map<String, String> saslProps = new HashMap<String, String>();
      saslProps.put(Sasl.QOP, ThriftUtilities.getQOP(conf).getSaslQop());
      TSaslServerTransport.Factory saslTransFactory = new TSaslServerTransport.Factory();
      saslTransFactory.addServerDefinition(
          ThriftUtilities.getAuthType(conf).getMechanismName(),
          serverProtocol, serverAddress,  // two parts of kerberos principal
          saslProps,
          new SaslRpcServer.SaslGssCallbackHandler());

      transportFactory = new ChainedTTransportFactory(transportFactory,
          new TUGIAssumingTransportFactory(saslTransFactory,
              UserGroupInformation.getCurrentUser()));
    }

    InetSocketAddress inetSocketAddress =
        bindToPort(conf.get("hbase.replication.thrift.address"), listenPort);
    LOG.info("Listening on "+inetSocketAddress.getHostName()+":"+inetSocketAddress.getPort());
    server = getTThreadPoolServer(protocolFactory, processor, transportFactory, inetSocketAddress);
  }

  public void run() {
    server.serve();
  }

  public void shutdown() {
    try {
      server.stop();
    } catch (Throwable e) {
      LOG.error("Error stopping thrift server", e);
    }
  }
}
