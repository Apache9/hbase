/*
 * Copyright 2010 The Apache Software Foundation
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

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportFactory;

import java.security.PrivilegedAction;

/**
 * A TransportFactory that wraps another one, but assumes a specified UGI
 * before calling through.
 *
 * This is used on the server side to assume the server's Principal when accepting
 * clients.
 */
public class TUGIAssumingTransportFactory extends TTransportFactory {
  private final UserGroupInformation ugi;
  private final TTransportFactory wrapped;

  public TUGIAssumingTransportFactory(TTransportFactory wrapped, UserGroupInformation ugi) {
    assert wrapped != null;
    assert ugi != null;

    this.wrapped = wrapped;
    this.ugi = ugi;
  }

  @Override
  public TTransport getTransport(final TTransport trans) {
    return ugi.doAs(new PrivilegedAction<TTransport>() {
      public TTransport run() {
        return wrapped.getTransport(trans);
      }
    });
  }
}
