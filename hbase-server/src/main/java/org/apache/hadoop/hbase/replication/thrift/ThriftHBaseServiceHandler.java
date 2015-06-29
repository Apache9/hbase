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
import org.apache.hadoop.hbase.replication.thrift.generated.TBatchEdit;
import org.apache.hadoop.hbase.replication.thrift.generated.THBaseService;
import org.apache.hadoop.hbase.replication.thrift.generated.TIOError;
import org.apache.thrift.TException;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

public class ThriftHBaseServiceHandler implements THBaseService.Iface {

  private final THBaseService.Iface serviceHandler;
  private static final Log LOG = LogFactory.getLog(ThriftHBaseServiceHandler.class);


  public static THBaseService.Iface newInstance(Configuration conf,
      THBaseService.Iface serviceHandler, ThriftMetrics metrics) {
    THBaseService.Iface handler = new ThriftHBaseServiceHandler(conf, serviceHandler);
    return (THBaseService.Iface) Proxy.newProxyInstance(handler.getClass().getClassLoader(),
        new Class[] { THBaseService.Iface.class }, new THBaseServiceMetricsProxy(handler, metrics));
  }

  private static class THBaseServiceMetricsProxy implements InvocationHandler {
    private final THBaseService.Iface handler;
    private final ThriftMetrics metrics;

    private THBaseServiceMetricsProxy(THBaseService.Iface handler, ThriftMetrics metrics) {
      this.handler = handler;
      this.metrics = metrics;
    }

    @Override
    public Object invoke(Object proxy, Method m, Object[] args) throws Throwable {
      Object result;
      try {
        long start = now();
        result = m.invoke(handler, args);
        int processTime = (int) (now() - start);
        metrics.incMethodTime(m.getName(), processTime);
      } catch (InvocationTargetException e) {
        throw e.getTargetException();
      } catch (Exception e) {
        throw new RuntimeException("unexpected invocation exception: " + e.getMessage());
      }
      return result;
    }
  }

  private static long now() {
    return System.nanoTime();
  }

  ThriftHBaseServiceHandler(Configuration conf, THBaseService.Iface serviceHandler) {
    this.serviceHandler = serviceHandler;
  }

  private TIOError getTIOError(IOException e) {
    TIOError err = new TIOError();
    err.setMessage(e.getMessage());
    return err;
  }

  @Override
  public void replicate(TBatchEdit edits) throws TException {
    serviceHandler.replicate(edits);
  }

  @Override public void ping() throws TException {
    serviceHandler.ping();
  }

  @Override public String getClusterUUID() throws TException {
    return serviceHandler.getClusterUUID();
  }

}
