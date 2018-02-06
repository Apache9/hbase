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

import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.RpcCallback;
import java.io.IOException;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.htrace.Span;
import org.apache.htrace.Trace;

import com.xiaomi.infra.thirdparty.io.netty.util.Timeout;

/** A call waiting for a value. */
@InterfaceAudience.Private
class Call {
  final int id; // call id
  final Message param; // rpc request method param object
  /**
   * Optionally has cells when making call. Optionally has cells set on response. Used passing cells
   * to the rpc and receiving the response.
   */
  CellScanner cells;
  Message response; // value, null if error
  // The return type. Used to create shell into which we deserialize the response if any.
  Message responseDefaultType;
  IOException error; // exception, null if value
  boolean done; // true when call is done
  long startTime;
  final MethodDescriptor md;
  final int timeout;
  final int priority;
  final RpcCallback<Call> callback;
  final Span span;
  Timeout timeoutTask;

  public Call(int id, final MethodDescriptor md, Message param, final CellScanner cells,
      final Message responseDefaultType, int timeout, int priority, RpcCallback<Call> callback) {
    this.param = param;
    this.md = md;
    this.cells = cells;
    this.startTime = EnvironmentEdgeManager.currentTimeMillis();
    this.responseDefaultType = responseDefaultType;
    this.id = id;
    this.timeout = timeout;
    this.priority = priority;
    this.callback = callback;
    this.span = Trace.currentSpan();
  }

  @Override
  public String toString() {
    return "callId: " + this.id + " methodName: " + this.md.getName() + " param {" +
        (this.param != null ? ProtobufUtil.getShortTextFormat(this.param) : "") + "}";
  }

  /**
   * Indicate when the call is complete and the value or error are available. Notifies by default.
   */
  private void callComplete() {
    if (timeoutTask != null) {
      timeoutTask.cancel();
    }
    callback.run(this);
  }

  /**
   * Call this method inside the timeoutTask to prevent cancel yourself...
   */
  public void setTimeout(IOException error) {
    synchronized (this) {
      if (done) {
        return;
      }
      this.done = true;
      this.error = error;
    }
    callback.run(this);
  }

  /**
   * Set the exception when there is an error. Notify the caller the call is done.
   * @param error exception thrown by the call; either local or remote
   */
  public void setException(IOException error) {
    synchronized (this) {
      if (done) {
        return;
      }
      this.done = true;
      this.error = error;
    }
    callComplete();
  }

  /**
   * Set the return value when there is no error. Notify the caller the call is done.
   * @param response return value of the call.
   * @param cells Can be null
   */
  public void setResponse(Message response, final CellScanner cells) {
    synchronized (this) {
      if (done) {
        return;
      }
      this.done = true;
      this.response = response;
      this.cells = cells;
    }
    callComplete();
  }

  public synchronized boolean isDone() {
    return done;
  }

  public long getStartTime() {
    return this.startTime;
  }
}