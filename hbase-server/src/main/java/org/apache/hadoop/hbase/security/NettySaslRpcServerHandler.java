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

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.concurrent.Promise;

/**
 * Implement the sasl handshake logic.
 */
@InterfaceAudience.Private
public class NettySaslRpcServerHandler extends SimpleChannelInboundHandler<ByteBuf> {

  private static final Log LOG = LogFactory.getLog(NettySaslRpcServerHandler.class);

  private final Promise<Void> saslPromise;

  private final HBaseSaslRpcServer saslRpcServer;

  public NettySaslRpcServerHandler(Promise<Void> saslPromise, HBaseSaslRpcServer saslRpcServer) {
    this.saslPromise = saslPromise;
    this.saslRpcServer = saslRpcServer;
  }

  private void writeChallenge(ChannelHandlerContext ctx, byte[] challenge) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Will send token of size " + challenge.length + " from saslServer.");
    }
    ctx.writeAndFlush(
      ctx.alloc().buffer(4 + challenge.length).writeInt(challenge.length).writeBytes(challenge));
  }

  @Override
  protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) throws Exception {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Have read input token of size " + msg.readableBytes() +
          " for processing by saslServer.evaluateResponse()");
    }
    byte[] response = new byte[msg.readableBytes()];
    msg.readBytes(response);
    byte[] challenge = saslRpcServer.evaluateResponse(response);
    if (challenge != null) {
      writeChallenge(ctx, challenge);
    }
    if (saslRpcServer.isComplete()) {
      saslPromise.trySuccess(null);
    }
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    saslRpcServer.dispose();
    saslPromise.tryFailure(new IOException("Connection closed"));
    ctx.fireChannelInactive();
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
    saslRpcServer.dispose();
    saslPromise.tryFailure(cause);
  }

}
