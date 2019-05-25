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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.codec.Codec;
import org.apache.hadoop.hbase.exceptions.ConnectionClosedException;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xiaomi.infra.thirdparty.com.google.protobuf.Message;
import com.xiaomi.infra.thirdparty.com.google.protobuf.Message.Builder;
import com.xiaomi.infra.thirdparty.com.google.protobuf.TextFormat;
import com.xiaomi.infra.thirdparty.io.netty.buffer.ByteBuf;
import com.xiaomi.infra.thirdparty.io.netty.buffer.ByteBufInputStream;
import com.xiaomi.infra.thirdparty.io.netty.buffer.ByteBufOutputStream;
import com.xiaomi.infra.thirdparty.io.netty.channel.ChannelDuplexHandler;
import com.xiaomi.infra.thirdparty.io.netty.channel.ChannelHandlerContext;
import com.xiaomi.infra.thirdparty.io.netty.channel.ChannelPromise;
import com.xiaomi.infra.thirdparty.io.netty.handler.timeout.IdleStateEvent;
import com.xiaomi.infra.thirdparty.io.netty.util.Timeout;
import com.xiaomi.infra.thirdparty.io.netty.util.TimerTask;
import com.xiaomi.infra.thirdparty.io.netty.util.concurrent.PromiseCombiner;

import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.CellBlockMeta;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.ExceptionResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.RequestHeader;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.ResponseHeader;

/**
 * The netty rpc handler.
 * @since 2.0.0
 */
@InterfaceAudience.Private
class NettyRpcDuplexHandler extends ChannelDuplexHandler {

  private static final Logger LOG = LoggerFactory.getLogger(NettyRpcDuplexHandler.class);

  private final NettyRpcConnection conn;

  private final CellBlockBuilder cellBlockBuilder;

  private final Codec codec;

  private final CompressionCodec compressor;

  private final Map<Integer, Call> id2Call = new HashMap<>();

  private Timeout pingTimeout;

  public NettyRpcDuplexHandler(NettyRpcConnection conn, CellBlockBuilder cellBlockBuilder,
      Codec codec, CompressionCodec compressor) {
    this.conn = conn;
    this.cellBlockBuilder = cellBlockBuilder;
    this.codec = codec;
    this.compressor = compressor;
  }

  private void writeRequest(ChannelHandlerContext ctx, Call call, ChannelPromise promise)
      throws IOException {
    id2Call.put(call.id, call);
    ByteBuf cellBlock = cellBlockBuilder.buildCellBlock(codec, compressor, call.cells, ctx.alloc());
    CellBlockMeta cellBlockMeta;
    if (cellBlock != null) {
      CellBlockMeta.Builder cellBlockMetaBuilder = CellBlockMeta.newBuilder();
      cellBlockMetaBuilder.setLength(cellBlock.writerIndex());
      cellBlockMeta = cellBlockMetaBuilder.build();
    } else {
      cellBlockMeta = null;
    }
    RequestHeader requestHeader = IPCUtil.buildRequestHeader(call, cellBlockMeta);
    int sizeWithoutCellBlock = IPCUtil.getTotalSizeWhenWrittenDelimited(requestHeader, call.param);
    int totalSize = cellBlock != null ? sizeWithoutCellBlock + cellBlock.writerIndex()
        : sizeWithoutCellBlock;
    ByteBuf buf = ctx.alloc().buffer(sizeWithoutCellBlock + 4);
    buf.writeInt(totalSize);
    try (ByteBufOutputStream bbos = new ByteBufOutputStream(buf)) {
      requestHeader.writeDelimitedTo(bbos);
      if (call.param != null) {
        call.param.writeDelimitedTo(bbos);
      }
      if (cellBlock != null) {
        ChannelPromise withoutCellBlockPromise = ctx.newPromise();
        ctx.write(buf, withoutCellBlockPromise);
        ChannelPromise cellBlockPromise = ctx.newPromise();
        ctx.write(cellBlock, cellBlockPromise);
        PromiseCombiner combiner = new PromiseCombiner();
        combiner.addAll(withoutCellBlockPromise, cellBlockPromise);
        combiner.finish(promise);
      } else {
        ctx.write(buf, promise);
      }
    }
  }

  @Override
  public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise)
      throws Exception {
    if (msg instanceof Call) {
      writeRequest(ctx, (Call) msg, promise);
    } else {
      ctx.write(msg, promise);
    }
  }

  private void readResponse(ChannelHandlerContext ctx, ByteBuf buf) throws IOException {
    int totalSize = buf.readInt();
    ByteBufInputStream in = new ByteBufInputStream(buf);
    ResponseHeader responseHeader = ResponseHeader.parseDelimitedFrom(in);
    int id = responseHeader.getCallId();
    if (LOG.isTraceEnabled()) {
      LOG.trace("got response header " + TextFormat.shortDebugString(responseHeader)
          + ", totalSize: " + totalSize + " bytes");
    }
    if (id == RpcClient.PING_CALL_ID) {
      // ping response, ignore
      LOG.debug("Receive ping response from {}", conn.remoteId());
      return;
    }
    RemoteException remoteExc;
    if (responseHeader.hasException()) {
      ExceptionResponse exceptionResponse = responseHeader.getException();
      remoteExc = IPCUtil.createRemoteException(exceptionResponse);
      if (IPCUtil.isFatalConnectionException(exceptionResponse)) {
        // Here we will cleanup all calls so do not need to fall back, just return.
        exceptionCaught(ctx, remoteExc);
        return;
      }
    } else {
      remoteExc = null;
    }
    Call call = id2Call.remove(id);
    if (call == null) {
      // So we got a response for which we have no corresponding 'call' here on the client-side.
      // We probably timed out waiting, cleaned up all references, and now the server decides
      // to return a response. There is nothing we can do w/ the response at this stage. Clean
      // out the wire of the response so its out of the way and we can get other responses on
      // this connection.
      int readSoFar = IPCUtil.getTotalSizeWhenWrittenDelimited(responseHeader);
      int whatIsLeftToRead = totalSize - readSoFar;
      if (LOG.isDebugEnabled()) {
        LOG.debug("Unknown callId: " + id + ", skipping over this response of " + whatIsLeftToRead
            + " bytes");
      }
      return;
    }
    if (remoteExc != null) {
      call.setException(remoteExc);
      return;
    }
    Message value;
    if (call.responseDefaultType != null) {
      Builder builder = call.responseDefaultType.newBuilderForType();
      builder.mergeDelimitedFrom(in);
      value = builder.build();
    } else {
      value = null;
    }
    CellScanner cellBlockScanner;
    if (responseHeader.hasCellBlockMeta()) {
      int size = responseHeader.getCellBlockMeta().getLength();
      // Maybe we could read directly from the ByteBuf.
      // The problem here is that we do not know when to release it.
      byte[] cellBlock = new byte[size];
      buf.readBytes(cellBlock);
      cellBlockScanner = cellBlockBuilder.createCellScanner(this.codec, this.compressor, cellBlock);
    } else {
      cellBlockScanner = null;
    }
    call.setResponse(value, cellBlockScanner);
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    // Ping is used to detect if the remote machine is down while the connection is still alive, so
    // if we get something back then we can make sure the remote machine is alive.
    if (pingTimeout != null) {
      pingTimeout.cancel();
      pingTimeout = null;
    }
    if (msg instanceof ByteBuf) {
      ByteBuf buf = (ByteBuf) msg;
      try {
        readResponse(ctx, buf);
      } finally {
        buf.release();
      }
    } else {
      super.channelRead(ctx, msg);
    }
  }

  private void cleanupCalls(ChannelHandlerContext ctx, IOException error) {
    for (Call call : id2Call.values()) {
      call.setException(error);
    }
    id2Call.clear();
  }

  @Override
  public void channelInactive(ChannelHandlerContext ctx) throws Exception {
    if (!id2Call.isEmpty()) {
      cleanupCalls(ctx, new ConnectionClosedException("Connection closed"));
    }
    conn.shutdown();
    ctx.fireChannelInactive();
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
    if (!id2Call.isEmpty()) {
      cleanupCalls(ctx, IPCUtil.toIOE(cause));
    }
    conn.shutdown();
  }

  @Override
  public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
    if (evt instanceof IdleStateEvent) {
      IdleStateEvent idleEvt = (IdleStateEvent) evt;
      switch (idleEvt.state()) {
        case READER_IDLE: {
          if (pingTimeout == null) {
            LOG.debug("send ping to {}", conn.remoteId());
            // write a ping
            ctx.writeAndFlush(ctx.alloc().buffer(4).writeInt(RpcClient.PING_CALL_ID));
            pingTimeout = conn.timeoutTimer.newTimeout(new TimerTask() {

              @Override
              public void run(Timeout timeout) throws Exception {
                LOG.warn("Haven't got ping response from {} in time, shutdown connection",
                  conn.remoteId());
                conn.shutdown();
              }
            }, conn.getPingTimeout(), TimeUnit.MILLISECONDS);
          }
          break;
        }
        case WRITER_IDLE:
          if (id2Call.isEmpty()) {
            LOG.debug("shutdown connection to {} because idle for a long time", conn.remoteId());
            // It may happen that there are still some pending calls in the event loop queue and
            // they will get a closed channel exception. But this is not a big deal as it rarely
            // rarely happens and the upper layer could retry immediately.
            conn.shutdown();
          }
          break;
        default:
          LOG.warn("Unrecognized idle state " + idleEvt.state());
          break;
      }
    } else if (evt instanceof CallEvent) {
      // just remove the call for now until we add other call event other than timeout and cancel.
      id2Call.remove(((CallEvent) evt).call.id);
    } else {
      ctx.fireUserEventTriggered(evt);
    }
  }
}
