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
package org.apache.hadoop.hbase.regionserver.wal;

import static io.netty.channel.ChannelOption.CONNECT_TIMEOUT_MILLIS;
import static org.apache.hadoop.fs.CreateFlag.CREATE;
import static org.apache.hadoop.fs.CreateFlag.OVERWRITE;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_SOCKET_TIMEOUT_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_USE_DN_HOSTNAME;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_USE_DN_HOSTNAME_DEFAULT;
import static org.apache.hadoop.hdfs.protocol.datatransfer.BlockConstructionStage.PIPELINE_SETUP_CREATE;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoop;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.CryptoProtocolVersion;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemLinkResolver;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.ConnectionUtils;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSOutputStream;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.datatransfer.BlockConstructionStage;
import org.apache.hadoop.hdfs.protocol.datatransfer.DataTransferProtoUtil;
import org.apache.hadoop.hdfs.protocol.datatransfer.DataTransferProtocol;
import org.apache.hadoop.hdfs.protocol.datatransfer.Op;
import org.apache.hadoop.hdfs.protocol.datatransfer.PipelineAck;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.BaseHeaderProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.BlockOpResponseProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.CachingStrategyProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.ChecksumProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.ClientOperationHeaderProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.OpWriteBlockProto;
import org.apache.hadoop.hdfs.protocol.proto.DataTransferProtos.Status;
import org.apache.hadoop.hdfs.protocolPB.PBHelper;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.namenode.LeaseExpiredException;
import org.apache.hadoop.io.EnumSetWritable;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.DataChecksum;

import com.google.protobuf.CodedOutputStream;

@InterfaceAudience.Private
class FanOutOneBlockAsyncDFSOutputHelper {

  private static final Log LOG = LogFactory.getLog(FanOutOneBlockAsyncDFSOutputHelper.class);

  private static final Method BEGIN_FILE_LEASE;

  private static final Method END_FILE_LEASE;

  private static final Method CREATE_CHECKSUM;

  public static final ByteBufAllocator ALLOC = PooledByteBufAllocator.DEFAULT;

  public static final long HEART_BEAT_SEQNO = -1L;

  static {
    try {
      BEGIN_FILE_LEASE =
          DFSClient.class.getDeclaredMethod("beginFileLease", long.class, DFSOutputStream.class);
      BEGIN_FILE_LEASE.setAccessible(true);
      END_FILE_LEASE = DFSClient.class.getDeclaredMethod("endFileLease", long.class);
      END_FILE_LEASE.setAccessible(true);
      CREATE_CHECKSUM = DFSClient.Conf.class.getDeclaredMethod("createChecksum");
      CREATE_CHECKSUM.setAccessible(true);
    } catch (NoSuchMethodException | SecurityException e) {
      throw new RuntimeException(e);
    }
  }

  public static void beginFileLease(DFSClient client, long inodeId) {
    try {
      BEGIN_FILE_LEASE.invoke(client, inodeId, null);
    } catch (IllegalAccessException | InvocationTargetException e) {
      throw new RuntimeException(e);
    }
  }

  public static void endFileLease(DFSClient client, long inodeId) {
    try {
      END_FILE_LEASE.invoke(client, inodeId);
    } catch (IllegalAccessException | InvocationTargetException e) {
      throw new RuntimeException(e);
    }
  }

  public static DataChecksum createChecksum(DFSClient client) {
    try {
      return (DataChecksum) CREATE_CHECKSUM.invoke(client.getConf());
    } catch (IllegalAccessException | InvocationTargetException e) {
      throw new RuntimeException(e);
    }
  }

  private static void processWriteBlockResponse(Channel channel, final DatanodeInfo dnInfo,
      final Promise<Channel> promise, final int timeoutMs) {
    channel.pipeline().addLast(new IdleStateHandler(timeoutMs, 0, 0, TimeUnit.MILLISECONDS),
      new ProtobufVarint32FrameDecoder(),
      new ProtobufDecoder(BlockOpResponseProto.getDefaultInstance()),
      new SimpleChannelInboundHandler<BlockOpResponseProto>() {

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, BlockOpResponseProto resp)
            throws Exception {
          Status pipelineStatus = resp.getStatus();
          if (PipelineAck.isRestartOOBStatus(pipelineStatus)) {
            throw new IOException("datanode " + dnInfo + " is restarting");
          }
          String logInfo = "ack with firstBadLink as " + resp.getFirstBadLink();
          DataTransferProtoUtil.checkBlockOpStatus(resp, logInfo);
          // success
          ChannelPipeline p = ctx.pipeline();
          p.removeLast();
          p.removeLast();
          p.removeLast();
          p.removeLast();
          ctx.channel().config().setAutoRead(false);
          promise.trySuccess(ctx.channel());
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
          promise.tryFailure(new IOException("connection to " + dnInfo + " is closed"));
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
          if (evt instanceof IdleStateEvent
              && ((IdleStateEvent) evt).state() == IdleState.READER_IDLE) {
            promise
                .tryFailure(new IOException("Timeout(" + timeoutMs + "ms) waiting for response"));
          } else {
            super.userEventTriggered(ctx, evt);
          }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
          promise.tryFailure(cause);
        }
      });
  }

  private static void requestWriteBlock(Channel channel, StorageType storageType,
      OpWriteBlockProto.Builder writeBlockProtoBuilder) throws IOException {
    // TODO: SASL negotiation
    OpWriteBlockProto proto =
        writeBlockProtoBuilder.setStorageType(PBHelper.convertStorageType(storageType)).build();
    int protoLen = proto.getSerializedSize();
    ByteBuf buffer =
        channel.alloc().buffer(3 + CodedOutputStream.computeRawVarint32Size(protoLen) + protoLen);
    buffer.writeShort(DataTransferProtocol.DATA_TRANSFER_VERSION);
    buffer.writeByte(Op.WRITE_BLOCK.code);
    proto.writeDelimitedTo(new ByteBufOutputStream(buffer));
    channel.writeAndFlush(buffer);
  }

  public static List<Future<Channel>> connectToDataNodes(Configuration conf, String clientName,
      LocatedBlock locatedBlock, long maxBytesRcvd, long latestGS, BlockConstructionStage stage,
      DataChecksum summer, EventLoop eventLoop) {
    StorageType[] storageTypes = locatedBlock.getStorageTypes();
    DatanodeInfo[] datanodeInfos = locatedBlock.getLocations();
    boolean connectToDnViaHostname =
        conf.getBoolean(DFS_CLIENT_USE_DN_HOSTNAME, DFS_CLIENT_USE_DN_HOSTNAME_DEFAULT);
    final int timeoutMs =
        conf.getInt(DFS_CLIENT_SOCKET_TIMEOUT_KEY, HdfsServerConstants.READ_TIMEOUT);
    ExtendedBlock blockCopy = new ExtendedBlock(locatedBlock.getBlock());
    blockCopy.setNumBytes(locatedBlock.getBlockSize());
    ClientOperationHeaderProto header =
        ClientOperationHeaderProto
            .newBuilder()
            .setBaseHeader(
              BaseHeaderProto.newBuilder().setBlock(PBHelper.convert(blockCopy))
                  .setToken(PBHelper.convert(locatedBlock.getBlockToken())))
            .setClientName(clientName).build();
    ChecksumProto checksumProto = DataTransferProtoUtil.toProto(summer);
    final OpWriteBlockProto.Builder writeBlockProtoBuilder =
        OpWriteBlockProto.newBuilder().setHeader(header)
            .setStage(OpWriteBlockProto.BlockConstructionStage.valueOf(stage.name()))
            .setPipelineSize(1).setMinBytesRcvd(locatedBlock.getBlock().getNumBytes())
            .setMaxBytesRcvd(maxBytesRcvd).setLatestGenerationStamp(latestGS)
            .setRequestedChecksum(checksumProto)
            .setCachingStrategy(CachingStrategyProto.newBuilder().setDropBehind(true).build())
            .setAllowLazyPersist(false).setPinning(false);
    List<Future<Channel>> futureList = new ArrayList<>(datanodeInfos.length);
    for (int i = 0; i < datanodeInfos.length; i++) {
      final DatanodeInfo dnInfo = datanodeInfos[i];
      final StorageType storageType = storageTypes[i];
      final Promise<Channel> promise = eventLoop.newPromise();
      futureList.add(promise);
      String dnAddr = dnInfo.getXferAddr(connectToDnViaHostname);
      new Bootstrap().group(eventLoop).channel(NioSocketChannel.class)
          .option(CONNECT_TIMEOUT_MILLIS, timeoutMs).handler(new ChannelInitializer<Channel>() {

            @Override
            protected void initChannel(Channel ch) throws Exception {
              processWriteBlockResponse(ch, dnInfo, promise, timeoutMs);
            }
          }).connect(NetUtils.createSocketAddr(dnAddr)).addListener(new ChannelFutureListener() {

            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
              if (future.isSuccess()) {
                requestWriteBlock(future.channel(), storageType, writeBlockProtoBuilder);
              } else {
                promise.tryFailure(future.cause());
              }
            }
          });
    }
    return futureList;
  }

  private static FanOutOneBlockAsyncDFSOutput createOutput(DistributedFileSystem dfs, String src,
      boolean overwrite, short replication, long blockSize, EventLoop eventLoop) throws IOException {
    Configuration conf = dfs.getConf();
    FSUtils fsUtils = FSUtils.getInstance(dfs, conf);
    DFSClient client = dfs.getClient();
    String clientName = client.getClientName();
    ClientProtocol namenode = client.getNamenode();
    HdfsFileStatus stat =
        namenode.create(src, FsPermission.getFileDefault().applyUMask(FsPermission.getUMask(conf)),
          clientName, new EnumSetWritable<CreateFlag>(overwrite ? EnumSet.of(CREATE, OVERWRITE)
              : EnumSet.of(CREATE)), true, replication, blockSize, CryptoProtocolVersion
              .supported());
    beginFileLease(client, stat.getFileId());
    boolean succ = false;
    LocatedBlock locatedBlock = null;
    List<Channel> datanodeList = new ArrayList<>();
    try {
      DataChecksum summer = createChecksum(client);
      locatedBlock =
          namenode.addBlock(src, client.getClientName(), null, null, stat.getFileId(), null);
      for (Future<Channel> future : connectToDataNodes(conf, clientName, locatedBlock, 0L, 0L,
        PIPELINE_SETUP_CREATE, summer, eventLoop)) {
        datanodeList.add(future.syncUninterruptibly().getNow());
      }
      succ = true;
      return new FanOutOneBlockAsyncDFSOutput(conf, fsUtils, dfs, client, namenode, clientName,
          src, stat.getFileId(), locatedBlock, eventLoop, datanodeList, summer, ALLOC);
    } finally {
      if (!succ) {
        for (Channel c : datanodeList) {
          c.close();
        }
        endFileLease(client, stat.getFileId());
        fsUtils.recoverFileLease(dfs, new Path(src), conf, null);
      }
    }
  }

  public static FanOutOneBlockAsyncDFSOutput createOutput(final DistributedFileSystem dfs, Path f,
      final boolean overwrite, final short replication, final long blockSize,
      final EventLoop eventLoop) throws IOException {
    return new FileSystemLinkResolver<FanOutOneBlockAsyncDFSOutput>() {

      @Override
      public FanOutOneBlockAsyncDFSOutput doCall(Path p) throws IOException,
          UnresolvedLinkException {
        return createOutput(dfs, p.toUri().getPath(), overwrite, replication, blockSize, eventLoop);
      }

      @Override
      public FanOutOneBlockAsyncDFSOutput next(FileSystem fs, Path p) throws IOException {
        throw new UnsupportedOperationException();
      }
    }.resolve(dfs, f);
  }

  public static LocatedBlock updateBlockForPipeline(ClientProtocol namenode, String src,
      ExtendedBlock block, String clientName) {
    for (int retry = 0;; retry++) {
      try {
        return namenode.updateBlockForPipeline(block, clientName);
      } catch (LeaseExpiredException e) {
        LOG.warn("lease for file " + src + " is expired, give up", e);
        return null;
      } catch (Exception e) {
        if (e.getMessage().contains("does not exist")) {
          LOG.warn("lease for file " + src + " should be expired, give up", e);
          return null;
        }
        LOG.warn("update block for file " + src + " failed, retry = " + retry);
        sleepIgnoreInterrupt(retry);
      }
    }
  }

  public static void completeFile(DFSClient client, ClientProtocol namenode, String src,
      String clientName, ExtendedBlock block, long fileId) {
    for (int retry = 0;; retry++) {
      try {
        if (namenode.complete(src, clientName, block, fileId)) {
          endFileLease(client, fileId);
          return;
        } else {
          LOG.warn("complete file " + src + " not finished, retry = " + retry);
        }
      } catch (LeaseExpiredException e) {
        LOG.warn("lease for file " + src + " is expired, give up", e);
        return;
      } catch (Exception e) {
        LOG.warn("complete file " + src + " failed, retry = " + retry, e);
      }
      sleepIgnoreInterrupt(retry);
    }
  }

  public static void sleepIgnoreInterrupt(int retry) {
    try {
      Thread.sleep(ConnectionUtils.getPauseTime(100, retry));
    } catch (InterruptedException e) {
    }
  }
}
