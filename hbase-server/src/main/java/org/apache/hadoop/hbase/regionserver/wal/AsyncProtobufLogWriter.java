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

import static org.apache.hadoop.hbase.regionserver.wal.ProtobufLogReader.DEFAULT_WAL_TRAILER_WARN_SIZE;
import static org.apache.hadoop.hbase.regionserver.wal.ProtobufLogReader.WAL_TRAILER_WARN_SIZE;
import io.netty.channel.EventLoop;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.channels.CompletionHandler;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.codec.Codec;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos.WALHeader;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos.WALTrailer;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.wal.DefaultWALProvider;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import com.google.common.base.Throwables;
import com.google.common.primitives.Ints;

/**
 * AsyncWriter for protobuf-based WAL.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class AsyncProtobufLogWriter extends WriterBase implements DefaultWALProvider.AsyncWriter {

  private static final Log LOG = LogFactory.getLog(AsyncProtobufLogWriter.class);

  private static final class ExposedByteArrayOutputStream extends ByteArrayOutputStream {

    public byte[] getBuf() {
      return buf;
    }
  }

  private static final class BlockingCompletionHandler implements CompletionHandler<Long, Void> {

    private long size;

    private Throwable error;

    private boolean finished;

    @Override
    public void completed(Long result, Void attachment) {
      synchronized (this) {
        size = result.longValue();
        finished = true;
        notifyAll();
      }
    }

    @Override
    public void failed(Throwable exc, Void attachment) {
      synchronized (this) {
        error = exc;
        finished = true;
        notifyAll();
      }
    }

    public long get() throws IOException {
      synchronized (this) {
        while (!finished) {
          try {
            wait();
          } catch (InterruptedException e) {
            throw new InterruptedIOException();
          }
        }
        if (error != null) {
          Throwables.propagateIfPossible(error, IOException.class);
        }
        return size;
      }
    }
  }

  private final EventLoop eventLoop;

  private FanOutOneBlockAsyncDFSOutput output;

  private ExposedByteArrayOutputStream buf;

  private Codec.Encoder cellEncoder;

  private WALCellCodec.ByteStringCompressor compressor;

  private WALTrailer trailer;
  // maximum size of the wal Trailer in bytes. If a user writes/reads a trailer with size larger
  // than this size, it is written/read respectively, with a WARN message in the log.
  private int trailerWarnSize;

  private AtomicLong length = new AtomicLong();

  public AsyncProtobufLogWriter(EventLoop eventLoop) {
    this.eventLoop = eventLoop;
  }

  private WALCellCodec getCodec(Configuration conf, CompressionContext compressionContext)
      throws IOException {
    return WALCellCodec.create(conf, null, compressionContext);
  }

  private WALHeader buildWALHeader(Configuration conf, WALHeader.Builder builder)
      throws IOException {
    if (!builder.hasWriterClsName()) {
      builder.setWriterClsName(ProtobufLogWriter.class.getSimpleName());
    }
    if (!builder.hasCellCodecClsName()) {
      builder.setCellCodecClsName(WALCellCodec.getWALCellCodecClass(conf));
    }
    return builder.build();
  }

  private void initAfterHeader(boolean doCompress) throws IOException {
    WALCellCodec codec = getCodec(conf, this.compressionContext);
    this.cellEncoder = codec.getEncoder(this.buf);
    if (doCompress) {
      this.compressor = codec.getByteStringCompressor();
    }
  }

  @Override
  public void init(FileSystem fs, Path path, Configuration conf, boolean overwritable)
      throws IOException {
    super.init(fs, path, conf, overwritable);
    assert this.output == null;
    assert fs instanceof DistributedFileSystem;
    boolean doCompress = initializeCompressionContext(conf, path);
    this.trailerWarnSize = conf.getInt(WAL_TRAILER_WARN_SIZE, DEFAULT_WAL_TRAILER_WARN_SIZE);
    short replication =
        (short) conf.getInt("hbase.regionserver.hlog.replication",
          FSUtils.getDefaultReplication(fs, path));
    long blockSize =
        conf.getLong("hbase.regionserver.hlog.blocksize", FSUtils.getDefaultBlockSize(fs, path));
    output =
        FanOutOneBlockAsyncDFSOutputHelper.createOutput((DistributedFileSystem) fs, path,
          overwritable, replication, blockSize, eventLoop);
    boolean doTagCompress =
        doCompress && conf.getBoolean(CompressionContext.ENABLE_WAL_TAGS_COMPRESSION, true);
    buf = new ExposedByteArrayOutputStream();
    buildWALHeader(conf,
      WALHeader.newBuilder().setHasCompression(doCompress).setHasTagCompression(doTagCompress))
        .writeDelimitedTo(buf);
    final BlockingCompletionHandler handler = new BlockingCompletionHandler();
    eventLoop.execute(new Runnable() {

      @Override
      public void run() {
        output.write(ProtobufLogReader.PB_WAL_MAGIC);
        output.write(buf.getBuf(), 0, buf.size());
        output.flush(null, handler, false);
      }
    });
    length.set(handler.get());
    initAfterHeader(doCompress);

    // instantiate trailer to default value.
    trailer = WALTrailer.newBuilder().build();
    if (LOG.isTraceEnabled()) {
      LOG.trace("Initialized async protobuf WAL=" + path + ", compression=" + doCompress);
    }
  }

  @Override
  public <A> void sync(CompletionHandler<Long, A> handler, A attachment) {
    output.flush(attachment, handler, false);
  }

  @Override
  public void append(Entry entry) {
    buf.reset();
    entry.setCompressionContext(compressionContext);
    try {
      entry.getKey().getBuilder(compressor).setFollowingKvCount(entry.getEdit().size()).build()
          .writeDelimitedTo(buf);
    } catch (IOException e) {
      throw new AssertionError("should not happen", e);
    }
    length.addAndGet(buf.size());
    output.write(buf.getBuf(), 0, buf.size());
    try {
      for (Cell cell : entry.getEdit().getCells()) {
        buf.reset();
        cellEncoder.write(cell);
        length.addAndGet(buf.size());
        output.write(buf.getBuf(), 0, buf.size());
      }
    } catch (IOException e) {
      throw new AssertionError("should not happen", e);
    }
  }

  @Override
  public long getLength() {
    return length.get();
  }

  private void writeWALTrailer() throws IOException {
    int trailerSize = 0;
    if (this.trailer == null) {
      // use default trailer.
      LOG.warn("WALTrailer is null. Continuing with default.");
      this.trailer = WALTrailer.newBuilder().build();
      trailerSize = this.trailer.getSerializedSize();
    } else if ((trailerSize = this.trailer.getSerializedSize()) > this.trailerWarnSize) {
      // continue writing after warning the user.
      LOG.warn("Please investigate WALTrailer usage. Trailer size > maximum size : " + trailerSize
          + " > " + this.trailerWarnSize);
    }
    buf.reset();
    trailer.writeDelimitedTo(buf);
    output.write(buf.getBuf(), 0, buf.size());
    output.write(Ints.toByteArray(trailerSize));
    output.write(ProtobufLogReader.PB_WAL_COMPLETE_MAGIC);
    BlockingCompletionHandler handler = new BlockingCompletionHandler();
    output.flush(null, handler, false);
    length.set(handler.get());
  }

  @Override
  public synchronized void close() throws IOException {
    if (output == null) {
      return;
    }
    try {
      writeWALTrailer();
      output.close();
    } catch (IOException e) {
      LOG.warn("normal close failed, try recover", e);
      output.recoverAndClose(null);
    }
  }
}
