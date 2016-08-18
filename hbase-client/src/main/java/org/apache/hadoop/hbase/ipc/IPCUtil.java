/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.ipc;

import com.google.common.base.Preconditions;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.Message;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufOutputStream;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HBaseIOException;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.codec.Codec;
import org.apache.hadoop.hbase.io.ByteBufferOutputStream;
import org.apache.hadoop.hbase.io.HeapSize;
import org.apache.hadoop.hbase.protobuf.generated.RPCProtos.CellBlockMeta;
import org.apache.hadoop.hbase.protobuf.generated.RPCProtos.ExceptionResponse;
import org.apache.hadoop.hbase.protobuf.generated.RPCProtos.RequestHeader;
import org.apache.hadoop.hbase.protobuf.generated.TracingProtos.RPCTInfo;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.htrace.Span;
import org.apache.htrace.Trace;

/**
 * Utility to help ipc'ing.
 */
@InterfaceAudience.Private
class IPCUtil {
  public static final Log LOG = LogFactory.getLog(IPCUtil.class);
  /**
   * How much we think the decompressor will expand the original compressed content.
   */
  private final int cellBlockDecompressionMultiplier;
  private final int cellBlockBuildingInitialBufferSize;
  private final Configuration conf;

  IPCUtil(final Configuration conf) {
    super();
    this.conf = conf;
    this.cellBlockDecompressionMultiplier = conf
        .getInt("hbase.ipc.cellblock.decompression.buffersize.multiplier", 3);
    // Guess that 16k is a good size for rpc buffer. Could go bigger. See the TODO below in
    // #buildCellBlock.
    this.cellBlockBuildingInitialBufferSize = ClassSize
        .align(conf.getInt("hbase.ipc.cellblock.building.initial.buffersize", 16 * 1024));
  }

  /**
   * Thrown if a cellscanner but no codec to encode it with.
   */
  @SuppressWarnings("serial")
  public static class CellScannerButNoCodecException extends HBaseIOException {
  };

  private interface OutputStreamSupplier {

    OutputStream get(int expectedSize);

    int size();
  }

  private boolean buildCellBlock(Codec codec, CompressionCodec compressor, CellScanner cellScanner,
      OutputStreamSupplier supplier) throws IOException {
    if (cellScanner == null) {
      return false;
    }
    if (codec == null) {
      throw new CellScannerButNoCodecException();
    }
    int bufferSize = this.cellBlockBuildingInitialBufferSize;
    if (cellScanner instanceof HeapSize) {
      long longSize = ((HeapSize) cellScanner).heapSize();
      // Just make sure we don't have a size bigger than an int.
      if (longSize > Integer.MAX_VALUE) {
        throw new IOException("Size " + longSize + " > " + Integer.MAX_VALUE);
      }
      bufferSize = ClassSize.align((int) longSize);
    } // TODO: Else, get estimate on size of buffer rather than have the buffer resize.
    // See TestIPCUtil main for experiment where we spin through the Cells getting estimate of
    // total size before creating the buffer. It costs somw small percentage. If we are usually
    // within the estimated buffer size, then the cost is not worth it. If we are often well
    // outside the guesstimated buffer size, the processing can be done in half the time if we
    // go w/ the estimated size rather than let the buffer resize.
    OutputStream os = supplier.get(bufferSize);
    Compressor poolCompressor = null;
    try {
      if (compressor != null) {
        if (compressor instanceof Configurable) {
          ((Configurable) compressor).setConf(this.conf);
        }
        poolCompressor = CodecPool.getCompressor(compressor);
        os = compressor.createOutputStream(os, poolCompressor);
      }
      Codec.Encoder encoder = codec.getEncoder(os);
      int count = 0;
      while (cellScanner.advance()) {
        encoder.write(cellScanner.current());
        count++;
      }
      encoder.flush();
      // If no cells, don't mess around. Just return null (could be a bunch of existence checking
      // gets or something -- stuff that does not return a cell).
      if (count == 0) {
        return false;
      }
    } finally {
      os.close();
      if (poolCompressor != null) {
        CodecPool.returnCompressor(poolCompressor);
      }
    }
    if (LOG.isTraceEnabled() && bufferSize < supplier.size()) {
      LOG.trace("Buffer grew from initial bufferSize=" + bufferSize + " to " + supplier.size()
          + "; up hbase.ipc.cellblock.building.initial.buffersize?");
    }
    return true;
  }

  private static final class ByteBufferOutputStreamSupplier implements OutputStreamSupplier {

    private ByteBufferOutputStream baos;

    @Override
    public OutputStream get(int expectedSize) {
      baos = new ByteBufferOutputStream(expectedSize);
      return baos;
    }

    @Override
    public int size() {
      return baos.size();
    }
  }

  /**
   * Puts CellScanner Cells into a cell block using passed in <code>codec</code> and/or
   * <code>compressor</code>.
   * @param codec
   * @param compressor
   * @Param cellScanner
   * @return Null or byte buffer filled with a cellblock filled with passed-in Cells encoded using
   *         passed in <code>codec</code> and/or <code>compressor</code>; the returned buffer has
   *         been flipped and is ready for reading. Use limit to find total size.
   * @throws IOException
   */
  ByteBuffer buildCellBlock(final Codec codec, final CompressionCodec compressor,
      final CellScanner cellScanner) throws IOException {
    ByteBufferOutputStreamSupplier supplier = new ByteBufferOutputStreamSupplier();
    if (buildCellBlock(codec, compressor, cellScanner, supplier)) {
      return supplier.baos.getByteBuffer();
    } else {
      return null;
    }
  }

  private static final class ByteBufOutputStreamSupplier implements OutputStreamSupplier {

    private final ByteBufAllocator alloc;

    private ByteBuf buf;

    public ByteBufOutputStreamSupplier(ByteBufAllocator alloc) {
      this.alloc = alloc;
    }

    @Override
    public OutputStream get(int expectedSize) {
      buf = alloc.buffer(expectedSize);
      return new ByteBufOutputStream(buf);
    }

    @Override
    public int size() {
      return buf.writerIndex();
    }
  }

  ByteBuf buildCellBlock(Codec codec, CompressionCodec compressor, CellScanner cellScanner,
      ByteBufAllocator alloc) throws IOException {
    ByteBufOutputStreamSupplier supplier = new ByteBufOutputStreamSupplier(alloc);
    if (buildCellBlock(codec, compressor, cellScanner, supplier)) {
      return supplier.buf;
    } else {
      return null;
    }
  }

  /**
   * @param codec
   * @param cellBlock
   * @return CellScanner to work against the content of <code>cellBlock</code>
   * @throws IOException
   */
  CellScanner createCellScanner(final Codec codec, final CompressionCodec compressor,
      final byte[] cellBlock) throws IOException {
    return createCellScanner(codec, compressor, cellBlock, 0, cellBlock.length);
  }

  /**
   * @param codec
   * @param cellBlock
   * @param offset
   * @param length
   * @return CellScanner to work against the content of <code>cellBlock</code>
   * @throws IOException
   */
  CellScanner createCellScanner(final Codec codec, final CompressionCodec compressor,
      final byte[] cellBlock, final int offset, final int length) throws IOException {
    // If compressed, decompress it first before passing it on else we will leak compression
    // resources if the stream is not closed properly after we let it out.
    InputStream is = null;
    if (compressor != null) {
      // GZIPCodec fails w/ NPE if no configuration.
      if (compressor instanceof Configurable) ((Configurable) compressor).setConf(this.conf);
      Decompressor poolDecompressor = CodecPool.getDecompressor(compressor);
      CompressionInputStream cis = compressor
          .createInputStream(new ByteArrayInputStream(cellBlock, offset, length), poolDecompressor);
      ByteBufferOutputStream bbos = null;
      try {
        // TODO: This is ugly. The buffer will be resized on us if we guess wrong.
        // TODO: Reuse buffers.
        bbos = new ByteBufferOutputStream(
            (length - offset) * this.cellBlockDecompressionMultiplier);
        IOUtils.copy(cis, bbos);
        bbos.close();
        ByteBuffer bb = bbos.getByteBuffer();
        is = new ByteArrayInputStream(bb.array(), 0, bb.limit());
      } finally {
        if (is != null) is.close();
        if (bbos != null) bbos.close();

        CodecPool.returnDecompressor(poolDecompressor);
      }
    } else {
      is = new ByteArrayInputStream(cellBlock, offset, length);
    }
    return codec.getDecoder(is);
  }

  static RequestHeader buildRequestHeader(Call call, CellBlockMeta cellBlockMeta) {
    RequestHeader.Builder builder = RequestHeader.newBuilder();
    builder.setCallId(call.id);
    if (Trace.isTracing()) {
      Span s = Trace.currentSpan();
      builder.setTraceInfo(
        RPCTInfo.newBuilder().setParentId(s.getSpanId()).setTraceId(s.getTraceId()));
    }
    builder.setMethodName(call.md.getName());
    builder.setRequestParam(call.param != null);
    if (cellBlockMeta != null) {
      builder.setCellBlockMeta(cellBlockMeta);
    }
    // Only pass priority if there one. Let zero be same as no priority.
    if (call.priority != 0) {
      builder.setPriority(call.priority);
    }
    builder.setTimeout(call.timeout);

    return builder.build();
  }

  /**
   * @param m Message to serialize delimited; i.e. w/ a vint of its size preceeding its
   *          serialization.
   * @return The passed in Message serialized with delimiter. Return null if <code>m</code> is null
   * @throws IOException
   */
  static ByteBuffer getDelimitedMessageAsByteBuffer(final Message m) throws IOException {
    if (m == null) return null;
    int serializedSize = m.getSerializedSize();
    int vintSize = CodedOutputStream.computeRawVarint32Size(serializedSize);
    byte[] buffer = new byte[serializedSize + vintSize];
    // Passing in a byte array saves COS creating a buffer which it does when using streams.
    CodedOutputStream cos = CodedOutputStream.newInstance(buffer);
    // This will write out the vint preamble and the message serialized.
    cos.writeMessageNoTag(m);
    cos.flush();
    cos.checkNoSpaceLeft();
    return ByteBuffer.wrap(buffer);
  }

  /**
   * Write out header, param, and cell block if there is one.
   * @param dos
   * @param header
   * @param param
   * @param cellBlock
   * @return Total number of bytes written.
   * @throws IOException
   */
  static int write(final OutputStream dos, final Message header, final Message param,
      final ByteBuffer cellBlock) throws IOException {
    // Must calculate total size and write that first so other side can read it all in in one
    // swoop. This is dictated by how the server is currently written. Server needs to change
    // if we are to be able to write without the length prefixing.
    int totalSize = IPCUtil.getTotalSizeWhenWrittenDelimited(header, param);
    if (cellBlock != null) {
      totalSize += cellBlock.remaining();
    }
    return write(dos, header, param, cellBlock, totalSize);
  }

  private static int write(final OutputStream dos, final Message header, final Message param,
      final ByteBuffer cellBlock, final int totalSize) throws IOException {
    // I confirmed toBytes does same as DataOutputStream#writeInt.
    dos.write(Bytes.toBytes(totalSize));
    // This allocates a buffer that is the size of the message internally.
    header.writeDelimitedTo(dos);
    if (param != null) param.writeDelimitedTo(dos);
    if (cellBlock != null) dos.write(cellBlock.array(), 0, cellBlock.remaining());
    dos.flush();
    return totalSize;
  }

  /**
   * Read in chunks of 8K (HBASE-7239)
   * @param in
   * @param dest
   * @param offset
   * @param len
   * @throws IOException
   */
  static void readChunked(final DataInput in, byte[] dest, int offset, int len) throws IOException {
    int maxRead = 8192;

    for (; offset < len; offset += maxRead) {
      in.readFully(dest, offset, Math.min(len - offset, maxRead));
    }
  }

  /**
   * @param header
   * @param body
   * @return Size on the wire when the two messages are written with writeDelimitedTo
   */
  static int getTotalSizeWhenWrittenDelimited(Message... messages) {
    int totalSize = 0;
    for (Message m : messages) {
      if (m == null) continue;
      totalSize += m.getSerializedSize();
      totalSize += CodedOutputStream.computeRawVarint32Size(m.getSerializedSize());
    }
    Preconditions.checkArgument(totalSize < Integer.MAX_VALUE);
    return totalSize;
  }

  /**
   * @param e
   * @return True if the exception is a fatal connection exception.
   */
  static boolean isFatalConnectionException(final ExceptionResponse e) {
    return e.getExceptionClassName().equals(FatalConnectionException.class.getName());
  }

  /**
   * @param e
   * @return RemoteException made from passed <code>e</code>
   */
  static RemoteException createRemoteException(final ExceptionResponse e) {
    String innerExceptionClassName = e.getExceptionClassName();
    boolean doNotRetry = e.getDoNotRetry();
    return e.hasHostname() ?
    // If a hostname then add it to the RemoteWithExtrasException
        new RemoteWithExtrasException(innerExceptionClassName, e.getStackTrace(), e.getHostname(),
            e.getPort(), doNotRetry)
        : new RemoteWithExtrasException(innerExceptionClassName, e.getStackTrace(), doNotRetry);
  }

  /**
   * Take an IOException and the address we were trying to connect to and return an IOException with
   * the input exception as the cause. The new exception provides the stack trace of the place where
   * the exception is thrown and some extra diagnostics information. If the exception is
   * ConnectException or SocketTimeoutException, return a new one of the same type; Otherwise return
   * an IOException.
   * @param addr target address
   * @param exception the relevant exception
   * @return an exception to throw
   */
  static IOException wrapException(InetSocketAddress addr, Throwable exception) {
    if (exception instanceof ConnectException) {
      // connection refused; include the host:port in the error
      return (ConnectException) new ConnectException(
          "Call to " + addr + " failed on connection exception: " + exception).initCause(exception);
    } else if (exception instanceof SocketTimeoutException) {
      return (SocketTimeoutException) new SocketTimeoutException(
          "Call to " + addr + " failed because " + exception).initCause(exception);
    } else {
      return (IOException) new IOException(
          "Call to " + addr + " failed on local exception: " + exception).initCause(exception);
    }
  }

  static IOException toIOE(Throwable t) {
    if (t instanceof IOException) {
      return (IOException) t;
    } else {
      return new IOException(t);
    }
  }
}
