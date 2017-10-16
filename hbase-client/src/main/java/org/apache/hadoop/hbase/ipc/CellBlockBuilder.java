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

import org.apache.hadoop.hbase.shaded.io.netty.buffer.ByteBuf;
import org.apache.hadoop.hbase.shaded.io.netty.buffer.ByteBufAllocator;
import org.apache.hadoop.hbase.shaded.io.netty.buffer.ByteBufOutputStream;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.codec.Codec;
import org.apache.hadoop.hbase.io.ByteBufferOutputStream;
import org.apache.hadoop.hbase.io.HeapSize;
import org.apache.hadoop.hbase.util.ClassSize;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;

/**
 * Helper class for building cell block.
 */
@InterfaceAudience.Private
public class CellBlockBuilder {

  // LOG is being used in TestCellBlockBuilder
  static final Log LOG = LogFactory.getLog(CellBlockBuilder.class);

  private final Configuration conf;

  /**
   * How much we think the decompressor will expand the original compressed content.
   */
  private final int cellBlockDecompressionMultiplier;

  private final int cellBlockBuildingInitialBufferSize;

  public CellBlockBuilder(Configuration conf) {
    this.conf = conf;
    this.cellBlockDecompressionMultiplier =
        conf.getInt("hbase.ipc.cellblock.decompression.buffersize.multiplier", 3);

    // Guess that 16k is a good size for rpc buffer. Could go bigger. See the TODO below in
    // #buildCellBlock.
    this.cellBlockBuildingInitialBufferSize =
        ClassSize.align(conf.getInt("hbase.ipc.cellblock.building.initial.buffersize", 16 * 1024));
  }

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
      LOG.trace("Buffer grew from initial bufferSize=" + bufferSize + " to " + supplier.size() +
          "; up hbase.ipc.cellblock.building.initial.buffersize?");
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
        bbos =
            new ByteBufferOutputStream((length - offset) * this.cellBlockDecompressionMultiplier);
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
}
