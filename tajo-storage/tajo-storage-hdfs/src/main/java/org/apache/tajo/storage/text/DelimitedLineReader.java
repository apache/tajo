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

package org.apache.tajo.storage.text;

import io.netty.buffer.ByteBuf;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.tajo.common.exception.NotImplementedException;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.exception.UnsupportedException;
import org.apache.tajo.storage.*;
import org.apache.tajo.storage.compress.CodecPool;
import org.apache.tajo.storage.fragment.FileFragment;
import org.apache.tajo.unit.StorageUnit;

import java.io.*;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;

public class DelimitedLineReader implements Closeable {
  private static final Log LOG = LogFactory.getLog(DelimitedLineReader.class);

  private FileSystem fs;
  private FSDataInputStream fis;
  private InputStream is; //decompressd stream
  private CompressionCodecFactory factory;
  private CompressionCodec codec;
  private Decompressor decompressor;

  private long startOffset, end, pos;
  private boolean eof = true;
  private ByteBufLineReader lineReader;
  private AtomicInteger lineReadBytes = new AtomicInteger();
  private FileFragment fragment;
  private Configuration conf;
  private int bufferSize;

  public DelimitedLineReader(Configuration conf, final FileFragment fragment) throws IOException {
    this(conf, fragment, 128 * StorageUnit.KB);
  }

  public DelimitedLineReader(Configuration conf, final FileFragment fragment, int bufferSize) throws IOException {
    this.fragment = fragment;
    this.conf = conf;
    this.factory = new CompressionCodecFactory(conf);
    this.codec = factory.getCodec(fragment.getPath());
    this.bufferSize = bufferSize;
    if (this.codec instanceof SplittableCompressionCodec) {
      // bzip2 does not support multi-thread model
      throw new NotImplementedException(this.getClass() + " does not support " + this.codec.getDefaultExtension());
    }
  }

  public void init() throws IOException {
    if (is != null) {
      throw new IOException(this.getClass() + " was already initialized.");
    }

    if (fs == null) {
      fs = FileScanner.getFileSystem((TajoConf) conf, fragment.getPath());
    }

    pos = startOffset = fragment.getStartKey();
    end = startOffset + fragment.getLength();

    if (codec != null) {
      fis = fs.open(fragment.getPath());

      decompressor = CodecPool.getDecompressor(codec);
      is = new DataInputStream(codec.createInputStream(fis, decompressor));

      ByteBuf buf = BufferPool.directBuffer(bufferSize);
      lineReader = new ByteBufLineReader(new ByteBufInputChannel(is), buf);
    } else {
      if (fs instanceof LocalFileSystem) {
        File file;
        try {
          if (fragment.getPath().toUri().getScheme() != null) {
            file = new File(fragment.getPath().toUri());
          } else {
            file = new File(fragment.getPath().toString());
          }
        } catch (IllegalArgumentException iae) {
          throw new IOException(iae);
        }
        FileInputStream inputStream = new FileInputStream(file);
        FileChannel channel = inputStream.getChannel();
        channel.position(startOffset);
        is = inputStream;
        lineReader = new ByteBufLineReader(new LocalFileInputChannel(channel),
            BufferPool.directBuffer((int) Math.min(bufferSize, end)));
      } else {
        fis = fs.open(fragment.getPath());
        fis.seek(startOffset);
        is = fis;
        lineReader = new ByteBufLineReader(new FSDataInputChannel(fis),
            BufferPool.directBuffer((int) Math.min(bufferSize, end)));
      }
    }
    eof = false;
  }

  public void seek(long offset) throws IOException {
    if (isCompressed()) throw new UnsupportedException();

    lineReader.seek(offset);
    pos = offset;
    eof = false;
  }

  public long getCompressedPosition() throws IOException {
    long retVal;
    if (isCompressed()) {
      retVal = fis.getPos();
    } else {
      retVal = pos;
    }
    return retVal;
  }

  public long getUnCompressedPosition() throws IOException {
    return pos;
  }

  public long getReadBytes() {
    return pos - startOffset;
  }

  public boolean isReadable() {
    return !eof;
  }

  public ByteBuf readLine() throws IOException {
    if (eof) {
      return null;
    }

    ByteBuf buf = lineReader.readLineBuf(lineReadBytes);
    pos += lineReadBytes.get();
    if (buf == null) {
      eof = true;
    }

    if (!isCompressed() && getCompressedPosition() > end) {
      eof = true;
    }
    return buf;
  }

  public boolean isCompressed() {
    return codec != null;
  }

  @Override
  public void close() throws IOException {
    try {
      IOUtils.cleanup(LOG, lineReader, is, fis);
      fs = null;
      is = null;
      fis = null;
      lineReader = null;
    } finally {
      if (decompressor != null) {
        CodecPool.returnDecompressor(decompressor);
        decompressor = null;
      }
    }
  }
}
