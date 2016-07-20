/*
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

package org.apache.tajo.storage.http;

import io.netty.buffer.ByteBuf;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.tajo.exception.TajoRuntimeException;
import org.apache.tajo.exception.UnsupportedException;
import org.apache.tajo.storage.BufferPool;
import org.apache.tajo.storage.ByteBufInputChannel;
import org.apache.tajo.storage.compress.CodecPool;
import org.apache.tajo.storage.fragment.AbstractFileFragment;
import org.apache.tajo.storage.text.ByteBufLineReader;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.concurrent.atomic.AtomicInteger;

public class ExampleHttpJsonLineReader implements Closeable {

  private final static Log LOG = LogFactory.getLog(ExampleHttpJsonLineReader.class);

  private Configuration conf;

  private HttpURLConnection connection;
  private InputStream is;

  private CompressionCodec codec;
  private Decompressor decompressor;

  private long startOffset, endOffset, pos;
  private boolean eof = true;
  private ByteBufLineReader lineReader;
  private AtomicInteger lineReadBytes = new AtomicInteger();
  private ExampleHttpFileFragment fragment;
  private final int bufferSize;

  public ExampleHttpJsonLineReader(Configuration conf,
                                   AbstractFileFragment fragment,
                                   int bufferSize) {
    this.conf = conf;
    this.fragment = (ExampleHttpFileFragment) fragment;
    this.bufferSize = bufferSize;

    CompressionCodecFactory factory = new CompressionCodecFactory(conf);
    codec = factory.getCodec(fragment.getPath());
    if (this.codec instanceof SplittableCompressionCodec) {
      // bzip2 does not support multi-thread model
      throw new TajoRuntimeException(new UnsupportedException(codec.getDefaultExtension()));
    }
  }

  public void init() throws IOException {
    if (connection != null) {
      throw new IOException(this.getClass() + " is already initialized");
    }

    pos = startOffset = fragment.getStartKey();
    endOffset = fragment.getEndKey();

    URL url = new URL(fragment.getUri().toASCIIString());
    connection = (HttpURLConnection) url.openConnection();

    is = connection.getInputStream();

    ByteBuf buf;
    if (codec != null) {
      decompressor = CodecPool.getDecompressor(codec);
      is = codec.createInputStream(is, decompressor);

      buf = BufferPool.directBuffer(bufferSize);

    } else {
      buf = BufferPool.directBuffer((int) Math.min(bufferSize, fragment.getLength()));

    }

    lineReader = new ByteBufLineReader(new ByteBufInputChannel(is), buf);

    eof = false;
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

    return buf;
  }

  public boolean isCompressed() {
    return codec != null;
  }

  public long getPos() {
    return pos;
  }

  public long getReadBytes() {
    return pos - startOffset;
  }

  public boolean isEof() {
    return eof;
  }

  @Override
  public void close() throws IOException {
    try {
      IOUtils.cleanup(LOG, lineReader);

      if (connection != null) {
        connection.disconnect();
      }

      is = null;
      lineReader = null;

    } finally {
      CodecPool.returnDecompressor(decompressor);
      decompressor = null;
    }
  }
}
