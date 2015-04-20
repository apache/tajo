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

package org.apache.tajo.storage;

import org.apache.hadoop.fs.ByteBufferReadable;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;

/**
 * FSDataInputChannel is a NIO channel implementation of direct read ability to read from HDFS
 */
public final class FSDataInputChannel extends InputChannel implements SeekableChannel {

  private ReadableByteChannel channel;
  private FSDataInputStream inputStream;
  private boolean isDirectRead;

  public FSDataInputChannel(FSDataInputStream inputStream) {
    if (inputStream.getWrappedStream() instanceof ByteBufferReadable) {
      this.isDirectRead = true;
    } else {
      /* LocalFileSystem, S3 does not support ByteBufferReadable */
      this.channel = Channels.newChannel(inputStream);
    }
    this.inputStream = inputStream;
  }

  @Override
  public int read(ByteBuffer dst) throws IOException {
    if (isDirectRead) {
      return inputStream.read(dst);
    } else {
      return channel.read(dst);
    }
  }

  @Override
  public void seek(long offset) throws IOException {
    inputStream.seek(offset);
  }

  @Override
  protected void implCloseChannel() throws IOException {
    IOUtils.cleanup(null, channel, inputStream);
  }
}
