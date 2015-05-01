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

package org.apache.tajo.plan.function.stream;

import org.apache.hadoop.fs.ByteBufferReadable;
import org.apache.tajo.util.FileUtil;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.ScatteringByteChannel;
import java.nio.channels.spi.AbstractInterruptibleChannel;

public class ByteBufInputChannel extends AbstractInterruptibleChannel implements ScatteringByteChannel {

  ByteBufferReadable byteBufferReadable;
  ReadableByteChannel channel;
  InputStream inputStream;

  public ByteBufInputChannel(InputStream inputStream) {
    if (inputStream instanceof ByteBufferReadable) {
      this.byteBufferReadable = (ByteBufferReadable) inputStream;
    } else {
      this.channel = Channels.newChannel(inputStream);
    }

    this.inputStream = inputStream;
  }

  @Override
  public long read(ByteBuffer[] dsts, int offset, int length) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long read(ByteBuffer[] dsts) {
    return read(dsts, 0, dsts.length);
  }

  @Override
  public int read(ByteBuffer dst) throws IOException {
    if (byteBufferReadable != null) {
      return byteBufferReadable.read(dst);
    } else {
      return channel.read(dst);
    }
  }

  @Override
  protected void implCloseChannel() throws IOException {
    FileUtil.cleanup(null, channel, inputStream);
  }
}
