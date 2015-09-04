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
import io.netty.util.CharsetUtil;
import org.apache.tajo.storage.BufferPool;
import org.apache.tajo.storage.InputChannel;
import org.apache.tajo.storage.SeekableChannel;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

public class ByteBufLineReader implements Closeable {
  public static final int DEFAULT_BUFFER = 64 * 1024;

  private int bufferSize;
  private long readBytes;
  private int startIndex;
  private boolean eof = false;
  private ByteBuf buffer;
  private final InputChannel channel;
  private final SeekableChannel seekableChannel;
  private final AtomicInteger lineReadBytes = new AtomicInteger();
  private final LineSplitProcessor processor = new LineSplitProcessor();

  public ByteBufLineReader(InputChannel channel) {
    this(channel, BufferPool.directBuffer(DEFAULT_BUFFER));
  }

  public ByteBufLineReader(InputChannel channel, ByteBuf buf) {
    this.readBytes = 0;
    this.channel = channel;
    this.buffer = buf;
    this.bufferSize = buf.capacity();
    if (channel instanceof SeekableChannel) {
      seekableChannel = (SeekableChannel) channel;
    } else {
      seekableChannel = null;
    }
  }

  public long readBytes() {
    return readBytes - buffer.readableBytes();
  }

  @Override
  public void close() throws IOException {
    if (this.buffer.refCnt() > 0) {
      this.buffer.release();
    }
    this.channel.close();
  }

  public void seek(long offset) throws IOException {
    if(seekableChannel != null) {
      seekableChannel.seek(offset);
      this.readBytes = 0;
      this.startIndex = 0;
      this.eof = false;
      this.buffer.clear();
      this.processor.reset();
    } else {
      throw new IllegalArgumentException("Channel is not an instance of SeekableChannel");
    }
  }

  public String readLine() throws IOException {
    ByteBuf buf = readLineBuf(lineReadBytes);
    if (buf != null) {
      return buf.toString(CharsetUtil.UTF_8);
    }
    return null;
  }

  private void fillBuffer() throws IOException {

    int tailBytes = 0;
    if (this.readBytes > 0) {
      //startIndex = 0, readIndex = tailBytes length, writable = (buffer capacity - tailBytes)
      this.buffer.markReaderIndex();
      this.buffer.discardReadBytes();  // compact the buffer
      tailBytes = this.buffer.writerIndex();
      if (!this.buffer.isWritable()) {
        // a line bytes is large than the buffer
        this.buffer = BufferPool.ensureWritable(buffer, bufferSize * 2);
        this.bufferSize = buffer.capacity();
      }
      this.startIndex = 0;
    }

    boolean release = true;
    try {
      int readBytes = tailBytes;
      for (; ; ) {
        int localReadBytes = buffer.writeBytes(channel, this.bufferSize - readBytes);
        if (localReadBytes < 0) {
          if (buffer.isWritable()) {
            //if read bytes is less than the buffer capacity,  there is no more bytes in the channel
            eof = true;
          }
          break;
        }
        readBytes += localReadBytes;
        if (readBytes == bufferSize) {
          break;
        }
      }
      this.readBytes += (readBytes - tailBytes);
      release = false;

      this.buffer.readerIndex(this.buffer.readerIndex() + tailBytes); //skip past buffer (tail)
    } finally {
      if (release) {
        buffer.release();
      }
    }
  }

  /**
   * Read a line terminated by one of CR, LF, or CRLF.
   */
  public ByteBuf readLineBuf(AtomicInteger reads) throws IOException {
    int readBytes = 0; // newline + text line bytes
    int newlineLength = 0; //length of terminating newline
    int readable;

    this.startIndex = buffer.readerIndex();

    loop:
    while (true) {
      readable = buffer.readableBytes();
      if (readable <= 0) {
        buffer.readerIndex(this.startIndex);
        fillBuffer(); //compact and fill buffer

        //if buffer.writerIndex() is zero, there is no bytes in buffer
        if (!buffer.isReadable() && buffer.writerIndex() == 0) {
          reads.set(0);
          return null;
        } else {
          //skip first newLine
          if (processor.isPrevCharCR() && buffer.getByte(buffer.readerIndex()) == LineSplitProcessor.LF) {
            buffer.skipBytes(1);
            if(eof && !buffer.isReadable()) {
              reads.set(1);
              return null;
            }

            newlineLength++;
            readBytes++;
            startIndex = buffer.readerIndex();
          }
        }
        readable = buffer.readableBytes();
      }

      int endIndex = buffer.forEachByte(buffer.readerIndex(), readable, processor);
      if (endIndex < 0) {
        //does not appeared terminating newline
        buffer.readerIndex(buffer.writerIndex()); // set to end buffer
        if(eof){
          readBytes += (buffer.readerIndex() - startIndex);
          break loop;
        }
      } else {
        buffer.readerIndex(endIndex + 1);
        readBytes += (buffer.readerIndex() - startIndex); //past newline + text line

        //appeared terminating CRLF
        if (processor.isPrevCharCR() && buffer.isReadable()
            && buffer.getByte(buffer.readerIndex()) == LineSplitProcessor.LF) {
          buffer.skipBytes(1);
          readBytes++;
          newlineLength += 2;
        } else {
          newlineLength += 1;
        }
        break loop;
      }
    }
    reads.set(readBytes);
    return buffer.slice(startIndex, readBytes - newlineLength);
  }
}
