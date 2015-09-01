/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.tuple.memory;

import com.google.common.base.Preconditions;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.storage.BufferPool;
import org.apache.tajo.util.FileUtil;
import org.apache.tajo.util.UnsafeUtil;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ScatteringByteChannel;

public class ResizableMemoryBlock implements MemoryBlock {
  private static final Log LOG = LogFactory.getLog(ResizableMemoryBlock.class);

  protected ByteBuf buffer;
  protected ResizableLimitSpec limitSpec;

  public ResizableMemoryBlock(ByteBuf buffer, ResizableLimitSpec limitSpec) {
    this.buffer = buffer.order(ByteOrder.LITTLE_ENDIAN);
    this.limitSpec = limitSpec;
  }

  public ResizableMemoryBlock(ByteBuf buffer) {
    this(buffer, new ResizableLimitSpec(buffer.capacity()));
  }

  public ResizableMemoryBlock(ByteBuffer buffer) {
    this.buffer = Unpooled.wrappedBuffer(buffer).order(ByteOrder.LITTLE_ENDIAN);
    this.limitSpec = new ResizableLimitSpec(buffer.capacity());
  }

  public ResizableMemoryBlock(ResizableLimitSpec limitSpec, boolean isDirect) {
    if (isDirect) {
      this.buffer = BufferPool.directBuffer((int) limitSpec.initialSize(), (int) limitSpec.limit());
    } else {
      this.buffer = BufferPool.heapBuffer((int) limitSpec.initialSize(), (int) limitSpec.limit());
    }
    this.limitSpec = limitSpec;
  }

  @Override
  public long address() {
    return buffer.memoryAddress();
  }

  @Override
  public boolean hasAddress() {
    return buffer.hasMemoryAddress();
  }

  @Override
  public int capacity() {
    return buffer.capacity();
  }

  @Override
  public void clear() {
    buffer.clear();
  }

  @Override
  public boolean isReadable() {
    return buffer.isReadable();
  }

  @Override
  public int readableBytes() {
    return buffer.readableBytes();
  }

  @Override
  public int readerPosition() {
    return buffer.readerIndex();
  }

  @Override
  public void readerPosition(int pos) {
    buffer.readerIndex(pos);
  }

  @Override
  public boolean isWritable() {
    return buffer.isWritable();
  }

  @Override
  public int writableBytes() {
    return buffer.writableBytes();
  }

  @Override
  public void writerPosition(int pos) {
    buffer.writerIndex(pos);
  }

  @Override
  public int writerPosition() {
    return buffer.writerIndex();
  }


  @Override
  public void ensureSize(int size) {
    if (!buffer.isWritable(size)) {
      if (!limitSpec.canIncrease(buffer.capacity())) {
        throw new RuntimeException("Cannot increase RowBlock anymore.");
      }

      int newBlockSize = limitSpec.increasedSize(buffer.capacity());
      resize(newBlockSize);
      LOG.info("Increase DirectRowBlock to " + FileUtil.humanReadableByteCount(newBlockSize, false));
    }
  }

  private void resize(int newSize) {
    Preconditions.checkArgument(newSize > 0, "Size must be greater than 0 bytes");

    if (newSize > limitSpec.limit()) {
      throw new RuntimeException("Resize cannot exceed the capacity limit");
    }

    if (newSize < buffer.capacity()) {
      LOG.warn("The capacity reduction is ignored.");
    }

    int newBlockSize = UnsafeUtil.alignedSize(newSize);
    buffer = BufferPool.ensureWritable(buffer, newBlockSize);
  }

  @Override
  public void release() {
    buffer.release();
  }

  @Override
  public MemoryBlock duplicate() {
    return new ResizableMemoryBlock(buffer.duplicate().readerIndex(0), limitSpec);
  }

  @Override
  public ByteBuf getBuffer() {
    return buffer;
  }

  @Override
  public int writeBytes(ScatteringByteChannel channel) throws IOException {

    if (buffer.readableBytes() > 0) {
      this.buffer.markReaderIndex();
      this.buffer.discardReadBytes();  // compact the buffer
    } else {
      buffer.clear();
    }

    int readBytes = 0;
    while (buffer.writableBytes() > 0) {
      int localReadBytes = buffer.writeBytes(channel, buffer.writableBytes());
      if (localReadBytes < 0) {
        break;
      }
      readBytes += localReadBytes;
    }

    return readBytes;
  }

  @Override
  public int getBytes(byte[] bytes, int dstIndex, int length) throws IOException {
    int readableBytes = buffer.readableBytes();
    buffer.readBytes(bytes, dstIndex, length);
    return readableBytes - buffer.readableBytes();
  }

  @Override
  public int getInt(int index) {
    return buffer.getInt(index);
  }

  @Override
  public int writeTo(GatheringByteChannel channel, int length) throws IOException {
    return buffer.readBytes(channel, length);
  }

  @Override
  public int writeTo(GatheringByteChannel channel) throws IOException {
    return buffer.readBytes(channel, buffer.readableBytes());
  }

  @Override
  public int writeTo(OutputStream outputStream, int length) throws IOException {
    buffer.readBytes(outputStream, length);
    return length;
  }

  @Override
  public int writeTo(OutputStream outputStream) throws IOException {
    int readableBytes = buffer.readableBytes();
    buffer.readBytes(outputStream, readableBytes);
    return readableBytes - buffer.readableBytes();
  }

  @Override
  public String toString() {
    return "memory=" + FileUtil.humanReadableByteCount(capacity(), false) + "," + limitSpec;
  }
}
