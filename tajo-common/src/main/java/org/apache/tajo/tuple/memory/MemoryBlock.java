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

import io.netty.buffer.ByteBuf;
import org.apache.tajo.util.Deallocatable;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ScatteringByteChannel;

/**
 * This interface provides a memory spec for off-heap or heap
 */
public interface MemoryBlock extends Deallocatable {

  /**
   * Current memory address of this buffer
   *
   * @return the memory address
   */
  long address();

  /**
   * @return if true, the buffer has a reference to the low-level memory address
   */
  boolean hasAddress();

  /**
   * @return the number of bytes this buffer can contain.
   */
  int capacity();

  /**
   * reset the buffer
   */
  void clear();

  /**
   * @return true if this buffer has an remaining bytes.
   */
  boolean isReadable();

  /**
   * @return the number of readable bytes in this buffer
   */
  int readableBytes();

  /**
   * @return the current reader position of this buffer
   */
  int readerPosition();

  /**
   * Sets the reader position of this buffer
   */
  void readerPosition(int pos);

  /**
   * @return true if this buffer is not full filled
   */
  boolean isWritable();

  /**
   * @return the number of writable bytes in this buffer
   */
  int writableBytes();

  /**
   * @return the current writer position of this buffer
   */
  int writerPosition();

  /**
   * Sets the writer position of this buffer
   */
  void writerPosition(int pos);

  /**
   * Ensure that this buffer has enough remaining space to add the capacity.
   * Creates and copies to a new buffer if necessary
   *
   * @param size Size to add
   */
  void ensureSize(int size);

  /**
   * Transfers the content of the channel to this buffer
   * @param in the input channel
   * @return the actual number of bytes read in channel
   */
  int writeBytes(ScatteringByteChannel in) throws IOException;

  /**
   * Transfers the content of this buffer to the byte array
   * @param dst the destination byte array
   * @param dstIndex the first index of the destination
   * @param length   the number of bytes to transfer
   * @return the actual number of bytes transfers to the destination byte array
   */
  int getBytes(byte[] dst, int dstIndex, int length) throws IOException;

  /**
   * This method does not modify {@code readerPosition} or {@code writerPosition} of this buffer.
   * @return a 32-bit integer in this buffer
   */
  int getInt(int index);

  /**
   * Transfers the content of this buffer to the channel
   * @param out the output channel
   * @param length the maximum number of bytes to transfer
   * @return the actual number of bytes transfers to the channel
   */
  int writeTo(GatheringByteChannel out, int length) throws IOException;

  int writeTo(GatheringByteChannel out) throws IOException;

  /**
   * Transfers the content of this buffer to the stream
   * @param out the output stream
   * @param length the maximum number of bytes to transfer
   * @return the actual number of bytes transfers to the stream
   */
  int writeTo(OutputStream out, int length) throws IOException;

  int writeTo(OutputStream out) throws IOException;

  /**
   * @return a MemoryBlock which shares the whole region of this.
   */
  MemoryBlock duplicate();

  /**
   * @return a internal buffer
   */
  ByteBuf getBuffer();
}
