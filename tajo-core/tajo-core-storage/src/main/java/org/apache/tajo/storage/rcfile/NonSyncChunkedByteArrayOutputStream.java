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

package org.apache.tajo.storage.rcfile;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.IOException;
import java.io.OutputStream;
import java.util.LinkedList;

/**
 * A thread-not-safe version of ByteArrayOutputStream, which removes all
 * synchronized modifiers.
 */
public class NonSyncChunkedByteArrayOutputStream extends ByteArrayOutputStream {
  private LinkedList<byte[]> buffer = new LinkedList<byte[]>();
  private int chunkSize;
  private int currentChunk = 0;
  private int currentPos = 0;


  public NonSyncChunkedByteArrayOutputStream(int chunkSize) {
    super(0);
    this.chunkSize = chunkSize;
    buffer.add(new byte[chunkSize]);
  }

  public NonSyncChunkedByteArrayOutputStream() {
    this(256 * 1024);
  }

  public int getLength() {
    return count;
  }

  @Override
  public int size() {
     return count;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void reset() {
    count = 0;
    currentChunk = 0;
    currentPos = 0;
    buffer.clear();
    buffer.add(new byte[chunkSize]);
    buf = new byte[0];

  }

  private static byte[] vLongBytes = new byte[9];
  public static int writeVLongToByteArray(byte[] bytes, int offset, long l) {
    if (l >= -112 && l <= 127) {
      bytes[offset] = (byte) l;
      return 1;
    }

    int len = -112;
    if (l < 0) {
      l ^= -1L; // take one's complement'
      len = -120;
    }

    long tmp = l;
    while (tmp != 0) {
      tmp = tmp >> 8;
      len--;
    }

    bytes[offset] = (byte) len;

    len = (len < -120) ? -(len + 120) : -(len + 112);

    for (int idx = len; idx != 0; idx--) {
      int shiftbits = (idx - 1) * 8;
      long mask = 0xFFL << shiftbits;
      bytes[offset+1-(idx - len)] = (byte) ((l & mask) >> shiftbits);
    }
    return 1 + len;
  }

  public int writeVLong(long l) {
    int len = writeVLongToByteArray(vLongBytes, 0, l);
    write(vLongBytes, 0, len);
    return len;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void write(int b) {
    ensureBuffer(1);
    buffer.get(currentChunk)[currentPos] = (byte) b;
    count += 1;
    currentPos += 1;
  }

  private int ensureBuffer(int bytes) {
    if(currentPos  >= chunkSize) {
      currentPos = 0;
      buffer.add(new byte[chunkSize]);
      currentChunk++;
    }

    int remainingChunk = buffer.size() -  (currentChunk + 1);
    int availableSize =  (remainingChunk * chunkSize) + (chunkSize - currentPos);

    while (availableSize <= bytes ){
      buffer.add(new byte[chunkSize]);
      availableSize += chunkSize;
    }
    return availableSize;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void write(byte b[]) {
    write(b, 0, b.length);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void write(byte b[], int off, int len) {
    if ((off < 0) || (off > b.length) || (len < 0) || ((off + len) > b.length)
        || ((off + len) < 0)) {
      throw new IndexOutOfBoundsException();
    } else if (len == 0) {
      return;
    }
    ensureBuffer(len);

    int remainingBytes = len;
    while (remainingBytes > 0 ){
      byte[] chunk = buffer.get(currentChunk);

      if(chunkSize - currentPos > remainingBytes){
        System.arraycopy(b, off, chunk, currentPos, remainingBytes);

        currentPos += remainingBytes;
        count += remainingBytes;
        remainingBytes -= remainingBytes;
        break;
      } else {
        int remainLen = chunkSize - currentPos;
        System.arraycopy(b, off, chunk, currentPos, remainLen);
        off += remainLen;
        remainingBytes -= remainLen;
        count += remainLen;
        currentPos = 0;
        currentChunk++;
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void writeTo(OutputStream out) throws IOException {
    byte[] buf;
    int remainingBytes = count;
    while ((buf = buffer.poll()) != null) {
      int size = Math.min(buf.length, remainingBytes);
      out.write(buf, 0, size);
      remainingBytes -= size;
    }
  }

  public void write(DataInput in, int len) throws IOException {
    if (len == 0) return;

    ensureBuffer(len);

    int remainingBytes = len;
    while (remainingBytes > 0) {
      byte[] chunk =  buffer.get(currentChunk);
      if(chunkSize - currentPos >= remainingBytes){
        in.readFully(chunk, currentPos, remainingBytes);
        currentPos += remainingBytes;
        count += remainingBytes;
        remainingBytes -= remainingBytes;
      } else {
        int remainLen = chunk.length - currentPos;
        in.readFully(chunk, currentPos, remainLen);
        remainingBytes -= remainLen;
        count += remainLen;
        currentPos = 0;
        currentChunk++;
      }
    }
  }

  public byte[] toArray(){
    byte[] b = new byte[count];
    int remainingBytes = count;

    for (byte[] bytes : buffer){
      int size = Math.min(bytes.length, remainingBytes);
      System.arraycopy(bytes, 0, b, count - remainingBytes, size);
      remainingBytes -= size;
    }
    return b;
  }
}
