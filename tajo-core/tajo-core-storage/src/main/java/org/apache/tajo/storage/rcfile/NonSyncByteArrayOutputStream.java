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

/**
 * A thread-not-safe version of ByteArrayOutputStream, which removes all
 * synchronized modifiers.
 */
public class NonSyncByteArrayOutputStream extends ByteArrayOutputStream {
  public NonSyncByteArrayOutputStream(int size) {
    super(size);
  }

  public NonSyncByteArrayOutputStream() {
    super(64 * 1024);
  }

  public byte[] getData() {
    return buf;
  }

  public int getLength() {
    return count;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void reset() {
    count = 0;
  }

  public void write(DataInput in, int length) throws IOException {
    enLargeBuffer(length);
    in.readFully(buf, count, length);
    count += length;
  }

  private byte[] vLongBytes = new byte[9];

  public int writeVLongToByteArray(byte[] bytes, int offset, long l) {
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

    bytes[offset++] = (byte) len;
    len = (len < -120) ? -(len + 120) : -(len + 112);

    for (int idx = len; idx != 0; idx--) {
      int shiftbits = (idx - 1) * 8;
      bytes[offset++] = (byte) ((l & (0xFFL << shiftbits)) >> shiftbits);
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
    enLargeBuffer(1);
    buf[count] = (byte) b;
    count += 1;
  }

  private int enLargeBuffer(int increment) {
    int temp = count + increment;
    int newLen = temp;
    if (temp > buf.length) {
      if ((buf.length << 1) > temp) {
        newLen = buf.length << 1;
      }
      byte newbuf[] = new byte[newLen];
      System.arraycopy(buf, 0, newbuf, 0, count);
      buf = newbuf;
    }
    return newLen;
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
    enLargeBuffer(len);
    System.arraycopy(b, off, buf, count, len);
    count += len;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void writeTo(OutputStream out) throws IOException {
    out.write(buf, 0, count);
  }
}
