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

package org.apache.tajo.storage.columnar;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Longs;
import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;

public class UnsafeBuf {
  long address;
  int length;
  Object reference;

  public UnsafeBuf(ByteBuffer bb) {
    DirectBuffer df = (DirectBuffer) bb;
    address = df.address();
    length = bb.capacity();
    reference = bb;
  }

  public UnsafeBuf(long address, int length) {
    this.address = address;
    this.length = length;
  }

  public long address() {
    return address;
  }

  public long getLong(int offset) {
    return UnsafeUtil.unsafe.getLong(address + offset);
  }

  public void putLong(int offset, long value) {
    UnsafeUtil.unsafe.putLong(address + offset, value);
  }

  public UnsafeBuf copyOf() {
    ByteBuffer bytebuf = ByteBuffer.allocateDirect(16);
    UnsafeUtil.unsafe.copyMemory(null, this.address, null, ((DirectBuffer)bytebuf).address(), length);
    return new UnsafeBuf(bytebuf);
  }

  public void copyTo(UnsafeBuf dest) {
    Preconditions.checkArgument(dest.length >= length, "Target buffer size is less than that of the source buffer");
    UnsafeUtil.unsafe.copyMemory(null, this.address, null, dest.address, length);
  }

  /**
   * This method is forked from Guava.
   */
  public boolean equals(Object obj) {
    if (obj instanceof UnsafeBuf) {
      UnsafeBuf another = (UnsafeBuf) obj;

      if (length != another.length) {
        return false;
      }

      long thisAddr = address;
      long anotherAddr = another.address;

      int minLength = Math.min(length, another.length);
      int minWords = minLength / Longs.BYTES;

    /*
       * Compare 8 bytes at a time. Benchmarking shows comparing 8 bytes at a
       * time is no slower than comparing 4 bytes at a time even on 32-bit.
       * On the other hand, it is substantially faster on 64-bit.
       */
      for (int i = 0; i < minWords * Longs.BYTES; i += Longs.BYTES) {
        long lw = UnsafeUtil.unsafe.getLong(thisAddr);
        long rw = UnsafeUtil.unsafe.getLong(anotherAddr);
        thisAddr += SizeOf.SIZE_OF_LONG;
        anotherAddr += SizeOf.SIZE_OF_LONG;

        long diff = lw ^ rw;

        if (diff != 0) {
          return false;
        }
      }

      // The epilogue to cover the last (minLength % 8) elements.
      for (int i = minWords * Longs.BYTES; i < minLength; i++) {
        byte r = (byte) (UnsafeUtil.unsafe.getByte(thisAddr++) ^ UnsafeUtil.unsafe.getByte(anotherAddr++));
        if (r != 0) {
          return false;
        }
      }

      return true;
    } else {
      return false;
    }
  }
}
