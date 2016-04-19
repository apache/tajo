/***
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

package org.apache.tajo.tuple.memory;

import com.google.common.primitives.Longs;
import com.google.common.primitives.UnsignedBytes;
import com.google.common.primitives.UnsignedLongs;
import org.apache.tajo.util.SizeOf;
import org.apache.tajo.util.UnsafeUtil;
import sun.misc.Unsafe;

import java.nio.ByteOrder;

/**
 * It directly access UTF bytes in UnSafeTuple without any copy. It is used by compiled TupleComparator.
 */
public class UnSafeTupleBytesComparator {
  private static final Unsafe UNSAFE = UnsafeUtil.unsafe;
  private static final int UNSIGNED_MASK = 0xFF;
  private static final boolean littleEndian = ByteOrder.nativeOrder().equals(ByteOrder.LITTLE_ENDIAN);

  // This code is borrowed from Guava's UnsignedBytes:::compare()
  public static int compare(long ptr1, long ptr2) {
    int lstrLen = UNSAFE.getInt(ptr1);
    int rstrLen = UNSAFE.getInt(ptr2);

    ptr1 += SizeOf.SIZE_OF_INT;
    ptr2 += SizeOf.SIZE_OF_INT;

    int minLength = Math.min(lstrLen, rstrLen);
    int minWords = minLength / Longs.BYTES;

    /*
     * Compare 8 bytes at a time. Benchmarking shows comparing 8 bytes at a
     * time is no slower than comparing 4 bytes at a time even on 32-bit.
     * On the other hand, it is substantially faster on 64-bit.
    */
    for (int i = 0; i < minWords * Longs.BYTES; i += Longs.BYTES) {
      long lw = UNSAFE.getLong(ptr1);
      long rw = UNSAFE.getLong(ptr2);

      if (lw != rw) {
        if (!littleEndian) {
          return UnsignedLongs.compare(lw, rw);
        }

        /*
         * We want to compare only the first index where left[index] != right[index].
         * This corresponds to the least significant nonzero byte in lw ^ rw, since lw
         * and rw are little-endian.  Long.numberOfTrailingZeros(diff) tells us the least
         * significant nonzero bit, and zeroing out the first three bits of L.nTZ gives us the
         * shift to get that least significant nonzero byte.
        */
        int n = Long.numberOfTrailingZeros(lw ^ rw) & ~0x7;
        return (int) (((lw >>> n) & UNSIGNED_MASK) - ((rw >>> n) & UNSIGNED_MASK));
      }

      ptr1 += SizeOf.SIZE_OF_LONG;
      ptr2 += SizeOf.SIZE_OF_LONG;
    }

    // The epilogue to cover the last (minLength % 8) elements.
    for (int i = minWords * Longs.BYTES; i < minLength; i++) {
      int result = UnsignedBytes.compare(UNSAFE.getByte(ptr1++), UNSAFE.getByte(ptr2++));
      if (result != 0) {
        return result;
      }
    }
    return lstrLen - rstrLen;
  }
}
