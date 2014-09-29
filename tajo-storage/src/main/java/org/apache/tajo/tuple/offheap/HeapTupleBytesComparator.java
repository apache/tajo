/*
 * Lisensed to the Apache Software Foundation (ASF) under one
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

package org.apache.tajo.tuple.offheap;

import com.google.common.primitives.Longs;
import com.google.common.primitives.UnsignedLongs;
import org.apache.tajo.util.SizeOf;
import org.apache.tajo.util.UnsafeUtil;
import sun.misc.Unsafe;

import java.nio.ByteOrder;

/**
 * It directly access UTF bytes in UnSafeTuple without any copy. It is used by compiled TupleComparator.
 */
public class HeapTupleBytesComparator {
  private static final Unsafe UNSAFE = UnsafeUtil.unsafe;

  static final boolean littleEndian =
      ByteOrder.nativeOrder().equals(ByteOrder.LITTLE_ENDIAN);

  public static int compare(HeapTuple t1, int fieldIdx1, HeapTuple t2, int fieldIdx2) {
    long offset1 = t1.checkNullAndGetOffset(fieldIdx1);
    long offset2 = t2.checkNullAndGetOffset(fieldIdx2);

    int lstrLen = UNSAFE.getInt(t1.data, Unsafe.ARRAY_BYTE_BASE_OFFSET + offset1);
    int rstrLen = UNSAFE.getInt(t2.data, Unsafe.ARRAY_BYTE_BASE_OFFSET + offset2);

    offset1 += SizeOf.SIZE_OF_INT;
    offset2 += SizeOf.SIZE_OF_INT;

    int minLength = Math.min(lstrLen, rstrLen);
    int minWords = minLength / Longs.BYTES;

    /*
     * Compare 8 bytes at a time. Benchmarking shows comparing 8 bytes at a
     * time is no slower than comparing 4 bytes at a time even on 32-bit.
     * On the other hand, it is substantially faster on 64-bit.
     */
    for (int i = 0; i < minWords * Longs.BYTES; i += Longs.BYTES) {
      long lw = UNSAFE.getLong(t1.data, Unsafe.ARRAY_BYTE_BASE_OFFSET + offset1);
      long rw = UNSAFE.getLong(t2.data, Unsafe.ARRAY_BYTE_BASE_OFFSET + offset2);
      long diff = lw ^ rw;

      if (diff != 0) {
        if (!littleEndian) {
          return UnsignedLongs.compare(lw, rw);
        }

        // Use binary search
        int n = 0;
        int y;
        int x = (int) diff;
        if (x == 0) {
          x = (int) (diff >>> 32);
          n = 32;
        }

        y = x << 16;
        if (y == 0) {
          n += 16;
        } else {
          x = y;
        }

        y = x << 8;
        if (y == 0) {
          n += 8;
        }
        return (int) (((lw >>> n) & 0xFFL) - ((rw >>> n) & 0xFFL));
      }

      offset1 += SizeOf.SIZE_OF_LONG;
      offset2 += SizeOf.SIZE_OF_LONG;
    }

    // The epilogue to cover the last (minLength % 8) elements.
    for (int i = minWords * Longs.BYTES; i < minLength; i++) {
      int result = UNSAFE.getByte(t1.data, Unsafe.ARRAY_BYTE_BASE_OFFSET + offset1) -
          UNSAFE.getByte(t2.data, Unsafe.ARRAY_BYTE_BASE_OFFSET + offset2);
      offset1++;
      offset2++;
      if (result != 0) {
        return result;
      }
    }
    return lstrLen - rstrLen;
  }
}
