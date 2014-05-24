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

package org.apache.tajo.storage.map;

import com.google.common.primitives.Longs;
import org.apache.tajo.storage.vector.SizeOf;
import org.apache.tajo.storage.vector.UnsafeUtil;
import org.apache.tajo.storage.vector.VecRowBlock;
import sun.misc.Unsafe;

/**
 * Example: WHERE l_shipdate <= '1980-04-01'
 */
public class SelStrLEFixedStrColVal {
  static Unsafe unsafe = UnsafeUtil.unsafe;

  public static int sel(int vecNum, int [] sel, VecRowBlock rowBlock, int lhsIdx, byte [] value, long nullFlags, long selId) {
    int selected = 0;

    long lstrAddr = rowBlock.getValueVecPtr(lhsIdx);

    outest:
    for (int rowIdx = 0; rowIdx < vecNum; rowIdx++) {
      boolean found = true;

      int minLength = Math.min(rowBlock.maxLengths[lhsIdx], value.length);
      int minWords = minLength / Longs.BYTES;

      long rhsAddrOffset = Unsafe.ARRAY_BYTE_BASE_OFFSET;
      /*
         * Compare 8 bytes at a time. Benchmarking shows comparing 8 bytes at a
         * time is no slower than comparing 4 bytes at a time even on 32-bit.
         * On the other hand, it is substantially faster on 4-bit.
         */
      for (int i = 0; i < minWords * Longs.BYTES; i += Longs.BYTES) {
        long lw = unsafe.getLong(lstrAddr);
        long rw = unsafe.getLong(value, rhsAddrOffset);
        lstrAddr += SizeOf.SIZE_OF_LONG;
        rhsAddrOffset += SizeOf.SIZE_OF_LONG;

        long diff = lw ^ rw;

        if (diff != 0) {
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

          sel[selected] = rowIdx;
          selected += (int) (((lw >>> n) & 0xFFL) - ((rw >>> n) & 0xFFL)) <= 0 ? 1 : 0;
          continue outest;
        }
      }

      // The epilogue to cover the last (minLength % 8) elements.
      for (int i = minWords * Longs.BYTES; i < minLength; i++) {
        byte l = unsafe.getByte(lstrAddr++);
        byte w = unsafe.getByte(value, rhsAddrOffset++);
        byte r = (byte) (l - w);
//        byte r = (byte) (unsafe.getByte(lstrAddr++) - unsafe.getByte(value, rhsAddrOffset++));
        if (r > 0) {
          found = false;
          break;
        }
      }

      sel[selected] = rowIdx;
      selected += found ? 1 : 0;
    }

    return selected;
  }
}
