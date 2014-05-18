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

package org.apache.tajo.storage.columnar.map;

import com.google.common.primitives.Longs;
import org.apache.tajo.storage.columnar.SizeOf;

public class VecFuncStrcmpStrStrColx2 extends MapBinaryOp {
  public void map(int vecnum, long result, long lhs, long rhs, long nullFlags, long selId) {
    outest:
    for (int rowIdx = 0; rowIdx < vecnum; rowIdx++) {
      boolean found = false;

      long lstrAddr = unsafe.getAddress(lhs);
      long rstrAddr = unsafe.getAddress(rhs);
      lhs += unsafe.ADDRESS_SIZE;
      rhs += unsafe.ADDRESS_SIZE;

      int lstrLen = unsafe.getShort(lstrAddr);
      int rstrLen = unsafe.getShort(rstrAddr);

      lstrAddr += SizeOf.SIZE_OF_SHORT;
      rstrAddr += SizeOf.SIZE_OF_SHORT;

      int minLength = Math.min(lstrLen, rstrLen);
      int minWords = minLength / Longs.BYTES;

      /*
         * Compare 8 bytes at a time. Benchmarking shows comparing 8 bytes at a
         * time is no slower than comparing 4 bytes at a time even on 32-bit.
         * On the other hand, it is substantially faster on 4-bit.
         */
      for (int i = 0; i < minWords * Longs.BYTES; i += Longs.BYTES) {
        long lw = unsafe.getLong(lstrAddr);
        long rw = unsafe.getLong(rstrAddr);
        lstrAddr += SizeOf.SIZE_OF_LONG;
        rstrAddr += SizeOf.SIZE_OF_LONG;

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

          unsafe.putInt(result, (int) (((lw >>> n) & 0xFFL) - ((rw >>> n) & 0xFFL)));
          result += SizeOf.SIZE_OF_INT;
          continue outest;
        }
      }


      outer:
      // The epilogue to cover the last (minLength % 8) elements.
      for (int i = minWords * Longs.BYTES; i < minLength; i++) {
        byte r = (byte) (unsafe.getByte(lstrAddr++) - unsafe.getByte(rstrAddr++));
        if (r != 0) {
          unsafe.putInt(result, r);
          result += SizeOf.SIZE_OF_INT;
          found = true;
          continue outer;
        }
      }

      if (!found) {
        unsafe.putInt(result, lstrLen - rstrLen);
        result += SizeOf.SIZE_OF_INT;
      }
    }
  }
}
