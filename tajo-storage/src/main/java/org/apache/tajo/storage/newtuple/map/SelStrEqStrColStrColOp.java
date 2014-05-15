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

package org.apache.tajo.storage.newtuple.map;

import com.google.common.primitives.Longs;
import org.apache.tajo.storage.newtuple.SizeOf;
import org.apache.tajo.storage.newtuple.UnsafeUtil;
import sun.misc.Unsafe;

public class SelStrEqStrColStrColOp {
  static Unsafe unsafe = UnsafeUtil.unsafe;
  public int sel(int vecnum, long result, long lhs, long rhs, long nullFlags, long selId) {
    int selNum = 0;

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
         * On the other hand, it is substantially faster on 64-bit.
         */
      for (int i = 0; i < minWords * Longs.BYTES; i += Longs.BYTES) {
        long lw = unsafe.getLong(lstrAddr);
        long rw = unsafe.getLong(rstrAddr);
        lstrAddr += SizeOf.SIZE_OF_LONG;
        rstrAddr += SizeOf.SIZE_OF_LONG;

        long diff = lw ^ rw;

        if (diff != 0) {
          continue outest;
        }
      }


      outer:
      // The epilogue to cover the last (minLength % 8) elements.
      for (int i = minWords * Longs.BYTES; i < minLength; i++) {
        byte r = (byte) (unsafe.getByte(lstrAddr++) - unsafe.getByte(rstrAddr++));
        if (r != 0) {
          unsafe.putInt(result, rowIdx);
          result += SizeOf.SIZE_OF_INT;
          found = true;
          selNum++;
          continue outer;
        }
      }

      if (!found) {
        unsafe.putInt(result, rowIdx);
        selNum++;
        result += SizeOf.SIZE_OF_INT;
      }
    }

    return selNum;
  }
}
