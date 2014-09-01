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

package org.apache.tajo.util;

import com.google.common.primitives.Longs;
import com.google.common.primitives.UnsignedBytes;
import com.google.common.primitives.UnsignedLongs;
import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.nio.ByteOrder;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Comparator;

/**
 * This is borrowed from Guava's UnsignedBytes.
 */
public class UnsafeComparer implements Comparator<byte[]> {

  public static final UnsafeComparer INSTANCE;

  static {
    INSTANCE = new UnsafeComparer();
  }

  public Comparator<byte []> get() {
    return INSTANCE;
  }

  private UnsafeComparer() {}

  static final boolean littleEndian =
      ByteOrder.nativeOrder().equals(ByteOrder.LITTLE_ENDIAN);

      /*
       * The following static final fields exist for performance reasons.
       *
       * In UnsignedBytesBenchmark, accessing the following objects via static
       * final fields is the fastest (more than twice as fast as the Java
       * implementation, vs ~1.5x with non-final static fields, on x86_32)
       * under the Hotspot server compiler. The reason is obviously that the
       * non-final fields need to be reloaded inside the loop.
       *
       * And, no, defining (final or not) local variables out of the loop still
       * isn't as good because the null check on the theUnsafe object remains
       * inside the loop and BYTE_ARRAY_BASE_OFFSET doesn't get
       * constant-folded.
       *
       * The compiler can treat static final fields as compile-time constants
       * and can constant-fold them while (final or not) local variables are
       * run time values.
       */

  static final Unsafe theUnsafe;

  /** The offset to the first element in a byte array. */
  static final int BYTE_ARRAY_BASE_OFFSET;

  static {
    theUnsafe = (Unsafe) AccessController.doPrivileged(
        new PrivilegedAction<Object>() {
          @Override
          public Object run() {
            try {
              Field f = Unsafe.class.getDeclaredField("theUnsafe");
              f.setAccessible(true);
              return f.get(null);
            } catch (NoSuchFieldException e) {
              // It doesn't matter what we throw;
              // it's swallowed in getBestComparator().
              throw new Error();
            } catch (IllegalAccessException e) {
              throw new Error();
            }
          }
        });

    BYTE_ARRAY_BASE_OFFSET = theUnsafe.arrayBaseOffset(byte[].class);

    // sanity check - this should never fail
    if (theUnsafe.arrayIndexScale(byte[].class) != 1) {
      throw new AssertionError();
    }
  }

  @SuppressWarnings("unused")
  public static int compareStatic(byte[] left, byte[] right) {
    return INSTANCE.compare(left, right);
  }

  @Override public int compare(byte[] left, byte[] right) {
    int minLength = Math.min(left.length, right.length);
    int minWords = minLength / Longs.BYTES;

        /*
         * Compare 8 bytes at a time. Benchmarking shows comparing 8 bytes at a
         * time is no slower than comparing 4 bytes at a time even on 32-bit.
         * On the other hand, it is substantially faster on 64-bit.
         */
    for (int i = 0; i < minWords * Longs.BYTES; i += Longs.BYTES) {
      long lw = theUnsafe.getLong(left, BYTE_ARRAY_BASE_OFFSET + (long) i);
      long rw = theUnsafe.getLong(right, BYTE_ARRAY_BASE_OFFSET + (long) i);
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
    }

    // The epilogue to cover the last (minLength % 8) elements.
    for (int i = minWords * Longs.BYTES; i < minLength; i++) {
      int result = UnsignedBytes.compare(left[i], right[i]);
      if (result != 0) {
        return result;
      }
    }
    return left.length - right.length;
  }
}
