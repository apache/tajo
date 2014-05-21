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

import org.apache.tajo.storage.columnar.SizeOf;
import org.apache.tajo.storage.columnar.UnsafeUtil;

/**
 * Created by hyunsik on 5/18/14.
 */
public class VecFuncMulMul3LongCol {

  public static void hashLongVector(int num, long resPtr, long vectorPtr, long nullFlags, long selId) {
    int rowIdx = 0;
    while(rowIdx < num) {
      long hashValue = hash64(UnsafeUtil.unsafe.getLong(vectorPtr));
      UnsafeUtil.unsafe.putLong(resPtr, hashValue);

      resPtr += SizeOf.SIZE_OF_LONG;
      vectorPtr += SizeOf.SIZE_OF_LONG;

      rowIdx++;
    }
  }

  private static final long C1 = 0x87c37b91114253d5L;
  private static final long C2 = 0x4cf5ad432745937fL;

  private final static long DEFAULT_SEED = 0;

  public static void hash(long seed, long resultPtr, long ptr, int offset, int length)
  {
    final int fastLimit = offset + length - (2 * SizeOf.SIZE_OF_LONG) + 1;

    long h1 = seed;
    long h2 = seed;

    long current = ptr + offset;
    while (current < fastLimit) {
      long k1 = UnsafeUtil.unsafe.getLong(current);
      current += SizeOf.SIZE_OF_LONG;

      long k2 = UnsafeUtil.unsafe.getLong(current);
      current += SizeOf.SIZE_OF_LONG;

      k1 *= C1;
      k1 = Long.rotateLeft(k1, 31);
      k1 *= C2;
      h1 ^= k1;

      h1 = Long.rotateLeft(h1, 27);
      h1 += h2;
      h1 = h1 * 5 + 0x52dce729L;

      k2 *= C2;
      k2 = Long.rotateLeft(k2, 33);
      k2 *= C1;
      h2 ^= k2;

      h2 = Long.rotateLeft(h2, 31);
      h2 += h1;
      h2 = h2 * 5 + 0x38495ab5L;
    }

    long k1 = 0;
    long k2 = 0;

    switch (length & 15) {
    case 15:
      k2 ^= ((long) UnsafeUtil.getUnsignedByte(current + 14)) << 48;
    case 14:
      k2 ^= ((long) UnsafeUtil.getUnsignedByte(current + 13)) << 40;
    case 13:
      k2 ^= ((long) UnsafeUtil.getUnsignedByte(current + 12)) << 32;
    case 12:
      k2 ^= ((long) UnsafeUtil.getUnsignedByte(current + 11)) << 24;
    case 11:
      k2 ^= ((long) UnsafeUtil.getUnsignedByte(current + 10)) << 16;
    case 10:
      k2 ^= ((long) UnsafeUtil.getUnsignedByte(current + 9)) << 8;
    case 9:
      k2 ^= ((long) UnsafeUtil.getUnsignedByte(current + 8)) << 0;

      k2 *= C2;
      k2 = Long.rotateLeft(k2, 33);
      k2 *= C1;
      h2 ^= k2;

    case 8:
      k1 ^= ((long) UnsafeUtil.getUnsignedByte(current + 7)) << 56;
    case 7:
      k1 ^= ((long) UnsafeUtil.getUnsignedByte(current + 6)) << 48;
    case 6:
      k1 ^= ((long) UnsafeUtil.getUnsignedByte(current + 5)) << 40;
    case 5:
      k1 ^= ((long) UnsafeUtil.getUnsignedByte(current + 4)) << 32;
    case 4:
      k1 ^= ((long) UnsafeUtil.getUnsignedByte(current + 3)) << 24;
    case 3:
      k1 ^= ((long) UnsafeUtil.getUnsignedByte(current + 2)) << 16;
    case 2:
      k1 ^= ((long) UnsafeUtil.getUnsignedByte(current + 1)) << 8;
    case 1:
      k1 ^= ((long) UnsafeUtil.getUnsignedByte(current + 0)) << 0;

      k1 *= C1;
      k1 = Long.rotateLeft(k1, 31);
      k1 *= C2;
      h1 ^= k1;
    }

    h1 ^= length;
    h2 ^= length;

    h1 += h2;
    h2 += h1;

    h1 = mix64(h1);
    h2 = mix64(h2);

    h1 += h2;
    h2 += h1;


    UnsafeUtil.unsafe.putLong(resultPtr + 0, h1);
    UnsafeUtil.unsafe.putLong(resultPtr + 8, h2);
  }

  /**
   * Returns the 64 most significant bits of the Murmur128 hash of the provided value
   */
  public static long hash64(long seed, long dataPtr, int offset, int length)
  {
    final int fastLimit = offset + length - (2 * SizeOf.SIZE_OF_LONG) + 1;

    long h1 = seed;
    long h2 = seed;

    long current = dataPtr + offset;
    while (current < fastLimit) {
      long k1 = UnsafeUtil.unsafe.getLong(current);
      current += SizeOf.SIZE_OF_LONG;

      long k2 = UnsafeUtil.unsafe.getLong(current);
      current += SizeOf.SIZE_OF_LONG;

      k1 *= C1;
      k1 = Long.rotateLeft(k1, 31);
      k1 *= C2;
      h1 ^= k1;

      h1 = Long.rotateLeft(h1, 27);
      h1 += h2;
      h1 = h1 * 5 + 0x52dce729L;

      k2 *= C2;
      k2 = Long.rotateLeft(k2, 33);
      k2 *= C1;
      h2 ^= k2;

      h2 = Long.rotateLeft(h2, 31);
      h2 += h1;
      h2 = h2 * 5 + 0x38495ab5L;
    }

    long k1 = 0;
    long k2 = 0;

    switch (length & 15) {
    case 15:
      k2 ^= ((long) UnsafeUtil.getUnsignedByte(current + 14)) << 48;
    case 14:
      k2 ^= ((long) UnsafeUtil.getUnsignedByte(current + 13)) << 40;
    case 13:
      k2 ^= ((long) UnsafeUtil.getUnsignedByte(current + 12)) << 32;
    case 12:
      k2 ^= ((long) UnsafeUtil.getUnsignedByte(current + 11)) << 24;
    case 11:
      k2 ^= ((long) UnsafeUtil.getUnsignedByte(current + 10)) << 16;
    case 10:
      k2 ^= ((long) UnsafeUtil.getUnsignedByte(current + 9)) << 8;
    case 9:
      k2 ^= ((long) UnsafeUtil.getUnsignedByte(current + 8)) << 0;

      k2 *= C2;
      k2 = Long.rotateLeft(k2, 33);
      k2 *= C1;
      h2 ^= k2;

    case 8:
      k1 ^= ((long) UnsafeUtil.getUnsignedByte(current + 7)) << 56;
    case 7:
      k1 ^= ((long) UnsafeUtil.getUnsignedByte(current + 6)) << 48;
    case 6:
      k1 ^= ((long) UnsafeUtil.getUnsignedByte(current + 5)) << 40;
    case 5:
      k1 ^= ((long) UnsafeUtil.getUnsignedByte(current + 4)) << 32;
    case 4:
      k1 ^= ((long) UnsafeUtil.getUnsignedByte(current + 3)) << 24;
    case 3:
      k1 ^= ((long) UnsafeUtil.getUnsignedByte(current + 2)) << 16;
    case 2:
      k1 ^= ((long) UnsafeUtil.getUnsignedByte(current + 1)) << 8;
    case 1:
      k1 ^= ((long) UnsafeUtil.getUnsignedByte(current + 0)) << 0;

      k1 *= C1;
      k1 = Long.rotateLeft(k1, 31);
      k1 *= C2;
      h1 ^= k1;
    }

    h1 ^= length;
    h2 ^= length;

    h1 += h2;
    h2 += h1;

    h1 = mix64(h1);
    h2 = mix64(h2);

    return h1 + h2;
  }

  /**
   * Special-purpose version for hashing a single long value. Value is treated as little-endian
   */
  public static long hash64(long value)
  {
    long h2 = DEFAULT_SEED ^ SizeOf.SIZE_OF_LONG;
    long h1 = h2 + (h2 ^ (Long.rotateLeft(value * C1, 31) * C2));

    return mix64(h1) + mix64(h1 + h2);
  }

  public static long hash64(long seed, long value)
  {
    long h2 = seed ^ SizeOf.SIZE_OF_LONG;
    long h1 = h2 + (h2 ^ (Long.rotateLeft(value * C1, 31) * C2));

    return mix64(h1) + mix64(h1 + h2);
  }

  private static long mix64(long k)
  {
    k ^= k >>> 33;
    k *= 0xff51afd7ed558ccdL;
    k ^= k >>> 33;
    k *= 0xc4ceb9fe1a85ec53L;
    k ^= k >>> 33;

    return k;
  }
}
