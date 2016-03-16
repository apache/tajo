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

package org.apache.tajo.util;

/**
 * This class is borrowed from following source codes and it is modified to adapt to tajo
 * https://github.com/google/guava/blob/v19.0/guava/src/com/google/common/hash/Murmur3_32HashFunction.java
 * https://github.com/yonik/java_util/blob/master/src/util/hash/MurmurHash3.java
 */
public class MurmurHash3_32 {
  // Constants for 32 bit variant
  private static final int C1 = 0xcc9e2d51;
  private static final int C2 = 0x1b873593;
  private static final int DEFAULT_SEED = 0;

  public static int hash(int input) {
    return hashInt(input);
  }

  public static int hash(long input) {
    return hashLong(input);
  }

  public static int hash(double input) {
    return hashLong(Double.doubleToRawLongBits(input));
  }

  public static int hash(float input) {
    return hashInt(Float.floatToRawIntBits(input));
  }

  public static int hash(String input) {
    return hash(input.getBytes());
  }

  public static int hash(byte[] input) {
    return hash(input, 0, input.length, DEFAULT_SEED);
  }

  public static int hash(byte[] data, int offset, int len, int seed) {
    int h1 = seed;
    int roundedEnd = offset + (len & 0xfffffffc);  // round down to 4 byte block

    for (int i = offset; i < roundedEnd; i += 4) {
      // little endian load order
      int k1 = (data[i] & 0xff) | ((data[i + 1] & 0xff) << 8) | ((data[i + 2] & 0xff) << 16) | (data[i + 3] << 24);

      k1 = mixK1(k1);
      h1 = mixH1(h1, k1);
    }

    // tail
    int k1 = 0;

    switch (len & 0x03) {
    case 3:
      k1 = (data[roundedEnd + 2] & 0xff) << 16;
      // fallthrough
    case 2:
      k1 |= (data[roundedEnd + 1] & 0xff) << 8;
      // fallthrough
    case 1:
      k1 |= (data[roundedEnd] & 0xff);
      k1 = mixK1(k1);
      h1 ^= k1;
    }

    // finalization
    h1 ^= len;

    // fmix(h1);
    h1 ^= h1 >>> 16;
    h1 *= 0x85ebca6b;
    h1 ^= h1 >>> 13;
    h1 *= 0xc2b2ae35;
    h1 ^= h1 >>> 16;

    return h1;
  }

  public static int hashInt(int input) {
    int k1 = mixK1(input);
    int h1 = mixH1(DEFAULT_SEED, k1);

    return fmix(h1, 4);
  }

  public static int hashLong(long input) {
    int low = (int) input;
    int high = (int) (input >>> 32);

    int k1 = mixK1(low);
    int h1 = mixH1(DEFAULT_SEED, k1);

    k1 = mixK1(high);
    h1 = mixH1(h1, k1);

    return fmix(h1, 8);
  }


  public static int hashUnsafeInt(long address) {
    return hashInt(UnsafeUtil.unsafe.getInt(address));
  }

  public static int hashUnsafeLong(long address) {
    return hashLong(UnsafeUtil.unsafe.getLong(address));
  }

  public static int hashUnsafeVariant(long address, int len) {
    return hashUnsafeVariant(address, len, DEFAULT_SEED);
  }

  public static int hashUnsafeVariant(long address, int len, int seed) {
    int h1 = seed;
    int roundedEnd = len & 0xfffffffc;  // round down to 4 byte block

    for (int i = 0; i < roundedEnd; i += 4) {
      // little endian load order
      int k1 = UnsafeUtil.unsafe.getInt(address + i);
      k1 = mixK1(k1);
      h1 = mixH1(h1, k1);
    }

    // tail
    int k1 = 0;
    long offset = address + roundedEnd;

    switch (len & 0x03) {
    case 3:
      k1 = (UnsafeUtil.unsafe.getByte(offset + 2) & 0xff) << 16;
      // fallthrough
    case 2:
      k1 |= (UnsafeUtil.unsafe.getByte(offset + 1) & 0xff) << 8;
      // fallthrough
    case 1:
      k1 |= (UnsafeUtil.unsafe.getByte(offset) & 0xff);
      k1 = mixK1(k1);
      h1 ^= k1;
    }

    // finalization
    h1 ^= len;

    // fmix(h1);
    h1 ^= h1 >>> 16;
    h1 *= 0x85ebca6b;
    h1 ^= h1 >>> 13;
    h1 *= 0xc2b2ae35;
    h1 ^= h1 >>> 16;

    return h1;
  }

  private static int mixK1(int k1) {
    k1 *= C1;
    k1 = Integer.rotateLeft(k1, 15);
    k1 *= C2;
    return k1;
  }

  private static int mixH1(int h1, int k1) {
    h1 ^= k1;
    h1 = Integer.rotateLeft(h1, 13);
    h1 = h1 * 5 + 0xe6546b64;
    return h1;
  }

  // Finalization mix - force all bits of a hash block to avalanche
  private static int fmix(int h1, int length) {
    h1 ^= length;
    h1 ^= h1 >>> 16;
    h1 *= 0x85ebca6b;
    h1 ^= h1 >>> 13;
    h1 *= 0xc2b2ae35;
    h1 ^= h1 >>> 16;
    return h1;
  }
}