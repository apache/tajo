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

package org.apache.tajo.storage.newtuple;

import com.google.common.primitives.Longs;
import com.google.common.primitives.UnsignedBytes;
import sun.misc.Unsafe;

public class VectorUtil {
  private static final Unsafe unsafe = UnsafeUtil.unsafe;

  public static void nullify(int vecNum, long result, long addr1, long addr2) {
    long nullFlagChunk;
    while (vecNum >= SizeOf.SIZE_OF_LONG) {
      nullFlagChunk =  (unsafe.getLong(addr1) | unsafe.getLong(addr2));
      unsafe.putLong(result, nullFlagChunk);

      result += SizeOf.SIZE_OF_LONG;
      addr1 += SizeOf.SIZE_OF_LONG;
      addr2 += SizeOf.SIZE_OF_LONG;
      vecNum -= SizeOf.SIZE_OF_LONG * SizeOf.SIZE_OF_BYTE;
    }

    byte nullFlagByte;
    while (vecNum > 0) {
      nullFlagByte = (byte) (unsafe.getByte(addr1) & unsafe.getByte(addr2));
      unsafe.putByte(result, nullFlagByte);

      result++;
      addr1++;
      addr2++;
      vecNum-= SizeOf.SIZE_OF_BYTE;
    }
  }

  public static void nullify(int vecNum, final long result, final long addr1, final long addr2, final long addr3) {
    long nullFlagChunk;
    for (int i = 0; i < vecNum; i++) {
      int offset = (i % WORD_SIZE);
      nullFlagChunk = unsafe.getLong(addr1 + offset) & unsafe.getLong(addr2 + offset) & unsafe.getLong(addr3 + offset);
      unsafe.putLong(result + offset, nullFlagChunk);
    }
  }

  private static final int WORD_SIZE = SizeOf.SIZE_OF_LONG * 8;

  public static void setNull(long nullVector, int index) {
    int chunkId = index / WORD_SIZE;
    int offset = index % WORD_SIZE;
    long address = nullVector + chunkId;
    long nullFlagChunk = unsafe.getLong(address);
    nullFlagChunk = (nullFlagChunk | (1 << offset));
    unsafe.putLong(address, nullFlagChunk);
  }

  public static int isNull(long nullVector, int index) {
    int chunkId = index / WORD_SIZE;
    long offset = index % WORD_SIZE;
    long address = nullVector + (chunkId * 8);
    long nullFlagChunk = unsafe.getLong(address);
    return (int) ((nullFlagChunk >> offset) & 1L);
  }
}
