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

import sun.misc.Unsafe;

public class VectorUtil {
  private static final Unsafe unsafe = UnsafeUtil.unsafe;

  public static void nullify(int vecNum, final long result, final long addr1, final long addr2) {
    long nullFlagChunk;
    for (int i = 0; i < vecNum; i++) {
      int offset = (i % WORD_SIZE);
      nullFlagChunk = unsafe.getLong(addr1 + offset) & unsafe.getLong(addr2 + offset);
      unsafe.putLong(result + offset, nullFlagChunk);
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

  private static final int WORD_SIZE = SizeOf.SIZE_OF_LONG;

  public static void setNull(long nullVector, int index) {
    int offset = index % WORD_SIZE;
    long address = nullVector + offset;
    long nullFlagChunk = unsafe.getLong(address);
    nullFlagChunk = (nullFlagChunk | (1 << offset));
    unsafe.putLong(address, nullFlagChunk);
  }

  public static int isNull(long nullVector, int index) {
    int offset = index % WORD_SIZE;
    long address = nullVector + offset;
    long nullFlagChunk = unsafe.getLong(address);
    return (int) ((nullFlagChunk >> offset) & 1);
  }

  public static void bzero(final long addr, final long length) {
    long offset = addr;
    while (offset < addr + length) {
      unsafe.putLong(offset, 0);
      offset += offset + SizeOf.SIZE_OF_LONG;
    }
  }
}
