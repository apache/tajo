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
package org.apache.tajo.storage.directmem;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Longs;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.common.TajoDataTypes;
import sun.misc.Cleaner;
import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;

public class UnsafeUtil {
  public static final Unsafe unsafe;

  static {
    Field field;
    try {
      field = Unsafe.class.getDeclaredField("theUnsafe");

      field.setAccessible(true);
      unsafe = (Unsafe) field.get(null);
      if (unsafe == null) {
        throw new RuntimeException("Unsafe access not available");
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static int alignedSize(int size) {
    int remain = size % SizeOf.BYTES_PER_WORD;
    if (remain > 0) {
      return size + (SizeOf.BYTES_PER_WORD - remain);
    } else {
      return size;
    }
  }

  public static long alloc(long size) {
    return unsafe.allocateMemory(size);
  }

  public static long allocVector(TajoDataTypes.Type type, int num) {
    return allocVector(CatalogUtil.newSimpleDataType(type), num);
  }

  public static long allocVector(TajoDataTypes.DataType dataType, int num) {
    return unsafe.allocateMemory(alignedSize(TypeUtil.sizeOf(dataType, num)));
  }

  public static long getAddress(ByteBuffer buffer) {
    Preconditions.checkArgument(buffer instanceof DirectBuffer, "ByteBuffer must be DirectBuffer");
    return ((DirectBuffer)buffer).address();
  }

  public static void free(long addr) {
    unsafe.freeMemory(addr);
  }

  public static void bzero(long addr, long nBytes) {
    long n = nBytes;
    while (n >= SizeOf.SIZE_OF_LONG) {
      unsafe.putLong(addr, 0);
      addr += SizeOf.SIZE_OF_LONG;
      n -= SizeOf.SIZE_OF_LONG;
    }

    while (n > 0) {
      unsafe.putByte(addr, (byte) 0);
      addr++;
      n--;
    }
  }

  public static byte getByte(long index) {
    return  unsafe.getByte(index);
  }

  public static short getUnsignedByte(long index) {
    return (short) (unsafe.getByte(index) & 0xFF);
  }

  public static int getShort(long addr, int index) {
    return unsafe.getShort(addr + (index * SizeOf.SIZE_OF_INT));
  }

  public static void putShort(long addr, int index, short val) {
    unsafe.putShort(addr + (index * SizeOf.SIZE_OF_INT), val);
  }

  public static int getInt(long addr, int index) {
    return unsafe.getInt(addr + (index * SizeOf.SIZE_OF_INT));
  }

  public static void putInt(long addr, int index, int val) {
    unsafe.putInt(addr + (index * SizeOf.SIZE_OF_INT), val);
  }

  public static long getLong(long addr, int index) {
    return unsafe.getLong(addr + (index * SizeOf.SIZE_OF_LONG));
  }

  public static void putLong(long addr, int index, long val) {
    unsafe.putLong(addr + (index * SizeOf.SIZE_OF_LONG), val);
  }

  public static void putFloat(long addr, int index, float val) {
    unsafe.putFloat(addr + (index * SizeOf.SIZE_OF_FLOAT), val);
  }

  public static void putDouble(long addr, int index, double val) {
    unsafe.putDouble(addr + (index * SizeOf.SIZE_OF_FLOAT), val);
  }

  public static void putBytes(long addr, byte [] val, int srcPos, int length) {
    unsafe.copyMemory(val, Unsafe.ARRAY_BYTE_BASE_OFFSET + srcPos, null, addr, length);
  }

  public static void getBytes(long addr, byte [] val, int offset, int length) {
    unsafe.copyMemory(addr, 0, val, Unsafe.ARRAY_BYTE_BASE_OFFSET, length);
  }

  public static boolean equalStrings(long thisAddr, int len1, long anotherAddr, int len2) {
    if (len1 != len2) {
      return false;
    }

    int minLength = Math.min(len1, len2);
    int minWords = minLength / Longs.BYTES;

    /*
       * Compare 8 bytes at a time. Benchmarking shows comparing 8 bytes at a
       * time is no slower than comparing 4 bytes at a time even on 32-bit.
       * On the other hand, it is substantially faster on 64-bit.
       */
    for (int i = 0; i < minWords * Longs.BYTES; i += Longs.BYTES) {
      long lw = UnsafeUtil.unsafe.getLong(thisAddr);
      long rw = UnsafeUtil.unsafe.getLong(anotherAddr);
      thisAddr += SizeOf.SIZE_OF_LONG;
      anotherAddr += SizeOf.SIZE_OF_LONG;

      long diff = lw ^ rw;

      if (diff != 0) {
        return false;
      }
    }

    // The epilogue to cover the last (minLength % 8) elements.
    for (int i = minWords * Longs.BYTES; i < minLength; i++) {
      byte r = (byte) (UnsafeUtil.unsafe.getByte(thisAddr++) ^ UnsafeUtil.unsafe.getByte(anotherAddr++));
      if (r != 0) {
        return false;
      }
    }

    return true;
  }

  public static void free(ByteBuffer bb) {
    Preconditions.checkNotNull(bb);
    Preconditions.checkState(bb instanceof DirectBuffer);

    Cleaner cleaner = ((DirectBuffer) bb).cleaner();
    if (cleaner != null) {
      cleaner.clean();
    }
  }
}
