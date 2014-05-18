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
package org.apache.tajo.storage.columnar;

import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.common.TajoDataTypes;
import sun.misc.Unsafe;

import java.lang.reflect.Field;

public class UnsafeUtil {
  public static final Unsafe unsafe;

  static {
    // fetch theUnsafe object
    Field field = null;
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

  public static long computeAlignedSize(long size) {
    long remain = size % SizeOf.BYTES_PER_WORD;
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
    return unsafe.allocateMemory(computeAlignedSize(TypeUtil.sizeOf(dataType, num)));
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

  public static short getUnsignedByte(long index) {
    return (short) (unsafe.getByte(index) & 0xFF);
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
}
