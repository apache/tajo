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

import com.google.common.base.Preconditions;
import io.netty.util.internal.PlatformDependent;
import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;

public class UnsafeUtil {
  public static final Unsafe unsafe;

  // copied from
  // http://stackoverflow.com/questions/52353/in-java-what-is-the-best-way-to-determine-the-size-of-an-object
  public static enum AddressMode {
    /** Unknown address mode. Size calculations may be unreliable. */
    UNKNOWN,
    /** 32-bit address mode using 32-bit references. */
    MEM_32BIT,
    /** 64-bit address mode using 64-bit references. */
    MEM_64BIT,
    /** 64-bit address mode using 32-bit compressed references. */
    MEM_64BIT_COMPRESSED_OOPS
  }

  public static final AddressMode ADDRESS_MODE;

  // offsets
  public static final int ARRAY_BOOLEAN_BASE_OFFSET;
  public static final int ARRAY_BYTE_BASE_OFFSET;
  public static final int ARRAY_SHORT_BASE_OFFSET;
  public static final int ARRAY_CHAR_BASE_OFFSET;
  public static final int ARRAY_INT_BASE_OFFSET;
  public static final int ARRAY_LONG_BASE_OFFSET;
  public static final int ARRAY_FLOAT_BASE_OFFSET;
  public static final int ARRAY_DOUBLE_BASE_OFFSET;
  public static final int ARRAY_OBJECT_BASE_OFFSET;

  // scale
  public static final int ARRAY_BOOLEAN_INDEX_SCALE;
  public static final int ARRAY_BYTE_INDEX_SCALE;
  public static final int ARRAY_SHORT_INDEX_SCALE;
  public static final int ARRAY_CHAR_INDEX_SCALE;
  public static final int ARRAY_INT_INDEX_SCALE;
  public static final int ARRAY_LONG_INDEX_SCALE;
  public static final int ARRAY_FLOAT_INDEX_SCALE;
  public static final int ARRAY_DOUBLE_INDEX_SCALE;
  public static final int ARRAY_OBJECT_INDEX_SCALE;

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

    ARRAY_BOOLEAN_BASE_OFFSET = unsafe.arrayBaseOffset(boolean[].class);
    ARRAY_BYTE_BASE_OFFSET = unsafe.arrayBaseOffset(byte[].class);
    ARRAY_SHORT_BASE_OFFSET = unsafe.arrayBaseOffset(short[].class);
    ARRAY_CHAR_BASE_OFFSET = unsafe.arrayBaseOffset(char[].class);
    ARRAY_INT_BASE_OFFSET = unsafe.arrayBaseOffset(int[].class);
    ARRAY_LONG_BASE_OFFSET = unsafe.arrayBaseOffset(long[].class);
    ARRAY_FLOAT_BASE_OFFSET = unsafe.arrayBaseOffset(float[].class);
    ARRAY_DOUBLE_BASE_OFFSET = unsafe.arrayBaseOffset(double[].class);
    ARRAY_OBJECT_BASE_OFFSET = unsafe.arrayBaseOffset(Object[].class);

    ARRAY_BOOLEAN_INDEX_SCALE = unsafe.arrayIndexScale(boolean[].class);
    ARRAY_BYTE_INDEX_SCALE = unsafe.arrayIndexScale(byte[].class);
    ARRAY_SHORT_INDEX_SCALE = unsafe.arrayIndexScale(short[].class);
    ARRAY_CHAR_INDEX_SCALE = unsafe.arrayIndexScale(char[].class);
    ARRAY_INT_INDEX_SCALE = unsafe.arrayIndexScale(int[].class);
    ARRAY_LONG_INDEX_SCALE = unsafe.arrayIndexScale(long[].class);
    ARRAY_FLOAT_INDEX_SCALE = unsafe.arrayIndexScale(float[].class);
    ARRAY_DOUBLE_INDEX_SCALE = unsafe.arrayIndexScale(double[].class);
    ARRAY_OBJECT_INDEX_SCALE = unsafe.arrayIndexScale(Object[].class);

    int addressSize = unsafe.addressSize();
    int referenceSize = unsafe.arrayIndexScale(Object[].class);

    if (addressSize == 4) {
      ADDRESS_MODE = AddressMode.MEM_32BIT;
    } else if (addressSize == 8 && referenceSize == 8) {
      ADDRESS_MODE = AddressMode.MEM_64BIT;
    } else if (addressSize == 8 && referenceSize == 4) {
      ADDRESS_MODE = AddressMode.MEM_64BIT_COMPRESSED_OOPS;
    } else {
      ADDRESS_MODE = AddressMode.UNKNOWN;
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

  public static long getAddress(ByteBuffer buffer) {
    Preconditions.checkArgument(buffer.isDirect(), "ByteBuffer must be DirectBuffer");
    return ((DirectBuffer)buffer).address();
  }

  public static void free(Deallocatable obj) {
    obj.release();
  }

  public static void free(ByteBuffer bb) {
    PlatformDependent.freeDirectBuffer(bb);
  }
}
