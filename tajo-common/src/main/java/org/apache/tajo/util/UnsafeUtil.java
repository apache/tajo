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

  public static long getAddress(ByteBuffer buffer) {
    Preconditions.checkArgument(buffer instanceof DirectBuffer, "ByteBuffer must be DirectBuffer");
    return ((DirectBuffer)buffer).address();
  }

  public static void free(Deallocatable obj) {
    obj.release();
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
