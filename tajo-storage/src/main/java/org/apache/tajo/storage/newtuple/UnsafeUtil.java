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

  public static long alloc(long size) {
    return unsafe.allocateMemory(size);
  }

  public static long alloc(TajoDataTypes.Type type, long size) {
    return alloc(CatalogUtil.newSimpleDataType(type), size);
  }

  public static long alloc(TajoDataTypes.DataType dataType, long size) {
    return unsafe.allocateMemory(TypeUtil.sizeOf(dataType) * size);
  }

  public static void free(long addr) {
    unsafe.freeMemory(addr);
  }

  public static long getLong(long addr, int index) {
    return unsafe.getLong(addr + (index * SizeOf.SIZE_OF_LONG));
  }

  public static void putLong(long addr, int index, long val) {
    unsafe.putLong(addr + (index * SizeOf.SIZE_OF_LONG), val);
  }
}
