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

import org.apache.tajo.catalog.Schema;
import sun.misc.Unsafe;

import java.lang.reflect.Field;

public class RowBlock {
  private static final Unsafe unsafe;

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

  long address;
  long size;
  Schema schema;
  long base;

  int rownum;

  public RowBlock(Schema schema, long size) {
    this.schema = schema;
    this.size = size;
    address = unsafe.allocateMemory(size);
    unsafe.setMemory(address, size, (byte) 0);
  }

  public int rownum() {
    return this.rownum;
  }

  public int getInt(int columnIdx, int index) {
    return unsafe.getInt(base, address + index);
  }

  public void destroy() {
    unsafe.freeMemory(address);
  }
}
