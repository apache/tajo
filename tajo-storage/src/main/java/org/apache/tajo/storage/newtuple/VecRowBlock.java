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

import com.google.common.collect.Lists;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import sun.misc.Unsafe;

import java.util.List;

public class VecRowBlock {
  private static final Unsafe unsafe = UnsafeUtil.unsafe;

  long address;
  long size;
  Schema schema;
  long vectorSize;

  int [] colIndices;
  long [] nullVectorsAddrs;
  long [] vectorsAddrs;

  List<Column> fixedLenColumns = Lists.newArrayList();
  List<Column> varLenColumns = Lists.newArrayList();

  public VecRowBlock(Schema schema, long vectorSize) {
    this.schema = schema;
    this.vectorSize = vectorSize;

    init();
  }

  public void allocate() {
    address = unsafe.allocateMemory(size);
    UnsafeUtil.unsafe.setMemory(address, size, (byte) 0);
  }

  public long size() {
    return size;
  }

  public void init() {

    for (Column column : schema.getColumns()) {
      if (TypeUtil.isFixedSize(column.getDataType())) {
        fixedLenColumns.add(column);
      } else {
        varLenColumns.add(column);
      }
    }

    colIndices = new int[schema.size()];
    for (int i = 0; i < fixedLenColumns.size(); i++) {
      int idx = schema.getColumnId(fixedLenColumns.get(i).getQualifiedName());
      colIndices[i] = idx;
    }

    for (int i = 0; i < fixedLenColumns.size(); i++) {
      int idx = schema.getColumnId(fixedLenColumns.get(i).getQualifiedName());
      colIndices[i] = idx;
    }

    for (int i = 0; i < varLenColumns.size(); i++) {
      int idx = schema.getColumnId(fixedLenColumns.get(i).getQualifiedName());
      colIndices[fixedLenColumns.size() + i] = fixedLenColumns.size() + idx;
    }

    long totalSize = 0;

    // add null flag array - the number of columns * vector size / 8
    totalSize += (schema.size() * vectorSize / 8) + 1;

    for (int i = 0; i < fixedLenColumns.size(); i++) {
      Column column = fixedLenColumns.get(i);
      totalSize += TypeUtil.sizeOf(column.getDataType()) * vectorSize;
    }
    size = totalSize;
    allocate();

    nullVectorsAddrs = new long[schema.size()];
    for (int i = 0; i < schema.size(); i++) {
      if (i == 0) {
        nullVectorsAddrs[i] = address;
      } else {
        nullVectorsAddrs[i] = nullVectorsAddrs[i - 1] + (vectorSize / 8 + 1);
      }
    }

    vectorsAddrs = new long[fixedLenColumns.size()];
    long perVecSize;
    for (int i = 0; i < fixedLenColumns.size(); i++) {
      if (i == 0) {
        vectorsAddrs[i] = nullVectorsAddrs[schema.size() - 1];
      } else {
        Column prevColumn = fixedLenColumns.get(i - 1);
        perVecSize = (TypeUtil.sizeOf(prevColumn.getDataType()) * vectorSize);
        vectorsAddrs[i] = vectorsAddrs[i - 1] + perVecSize;
      }
    }
  }

  public long getVecAddress(int columnIdx) {
    return vectorsAddrs[columnIdx];
  }

  private static final int WORD_SIZE = SizeOf.SIZE_OF_LONG * 8;

  public void setNull(int columnIdx, int index) {
    int chunkId = index / WORD_SIZE;
    long offset = index % WORD_SIZE;
    long address = nullVectorsAddrs[columnIdx] + chunkId;
    long nullFlagChunk = unsafe.getLong(address);
    nullFlagChunk = (nullFlagChunk | (1L << offset));
    unsafe.putLong(address, nullFlagChunk);
  }

  public int isNull(int columnIdx, int index) {
    int chunkId = index / WORD_SIZE;
    long offset = index % WORD_SIZE;
    long address = nullVectorsAddrs[columnIdx] + chunkId;
    long nullFlagChunk = unsafe.getLong(address);
    return (int) ((nullFlagChunk >> offset) & 1L);
  }

  public short getInt2(int columnIdx, int index) {
    return unsafe.getShort(vectorsAddrs[columnIdx] + (index * SizeOf.SIZE_OF_SHORT));
  }

  public void putInt2(int columnIdx, int index, short val) {
    unsafe.putShort(vectorsAddrs[columnIdx] + (index * SizeOf.SIZE_OF_SHORT), val);
  }

  public void putInt4(int columnIdx, int index, int val) {
    unsafe.putInt(vectorsAddrs[columnIdx] + (index * SizeOf.SIZE_OF_INT), val);
  }

  public int getInt4(int columnIdx, int index) {
    return unsafe.getInt(vectorsAddrs[columnIdx] + (index * SizeOf.SIZE_OF_INT));
  }

  public void putInt8(int columnIdx, int index, int val) {
    unsafe.putInt(vectorsAddrs[columnIdx] + (index * SizeOf.SIZE_OF_LONG), val);
  }

  public int getInt8(int columnIdx, int index) {
    return unsafe.getInt(vectorsAddrs[columnIdx] + (index * SizeOf.SIZE_OF_LONG));
  }

  public void putFloat4(int columnIdx, int index, float val) {
    unsafe.putFloat(vectorsAddrs[columnIdx] + (index * SizeOf.SIZE_OF_FLOAT), val);
  }

  public float getFloat4(int columnIdx, int index) {
    return unsafe.getFloat(vectorsAddrs[columnIdx] + (index * SizeOf.SIZE_OF_FLOAT));
  }

  public void putFloat8(int columnIdx, int index, double val) {
    unsafe.putDouble(vectorsAddrs[columnIdx] + (index * SizeOf.SIZE_OF_DOUBLE), val);
  }

  public double getFloat8(int columnIdx, int index) {
    return unsafe.getDouble(vectorsAddrs[columnIdx] + (index * SizeOf.SIZE_OF_DOUBLE));
  }

  public void destroy() {
    unsafe.freeMemory(address);
  }
}
