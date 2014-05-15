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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import sun.misc.Unsafe;

import java.util.List;
import java.util.Vector;

public class VecRowBlock {
  private static final Unsafe unsafe = UnsafeUtil.unsafe;

  long address;
  long size;
  Schema schema;
  int vectorSize;

  long [] nullVectorsAddrs;
  long [] vectorsAddrs;

  public VecRowBlock(Schema schema, int vectorSize) {
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
    long totalSize = 0;
    long eachNullHeaderBytes = computeNullHeaderSizePerColumn(vectorSize);
    long totalNullHeaderBytes = computeNullHeaderSize(vectorSize, schema.size());
    totalSize += totalNullHeaderBytes;

    for (int i = 0; i < schema.size(); i++) {
      Column column = schema.getColumn(i);
      totalSize += TypeUtil.sizeOf(column.getDataType()) * vectorSize;
    }
    size = totalSize;
    allocate();

    nullVectorsAddrs = new long[schema.size()];
    for (int i = 0; i < schema.size(); i++) {
      if (i == 0) {
        nullVectorsAddrs[i] = address;
      } else {
        nullVectorsAddrs[i] = nullVectorsAddrs[i - 1] + eachNullHeaderBytes;
      }
    }
    UnsafeUtil.bzero(nullVectorsAddrs[0], totalNullHeaderBytes);

    vectorsAddrs = new long[schema.size()];
    long perVecSize;
    for (int i = 0; i < schema.size(); i++) {
      if (i == 0) {
        vectorsAddrs[i] = nullVectorsAddrs[schema.size() - 1];
      } else {
        Column prevColumn = schema.getColumn(i - 1);
        perVecSize = (TypeUtil.sizeOf(prevColumn.getDataType()) * vectorSize);
        vectorsAddrs[i] = vectorsAddrs[i - 1] + perVecSize;
      }
    }
  }

  public long getValueVecPtr(int columnIdx) {
    return vectorsAddrs[columnIdx];
  }

  public long getNullVecPtr(int columnIdx) {
    return nullVectorsAddrs[columnIdx];
  }

  private static final int WORD_SIZE = SizeOf.SIZE_OF_LONG * 8;

  public void setNull(int columnIdx, int index) {
    Preconditions.checkArgument(index < vectorSize, "Index Out of Vectors");
    int chunkId = index / WORD_SIZE;
    long offset = index % WORD_SIZE;
    long address = nullVectorsAddrs[columnIdx] + (chunkId * 8);
    long nullFlagChunk = unsafe.getLong(address);
    nullFlagChunk = (nullFlagChunk | (1L << offset));
    unsafe.putLong(address, nullFlagChunk);
  }

  public int isNull(int columnIdx, int index) {
    Preconditions.checkArgument(index < vectorSize, "Index Out of Vectors");
    int chunkId = index / WORD_SIZE;
    long offset = index % WORD_SIZE;
    long address = nullVectorsAddrs[columnIdx] + (chunkId * 8);
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

  public void putText(int columnIdx, int index, byte [] val) {
    long currentPage = 0L;

    long ptr = currentPage;
    long nextPtr = 0L;
    long usedMemory = nextPtr - ptr;

    if (PAGE_SIZE - usedMemory < val.length) {
      ptr = createPage();
      currentPage = ptr;
    }

    unsafe.putShort(currentPage, (short) val.length);
    unsafe.putByte();
  }

  public byte [] getText(int columnIdx, int index) {
    long ptr = vectorsAddrs[columnIdx];
    short length = unsafe.getShort(ptr);
    return null;
  }

  public static final int PAGE_SIZE = 4096;

  /**
   * [next address, 0x0 last page. Otherwise, has next page (8 bytes) ] [PAGE payload (4096 by default) ]
   *
   * @return
   */
  private long createPage() {
    long pagePtr = unsafe.allocateMemory(PAGE_SIZE + 8);
    unsafe.putLong(pagePtr, 0); // set
    return pagePtr;
  }

  public void destroy() {
    unsafe.freeMemory(address);
  }

  public static long allocateNullVector(int vectorSize) {
    long nBytes = computeNullHeaderSizePerColumn(vectorSize);
    long ptr = UnsafeUtil.alloc(nBytes);
    UnsafeUtil.bzero(ptr, nBytes);
    return ptr;
  }

  public static long computeNullHeaderSizePerColumn(int vectorSize) {
    return (long) Math.ceil(vectorSize / SizeOf.SIZE_OF_BYTE);
  }

  public static long computeNullHeaderSize(int vectorSize, int columnNum) {
    long eachNullHeaderBytes = (long) Math.ceil(vectorSize / SizeOf.SIZE_OF_BYTE);
    return eachNullHeaderBytes * columnNum;
  }
}
