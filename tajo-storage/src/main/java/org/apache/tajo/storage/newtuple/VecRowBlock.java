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
import org.apache.commons.codec.StringEncoder;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.common.TajoDataTypes;
import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;
import java.util.List;

public class VecRowBlock {
  private static final Unsafe unsafe = UnsafeUtil.unsafe;

  private final Schema schema;
  private long fixedAreaPtr;
  private long fixedAreaMemorySize = 0;
  private int limitedVecSize;
  private final int maxVectorSize;

  private final long [] nullVectorsAddrs;
  private final long [] vectorsAddrs;

  private long variableAreaMemorySize = 0;
  private long [] currentPageAddrs;
  private long [] nextPtr;
  private List<Long> pageReferences;

  public VecRowBlock(Schema schema, int maxVectorSize) {
    this.schema = schema;
    this.maxVectorSize = maxVectorSize;

    nullVectorsAddrs = new long[schema.size()];
    vectorsAddrs = new long[schema.size()];

    boolean variableAreaNeededd = false;
    for (Column column : schema.getColumns()) {
      variableAreaNeededd |= !TypeUtil.isFixedSize(column.getDataType());
    }

    if (variableAreaNeededd) {
      currentPageAddrs = new long[schema.size()];
      nextPtr = new long[schema.size()];
      pageReferences = Lists.newArrayList();
    }

    init(schema, variableAreaNeededd);
  }

  public void allocateFixedArea() {
    fixedAreaPtr = unsafe.allocateMemory(fixedAreaMemorySize);
    UnsafeUtil.unsafe.setMemory(fixedAreaPtr, fixedAreaMemorySize, (byte) 0x0);
  }

  public long totalMemory() {
    return fixedAreaMemorySize + variableAreaMemorySize;
  }

  public void setLimitedVecSize(int num) {
    this.limitedVecSize = num;
  }

  public int limitedVecSize() {
    return limitedVecSize;
  }

  public int maxVecSize() {
    return maxVectorSize;
  }

  public long fixedAreaMemory() {
    return fixedAreaMemorySize;
  }

  public long variableAreaMemory() {
    return variableAreaMemorySize;
  }

  // one indicates a byte in address system.
  public void init(Schema schema, boolean variableAreaInitNeeded) {
    long totalSize = 0;
    long eachNullHeaderBytes = computeNullHeaderBytesPerColumn(maxVectorSize);
    long totalNullHeaderBytes = computeNullHeaderBytes(maxVectorSize, schema.size());
    totalSize += totalNullHeaderBytes;

    for (int i = 0; i < schema.size(); i++) {
      Column column = schema.getColumn(i);
      totalSize += UnsafeUtil.computeAlignedSize(TypeUtil.sizeOf(column.getDataType(), maxVectorSize));
    }
    fixedAreaMemorySize = totalSize;
    allocateFixedArea();

    // set addresses to null vectors
    for (int i = 0; i < schema.size(); i++) {
      if (i == 0) {
        nullVectorsAddrs[i] = fixedAreaPtr;
      } else {
        nullVectorsAddrs[i] = nullVectorsAddrs[i - 1] + eachNullHeaderBytes;

        assert (nullVectorsAddrs[i] - nullVectorsAddrs[i - 1]) == (Math.ceil(maxVectorSize / SizeOf.BITS_PER_BYTE));
      }
    }

    long perVecSize;
    for (int i = 0; i < schema.size(); i++) {
      if (i == 0) {
        vectorsAddrs[i] = fixedAreaPtr + totalNullHeaderBytes;
      } else {
        Column prevColumn = schema.getColumn(i - 1);
        perVecSize = UnsafeUtil.computeAlignedSize(TypeUtil.sizeOf(prevColumn.getDataType(), maxVectorSize));
        vectorsAddrs[i] = vectorsAddrs[i - 1] + perVecSize;
      }
    }

    initializeMemory();
    if (variableAreaInitNeeded) {
      initVariableArea();
    }

    long assertMem = nullVectorsAddrs[0];
    long remainMemory = totalNullHeaderBytes;
    while(remainMemory > 0) {
      assert unsafe.getLong(assertMem) == (0xFFFFFFFF << 32 | 0xFFFFFFFF);
      assertMem += SizeOf.SIZE_OF_LONG;
      remainMemory -= SizeOf.SIZE_OF_LONG;
    }
  }

  private void initializeMemory() {
    // initialize null headers
    unsafe.setMemory(nullVectorsAddrs[0], computeNullHeaderBytes(maxVectorSize, schema.size()), (byte) 0xFF);

    // initializes boolean type vector(s)
    for (int i = 0; i < schema.size(); i++) {
      if (schema.getColumn(i).getDataType().getType() == TajoDataTypes.Type.BOOLEAN) {
        long vecSize = TypeUtil.sizeOf(schema.getColumn(i).getDataType(), maxVectorSize);
        unsafe.setMemory(vectorsAddrs[i], vecSize, (byte) 0x00);
      }
    }
  }

  private void initVariableArea() {
    for (int i = 0; i < schema.size(); i++) {
      Column column = schema.getColumn(i);
      if (!TypeUtil.isFixedSize(column.getDataType())) {
        currentPageAddrs[i] = createPage();
        nextPtr[i] = currentPageAddrs[i];
      }
    }
    variableAreaMemorySize = 0;
  }

  public long getValueVecPtr(int columnIdx) {
    return vectorsAddrs[columnIdx];
  }

  public long getNullVecPtr(int columnIdx) {
    return nullVectorsAddrs[columnIdx];
  }

  public void setNull(int columnIdx, int rowIdx) {
    unsetBitToBitMap(nullVectorsAddrs[columnIdx], rowIdx);
  }

  public void unsetNull(int columnIdx, int rowIdx) {
    setBitToBitMap(nullVectorsAddrs[columnIdx], rowIdx);
  }

  public void setNull(int columnIdx, int rowIdx, int nullValue) {
    if (nullValue != 0) {
      setBitToBitMap(nullVectorsAddrs[columnIdx], rowIdx);
    } else {
      unsetBitToBitMap(nullVectorsAddrs[columnIdx], rowIdx);
    }
  }

  public int getNullFlag(int columnIdx, int rowIdx) {
    return getBitFromBitMap(nullVectorsAddrs[columnIdx], rowIdx);
  }

  public void putBool(int columnIdx, int rowIdx, boolean bool) {
    putBool(columnIdx, rowIdx, bool ? 1 : 0);
  }

  public void putBool(int columnIdx, int rowIdx, int bool) {
    if (bool != 0) {
      setBitToBitMap(vectorsAddrs[columnIdx], rowIdx);
    } else {
      unsetBitToBitMap(vectorsAddrs[columnIdx], rowIdx);
    }
  }

  public int getBool(int columnIdx, int rowIdx) {
    return getBitFromBitMap(vectorsAddrs[columnIdx], rowIdx);
  }

  public final void setBitToBitMap(final long bitVecAddr, int rowIdx) {
    int chunkId = rowIdx / SizeOf.BITS_PER_WORD;
    long offset = rowIdx % SizeOf.BITS_PER_WORD;
    long address = bitVecAddr + (chunkId * 8);
    long nullFlagChunk = unsafe.getLong(address);
    nullFlagChunk = (nullFlagChunk | (1L << offset));
    unsafe.putLong(address, nullFlagChunk);
  }

  public final void unsetBitToBitMap(final long bitVecAddr, int rowIdx) {
    int chunkId = rowIdx / SizeOf.BITS_PER_WORD;
    long offset = rowIdx % SizeOf.BITS_PER_WORD;
    long address = bitVecAddr + (chunkId * 8);
    long nullFlagChunk = unsafe.getLong(address);
    nullFlagChunk = (nullFlagChunk & ~(1L << offset));
    unsafe.putLong(address, nullFlagChunk);
  }

  public final int getBitFromBitMap(final long bitVectorAddr, int rowIdx) {
    int chunkId = rowIdx / SizeOf.BITS_PER_WORD;
    long offset = rowIdx % SizeOf.BITS_PER_WORD;
    long address = bitVectorAddr + (chunkId * 8);
    long nullFlagChunk = unsafe.getLong(address);
    return (int) ((nullFlagChunk >> offset) & 1L);
  }

  public short getInt2(int columnIdx, int rowIdx) {
    return unsafe.getShort(vectorsAddrs[columnIdx] + (rowIdx * SizeOf.SIZE_OF_SHORT));
  }

  public void putInt2(int columnIdx, int rowIdx, short val) {
    unsafe.putShort(vectorsAddrs[columnIdx] + (rowIdx * SizeOf.SIZE_OF_SHORT), val);
  }

  public void putInt4(int columnIdx, int rowIdx, int val) {
    unsafe.putInt(vectorsAddrs[columnIdx] + (rowIdx * SizeOf.SIZE_OF_INT), val);
  }

  public int getInt4(int columnIdx, int rowIdx) {
    return unsafe.getInt(vectorsAddrs[columnIdx] + (rowIdx * SizeOf.SIZE_OF_INT));
  }

  public void putInt8(int columnIdx, int rowIdx, long val) {
    unsafe.putLong(vectorsAddrs[columnIdx] + (rowIdx * SizeOf.SIZE_OF_LONG), val);
  }

  public long getInt8(int columnIdx, int rowIdx) {
    return unsafe.getLong(vectorsAddrs[columnIdx] + (rowIdx * SizeOf.SIZE_OF_LONG));
  }

  public void putFloat4(int columnIdx, int rowIdx, float val) {
    unsafe.putFloat(vectorsAddrs[columnIdx] + (rowIdx * SizeOf.SIZE_OF_FLOAT), val);
  }

  public float getFloat4(int columnIdx, int rowIdx) {
    return unsafe.getFloat(vectorsAddrs[columnIdx] + (rowIdx * SizeOf.SIZE_OF_FLOAT));
  }

  public void putFloat8(int columnIdx, int rowIdx, double val) {
    unsafe.putDouble(vectorsAddrs[columnIdx] + (rowIdx * SizeOf.SIZE_OF_DOUBLE), val);
  }

  public double getFloat8(int columnIdx, int rowIdx) {
    return unsafe.getDouble(vectorsAddrs[columnIdx] + (rowIdx * SizeOf.SIZE_OF_DOUBLE));
  }

  public void putText(int columnIdx, int rowIdx, String string) {
    putText(columnIdx, rowIdx, string.getBytes());
  }

  public void putText(int columnIdx, int rowIdx, byte [] val) {
    putText(columnIdx, rowIdx, val, 0, val.length);
  }

  public void putText(int columnIdx, int rowIdx, byte [] val, int srcPos, int length) {
    long currentPage = currentPageAddrs[columnIdx];

    long ptr = currentPage;
    long nextPtr = this.nextPtr[columnIdx];
    long usedMemory = nextPtr - ptr;

    if (PAGE_SIZE - usedMemory < val.length) { // create newly page
      ptr = createPage();
      currentPage = ptr;
      currentPageAddrs[columnIdx] = currentPage;
      nextPtr = currentPage;
    }

    unsafe.putShort(nextPtr, (short) val.length);
    nextPtr += SizeOf.SIZE_OF_SHORT;
    UnsafeUtil.bzero(nextPtr, length);
    unsafe.copyMemory(val, Unsafe.ARRAY_BYTE_BASE_OFFSET + srcPos, null, nextPtr, length);
    unsafe.putAddress(vectorsAddrs[columnIdx] + (Unsafe.ADDRESS_SIZE * rowIdx), nextPtr - SizeOf.SIZE_OF_SHORT);
    this.nextPtr[columnIdx] = nextPtr + length;
  }

  public void putText(int columnIdx, int rowIdx, ByteBuffer byteBuf) {
    long currentPage = currentPageAddrs[columnIdx];

    long ptr = currentPage;
    long nextPtr = this.nextPtr[columnIdx];
    long usedMemory = nextPtr - ptr;

    short length = (short) byteBuf.limit();

    if (PAGE_SIZE - usedMemory < length) { // create newly page
      ptr = createPage();
      currentPage = ptr;
      currentPageAddrs[columnIdx] = currentPage;
      nextPtr = currentPage;
    }

    unsafe.putShort(nextPtr, length);
    nextPtr += SizeOf.SIZE_OF_SHORT;
    unsafe.setMemory(nextPtr, length, (byte) 0x00);
    unsafe.copyMemory(null, ((DirectBuffer)byteBuf).address(), null, nextPtr, length);
    unsafe.putAddress(vectorsAddrs[columnIdx] + (Unsafe.ADDRESS_SIZE * rowIdx), nextPtr - SizeOf.SIZE_OF_SHORT);
    this.nextPtr[columnIdx] = nextPtr + length;
  }

  public int getText(int columnIdx, int rowIdx, byte[] val, int srcPos, int length) {
    long ptr = vectorsAddrs[columnIdx];
    long dataPtr = unsafe.getAddress(ptr + ((rowIdx) * Unsafe.ADDRESS_SIZE));

    int strLen = unsafe.getShort(dataPtr);
    dataPtr += SizeOf.SIZE_OF_SHORT;
    int actualLen = strLen < length ? strLen : length;
    unsafe.copyMemory(null, dataPtr, val, unsafe.ARRAY_BYTE_BASE_OFFSET + srcPos, actualLen);
    return actualLen;
  }

  public int getText(int columnIdx, int rowIdx, byte [] buf) {
    return getText(columnIdx, rowIdx, buf, 0, buf.length);
  }

  public int getText(int columnIdx, int rowIdx, ByteBuffer buf) {
    long ptr = vectorsAddrs[columnIdx];
    long dataPtr = unsafe.getAddress(ptr + ((rowIdx) * Unsafe.ADDRESS_SIZE));

    System.out.println("get length >>>>>> ");
    int strLen = unsafe.getShort(dataPtr);
    dataPtr += SizeOf.SIZE_OF_SHORT;
    System.out.println("get length finish >>>>>>");

    DirectBuffer directBuffer = (DirectBuffer) buf;
    unsafe.copyMemory(null, dataPtr, null, directBuffer.address(), strLen);
    buf.position(strLen);
    buf.limit(strLen);
    return strLen;
  }

  public byte [] getBytes(int columnIdx, int rowIdx) {
    long ptr = vectorsAddrs[columnIdx];
    long dataPtr = unsafe.getAddress(ptr + ((rowIdx) * Unsafe.ADDRESS_SIZE));

    int strLen = unsafe.getShort(dataPtr);
    dataPtr += SizeOf.SIZE_OF_SHORT;

    byte [] bytes = new byte[strLen];
    unsafe.copyMemory(null, dataPtr, bytes, unsafe.ARRAY_BYTE_BASE_OFFSET, strLen);
    return bytes;
  }

  public String getString(int columnIdx, int rowIdx) {
    long ptr = vectorsAddrs[columnIdx];
    long dataPtr = unsafe.getAddress(ptr + ((rowIdx) * Unsafe.ADDRESS_SIZE));

    int strLen = unsafe.getShort(dataPtr);
    dataPtr += SizeOf.SIZE_OF_SHORT;

    byte [] bytes = new byte[strLen];
    unsafe.copyMemory(null, dataPtr, bytes, unsafe.ARRAY_BYTE_BASE_OFFSET, strLen);
    return new String(bytes);
  }

  public static final int PAGE_SIZE = 4096;

  /**
   * [next address, 0x0 last page. Otherwise, has next page (8 bytes) ] [PAGE payload (4096 by default) ]
   *
   * @return
   */
  private long createPage() {
    long pagePtr = unsafe.allocateMemory(PAGE_SIZE);
    pageReferences.add(pagePtr);
    variableAreaMemorySize += PAGE_SIZE;
    return pagePtr;
  }

  public void clear() {
    if (pageReferences != null && pageReferences.size() > 0) {
      freeVariableAreas();
      initVariableArea();
    }

    initializeMemory();
    setLimitedVecSize(0);
  }

  private void freeVariableAreas() {
    if (pageReferences != null && pageReferences.size() > 0) {
      for (long ref : pageReferences) {
        unsafe.freeMemory(ref);
      }
      pageReferences.clear();
    }
  }

  public void free() {
    freeVariableAreas();
    unsafe.freeMemory(fixedAreaPtr);
  }

  public static long allocateNullVector(int vectorSize) {
    long nBytes = computeNullHeaderBytesPerColumn(vectorSize);
    long ptr = UnsafeUtil.alloc(nBytes);
    unsafe.setMemory(ptr, nBytes, (byte) 0xFF);
    return ptr;
  }

  private static long computeNullHeaderBytes(int vectorSize, int columnNum) {
    return computeNullHeaderBytesPerColumn(vectorSize) * columnNum;
  }

  public static long computeNullHeaderBytesPerColumn(int vectorSize) {
    return UnsafeUtil.computeAlignedSize((long) Math.ceil(vectorSize / SizeOf.BITS_PER_BYTE));
  }
}
