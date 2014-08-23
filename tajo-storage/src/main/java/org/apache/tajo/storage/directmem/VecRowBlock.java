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

import com.google.common.collect.Lists;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.common.TajoDataTypes;
import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;
import java.util.List;

public class VecRowBlock {
  private static final Unsafe unsafe = UnsafeUtil.unsafe;

  // Schema
  public TajoDataTypes.Type [] types;
  public int [] maxLengths;

  // Fixed Areas
  private long fixedAreaPtr;
  private long fixedAreaMemorySize = 0;
  private int limitedVecSize;
  private final int maxVectorSize;

  // Null and Vector Addrs
  private final long [] nullVectorsAddrs;
  private final long [] vectorsAddrs;

  // Variable Areas
  private long variableAreaMemorySize = 0;
  private long [] currentPageAddrs;
  private long [] nextPtr;
  private List<Long> pageReferences;

  public VecRowBlock(Schema schema, int maxVectorSize) {
    this.maxVectorSize = maxVectorSize;

    nullVectorsAddrs = new long[schema.size()];
    vectorsAddrs = new long[schema.size()];

    boolean variableAreaNeededd = false;
    for (Column column : schema.getColumns()) {
      variableAreaNeededd |= !TypeUtil.isFixedSize(column.getDataType().getType());
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

    this.types = new TajoDataTypes.Type[schema.size()];
    this.maxLengths = new int[schema.size()];
    for (int i = 0; i < schema.size(); i++) {
      Column c = schema.getColumn(i);
      types[i] = c.getDataType().getType();
      maxLengths[i] = c.getDataType().getLength();
    }


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

    clearFixedArea();
    if (variableAreaInitNeeded) {
      initVariableArea();
    }

    long assertMem = nullVectorsAddrs[0];
    long remainMemory = totalNullHeaderBytes;
    while(remainMemory > 0) {
      assert unsafe.getLong(assertMem) == 0x0000000000000000L;
      assertMem += SizeOf.SIZE_OF_LONG;
      remainMemory -= SizeOf.SIZE_OF_LONG;
    }
  }

  private void clearFixedArea() {
    // initialize null headers
    unsafe.setMemory(nullVectorsAddrs[0], computeNullHeaderBytes(maxVectorSize, types.length), (byte) 0x00);

    // initializes boolean type vector(s)
    for (int i = 0; i < types.length; i++) {
      if (types[i] == TajoDataTypes.Type.BOOLEAN) {
        long vecSize = TypeUtil.sizeOf(types[i], maxLengths[i], maxVectorSize);
        unsafe.setMemory(vectorsAddrs[i], vecSize, (byte) 0x00);
      }
    }
  }

  private void initVariableArea() {
    for (int i = 0; i < types.length; i++) {
      if (!TypeUtil.isFixedSize(types[i])) {
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

  public void setNullBit(int columnIdx, int rowIdx) {
    setToBitVec(nullVectorsAddrs[columnIdx], rowIdx);
  }

  public void unsetNullBit(int columnIdx, int rowIdx) {
    unsetBitToBitMap(nullVectorsAddrs[columnIdx], rowIdx);
  }

  public void setNull(int columnIdx, int rowIdx, int nullValue) {
    if (nullValue != 0) {
      setToBitVec(nullVectorsAddrs[columnIdx], rowIdx);
    } else {
      unsetBitToBitMap(nullVectorsAddrs[columnIdx], rowIdx);
    }
  }

  public int getNullBit(int columnIdx, int rowIdx) {
    return getBitFromBitMap(nullVectorsAddrs[columnIdx], rowIdx);
  }

  public void putBool(int columnIdx, int rowIdx, boolean bool) {
    putBool(columnIdx, rowIdx, bool ? 1 : 0);
  }

  public void putBool(int columnIdx, int rowIdx, int bool) {
    setNullBit(columnIdx, rowIdx);
    if (bool != 0) {
      setToBitVec(vectorsAddrs[columnIdx], rowIdx);
    } else {
      unsetBitToBitMap(vectorsAddrs[columnIdx], rowIdx);
    }
  }

  public int getBool(int columnIdx, int rowIdx) {
    return getBitFromBitMap(vectorsAddrs[columnIdx], rowIdx);
  }

  public final void setToBitVec(final long bitVecAddr, int rowIdx) {
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
    setNullBit(columnIdx, rowIdx);
    unsafe.putShort(vectorsAddrs[columnIdx] + (rowIdx * SizeOf.SIZE_OF_SHORT), val);
  }

  public void putInt4(int columnIdx, int rowIdx, int val) {
    setNullBit(columnIdx, rowIdx);
    unsafe.putInt(vectorsAddrs[columnIdx] + (rowIdx * SizeOf.SIZE_OF_INT), val);
  }

  public int getInt4(int columnIdx, int rowIdx) {
    return unsafe.getInt(vectorsAddrs[columnIdx] + (rowIdx * SizeOf.SIZE_OF_INT));
  }

  public void putInt8(int columnIdx, int rowIdx, long val) {
    setNullBit(columnIdx, rowIdx);
    unsafe.putLong(vectorsAddrs[columnIdx] + (rowIdx * SizeOf.SIZE_OF_LONG), val);
  }

  public long getInt8(int columnIdx, int rowIdx) {
    return unsafe.getLong(vectorsAddrs[columnIdx] + (rowIdx * SizeOf.SIZE_OF_LONG));
  }

  public void putFloat4(int columnIdx, int rowIdx, float val) {
    setNullBit(columnIdx, rowIdx);
    unsafe.putFloat(vectorsAddrs[columnIdx] + (rowIdx * SizeOf.SIZE_OF_FLOAT), val);
  }

  public float getFloat4(int columnIdx, int rowIdx) {
    return unsafe.getFloat(vectorsAddrs[columnIdx] + (rowIdx * SizeOf.SIZE_OF_FLOAT));
  }

  public void putFloat8(int columnIdx, int rowIdx, double val) {
    setNullBit(columnIdx, rowIdx);
    unsafe.putDouble(vectorsAddrs[columnIdx] + (rowIdx * SizeOf.SIZE_OF_DOUBLE), val);
  }

  public double getFloat8(int columnIdx, int rowIdx) {
    return unsafe.getDouble(vectorsAddrs[columnIdx] + (rowIdx * SizeOf.SIZE_OF_DOUBLE));
  }

  public void putFixedBytes(int columnIdx, int rowIdx, byte[] bytes) {
    setNullBit(columnIdx, rowIdx);
    unsafe.copyMemory(bytes, Unsafe.ARRAY_BYTE_BASE_OFFSET, null,
        vectorsAddrs[columnIdx] + (rowIdx * maxLengths[columnIdx]), maxLengths[columnIdx]);
  }

  public void getFixedText(int columnIdx, int rowIdx, long destPtr) {
    unsafe.copyMemory(null, vectorsAddrs[columnIdx] + (rowIdx * maxLengths[columnIdx]),
        null, destPtr, maxLengths[columnIdx]);
  }

  public void getFixedText(int columnIdx, int rowIdx, UnsafeBuf buf, int offset) {
    unsafe.copyMemory(null, vectorsAddrs[columnIdx] + (rowIdx * maxLengths[columnIdx]),
        null, buf.address + offset, maxLengths[columnIdx]);
  }

  public void getFixedText(int columnIdx, int rowIdx, byte [] bytes) {
    unsafe.copyMemory(null, vectorsAddrs[columnIdx] + (rowIdx * maxLengths[columnIdx]),
        bytes, Unsafe.ARRAY_BYTE_BASE_OFFSET, maxLengths[columnIdx]);
  }

  public byte [] getFixedText(int columnIdx, int rowIdx) {
    byte [] bytes = new byte[maxLengths[columnIdx]];
    unsafe.copyMemory(null, vectorsAddrs[columnIdx] + (rowIdx * maxLengths[columnIdx]),
        bytes, Unsafe.ARRAY_BYTE_BASE_OFFSET, maxLengths[columnIdx]);
    return bytes;
  }

  public void putVarText(int columnIdx, int rowIdx, String string) {
    putVarText(columnIdx, rowIdx, string.getBytes());
  }

  public void putVarText(int columnIdx, int rowIdx, byte[] val) {
    putVarText(columnIdx, rowIdx, val, 0, val.length);
  }

  public void putVarText(int columnIdx, int rowIdx, byte[] val, int srcPos, int length) {
    setNullBit(columnIdx, rowIdx);

    long currentPage = currentPageAddrs[columnIdx];

    long ptr = currentPage;
    long nextPtr = this.nextPtr[columnIdx];
    long usedMemory = nextPtr - ptr;

    // val.length + length field
    if (PAGE_SIZE - usedMemory < (val.length + 2)) { // create newly page
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

  public void putVarText(int columnIdx, int rowIdx, ByteBuffer byteBuf) {
    setNullBit(columnIdx, rowIdx);

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

  public int getVarText(int columnIdx, int rowIdx, byte[] val, int srcPos, int length) {
    setNullBit(columnIdx, rowIdx);

    long ptr = vectorsAddrs[columnIdx];
    long dataPtr = unsafe.getAddress(ptr + ((rowIdx) * Unsafe.ADDRESS_SIZE));

    int strLen = unsafe.getShort(dataPtr);
    dataPtr += SizeOf.SIZE_OF_SHORT;
    int actualLen = strLen < length ? strLen : length;
    unsafe.copyMemory(null, dataPtr, val, unsafe.ARRAY_BYTE_BASE_OFFSET + srcPos, actualLen);
    return actualLen;
  }

  public int getVarText(int columnIdx, int rowIdx, byte[] buf) {
    return getVarText(columnIdx, rowIdx, buf, 0, buf.length);
  }

  public int getVarText(int columnIdx, int rowIdx, ByteBuffer buf) {
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

  public byte [] getVarBytes(int columnIdx, int rowIdx) {
    long ptr = vectorsAddrs[columnIdx];
    long dataPtr = unsafe.getAddress(ptr + ((rowIdx) * Unsafe.ADDRESS_SIZE));

    int strLen = unsafe.getShort(dataPtr);
    dataPtr += SizeOf.SIZE_OF_SHORT;

    byte [] bytes = new byte[strLen];
    unsafe.copyMemory(null, dataPtr, bytes, unsafe.ARRAY_BYTE_BASE_OFFSET, strLen);
    return bytes;
  }

  public String getVarcharAsString(int columnIdx, int rowIdx) {
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

    clearFixedArea();
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
    unsafe.setMemory(ptr, nBytes, (byte) 0x00);
    return ptr;
  }

  private static long computeNullHeaderBytes(int vectorSize, int columnNum) {
    return computeNullHeaderBytesPerColumn(vectorSize) * columnNum;
  }

  public static long computeNullHeaderBytesPerColumn(int vectorSize) {
    return UnsafeUtil.computeAlignedSize((long) Math.ceil(vectorSize / SizeOf.BITS_PER_BYTE));
  }
}
