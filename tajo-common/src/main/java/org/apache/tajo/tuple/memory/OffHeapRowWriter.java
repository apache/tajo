/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.tuple.memory;

import io.netty.util.internal.PlatformDependent;
import org.apache.tajo.common.TajoDataTypes.DataType;
import org.apache.tajo.datum.IntervalDatum;
import org.apache.tajo.datum.ProtobufDatum;
import org.apache.tajo.datum.TextDatum;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.util.SizeOf;
import org.apache.tajo.util.TUtil;

/**
 *
 * Row Record Structure
 *
 * | row length (4 bytes) | field 1 offset | field 2 offset | ... | field N offset| field 1 | field 2| ... | field N |
 *                              4 bytes          4 bytes               4 bytes
 *
 */
public abstract class OffHeapRowWriter implements RowWriter {
  /** record capacity + offset list */
  private final int headerSize;

  private final DataType[] dataTypes;

  private int curFieldIdx;
  private int curFieldOffset;
  private int curOffset;

  public OffHeapRowWriter(final DataType[] dataTypes) {
    this.dataTypes = dataTypes;
    this.headerSize = SizeOf.SIZE_OF_INT * (dataTypes.length + 1);
    this.curFieldOffset = SizeOf.SIZE_OF_INT;
  }

  /**
   * Current memory address of the row
   *
   * @return The memory address
   */
  public long recordStartAddr() {
    return currentAddr() - curOffset;
  }

  /**
   * Memory address that point to the first byte of the buffer
   *
   * @return The memory address
   */
  private long currentAddr() {
    return address() + position();
  }

  /**
   * Current memory address of the buffer
   *
   * @return The memory address
   */
  public abstract long address();

  public abstract void ensureSize(int size);

  public int offset() {
    return position();
  }

  /**
   * Current position
   *
   * @return The position
   */
  public abstract int position();

  /**
   * Forward the address;
   *
   * @param length Length to be forwarded
   */
  public abstract void forward(int length);


  @Override
  public void clear() {
    curOffset = 0;
    curFieldIdx = 0;
    curFieldOffset = SizeOf.SIZE_OF_INT;
  }

  @Override
  public DataType[] dataTypes() {
    return dataTypes;
  }

  @Override
  public boolean startRow() {
    ensureSize(headerSize);
    curOffset = headerSize;
    curFieldOffset = SizeOf.SIZE_OF_INT;
    curFieldIdx = 0;
    forward(headerSize);
    return true;
  }

  @Override
  public void endRow() {
    long rowHeaderPos = recordStartAddr();
    // curOffset is equivalent to a byte length of this row.
    PlatformDependent.putInt(rowHeaderPos, curOffset);

    //forward (record offset + fields offset)
    rowHeaderPos += SizeOf.SIZE_OF_INT + curFieldOffset;
    // set remain header field length
    for (int i = curFieldIdx; i < dataTypes.length; i++) {
      PlatformDependent.putInt(rowHeaderPos, MemoryRowBlock.NULL_FIELD_OFFSET);
      rowHeaderPos += SizeOf.SIZE_OF_INT;
    }
  }

  @Override
  public void skipField() {
    // set header field length
    putFieldHeader(currentAddr(), MemoryRowBlock.NULL_FIELD_OFFSET);
  }

  /**
   * set current buffer position and forward field length
   * @param fieldLength
   */
  private void forwardField(int fieldLength) {
    forward(fieldLength);
    curOffset += fieldLength;

  }

  private void putFieldHeader(long currentAddr, int length) {
    long currentHeaderAddr = currentAddr - curOffset + curFieldOffset;

    // set header field length
    PlatformDependent.putInt(currentHeaderAddr, length);
    curFieldOffset += SizeOf.SIZE_OF_INT;
    curFieldIdx++;
  }

  @Override
  public void putByte(byte val) {
    ensureSize(SizeOf.SIZE_OF_BYTE);
    long addr = currentAddr();

    PlatformDependent.putByte(addr, val);
    putFieldHeader(addr, curOffset);
    forwardField(SizeOf.SIZE_OF_BYTE);
  }

  @Override
  public void putBool(boolean val) {
    ensureSize(SizeOf.SIZE_OF_BOOL);
    long addr = currentAddr();

    PlatformDependent.putByte(addr, (byte) (val ? 0x01 : 0x00));
    putFieldHeader(addr, curOffset);
    forwardField(SizeOf.SIZE_OF_BOOL);
  }

  @Override
  public void putInt2(short val) {
    ensureSize(SizeOf.SIZE_OF_SHORT);
    long addr = currentAddr();

    PlatformDependent.putShort(addr, val);
    putFieldHeader(addr, curOffset);
    forwardField(SizeOf.SIZE_OF_SHORT);
  }

  @Override
  public void putInt4(int val) {
    ensureSize(SizeOf.SIZE_OF_INT);
    long addr = currentAddr();

    PlatformDependent.putInt(addr, val);
    putFieldHeader(addr, curOffset);
    forwardField(SizeOf.SIZE_OF_INT);
  }

  @Override
  public void putInt8(long val) {
    ensureSize(SizeOf.SIZE_OF_LONG);
    long addr = currentAddr();

    PlatformDependent.putLong(addr, val);
    putFieldHeader(addr, curOffset);
    forwardField(SizeOf.SIZE_OF_LONG);
  }

  @Override
  public void putFloat4(float val) {
    ensureSize(SizeOf.SIZE_OF_INT);
    long addr = currentAddr();

    PlatformDependent.putInt(addr, Float.floatToRawIntBits(val));
    putFieldHeader(addr, curOffset);
    forwardField(SizeOf.SIZE_OF_INT);
  }

  @Override
  public void putFloat8(double val) {
    ensureSize(SizeOf.SIZE_OF_LONG);
    long addr = currentAddr();

    PlatformDependent.putLong(addr, Double.doubleToRawLongBits(val));
    putFieldHeader(addr, curOffset);
    forwardField(SizeOf.SIZE_OF_LONG);
  }

  @Override
  public void putText(String val) {
    byte[] bytes = val.getBytes(TextDatum.DEFAULT_CHARSET);
    putText(bytes);
  }

  @Override
  public void putText(byte[] val) {
    putBlob(val);
  }

  @Override
  public void putBlob(byte[] val) {
    int bytesLen = val.length;
    int fieldLen = SizeOf.SIZE_OF_INT + bytesLen;

    ensureSize(fieldLen);
    long addr = currentAddr();

    PlatformDependent.putInt(addr, bytesLen);
    PlatformDependent.copyMemory(val, 0, addr + SizeOf.SIZE_OF_INT, bytesLen);
    putFieldHeader(addr, curOffset);
    forwardField(fieldLen);
  }

  @Override
  public void putTimestamp(long val) {
    putInt8(val);
  }

  @Override
  public void putDate(int val) {
    putInt4(val);
  }

  @Override
  public void putTime(long val) {
    putInt8(val);
  }

  @Override
  public void putInterval(IntervalDatum val) {
    ensureSize(SizeOf.SIZE_OF_INT + SizeOf.SIZE_OF_LONG);
    long addr = currentAddr();

    PlatformDependent.putInt(addr, val.getMonths());
    PlatformDependent.putLong(addr + SizeOf.SIZE_OF_INT, val.getMilliSeconds());
    putFieldHeader(addr, curOffset);
    forwardField(SizeOf.SIZE_OF_INT + SizeOf.SIZE_OF_LONG);
  }

  @Override
  public void putInet4(int val) {
    putInt4(val);
  }

  @Override
  public void putProtoDatum(ProtobufDatum val) {
    putBlob(val.asByteArray());
  }

  @Override
  public void addTuple(Tuple tuple) {
    if (tuple instanceof UnSafeTuple) {
      UnSafeTuple unSafeTuple = TUtil.checkTypeAndGet(tuple, UnSafeTuple.class);
      int length = unSafeTuple.getLength();
      ensureSize(length);
      PlatformDependent.copyMemory(unSafeTuple.address(), address() + position(), length);
      forward(length);
    } else {
      OffHeapRowBlockUtils.convert(tuple, this);
    }
  }
}
