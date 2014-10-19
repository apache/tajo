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

package org.apache.tajo.tuple.offheap;

import com.google.common.base.Preconditions;
import org.apache.tajo.annotation.UsedByJIT;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.datum.IntervalDatum;
import org.apache.tajo.datum.ProtobufDatum;
import org.apache.tajo.datum.TextDatum;
import org.apache.tajo.util.SizeOf;
import org.apache.tajo.util.UnsafeUtil;
import sun.misc.Unsafe;

/**
 *
 * Row Record Structure
 *
 * | row length (4 bytes) | field 1 offset | field 2 offset | ... | field N offset| field 1 | field 2| ... | field N |
 *                              4 bytes          4 bytes               4 bytes
 *
 */
public abstract class OffHeapRowWriter implements RowWriter {
  /** record size + offset list */
  private final int headerSize;
  /** field offsets */
  private final int [] fieldOffsets;
  private final TajoDataTypes.DataType [] dataTypes;

  private int curFieldIdx;
  private int curOffset;

  public OffHeapRowWriter(final TajoDataTypes.DataType [] dataTypes) {
    this.dataTypes = dataTypes;
    fieldOffsets = new int[dataTypes.length];
    headerSize = SizeOf.SIZE_OF_INT * (dataTypes.length + 1);
  }

  public void clear() {
    curOffset = 0;
    curFieldIdx = 0;
  }

  public long recordStartAddr() {
    return address() + position();
  }

  public abstract long address();

  public abstract void ensureSize(int size);

  public int offset() {
    return curOffset;
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
  public TajoDataTypes.DataType[] dataTypes() {
    return dataTypes;
  }

  public boolean startRow() {
    curOffset = headerSize;
    curFieldIdx = 0;
    return true;
  }

  public void endRow() {
    long rowHeaderPos = address() + position();
    OffHeapMemory.UNSAFE.putInt(rowHeaderPos, curOffset);
    rowHeaderPos += SizeOf.SIZE_OF_INT;

    for (int i = 0; i < curFieldIdx; i++) {
      OffHeapMemory.UNSAFE.putInt(rowHeaderPos, fieldOffsets[i]);
      rowHeaderPos += SizeOf.SIZE_OF_INT;
    }
    for (int i = curFieldIdx; i < dataTypes.length; i++) {
      OffHeapMemory.UNSAFE.putInt(rowHeaderPos, OffHeapRowBlock.NULL_FIELD_OFFSET);
      rowHeaderPos += SizeOf.SIZE_OF_INT;
    }

    // rowOffset is equivalent to a byte length of this row.
    forward(curOffset);
  }

  public int currentField() {
    return curFieldIdx;
  }

  public void skipField() {
    fieldOffsets[curFieldIdx] = OffHeapRowBlock.NULL_FIELD_OFFSET;
    curFieldIdx++;
  }

  public void skipField(int num) {
    for (int i = curFieldIdx; i < num && i < dataTypes.length; i++) {
      fieldOffsets[curFieldIdx] = OffHeapRowBlock.NULL_FIELD_OFFSET;
      curFieldIdx++;
    }
  }

  @UsedByJIT
  private void forwardField() {
    fieldOffsets[curFieldIdx] = curOffset;
    curFieldIdx++;
  }

  @UsedByJIT
  public void putBool(boolean val) {
    ensureSize(SizeOf.SIZE_OF_BOOL);
    forwardField();

    OffHeapMemory.UNSAFE.putByte(recordStartAddr() + curOffset, (byte) (val ? 0x01 : 0x02));

    curOffset += SizeOf.SIZE_OF_BOOL;
  }

  @UsedByJIT
  public void putBool(byte val) {
    ensureSize(SizeOf.SIZE_OF_BOOL);
    forwardField();

    OffHeapMemory.UNSAFE.putByte(recordStartAddr() + curOffset, val);

    curOffset += SizeOf.SIZE_OF_BOOL;
  }

  @UsedByJIT
  public void putInt2(short val) {
    ensureSize(SizeOf.SIZE_OF_SHORT);
    forwardField();

    OffHeapMemory.UNSAFE.putShort(recordStartAddr() + curOffset, val);
    curOffset += SizeOf.SIZE_OF_SHORT;
  }

  @UsedByJIT
  public void putInt4(int val) {
    ensureSize(SizeOf.SIZE_OF_INT);
    forwardField();

    OffHeapMemory.UNSAFE.putInt(recordStartAddr() + curOffset, val);
    curOffset += SizeOf.SIZE_OF_INT;
  }

  @UsedByJIT
  public void putInt8(long val) {
    ensureSize(SizeOf.SIZE_OF_LONG);
    forwardField();

    OffHeapMemory.UNSAFE.putLong(recordStartAddr() + curOffset, val);
    curOffset += SizeOf.SIZE_OF_LONG;
  }

  @UsedByJIT
  public void putFloat4(float val) {
    ensureSize(SizeOf.SIZE_OF_FLOAT);
    forwardField();

    OffHeapMemory.UNSAFE.putFloat(recordStartAddr() + curOffset, val);
    curOffset += SizeOf.SIZE_OF_FLOAT;
  }

  @UsedByJIT
  public void putFloat8(double val) {
    ensureSize(SizeOf.SIZE_OF_DOUBLE);
    forwardField();

    OffHeapMemory.UNSAFE.putDouble(recordStartAddr() + curOffset, val);
    curOffset += SizeOf.SIZE_OF_DOUBLE;
  }

  @UsedByJIT
  public void putText(String val) {
    byte[] bytes = val.getBytes(TextDatum.DEFAULT_CHARSET);
    putText(bytes);
  }

  @UsedByJIT
  public void putText(byte[] val) {
    putText(val, 0, val.length);
  }

  @UsedByJIT
  public void putText(byte[] val, int offset, int length) {
    putBlob(val, offset, length);
  }

  @UsedByJIT
  public void copyTextFrom(UnSafeTuple tuple, int fieldIdx) {
    long address = tuple.getFieldAddr(fieldIdx);
    int strLen = OffHeapMemory.UNSAFE.getInt(address);
    address += SizeOf.SIZE_OF_INT;

    ensureSize(SizeOf.SIZE_OF_INT + strLen);
    forwardField();

    OffHeapMemory.UNSAFE.putInt(recordStartAddr() + curOffset, strLen);
    curOffset += SizeOf.SIZE_OF_INT;

    OffHeapMemory.UNSAFE.copyMemory(null, address, null, recordStartAddr() + curOffset, strLen);
    curOffset += strLen;
  }

  @UsedByJIT
  public void putBlob(byte[] val) {
    putBlob(val, 0, val.length);
  }

  @UsedByJIT
  public void putBlob(byte[] val, int offset, int length) {
    Preconditions.checkArgument(offset >= 0 && offset <= val.length);

    int bytesLen = length;

    ensureSize(SizeOf.SIZE_OF_INT + bytesLen);
    forwardField();

    OffHeapMemory.UNSAFE.putInt(recordStartAddr() + curOffset, bytesLen);
    curOffset += SizeOf.SIZE_OF_INT;

    OffHeapMemory.UNSAFE.copyMemory(val, UnsafeUtil.ARRAY_BYTE_BASE_OFFSET + offset, null,
        recordStartAddr() + curOffset, bytesLen);
    curOffset += bytesLen;
  }

  @UsedByJIT
  public void putTimestamp(long val) {
    putInt8(val);
  }

  @UsedByJIT
  public void putDate(int val) {
    putInt4(val);
  }

  @UsedByJIT
  public void putTime(long val) {
    putInt8(val);
  }

  @UsedByJIT
  public void putInterval(IntervalDatum val) {
    ensureSize(SizeOf.SIZE_OF_INT + SizeOf.SIZE_OF_LONG);
    forwardField();

    long offset = recordStartAddr() + curOffset;
    OffHeapMemory.UNSAFE.putInt(offset, val.getMonths());
    offset += SizeOf.SIZE_OF_INT;
    OffHeapMemory.UNSAFE.putLong(offset, val.getMilliSeconds());
    curOffset += SizeOf.SIZE_OF_INT + SizeOf.SIZE_OF_LONG;
  }

  @UsedByJIT
  public void putInet4(int val) {
    putInt4(val);
  }

  @UsedByJIT
  public void putProtoDatum(ProtobufDatum val) {
    putBlob(val.asByteArray());
  }
}
