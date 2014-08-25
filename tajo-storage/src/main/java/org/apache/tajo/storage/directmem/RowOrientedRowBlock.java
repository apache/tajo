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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.datum.IntervalDatum;
import org.apache.tajo.datum.TextDatum;
import org.apache.tajo.util.FileUtil;
import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;

import static org.apache.tajo.common.TajoDataTypes.Type;

public class RowOrientedRowBlock implements RowBlock, RowBlockWriter {
  private static final Log LOG = LogFactory.getLog(RowOrientedRowBlock.class);
  private static final Unsafe UNSAFE = UnsafeUtil.unsafe;

  private Type[] types;
  private int [] maxLengths;
  private int bytesLen;
  private ByteBuffer buffer;
  private long address;
  private int fieldIndexBytesLen;

  // Basic States
  private int totalRowNum;

  // Read States
  private int curRowIdx;
  private int curReadPos;

  // Write States --------------------
  private int curWritePos;
  private int rowOffset;
  private int curFieldIdx;
  private int [] fieldIndexes;

  // row state
  private long rowStartOffset;

  public RowOrientedRowBlock(Schema schema, int bytes) {
    this.buffer = ByteBuffer.allocateDirect(bytes);
    this.address = ((DirectBuffer) buffer).address();
    this.bytesLen = bytes;

    types = new Type[schema.size()];
    maxLengths = new int[schema.size()];
    for (int i = 0; i < schema.size(); i++) {
      TajoDataTypes.DataType dataType = schema.getColumn(i).getDataType();
      types[i] = dataType.getType();
      maxLengths[i] = dataType.getLength();
    }
    fieldIndexes = new int[schema.size()];
    fieldIndexBytesLen = SizeOf.SIZE_OF_INT * schema.size();

    curWritePos = 0;
  }

  public void free() {
    UnsafeUtil.free(this.buffer);
    this.buffer = null;
    this.address = 0;
    this.bytesLen = 0;
  }

  public long address() {
    return address;
  }

  public long totalMem() {
    return bytesLen;
  }

  public long remain() {
    return bytesLen - curWritePos - rowOffset;
  }

  public int totalRowNum() {
    return totalRowNum;
  }

  /**
   * Tuple for legacy
   * @return
   */
  public boolean next(UnSafeTuple tuple) {
    if (curRowIdx < totalRowNum) {

      long recordStartPtr = address + curReadPos;
      int recordLen = UNSAFE.getInt(recordStartPtr);
      tuple.set(recordStartPtr, types);

      curReadPos += recordLen;
      curRowIdx++;

      return true;
    } else {
      return false;
    }
  }

  public void resetRowCursor() {
    curReadPos = 0;
    curRowIdx = 0;
  }

  /**
   * Ensure that this buffer has enough remaining space to add the size.
   * Creates and copies to a new buffer if necessary
   *
   * @param size Size to add
   */
  private void ensureSize(int size) {
    if (remain() - size < 0) {
      int newBlockSize = UnsafeUtil.alignedSize(bytesLen << 1);
      ByteBuffer newByteBuf = ByteBuffer.allocateDirect(newBlockSize);
      long newAddress = ((DirectBuffer)newByteBuf).address();
      UNSAFE.copyMemory(this.address, newAddress, bytesLen);

      LOG.info("Increase DirectRowBlock to " + FileUtil.humanReadableByteCount(newBlockSize, false));

      // compute the relative position of current row start offset
      long relativeRowStartPos = this.rowStartOffset - this.address;

      // Update current write states
      rowStartOffset = newAddress + relativeRowStartPos;
      bytesLen = newBlockSize;
      UnsafeUtil.free(buffer);
      buffer = newByteBuf;
      address = newAddress;
    }
  }


  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  // RowBlockWriter Implementation
  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  // <Row Record Structure>
  //
  // | row length (4 bytes) | field 1 offset | field 2 offset | ... | field N offset| field 1 | field 2| ... | field N |
  //                               4 bytes          4 bytes               4 bytes

  public boolean startRow() {
    rowStartOffset = address + curWritePos;
    rowOffset = 0;
    rowOffset += 4; // skip row header
    rowOffset += fieldIndexBytesLen; // skip an array of field indices
    curFieldIdx = 0;
    return true;
  }

  public void endRow() {
    totalRowNum++;

    long rowHeaderPos = rowStartOffset;
    UNSAFE.putInt(rowHeaderPos, rowOffset);
    rowHeaderPos += SizeOf.SIZE_OF_INT;

    for (int i = 0; i < curFieldIdx; i++) {
      UNSAFE.putInt(rowHeaderPos, fieldIndexes[i]);
      rowHeaderPos += SizeOf.SIZE_OF_INT;
    }
    for (int i = curFieldIdx; i < types.length; i++) {
      UNSAFE.putInt(rowHeaderPos, -1);
      rowHeaderPos += SizeOf.SIZE_OF_INT;
    }

    // rowOffset is equivalent to a byte length of this row.
    curWritePos += rowOffset;
  }

  public void skipField() {
    fieldIndexes[curFieldIdx++] = -1;
  }

  public void putBool(boolean val) {
    ensureSize(SizeOf.SIZE_OF_BOOL);

    fieldIndexes[curFieldIdx] = rowOffset;

    UNSAFE.putByte(rowStartOffset + rowOffset, (byte) (val ? 0x01 : 0x00));

    rowOffset += SizeOf.SIZE_OF_BOOL;

    curFieldIdx++;
  }

  public void putInt2(short val) {
    ensureSize(SizeOf.SIZE_OF_SHORT);

    fieldIndexes[curFieldIdx] = rowOffset;

    UNSAFE.putShort(rowStartOffset + rowOffset, val);
    rowOffset += SizeOf.SIZE_OF_SHORT;

    curFieldIdx++;
  }

  public void putInt4(int val) {
    ensureSize(SizeOf.SIZE_OF_INT);

    fieldIndexes[curFieldIdx] = rowOffset;

    UNSAFE.putInt(rowStartOffset + rowOffset, val);
    rowOffset += SizeOf.SIZE_OF_INT;

    curFieldIdx++;
  }

  public void putInt8(long val) {
    ensureSize(SizeOf.SIZE_OF_LONG);

    fieldIndexes[curFieldIdx] = rowOffset;
    UNSAFE.putLong(rowStartOffset + rowOffset, val);
    rowOffset += SizeOf.SIZE_OF_LONG;

    curFieldIdx++;
  }

  public void putFloat4(float val) {
    ensureSize(SizeOf.SIZE_OF_FLOAT);

    fieldIndexes[curFieldIdx] = rowOffset;
    UNSAFE.putFloat(rowStartOffset + rowOffset, val);
    rowOffset += SizeOf.SIZE_OF_FLOAT;

    curFieldIdx++;
  }

  public void putFloat8(double val) {
    ensureSize(SizeOf.SIZE_OF_DOUBLE);

    fieldIndexes[curFieldIdx] = rowOffset;
    UNSAFE.putDouble(rowStartOffset + rowOffset, val);
    rowOffset += SizeOf.SIZE_OF_DOUBLE;

    curFieldIdx++;
  }

  public void putText(String val) {
    byte [] bytes = val.getBytes(TextDatum.DEFAULT_CHARSET);
    int bytesLen = bytes.length;

    ensureSize(4 + bytesLen);

    fieldIndexes[curFieldIdx] = rowOffset;
    UNSAFE.putInt(rowStartOffset + rowOffset, bytesLen);
    rowOffset += SizeOf.SIZE_OF_INT;

    UNSAFE.copyMemory(bytes, Unsafe.ARRAY_BYTE_BASE_OFFSET, null, rowStartOffset + rowOffset, bytesLen);
    rowOffset += bytesLen;

    curFieldIdx++;
  }

  public void putText(byte [] val) {
    putBlob(val);
  }

  public void putBlob(byte[] val) {
    int bytesLen = val.length;

    ensureSize(4 + bytesLen);

    fieldIndexes[curFieldIdx] = rowOffset;
    UNSAFE.putInt(rowStartOffset + rowOffset, bytesLen);
    rowOffset += SizeOf.SIZE_OF_INT;

    UNSAFE.copyMemory(val, Unsafe.ARRAY_BYTE_BASE_OFFSET, null, rowStartOffset + rowOffset, bytesLen);
    rowOffset += bytesLen;

    curFieldIdx++;
  }

  public void putTimestamp(long val) {
    putInt8(val);
  }

  public void putDate(int val) {
    putInt4(val);
  }

  public void putTime(long val) {
    putInt8(val);
  }

  public void putInterval(IntervalDatum val) {
    ensureSize(SizeOf.SIZE_OF_INT + SizeOf.SIZE_OF_LONG);
    fieldIndexes[curFieldIdx] = rowOffset;

    long offset = rowStartOffset + rowOffset;
    UNSAFE.putInt(offset, val.getMonths());
    offset += SizeOf.SIZE_OF_INT;
    UNSAFE.putLong(offset + SizeOf.SIZE_OF_INT, val.getMilliSeconds());
    rowOffset += SizeOf.SIZE_OF_INT + SizeOf.SIZE_OF_LONG;

    curFieldIdx++;
  }
}
