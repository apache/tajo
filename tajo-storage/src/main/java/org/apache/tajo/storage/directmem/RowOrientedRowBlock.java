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
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.datum.IntervalDatum;
import org.apache.tajo.datum.TextDatum;
import org.apache.tajo.util.FileUtil;
import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;

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
  private int maxRowNum = Integer.MAX_VALUE; // optional
  private int filledRowNum;

  // Read States
  private int curRowIdxForRead;
  private int curPosForRead;

  // Write States --------------------
  private int curOffsetForWrite;
  private long rowStartAddrForWrite;
  private int rowOffsetForWrite;
  private int curFieldIdxForWrite;
  private int [] fieldIndexesForWrite;

  public RowOrientedRowBlock(Schema schema, int bytes) {
    this(schema, ByteBuffer.allocateDirect(bytes).order(ByteOrder.nativeOrder()));
  }

  public RowOrientedRowBlock(Schema schema, ByteBuffer buffer) {
    this.buffer = buffer;
    this.address = ((DirectBuffer) buffer).address();
    this.bytesLen = buffer.limit();

    types = new Type[schema.size()];
    maxLengths = new int[schema.size()];
    for (int i = 0; i < schema.size(); i++) {
      TajoDataTypes.DataType dataType = schema.getColumn(i).getDataType();
      types[i] = dataType.getType();
      maxLengths[i] = dataType.getLength();
    }
    fieldIndexesForWrite = new int[schema.size()];
    fieldIndexBytesLen = SizeOf.SIZE_OF_INT * schema.size();

    curOffsetForWrite = 0;
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

  public ByteBuffer nioBuffer() {
    buffer.flip();
    buffer.limit(curOffsetForWrite);
    return buffer;
  }

  public long totalMem() {
    return bytesLen;
  }

  public long remain() {
    return bytesLen - curOffsetForWrite - rowOffsetForWrite;
  }

  public int maxRowNum() {
    return maxRowNum;
  }
  public int totalRowNum() {
    return filledRowNum;
  }

  /**
   * Return for each tuple
   *
   * @return True if tuple block is filled with tuples. Otherwise, It will return false.
   */
  public boolean next(UnSafeTuple tuple) {
    if (curRowIdxForRead < filledRowNum) {

      long recordStartPtr = address + curPosForRead;
      int recordLen = UNSAFE.getInt(recordStartPtr);
      tuple.set(buffer, curPosForRead, recordLen, types);

      curPosForRead += recordLen;
      curRowIdxForRead++;

      return true;
    } else {
      return false;
    }
  }

  public void resetRowCursor() {
    curPosForRead = 0;
    curRowIdxForRead = 0;
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
      long relativeRowStartPos = this.rowStartAddrForWrite - this.address;

      // Update current write states
      rowStartAddrForWrite = newAddress + relativeRowStartPos;
      bytesLen = newBlockSize;
      UnsafeUtil.free(buffer);
      buffer = newByteBuf;
      address = newAddress;
    }
  }

  public boolean copyFromChannel(FileChannel channel, TableStats stats) throws IOException {
    if (channel.position() < channel.size()) {
      filledRowNum = 0;
      buffer.clear();
      channel.read(buffer);
      bytesLen = buffer.position();

      curOffsetForWrite = 0;
      rowOffsetForWrite = 0;
      while (curOffsetForWrite < bytesLen) {
        rowStartAddrForWrite = address + curOffsetForWrite;

        rowOffsetForWrite = 0;
        int recordSize = UNSAFE.getInt(rowStartAddrForWrite + rowOffsetForWrite);

        if (remain() < recordSize) {
          channel.position(channel.position() - remain());
          return true;
        }

        curOffsetForWrite += recordSize;
        filledRowNum++;
      }

      return true;
    } else {
      return false;
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
    rowStartAddrForWrite = address + curOffsetForWrite;
    rowOffsetForWrite = 0;
    rowOffsetForWrite += 4; // skip row header
    rowOffsetForWrite += fieldIndexBytesLen; // skip an array of field indices
    curFieldIdxForWrite = 0;
    return true;
  }

  public void endRow() {
    filledRowNum++;

    long rowHeaderPos = rowStartAddrForWrite;
    UNSAFE.putInt(rowHeaderPos, rowOffsetForWrite);
    rowHeaderPos += SizeOf.SIZE_OF_INT;

    for (int i = 0; i < curFieldIdxForWrite; i++) {
      UNSAFE.putInt(rowHeaderPos, fieldIndexesForWrite[i]);
      rowHeaderPos += SizeOf.SIZE_OF_INT;
    }
    for (int i = curFieldIdxForWrite; i < types.length; i++) {
      UNSAFE.putInt(rowHeaderPos, -1);
      rowHeaderPos += SizeOf.SIZE_OF_INT;
    }

    // rowOffset is equivalent to a byte length of this row.
    curOffsetForWrite += rowOffsetForWrite;
  }

  public void skipField() {
    fieldIndexesForWrite[curFieldIdxForWrite++] = -1;
  }

  public void putBool(boolean val) {
    ensureSize(SizeOf.SIZE_OF_BOOL);

    fieldIndexesForWrite[curFieldIdxForWrite] = rowOffsetForWrite;

    UNSAFE.putByte(rowStartAddrForWrite + rowOffsetForWrite, (byte) (val ? 0x01 : 0x00));

    rowOffsetForWrite += SizeOf.SIZE_OF_BOOL;

    curFieldIdxForWrite++;
  }

  public void putInt2(short val) {
    ensureSize(SizeOf.SIZE_OF_SHORT);

    fieldIndexesForWrite[curFieldIdxForWrite] = rowOffsetForWrite;

    UNSAFE.putShort(rowStartAddrForWrite + rowOffsetForWrite, val);
    rowOffsetForWrite += SizeOf.SIZE_OF_SHORT;

    curFieldIdxForWrite++;
  }

  public void putInt4(int val) {
    ensureSize(SizeOf.SIZE_OF_INT);

    fieldIndexesForWrite[curFieldIdxForWrite] = rowOffsetForWrite;

    UNSAFE.putInt(rowStartAddrForWrite + rowOffsetForWrite, val);
    rowOffsetForWrite += SizeOf.SIZE_OF_INT;

    curFieldIdxForWrite++;
  }

  public void putInt8(long val) {
    ensureSize(SizeOf.SIZE_OF_LONG);

    fieldIndexesForWrite[curFieldIdxForWrite] = rowOffsetForWrite;
    UNSAFE.putLong(rowStartAddrForWrite + rowOffsetForWrite, val);
    rowOffsetForWrite += SizeOf.SIZE_OF_LONG;

    curFieldIdxForWrite++;
  }

  public void putFloat4(float val) {
    ensureSize(SizeOf.SIZE_OF_FLOAT);

    fieldIndexesForWrite[curFieldIdxForWrite] = rowOffsetForWrite;
    UNSAFE.putFloat(rowStartAddrForWrite + rowOffsetForWrite, val);
    rowOffsetForWrite += SizeOf.SIZE_OF_FLOAT;

    curFieldIdxForWrite++;
  }

  public void putFloat8(double val) {
    ensureSize(SizeOf.SIZE_OF_DOUBLE);

    fieldIndexesForWrite[curFieldIdxForWrite] = rowOffsetForWrite;
    UNSAFE.putDouble(rowStartAddrForWrite + rowOffsetForWrite, val);
    rowOffsetForWrite += SizeOf.SIZE_OF_DOUBLE;

    curFieldIdxForWrite++;
  }

  public void putText(String val) {
    byte [] bytes = val.getBytes(TextDatum.DEFAULT_CHARSET);
    int bytesLen = bytes.length;

    ensureSize(4 + bytesLen);

    fieldIndexesForWrite[curFieldIdxForWrite] = rowOffsetForWrite;
    UNSAFE.putInt(rowStartAddrForWrite + rowOffsetForWrite, bytesLen);
    rowOffsetForWrite += SizeOf.SIZE_OF_INT;

    UNSAFE.copyMemory(bytes, Unsafe.ARRAY_BYTE_BASE_OFFSET, null, rowStartAddrForWrite + rowOffsetForWrite, bytesLen);
    rowOffsetForWrite += bytesLen;

    curFieldIdxForWrite++;
  }

  public void putText(byte [] val) {
    putBlob(val);
  }

  public void putBlob(byte[] val) {
    int bytesLen = val.length;

    ensureSize(4 + bytesLen);

    fieldIndexesForWrite[curFieldIdxForWrite] = rowOffsetForWrite;
    UNSAFE.putInt(rowStartAddrForWrite + rowOffsetForWrite, bytesLen);
    rowOffsetForWrite += SizeOf.SIZE_OF_INT;

    UNSAFE.copyMemory(val, Unsafe.ARRAY_BYTE_BASE_OFFSET, null, rowStartAddrForWrite + rowOffsetForWrite, bytesLen);
    rowOffsetForWrite += bytesLen;

    curFieldIdxForWrite++;
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
    fieldIndexesForWrite[curFieldIdxForWrite] = rowOffsetForWrite;

    long offset = rowStartAddrForWrite + rowOffsetForWrite;
    UNSAFE.putInt(offset, val.getMonths());
    offset += SizeOf.SIZE_OF_INT;
    UNSAFE.putLong(offset, val.getMilliSeconds());
    rowOffsetForWrite += SizeOf.SIZE_OF_INT + SizeOf.SIZE_OF_LONG;

    curFieldIdxForWrite++;
  }

  public void putInet4(int val) {
    putInt4(val);
  }
}
