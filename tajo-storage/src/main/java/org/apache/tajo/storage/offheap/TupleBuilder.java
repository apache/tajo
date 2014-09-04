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

package org.apache.tajo.storage.offheap;

import org.apache.tajo.datum.IntervalDatum;
import org.apache.tajo.datum.ProtobufDatum;
import org.apache.tajo.datum.TextDatum;
import org.apache.tajo.exception.UnsupportedException;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.util.SizeOf;
import sun.misc.Unsafe;

/**
 *
 * Row Record Structure
 *
 * | row length (4 bytes) | field 1 offset | field 2 offset | ... | field N offset| field 1 | field 2| ... | field N |
 *                              4 bytes          4 bytes               4 bytes
 *
 */
public class TupleBuilder {
  int fieldIndexBytesLen;
  private int [] fieldIndexesForWrite;
  private int curFieldIdxForWrite;

  private int curOffset;

  OffHeapRowBlock rowBlock;

  TupleBuilder(OffHeapRowBlock rowBlock) {
    this.rowBlock = rowBlock;
    fieldIndexesForWrite = new int[rowBlock.dataTypes.length];
    fieldIndexBytesLen = SizeOf.SIZE_OF_INT * rowBlock.dataTypes.length;
  }

  void clear() {
    curOffset = 0;
    curFieldIdxForWrite = 0;
  }

  public long rowAddress() {
    return rowBlock.address() + rowBlock.position();
  }

  public int offset() {
    return curOffset;
  }

  public void addTuple(Tuple tuple) {
    startRow();

    for (int i = 0; i < rowBlock.dataTypes.length; i++) {
      if (tuple.isNull(i)) {
        skipField();
        continue;
      }
      switch (rowBlock.dataTypes[i].getType()) {
      case BOOLEAN:
        putBool(tuple.getBool(i));
        break;
      case INT1:
      case INT2:
        putInt2(tuple.getInt2(i));
        break;
      case INT4:
      case DATE:
      case INET4:
        putInt4(tuple.getInt4(i));
        break;
      case INT8:
      case TIMESTAMP:
      case TIME:
        putInt8(tuple.getInt8(i));
        break;
      case FLOAT4:
        putFloat4(tuple.getFloat4(i));
        break;
      case FLOAT8:
        putFloat8(tuple.getFloat8(i));
        break;
      case TEXT:
        putText(tuple.getBytes(i));
        break;
      case INTERVAL:
        putInterval((IntervalDatum) tuple.getInterval(i));
        break;
      case PROTOBUF:
        putProtoDatum((ProtobufDatum) tuple.getProtobufDatum(i));
        break;
      default:
        throw new UnsupportedException("Unknown data type: " + rowBlock.dataTypes[i]);
      }
    }

    endRow();
  }

  public boolean startRow() {
    curOffset = 0;
    curOffset += 4; // skip row header
    curOffset += fieldIndexBytesLen; // skip an array of field indices
    curFieldIdxForWrite = 0;
    return true;
  }

  public void endRow() {
    rowBlock.setRows(rowBlock.rows() + 1);

    long rowHeaderPos = rowBlock.address() + rowBlock.position();
    OffHeapMemory.UNSAFE.putInt(rowHeaderPos, curOffset);
    rowHeaderPos += SizeOf.SIZE_OF_INT;

    for (int i = 0; i < curFieldIdxForWrite; i++) {
      OffHeapMemory.UNSAFE.putInt(rowHeaderPos, fieldIndexesForWrite[i]);
      rowHeaderPos += SizeOf.SIZE_OF_INT;
    }
    for (int i = curFieldIdxForWrite; i < rowBlock.dataTypes.length; i++) {
      OffHeapMemory.UNSAFE.putInt(rowHeaderPos, OffHeapRowBlock.NULL_FIELD_OFFSET);
      rowHeaderPos += SizeOf.SIZE_OF_INT;
    }

    // rowOffset is equivalent to a byte length of this row.
    rowBlock.position(rowBlock.position() + curOffset);
  }

  public void skipField() {
    fieldIndexesForWrite[curFieldIdxForWrite++] = OffHeapRowBlock.NULL_FIELD_OFFSET;
  }

  public void putBool(boolean val) {
    rowBlock.ensureSize(SizeOf.SIZE_OF_BOOL);

    fieldIndexesForWrite[curFieldIdxForWrite] = curOffset;

    OffHeapMemory.UNSAFE.putByte(rowAddress() + curOffset, (byte) (val ? 0x01 : 0x00));

    curOffset += SizeOf.SIZE_OF_BOOL;

    curFieldIdxForWrite++;
  }

  public void putInt2(short val) {
    rowBlock.ensureSize(SizeOf.SIZE_OF_SHORT);

    fieldIndexesForWrite[curFieldIdxForWrite] = curOffset;

    OffHeapMemory.UNSAFE.putShort(rowAddress() + curOffset, val);
    curOffset += SizeOf.SIZE_OF_SHORT;

    curFieldIdxForWrite++;
  }

  public void putInt4(int val) {
    rowBlock.ensureSize(SizeOf.SIZE_OF_INT);

    fieldIndexesForWrite[curFieldIdxForWrite] = curOffset;

    OffHeapMemory.UNSAFE.putInt(rowAddress() + curOffset, val);
    curOffset += SizeOf.SIZE_OF_INT;

    curFieldIdxForWrite++;
  }

  public void putInt8(long val) {
    rowBlock.ensureSize(SizeOf.SIZE_OF_LONG);

    fieldIndexesForWrite[curFieldIdxForWrite] = curOffset;
    OffHeapMemory.UNSAFE.putLong(rowAddress() + curOffset, val);
    curOffset += SizeOf.SIZE_OF_LONG;

    curFieldIdxForWrite++;
  }

  public void putFloat4(float val) {
    rowBlock.ensureSize(SizeOf.SIZE_OF_FLOAT);

    fieldIndexesForWrite[curFieldIdxForWrite] = curOffset;
    OffHeapMemory.UNSAFE.putFloat(rowAddress() + curOffset, val);
    curOffset += SizeOf.SIZE_OF_FLOAT;

    curFieldIdxForWrite++;
  }

  public void putFloat8(double val) {
    rowBlock.ensureSize(SizeOf.SIZE_OF_DOUBLE);

    fieldIndexesForWrite[curFieldIdxForWrite] = curOffset;
    OffHeapMemory.UNSAFE.putDouble(rowAddress() + curOffset, val);
    curOffset += SizeOf.SIZE_OF_DOUBLE;

    curFieldIdxForWrite++;
  }

  public void putText(String val) {
    byte[] bytes = val.getBytes(TextDatum.DEFAULT_CHARSET);
    putText(bytes);
  }

  public void putText(byte[] val) {
    int bytesLen = val.length;

    rowBlock.ensureSize(SizeOf.SIZE_OF_INT + bytesLen);

    fieldIndexesForWrite[curFieldIdxForWrite] = curOffset;
    OffHeapMemory.UNSAFE.putInt(rowAddress() + curOffset, bytesLen);
    curOffset += SizeOf.SIZE_OF_INT;

    OffHeapMemory.UNSAFE.copyMemory(val, Unsafe.ARRAY_BYTE_BASE_OFFSET, null, rowAddress() + curOffset, bytesLen);
    curOffset += bytesLen;

    curFieldIdxForWrite++;
  }

  public void putBlob(byte[] val) {
    int bytesLen = val.length;

    rowBlock.ensureSize(SizeOf.SIZE_OF_INT + bytesLen);

    fieldIndexesForWrite[curFieldIdxForWrite] = curOffset;
    OffHeapMemory.UNSAFE.putInt(rowAddress() + curOffset, bytesLen);
    curOffset += SizeOf.SIZE_OF_INT;

    OffHeapMemory.UNSAFE.copyMemory(val, Unsafe.ARRAY_BYTE_BASE_OFFSET, null, rowAddress() + curOffset, bytesLen);
    curOffset += bytesLen;

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
    rowBlock.ensureSize(SizeOf.SIZE_OF_INT + SizeOf.SIZE_OF_LONG);
    fieldIndexesForWrite[curFieldIdxForWrite] = curOffset;

    long offset = rowAddress() + curOffset;
    OffHeapMemory.UNSAFE.putInt(offset, val.getMonths());
    offset += SizeOf.SIZE_OF_INT;
    OffHeapMemory.UNSAFE.putLong(offset, val.getMilliSeconds());
    curOffset += SizeOf.SIZE_OF_INT + SizeOf.SIZE_OF_LONG;

    curFieldIdxForWrite++;
  }

  public void putInet4(int val) {
    putInt4(val);
  }

  public void putProtoDatum(ProtobufDatum val) {
    putBlob(val.asByteArray());
  }
}
