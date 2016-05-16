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
import org.apache.tajo.datum.BooleanDatum;
import org.apache.tajo.datum.IntervalDatum;
import org.apache.tajo.datum.ProtobufDatum;
import org.apache.tajo.datum.TextDatum;
import org.apache.tajo.exception.TajoInternalError;
import org.apache.tajo.exception.ValueOutOfRangeException;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.util.BitArray;
import org.apache.tajo.util.SizeOf;
import org.apache.tajo.util.UnsafeUtil;

/**
 * This class represent serialization of RawFile
 *
 * Row Record Structure
 *
 * | row length | null flags length | null flags | field 1 | field 2| ... | field N |;
 *
 * |  (4 bytes)        (2 bytes)      (N bytes)  |                                  |;
 *                Header                                         values
 */
public class CompactRowBlockWriter implements RowWriter {
  private static final int RECORD_FIELD_SIZE = 4;
  // Maximum variant int32 size is 5
  private static final short MAXIMUM_VARIANT_INT32 = 5;
  // Maximum variant int64 size is 10
  private static final short MAXIMUM_VARIANT_INT64 = 10;

  private final RowBlock rowBlock;
  private final BitArray nullFlags;

  /** record capacity + offset list */
  private final int headerSize;

  private final DataType[] dataTypes;

  private int curFieldIdx;
  private int curOffset;


  public CompactRowBlockWriter(RowBlock rowBlock) {
    this.dataTypes = rowBlock.getDataTypes();
    this.rowBlock = rowBlock;

    // compute the number of bytes, representing the null flags
    nullFlags = new BitArray(dataTypes.length);
    headerSize = RECORD_FIELD_SIZE + SizeOf.SIZE_OF_SHORT + nullFlags.bytesLength();

    if (!rowBlock.getMemory().hasAddress()) {
      throw new TajoInternalError(rowBlock.getMemory().getClass().getSimpleName()
          + " does not support to direct memory access");
    }
  }


  /**
   * Encode a ZigZag-encoded 32-bit value.  ZigZag encodes signed integers
   * into values that can be efficiently encoded with varint.  (Otherwise,
   * negative values must be sign-extended to 64 bits to be varint encoded,
   * thus always taking 10 bytes on the wire.)
   *
   * @param n A signed 32-bit integer.
   * @return An unsigned 32-bit integer, stored in a signed int because
   *         Java has no explicit unsigned support.
   */
  public static int encodeZigZag32(final int n) {
    // Note:  the right-shift must be arithmetic
    return (n << 1) ^ (n >> 31);
  }

  /**
   * Encode a ZigZag-encoded 64-bit value.  ZigZag encodes signed integers
   * into values that can be efficiently encoded with varint.  (Otherwise,
   * negative values must be sign-extended to 64 bits to be varint encoded,
   * thus always taking 10 bytes on the wire.)
   *
   * @param n A signed 64-bit integer.
   * @return An unsigned 64-bit integer, stored in a signed int because
   *         Java has no explicit unsigned support.
   */
  public static long encodeZigZag64(final long n) {
    // Note:  the right-shift must be arithmetic
    return (n << 1) ^ (n >> 63);
  }

  /**
   * Encode and write a varint.  {@code value} is treated as
   * unsigned, so it won't be sign-extended if negative.
   */
  public static short writeRawVarint32(long address, int value) {
    short length = 0;
    while (true) {
      if ((value & ~0x7F) == 0) {
        PlatformDependent.putByte(address + length, (byte) value);
        length++;
        return length;
      } else {
        PlatformDependent.putByte(address + length, (byte) ((value & 0x7F) | 0x80));
        value >>>= 7;
        length++;
      }
    }
  }

  /**
   * Encode and write a varint64.
   */
  public static short writeRawVarint64(long address, long value) {
    short length = 0;
    while (true) {
      if ((value & ~0x7FL) == 0) {
        PlatformDependent.putByte(address + length, (byte) value);
        length++;
        return length;
      } else {
        PlatformDependent.putByte(address + length, (byte) ((value & 0x7F) | 0x80));
        value >>>= 7;
        length++;
      }
    }
  }

  /**
   * Compute the number of bytes that would be needed to encode a varint.
   * {@code value} is treated as unsigned, so it won't be sign-extended if
   * negative.
   */
  public static int computeRawVarint32Size(final int value) {
    if ((value & (0xffffffff <<  7)) == 0) return 1;
    if ((value & (0xffffffff << 14)) == 0) return 2;
    if ((value & (0xffffffff << 21)) == 0) return 3;
    if ((value & (0xffffffff << 28)) == 0) return 4;
    return 5;
  }

  /**
   * Current memory address of the buffer
   *
   * @return The memory address
   */
  public long address() {
    return rowBlock.getMemory().address();
  }

  /**
   * Current position
   *
   * @return The position
   */
  public int position() {
    return rowBlock.getMemory().writerPosition();
  }


  /**
   * Forward the address;
   *
   * @param length Length to be forwarded
   */
  public void forward(int length) {
    rowBlock.getMemory().writerPosition(rowBlock.getMemory().writerPosition() + length);
  }

  /**
   * Backward the address;
   *
   * @param length Length to be backwarded
   */
  public void backward(int length) {
    rowBlock.getMemory().writerPosition(rowBlock.getMemory().writerPosition() - length);
  }

  public void ensureSize(int size) {
    rowBlock.getMemory().ensureSize(size);
  }

  @Override
  public DataType[] dataTypes() {
    return rowBlock.getDataTypes();
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

  public int offset() {
    return position();
  }


  @Override
  public void clear() {
    curOffset = 0;
    curFieldIdx = 0;
    nullFlags.clear();
  }

  @Override
  public boolean startRow() {
    ensureSize(headerSize);
    nullFlags.clear();

    curOffset = headerSize;
    curFieldIdx = 0;
    forward(headerSize);
    return true;
  }

  @Override
  public void endRow() {
    long rowHeaderPos = recordStartAddr();
    // curOffset is equivalent to a byte length of this row.
    PlatformDependent.putInt(rowHeaderPos, curOffset);
    rowHeaderPos += SizeOf.SIZE_OF_INT;

    //set null flags
    byte [] flags = nullFlags.toArray();
    PlatformDependent.putShort(rowHeaderPos, (short) flags.length);
    rowHeaderPos += SizeOf.SIZE_OF_SHORT;
    PlatformDependent.copyMemory(flags, 0, rowHeaderPos, flags.length);

    rowBlock.setRows(rowBlock.rows() + 1);
  }

  @Override
  public void cancelRow() {
    // curOffset is equivalent to a byte length of current row.
    backward(curOffset);
    curOffset = 0;
    nullFlags.clear();
    curFieldIdx = 0;
  }

  @Override
  public void skipField() {
    // set null flag
    nullFlags.set(curFieldIdx);
    curFieldIdx++;
  }

  /**
   * set current buffer position and forward field length
   * @param fieldLength
   */
  private void forwardField(int fieldLength) {
    forward(fieldLength);
    curOffset += fieldLength;

  }

  @Override
  public void putByte(byte val) {
    ensureSize(SizeOf.SIZE_OF_BYTE);
    long addr = currentAddr();

    PlatformDependent.putByte(addr, val);
    curFieldIdx++;
    forwardField(SizeOf.SIZE_OF_BYTE);
  }

  @Override
  public void putBool(boolean val) {
    putByte(val ? BooleanDatum.TRUE_INT : BooleanDatum.FALSE_INT);
  }

  @Override
  public void putInt2(short val) {
    ensureSize(SizeOf.SIZE_OF_SHORT);
    long addr = currentAddr();

    PlatformDependent.putShort(addr, val);
    curFieldIdx++;
    forwardField(SizeOf.SIZE_OF_SHORT);
  }

  @Override
  public void putInt4(int val) {
    ensureSize(MAXIMUM_VARIANT_INT32);

    curFieldIdx++;
    forwardField(writeRawVarint32(currentAddr(), encodeZigZag32(val)));
  }

  @Override
  public void putInt8(long val) {
    ensureSize(MAXIMUM_VARIANT_INT64);

    curFieldIdx++;
    forwardField(writeRawVarint64(currentAddr(), encodeZigZag64(val)));
  }

  @Override
  public void putFloat4(float val) {
    ensureSize(SizeOf.SIZE_OF_FLOAT);
    long addr = currentAddr();

    UnsafeUtil.unsafe.putFloat(addr, val);
    curFieldIdx++;
    forwardField(SizeOf.SIZE_OF_FLOAT);
  }

  @Override
  public void putFloat8(double val) {
    ensureSize(SizeOf.SIZE_OF_DOUBLE);
    long addr = currentAddr();

    UnsafeUtil.unsafe.putDouble(addr, val);
    curFieldIdx++;
    forwardField(SizeOf.SIZE_OF_DOUBLE);
  }

  @Override
  public void putText(String val) {
    putText(val.getBytes(TextDatum.DEFAULT_CHARSET));
  }

  @Override
  public void putText(byte[] val) {
    putBlob(val);
  }

  @Override
  public void putBlob(byte[] val) {
    int bytesLen = val.length;

    ensureSize(MAXIMUM_VARIANT_INT32 + bytesLen);
    long addr = currentAddr();

    short length = writeRawVarint32(addr, bytesLen);
    PlatformDependent.copyMemory(val, 0, addr + length, bytesLen);
    curFieldIdx++;
    forwardField(length + bytesLen);
  }

  @Override
  public void putDate(int val) {
    ensureSize(SizeOf.SIZE_OF_INT);
    long addr = currentAddr();

    PlatformDependent.putInt(addr, val);
    curFieldIdx++;
    forwardField(SizeOf.SIZE_OF_INT);
  }

  @Override
  public void putTime(long val) {
    ensureSize(SizeOf.SIZE_OF_LONG);
    long addr = currentAddr();

    PlatformDependent.putLong(addr, val);
    curFieldIdx++;
    forwardField(SizeOf.SIZE_OF_LONG);
  }

  @Override
  public void putTimestamp(long val) {
    putTime(val);
  }

  @Override
  public void putInterval(IntervalDatum val) {
    ensureSize(MAXIMUM_VARIANT_INT32 + MAXIMUM_VARIANT_INT64);
    long addr = currentAddr();

    short length = writeRawVarint32(addr, encodeZigZag32(val.getMonths()));
    length += writeRawVarint64(addr, encodeZigZag64(val.getMilliSeconds()));

    curFieldIdx++;
    forwardField(length);
  }

  @Override
  public void putProtoDatum(ProtobufDatum val) {
    putBlob(val.asByteArray());
  }

  @Override
  public boolean addTuple(Tuple tuple) {
    try {
      OffHeapRowBlockUtils.convert(tuple, this);
    } catch (ValueOutOfRangeException e) {
      return false;
    }
    return true;
  }
}
