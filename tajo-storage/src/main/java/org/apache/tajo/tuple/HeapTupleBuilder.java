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

package org.apache.tajo.tuple;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SchemaUtil;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.datum.IntervalDatum;
import org.apache.tajo.storage.offheap.HeapTuple;
import org.apache.tajo.storage.offheap.ZeroCopyTuple;
import org.apache.tajo.unit.StorageUnit;
import org.apache.tajo.util.FileUtil;
import org.apache.tajo.util.SizeOf;
import org.apache.tajo.util.UnsafeUtil;
import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class HeapTupleBuilder implements TupleBuilder {
  private static final Log LOG = LogFactory.getLog(HeapTupleBuilder.class);

  private static final Unsafe UNSAFE = UnsafeUtil.unsafe;
  private TajoDataTypes.Type[] types;
  private ByteBuffer buffer;
  private long address;
  private int rowOffset;

  private int rowStartPos;
  private int curFieldIdx;
  int [] fieldOffsets;

  public HeapTupleBuilder(Schema schema) {
    this.types = SchemaUtil.toTypes(schema);
    buffer = ByteBuffer.allocateDirect(64 * StorageUnit.KB).order(ByteOrder.nativeOrder());
    address = UnsafeUtil.getAddress(buffer);

    fieldOffsets = new int[types.length];
  }

  private void ensureSize(int size) {
    if (buffer.remaining() - size < 0) { // check the remain size
      // enlarge new buffer and copy writing data
      int newBlockSize = UnsafeUtil.alignedSize(buffer.capacity() << 1);
      ByteBuffer newByteBuf = ByteBuffer.allocateDirect(newBlockSize);
      long newAddress = ((DirectBuffer)newByteBuf).address();
      UNSAFE.copyMemory(this.address, newAddress, buffer.limit());
      LOG.debug("Increase DirectRowBlock to " + FileUtil.humanReadableByteCount(newBlockSize, false));

      // release existing buffer and replace variables
      UnsafeUtil.free(buffer);
      buffer = newByteBuf;
      address = newAddress;
    }
  }

  @Override
  public boolean startRow() {
    buffer.clear();
    rowOffset = 0;

    rowOffset += SizeOf.SIZE_OF_INT * (types.length + 1); // record size + offset list

    return true;
  }

  @Override
  public TupleBuilder endRow() {
    return null;
  }

  @Override
  public HeapTuple asHeapTuple() {
    return null;
  }

  @Override
  public ZeroCopyTuple asZeroCopyTuple() {
    return null;
  }

  @Override
  public void skipField() {
    fieldOffsets[curFieldIdx - 1] = -1;
  }

  private void forwardField() {
    fieldOffsets[curFieldIdx++] = rowOffset;
  }

  @Override
  public void putBool(boolean val) {
    forwardField();

    ensureSize(SizeOf.SIZE_OF_BYTE);

    UNSAFE.putByte(address + rowOffset, (byte) (val ? 0x01 : 0x00));
    rowOffset += SizeOf.SIZE_OF_BYTE;
  }

  @Override
  public void putInt2(short val) {
    forwardField();

    ensureSize(SizeOf.SIZE_OF_SHORT);

    UNSAFE.putShort(address + rowOffset, val);
    rowOffset += SizeOf.SIZE_OF_SHORT;
  }

  @Override
  public void putInt4(int val) {
    forwardField();

    ensureSize(SizeOf.SIZE_OF_INT);
    UNSAFE.putInt(address + rowOffset, val);
    rowOffset += SizeOf.SIZE_OF_INT;
  }

  @Override
  public void putInt8(long val) {
    forwardField();

    ensureSize(SizeOf.SIZE_OF_LONG);
    UNSAFE.putLong(address + rowOffset, val);
    rowOffset += SizeOf.SIZE_OF_LONG;
  }

  @Override
  public void putFloat4(float val) {
    forwardField();

    ensureSize(SizeOf.SIZE_OF_FLOAT);
    UNSAFE.putFloat(address + rowOffset, val);
    rowOffset += SizeOf.SIZE_OF_FLOAT;
  }

  @Override
  public void putFloat8(double val) {
    forwardField();

    ensureSize(SizeOf.SIZE_OF_DOUBLE);
    UNSAFE.putDouble(address + rowOffset, val);
    rowOffset += SizeOf.SIZE_OF_DOUBLE;
  }

  @Override
  public void putText(String val) {
    putText(val.getBytes());
  }

  @Override
  public void putText(byte[] val) {
    putBlob(val);
  }

  @Override
  public void putBlob(byte[] bytes) {
    forwardField();

    ensureSize(SizeOf.SIZE_OF_INT + bytes.length);

    UNSAFE.putInt(address + rowOffset, bytes.length);
    rowOffset += SizeOf.SIZE_OF_INT;

    UNSAFE.copyMemory(bytes, Unsafe.ARRAY_BYTE_BASE_OFFSET, null, address + rowOffset, bytes.length);
    rowOffset += bytes.length;
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
    forwardField();
  }

  @Override
  public void putInterval(IntervalDatum val) {
    forwardField();
  }

  @Override
  public void putInet4(int val) {
    putInt4(val);
  }
}
