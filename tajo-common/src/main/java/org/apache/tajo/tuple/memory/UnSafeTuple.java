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

package org.apache.tajo.tuple.memory;

import com.google.common.base.Preconditions;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import io.netty.util.internal.PlatformDependent;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.datum.*;
import org.apache.tajo.exception.TajoRuntimeException;
import org.apache.tajo.exception.UnsupportedException;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.util.SizeOf;
import org.apache.tajo.util.StringUtils;
import org.apache.tajo.util.UnsafeUtil;
import org.apache.tajo.util.datetime.TimeMeta;
import sun.misc.Unsafe;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;

import static org.apache.tajo.common.TajoDataTypes.DataType;

public class UnSafeTuple extends ZeroCopyTuple {
  private static final Unsafe UNSAFE = UnsafeUtil.unsafe;

  private MemoryBlock memoryBlock;
  private DataType[] types;

  @Override
  public void set(MemoryBlock memoryBlock, int relativePos, DataType[] types) {
    Preconditions.checkArgument(memoryBlock.hasAddress());

    this.memoryBlock = memoryBlock;
    this.types = types;
    super.set(relativePos);
  }

  public void set(UnSafeTuple tuple) {
    this.memoryBlock = tuple.memoryBlock;
    this.types = tuple.types;
    super.set(tuple.getRelativePos());
  }

  @Override
  public int size() {
    return types.length;
  }

  @Override
  public int getLength() {
    return PlatformDependent.getInt(address());
  }

  @Override
  public TajoDataTypes.Type type(int fieldId) {
    return types[fieldId].getType();
  }

  @Override
  public int size(int fieldId) {
    return PlatformDependent.getInt(getFieldAddr(fieldId));
  }

  public void writeTo(ByteBuffer bb) {
    if (bb.remaining() < getLength()) {
      throw new IndexOutOfBoundsException("remaining length: " + bb.remaining()
          + ", tuple length: " + getLength());
    }

    if (getLength() > 0) {
      if (bb.isDirect()) {
        PlatformDependent.copyMemory(address(), PlatformDependent.directBufferAddress(bb) + bb.position(), getLength());
        bb.position(bb.position() + getLength());
      } else {
        PlatformDependent.copyMemory(address(), bb.array(), bb.arrayOffset() + bb.position(), getLength());
        bb.position(bb.position() + getLength());
      }
    }
  }

  public long address() {
    return memoryBlock.address() + getRelativePos();
  }

  public HeapTuple toHeapTuple() {
    HeapTuple heapTuple = new HeapTuple();
    byte [] bytes = new byte[getLength()];
    PlatformDependent.copyMemory(address(), bytes, 0, getLength());
    heapTuple.set(bytes, types);
    return heapTuple;
  }

  private int getFieldOffset(int fieldId) {
    return PlatformDependent.getInt(address()+ (long)(SizeOf.SIZE_OF_INT + (fieldId * SizeOf.SIZE_OF_INT)));
  }

  public long getFieldAddr(int fieldId) {
    int fieldOffset = getFieldOffset(fieldId);
    if (fieldOffset < 0 || fieldOffset > getLength()) {
      throw new RuntimeException("Invalid Access. Field : " + fieldId
          + ", Offset:" + fieldOffset + ", Record length:" + getLength());
    }
    return address() + fieldOffset;
  }

  @Override
  public boolean contains(int fieldid) {
    return getFieldOffset(fieldid) > MemoryRowBlock.NULL_FIELD_OFFSET;
  }

  @Override
  public boolean isBlank(int fieldid) {
    return getFieldOffset(fieldid) == MemoryRowBlock.NULL_FIELD_OFFSET;
  }

  @Override
  public boolean isBlankOrNull(int fieldid) {
    return getFieldOffset(fieldid) == MemoryRowBlock.NULL_FIELD_OFFSET;
  }

  @Override
  public void clear() {
    // nothing to do
  }

  @Override
  public void put(int fieldId, Datum value) {
    throw new TajoRuntimeException(new UnsupportedException());
  }

  @Override
  public void insertTuple(int fieldId, Tuple tuple) {
    throw new TajoRuntimeException(new UnsupportedException());
  }

  @Override
  public void put(Datum[] values) {
    throw new TajoRuntimeException(new UnsupportedException());
  }

  @Override
  public Datum asDatum(int fieldId) {
    if (isBlankOrNull(fieldId)) {
      return NullDatum.get();
    }

    switch (types[fieldId].getType()) {
    case BOOLEAN:
      return DatumFactory.createBool(getBool(fieldId));
    case BIT:
      return DatumFactory.createBit(getByte(fieldId));
    case INT1:
    case INT2:
      return DatumFactory.createInt2(getInt2(fieldId));
    case INT4:
      return DatumFactory.createInt4(getInt4(fieldId));
    case INT8:
      return DatumFactory.createInt8(getInt8(fieldId));
    case FLOAT4:
      return DatumFactory.createFloat4(getFloat4(fieldId));
    case FLOAT8:
      return DatumFactory.createFloat8(getFloat8(fieldId));
    case CHAR:
      return DatumFactory.createChar(getBytes(fieldId));
    case TEXT:
      return DatumFactory.createText(getBytes(fieldId));
    case BLOB:
      return DatumFactory.createBlob(getBytes(fieldId));
    case TIMESTAMP:
      return DatumFactory.createTimestamp(getInt8(fieldId));
    case DATE:
      return DatumFactory.createDate(getInt4(fieldId));
    case TIME:
      return DatumFactory.createTime(getInt8(fieldId));
    case INTERVAL:
      return getInterval(fieldId);
    case INET4:
      return DatumFactory.createInet4(getInt4(fieldId));
    case PROTOBUF:
      return getProtobufDatum(fieldId);
    case NULL_TYPE:
      return NullDatum.get();
    default:
      throw new TajoRuntimeException(new UnsupportedException("data type '" + types[fieldId] + "'"));
    }
  }

  @Override
  public void clearOffset() {
  }

  @Override
  public void setOffset(long offset) {
  }

  @Override
  public long getOffset() {
    return 0;
  }

  @Override
  public boolean getBool(int fieldId) {
    return PlatformDependent.getByte(getFieldAddr(fieldId)) == 0x01;
  }

  @Override
  public byte getByte(int fieldId) {
    return PlatformDependent.getByte(getFieldAddr(fieldId));
  }

  @Override
  public char getChar(int fieldId) {
    return UNSAFE.getChar(getFieldAddr(fieldId));
  }

  @Override
  public byte[] getBytes(int fieldId) {
    long pos = getFieldAddr(fieldId);
    int len = PlatformDependent.getInt(pos);
    pos += SizeOf.SIZE_OF_INT;

    byte [] bytes = new byte[len];
    PlatformDependent.copyMemory(pos, bytes, 0, len);
    return bytes;
  }

  @Override
  public byte[] getTextBytes(int fieldId) {
    return asDatum(fieldId).asTextBytes();
  }

  @Override
  public short getInt2(int fieldId) {
    long addr = getFieldAddr(fieldId);
    return PlatformDependent.getShort(addr);
  }

  @Override
  public int getInt4(int fieldId) {
    return PlatformDependent.getInt(getFieldAddr(fieldId));
  }

  @Override
  public long getInt8(int fieldId) {
    return PlatformDependent.getLong(getFieldAddr(fieldId));
  }

  @Override
  public float getFloat4(int fieldId) {
    return Float.intBitsToFloat(PlatformDependent.getInt(getFieldAddr(fieldId)));
  }

  @Override
  public double getFloat8(int fieldId) {
    return Double.longBitsToDouble(PlatformDependent.getLong(getFieldAddr(fieldId)));
  }

  @Override
  public String getText(int fieldId) {
    return new String(getBytes(fieldId), TextDatum.DEFAULT_CHARSET);
  }

  @Override
  public IntervalDatum getInterval(int fieldId) {
    long pos = getFieldAddr(fieldId);
    int months = PlatformDependent.getInt(pos);
    pos += SizeOf.SIZE_OF_INT;
    long millisecs = PlatformDependent.getLong(pos);
    return new IntervalDatum(months, millisecs);
  }

  @Override
  public Datum getProtobufDatum(int fieldId) {
    byte [] bytes = getBytes(fieldId);

    ProtobufDatumFactory factory = ProtobufDatumFactory.get(types[fieldId]);
    Message.Builder builder = factory.newBuilder();
    try {
      builder.mergeFrom(bytes);
    } catch (InvalidProtocolBufferException e) {
      return NullDatum.get();
    }

    return new ProtobufDatum(builder.build());
  }

  @Override
  public char[] getUnicodeChars(int fieldId) {
    long pos = getFieldAddr(fieldId);
    int len = PlatformDependent.getInt(pos);
    pos += SizeOf.SIZE_OF_INT;

    byte [] bytes = new byte[len];
    PlatformDependent.copyMemory(pos, bytes, 0, len);
    return StringUtils.convertBytesToChars(bytes, Charset.forName("UTF-8"));
  }

  @Override
  public TimeMeta getTimeDate(int fieldId) {
    return asDatum(fieldId).asTimeMeta();
  }

  @Override
  public Tuple clone() throws CloneNotSupportedException {
    return toHeapTuple();
  }

  @Override
  public Datum[] getValues() {
    Datum [] datums = new Datum[size()];
    for (int i = 0; i < size(); i++) {
      if (contains(i)) {
        datums[i] = asDatum(i);
      } else {
        datums[i] = NullDatum.get();
      }
    }
    return datums;
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(getValues());
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof Tuple) {
      Tuple other = (Tuple) obj;
      return Arrays.equals(getValues(), other.getValues());
    }
    return false;
  }
  @Override
  public String toString() {
    return VTuple.toDisplayString(getValues());
  }

  public void release() {

  }
}
