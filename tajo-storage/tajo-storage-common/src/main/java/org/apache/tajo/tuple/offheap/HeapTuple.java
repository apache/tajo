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

package org.apache.tajo.tuple.offheap;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;

import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.datum.*;
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

import static org.apache.tajo.common.TajoDataTypes.DataType;

public class HeapTuple implements Tuple {
  private static final Unsafe UNSAFE = UnsafeUtil.unsafe;
  private static final long BASE_OFFSET = UnsafeUtil.ARRAY_BYTE_BASE_OFFSET;

  private final byte [] data;
  private final DataType [] types;

  public HeapTuple(final byte [] bytes, final DataType [] types) {
    this.data = bytes;
    this.types = types;
  }

  @Override
  public int size() {
    return data.length;
  }

  @Override
  public TajoDataTypes.Type type(int fieldId) {
    return types[fieldId].getType();
  }

  @Override
  public int size(int fieldId) {
    return UNSAFE.getInt(data, BASE_OFFSET + checkNullAndGetOffset(fieldId));
  }

  public ByteBuffer nioBuffer() {
    return ByteBuffer.wrap(data);
  }

  private int getFieldOffset(int fieldId) {
    return UNSAFE.getInt(data, BASE_OFFSET + SizeOf.SIZE_OF_INT + (fieldId * SizeOf.SIZE_OF_INT));
  }

  private int checkNullAndGetOffset(int fieldId) {
    int offset = getFieldOffset(fieldId);
    if (offset == OffHeapRowBlock.NULL_FIELD_OFFSET) {
      throw new RuntimeException("Invalid Field Access: " + fieldId);
    }
    return offset;
  }

  @Override
  public boolean contains(int fieldid) {
    return getFieldOffset(fieldid) > OffHeapRowBlock.NULL_FIELD_OFFSET;
  }

  @Override
  public boolean isBlank(int fieldid) {
    return getFieldOffset(fieldid) == OffHeapRowBlock.NULL_FIELD_OFFSET;
  }

  @Override
  public boolean isBlankOrNull(int fieldid) {
    return getFieldOffset(fieldid) == OffHeapRowBlock.NULL_FIELD_OFFSET;
  }

  @Override
  public void put(int fieldId, Tuple tuple) {
    throw new UnsupportedException("UnSafeTuple does not support put(int, Tuple).");
  }

  @Override
  public void clear() {
    // nothing to do
  }

  @Override
  public void put(int fieldId, Datum value) {
    throw new UnsupportedException("UnSafeTuple does not support put(int, Datum).");
  }

  @Override
  public void put(Datum[] values) {
    throw new UnsupportedException("UnSafeTuple does not support put(Datum[]).");
  }

  @Override
  public Datum asDatum(int fieldId) {
    if (isBlankOrNull(fieldId)) {
      return NullDatum.get();
    }

    switch (types[fieldId].getType()) {
    case BOOLEAN:
      return DatumFactory.createBool(getBool(fieldId));
    case INT1:
    case INT2:
      return DatumFactory.createInt2(getInt2(fieldId));
    case INT4:
      return DatumFactory.createInt4(getInt4(fieldId));
    case INT8:
      return DatumFactory.createInt8(getInt4(fieldId));
    case FLOAT4:
      return DatumFactory.createFloat4(getFloat4(fieldId));
    case FLOAT8:
      return DatumFactory.createFloat8(getFloat8(fieldId));
    case TEXT:
      return DatumFactory.createText(getText(fieldId));
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
    default:
      throw new UnsupportedException("Unknown type: " + types[fieldId]);
    }
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
    return UNSAFE.getByte(data, BASE_OFFSET + checkNullAndGetOffset(fieldId)) == 0x01;
  }

  @Override
  public byte getByte(int fieldId) {
    return UNSAFE.getByte(data, BASE_OFFSET + checkNullAndGetOffset(fieldId));
  }

  @Override
  public char getChar(int fieldId) {
    return UNSAFE.getChar(data, BASE_OFFSET + checkNullAndGetOffset(fieldId));
  }

  @Override
  public byte[] getBytes(int fieldId) {
    long pos = checkNullAndGetOffset(fieldId);
    int len = UNSAFE.getInt(data, BASE_OFFSET + pos);
    pos += SizeOf.SIZE_OF_INT;

    byte [] bytes = new byte[len];
    UNSAFE.copyMemory(data, BASE_OFFSET + pos, bytes, UnsafeUtil.ARRAY_BYTE_BASE_OFFSET, len);
    return bytes;
  }

  @Override
  public byte[] getTextBytes(int fieldId) {
    return getText(fieldId).getBytes();
  }

  @Override
  public short getInt2(int fieldId) {
    return UNSAFE.getShort(data, BASE_OFFSET + checkNullAndGetOffset(fieldId));
  }

  @Override
  public int getInt4(int fieldId) {
    return UNSAFE.getInt(data, BASE_OFFSET + checkNullAndGetOffset(fieldId));
  }

  @Override
  public long getInt8(int fieldId) {
    return UNSAFE.getLong(data, BASE_OFFSET + checkNullAndGetOffset(fieldId));
  }

  @Override
  public float getFloat4(int fieldId) {
    return UNSAFE.getFloat(data, BASE_OFFSET + checkNullAndGetOffset(fieldId));
  }

  @Override
  public double getFloat8(int fieldId) {
    return UNSAFE.getDouble(data, BASE_OFFSET + checkNullAndGetOffset(fieldId));
  }

  @Override
  public String getText(int fieldId) {
    return new String(getBytes(fieldId));
  }

  @Override
  public TimeMeta getTimeDate(int fieldId) {
    return asDatum(fieldId).asTimeMeta();
  }

  public IntervalDatum getInterval(int fieldId) {
    long pos = checkNullAndGetOffset(fieldId);
    int months = UNSAFE.getInt(data, BASE_OFFSET + pos);
    pos += SizeOf.SIZE_OF_INT;
    long millisecs = UNSAFE.getLong(data, BASE_OFFSET + pos);
    return new IntervalDatum(months, millisecs);
  }

  @Override
  public Datum getProtobufDatum(int fieldId) {
    byte [] bytes = getBytes(fieldId);

    ProtobufDatumFactory factory = ProtobufDatumFactory.get(types[fieldId].getCode());
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
    long pos = checkNullAndGetOffset(fieldId);
    int len = UNSAFE.getInt(data, BASE_OFFSET + pos);
    pos += SizeOf.SIZE_OF_INT;

    byte [] bytes = new byte[len];
    UNSAFE.copyMemory(data, BASE_OFFSET + pos, bytes, UnsafeUtil.ARRAY_BYTE_BASE_OFFSET, len);
    return StringUtils.convertBytesToChars(bytes, Charset.forName("UTF-8"));
  }

  @Override
  public Tuple clone() throws CloneNotSupportedException {
    return this;
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
  public String toString() {
    return VTuple.toDisplayString(getValues());
  }
}
