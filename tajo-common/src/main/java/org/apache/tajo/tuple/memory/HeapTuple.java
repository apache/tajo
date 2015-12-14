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

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.datum.*;
import org.apache.tajo.exception.TajoRuntimeException;
import org.apache.tajo.exception.UnsupportedException;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.util.SizeOf;
import org.apache.tajo.util.StringUtils;
import org.apache.tajo.util.datetime.TimeMeta;

import java.nio.ByteOrder;
import java.util.Arrays;

import static org.apache.tajo.common.TajoDataTypes.DataType;

public class HeapTuple extends ZeroCopyTuple implements Cloneable {
  private ByteBuf buffer;
  private DataType[] types;

  @Override
  public void set(MemoryBlock memoryBlock, int relativePos, DataType[] types) {
    this.buffer = memoryBlock.getBuffer();
    this.types = types;
    super.set(relativePos);
  }

  protected void set(final byte[] bytes, final DataType[] types) {
    this.buffer = Unpooled.wrappedBuffer(bytes).order(ByteOrder.LITTLE_ENDIAN);
    this.types = types;
    super.set(0);
  }

  @Override
  public int size() {
    return types.length;
  }

  @Override
  public int getLength() {
    return buffer.getInt(getRelativePos());
  }

  @Override
  public TajoDataTypes.Type type(int fieldId) {
    return types[fieldId].getType();
  }

  @Override
  public int size(int fieldId) {
    return buffer.getInt(checkNullAndGetOffset(fieldId));
  }

  @Override
  public void clearOffset() {
  }

  private int getFieldOffset(int fieldId) {
    return buffer.getInt(getRelativePos() + SizeOf.SIZE_OF_INT + (fieldId * SizeOf.SIZE_OF_INT));
  }

  private int checkNullAndGetOffset(int fieldId) {
    int offset = getFieldOffset(fieldId);
    if (offset == MemoryRowBlock.NULL_FIELD_OFFSET) {
      throw new RuntimeException("Invalid Field Access: " + fieldId);
    }
    return offset + getRelativePos();
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
  public void insertTuple(int fieldId, Tuple tuple) {
    throw new TajoRuntimeException(new UnsupportedException());
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
    case BLOB :
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
  public void setOffset(long offset) {
  }

  @Override
  public long getOffset() {
    return 0;
  }

  @Override
  public boolean getBool(int fieldId) {
    return buffer.getByte(checkNullAndGetOffset(fieldId)) == 0x01;
  }

  @Override
  public byte getByte(int fieldId) {
    return buffer.getByte(checkNullAndGetOffset(fieldId));
  }

  @Override
  public char getChar(int fieldId) {
    return buffer.getChar(checkNullAndGetOffset(fieldId));
  }

  @Override
  public byte[] getBytes(int fieldId) {
    int pos = checkNullAndGetOffset(fieldId);
    int len = buffer.getInt(pos);

    byte [] bytes = new byte[len];
    buffer.getBytes(pos + SizeOf.SIZE_OF_INT, bytes);
    return bytes;
  }

  @Override
  public byte[] getTextBytes(int fieldId) {
    return asDatum(fieldId).asTextBytes();
  }

  @Override
  public short getInt2(int fieldId) {
    return buffer.getShort(checkNullAndGetOffset(fieldId));
  }

  @Override
  public int getInt4(int fieldId) {
    return buffer.getInt(checkNullAndGetOffset(fieldId));
  }

  @Override
  public long getInt8(int fieldId) {
    return buffer.getLong(checkNullAndGetOffset(fieldId));
  }

  @Override
  public float getFloat4(int fieldId) {
    return buffer.getFloat(checkNullAndGetOffset(fieldId));
  }

  @Override
  public double getFloat8(int fieldId) {
    return buffer.getDouble(checkNullAndGetOffset(fieldId));
  }

  @Override
  public String getText(int fieldId) {
    return new String(getBytes(fieldId), TextDatum.DEFAULT_CHARSET);
  }

  @Override
  public TimeMeta getTimeDate(int fieldId) {
    return asDatum(fieldId).asTimeMeta();
  }

  public IntervalDatum getInterval(int fieldId) {
    int pos = checkNullAndGetOffset(fieldId);
    int months = buffer.getInt(pos);
    long millisecs = buffer.getLong(pos + SizeOf.SIZE_OF_INT);
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
    int pos = checkNullAndGetOffset(fieldId);
    int len = buffer.getInt(pos);

    byte [] bytes = new byte[len];
    buffer.getBytes(pos + SizeOf.SIZE_OF_INT, bytes);
    return StringUtils.convertBytesToChars(bytes, TextDatum.DEFAULT_CHARSET);
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
  public Tuple clone() throws CloneNotSupportedException {
    HeapTuple heapTuple = (HeapTuple) super.clone();
    heapTuple.buffer = buffer.copy(getRelativePos(), getLength());
    heapTuple.relativePos = 0;
    return heapTuple;
  }
}
