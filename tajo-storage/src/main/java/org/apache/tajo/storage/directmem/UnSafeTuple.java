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

import com.sun.tools.javac.util.Convert;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.DatumFactory;
import org.apache.tajo.datum.IntervalDatum;
import org.apache.tajo.datum.ProtobufDatum;
import org.apache.tajo.exception.UnsupportedException;
import org.apache.tajo.storage.Tuple;
import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;

import static org.apache.tajo.common.TajoDataTypes.Type;

public class UnSafeTuple implements Tuple {
  private static final Unsafe UNSAFE = UnsafeUtil.unsafe;

  private DirectBuffer bb;
  private int relativePos;
  private int length;
  private Type [] types;

  void set(ByteBuffer bb, int relativePos, int length, Type [] types) {
    this.bb = (DirectBuffer) bb;
    this.relativePos = relativePos;
    this.length = length;
    this.types = types;
  }

  @Override
  public int size() {
    return 0;
  }

  public ByteBuffer byteBuffer() {
    return ((ByteBuffer)((ByteBuffer)bb).duplicate().position(relativePos).limit(length)).slice();
  }

  private int getFieldOffset(int fieldId) {
    return UNSAFE.getInt(bb.address() + relativePos + SizeOf.SIZE_OF_INT + (fieldId * SizeOf.SIZE_OF_INT));
  }

  private long getFieldAddr(int fieldId) {
    int fieldOffset = getFieldOffset(fieldId);
    return bb.address() + relativePos + fieldOffset;
  }

  @Override
  public boolean contains(int fieldid) {
    return getFieldOffset(fieldid) > 0;
  }

  @Override
  public boolean isNull(int fieldid) {
    return getFieldOffset(fieldid) > 0;
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
  public void put(int fieldId, Datum[] values) {
    throw new UnsupportedException("UnSafeTuple does not support put(int, Datum []).");
  }

  @Override
  public void put(int fieldId, Tuple tuple) {
    throw new UnsupportedException("UnSafeTuple does not support put(int, Tuple).");
  }

  @Override
  public void put(Datum[] values) {
    throw new UnsupportedException("UnSafeTuple does not support put(Datum []).");
  }

  @Override
  public Datum get(int fieldId) {
    switch (types[fieldId]) {
    case BOOLEAN:
      return DatumFactory.createBool(getBool(fieldId));
    case INT1:
    case INT2:
      return DatumFactory.createInt2(getInt2(fieldId));
    case INT4:
      return DatumFactory.createInt4(getInt4(fieldId));
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
    case INET4:
      return DatumFactory.createInet4(getInt4(fieldId));
    default:
      throw new UnsupportedException("Unknown type: " + types[fieldId]);
    }
  }

  @Override
  public void setOffset(long offset) {
    throw new UnsupportedException("UnSafeTuple does not support setOffset().");
  }

  @Override
  public long getOffset() {
    throw new UnsupportedException("UnSafeTuple does not support getOffset().");
  }

  @Override
  public boolean getBool(int fieldId) {
    return UNSAFE.getByte(getFieldAddr(fieldId)) == 0x01;
  }

  @Override
  public byte getByte(int fieldId) {
    return UNSAFE.getByte(getFieldAddr(fieldId));
  }

  @Override
  public char getChar(int fieldId) {
    return UNSAFE.getChar(getFieldAddr(fieldId));
  }

  @Override
  public byte[] getBytes(int fieldId) {
    long pos = getFieldAddr(fieldId);
    int len = UNSAFE.getInt(pos);
    pos += SizeOf.SIZE_OF_INT;

    byte [] bytes = new byte[len];
    UNSAFE.copyMemory(null, pos, bytes, UNSAFE.ARRAY_BYTE_BASE_OFFSET, len);
    return bytes;
  }

  @Override
  public short getInt2(int fieldId) {
    long addr = getFieldAddr(fieldId);
    return UNSAFE.getShort(addr);
  }

  @Override
  public int getInt4(int fieldId) {
    return UNSAFE.getInt(getFieldAddr(fieldId));
  }

  @Override
  public long getInt8(int fieldId) {
    return UNSAFE.getLong(getFieldAddr(fieldId));
  }

  @Override
  public float getFloat4(int fieldId) {
    return UNSAFE.getFloat(getFieldAddr(fieldId));
  }

  @Override
  public double getFloat8(int fieldId) {
    return UNSAFE.getDouble(getFieldAddr(fieldId));
  }

  @Override
  public String getText(int fieldId) {
    long pos = getFieldAddr(fieldId);
    int len = UNSAFE.getInt(pos);
    pos += SizeOf.SIZE_OF_INT;

    byte [] bytes = new byte[len];
    UNSAFE.copyMemory(null, pos, bytes, UNSAFE.ARRAY_BYTE_BASE_OFFSET, len);
    return new String(bytes);
  }

  public IntervalDatum getInterval(int fieldId) {
    long pos = getFieldAddr(fieldId);
    int months = UNSAFE.getInt(pos);
    pos += SizeOf.SIZE_OF_INT;
    long millisecs = UNSAFE.getLong(pos);
    return new IntervalDatum(months, millisecs);
  }

  @Override
  public ProtobufDatum getProtobufDatum(int fieldId) {
    throw new UnsupportedException("UnSafeTuple does not support getOffset().");
  }

  @Override
  public char[] getUnicodeChars(int fieldId) {
    long pos = getFieldAddr(fieldId);
    int len = UNSAFE.getInt(pos);
    pos += SizeOf.SIZE_OF_INT;

    byte [] bytes = new byte[len];
    UNSAFE.copyMemory(null, pos, bytes, UNSAFE.ARRAY_BYTE_BASE_OFFSET, len);
    return Convert.utf2chars(bytes);
  }

  @Override
  public Tuple clone() throws CloneNotSupportedException {
    return this;
  }

  @Override
  public Datum[] getValues() {
    throw new UnsupportedException("UnSafeTuple does not support setOffset().");
  }
}
