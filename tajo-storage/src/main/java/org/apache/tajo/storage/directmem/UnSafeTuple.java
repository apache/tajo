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
import org.apache.tajo.datum.ProtobufDatum;
import org.apache.tajo.exception.UnsupportedException;
import org.apache.tajo.storage.Tuple;
import sun.misc.Unsafe;

import static org.apache.tajo.common.TajoDataTypes.Type;

public class UnSafeTuple implements Tuple {
  private static final Unsafe UNSAFE = UnsafeUtil.unsafe;

  private long recordPtr;
  private int recordSize;

  private Type [] types;

  void set(long address, int length, Type [] types) {
    this.recordPtr = address;
    this.recordSize = length;
    this.types = types;
  }

  @Override
  public int size() {
    return 0;
  }

  private int getFieldOffset(int fieldId) {
    return UNSAFE.getInt(recordPtr + (fieldId * SizeOf.SIZE_OF_INT));
  }

  private long getFieldAddr(int fieldId) {
    int relativePos = UNSAFE.getInt(recordPtr + 4 + (fieldId * SizeOf.SIZE_OF_INT));
    return recordPtr + relativePos;
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

    }
    return null;
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
