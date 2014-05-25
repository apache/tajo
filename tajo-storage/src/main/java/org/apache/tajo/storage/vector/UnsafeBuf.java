/**
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

package org.apache.tajo.storage.vector;

import com.google.common.base.Preconditions;
import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;

public class UnsafeBuf {
  public long address;
  public int length;
  public Object reference;

  public UnsafeBuf(ByteBuffer bytebuffer) {
    DirectBuffer df = (DirectBuffer) bytebuffer;
    address = df.address();
    length = bytebuffer.capacity();
    reference = bytebuffer;
  }

  public UnsafeBuf(long address, int length) {
    this.address = address;
    this.length = length;
  }

  public long address() {
    return address;
  }

  public byte getByte(int offset) {
    return UnsafeUtil.unsafe.getByte(address + offset);
  }

  public void putByte(int offset, byte value) {
    UnsafeUtil.unsafe.putByte(address + offset, value);
  }

  public short getShort(int offset) {
    return UnsafeUtil.unsafe.getShort(address + offset);
  }

  public void putShort(int offset, short value) {
    UnsafeUtil.unsafe.putShort(address + offset, value);
  }

  public long getLong(int offset) {
    return UnsafeUtil.unsafe.getLong(address + offset);
  }

  public void putLong(int offset, long value) {
    UnsafeUtil.unsafe.putLong(address + offset, value);
  }

  public double getDouble(int offset) {
    return UnsafeUtil.unsafe.getDouble(address + offset);
  }

  public void putFloat8(int offset, double value) {
    UnsafeUtil.unsafe.putDouble(address + offset, value);
  }

  public void putBytes(byte[] bytes, int offset) {
    UnsafeUtil.putBytes(address + offset, bytes, 0, bytes.length);
  }

  public void putBytes(int destPos, long srcAddr, int length) {
    UnsafeUtil.unsafe.copyMemory(null, srcAddr, null, address + destPos, length);
  }

  public UnsafeBuf copyOf() {
    ByteBuffer bytebuf = ByteBuffer.allocateDirect(length);
    UnsafeUtil.unsafe.copyMemory(null, this.address, null, ((DirectBuffer)bytebuf).address(), length);
    return new UnsafeBuf(bytebuf);
  }

  public void copyTo(UnsafeBuf dest) {
    Preconditions.checkArgument(dest.length >= length, "Target buffer size is less than that of the source buffer");
    UnsafeUtil.unsafe.copyMemory(null, this.address, null, dest.address, length);
  }

  /**
   * This method is forked from Guava.
   */
  public boolean equals(Object obj) {
    if (obj instanceof UnsafeBuf) {
      UnsafeBuf another = (UnsafeBuf) obj;
      return UnsafeUtil.equalStrings(this.address, length, another.address, another.length);
    } else {
      return false;
    }
  }

  public String toString() {
    return "addr=" + address + ",len=" + length;
  }
}
