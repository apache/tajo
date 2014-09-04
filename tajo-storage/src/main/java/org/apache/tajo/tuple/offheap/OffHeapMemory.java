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

package org.apache.tajo.tuple.offheap;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.util.Deallocatable;
import org.apache.tajo.util.FileUtil;
import org.apache.tajo.util.UnsafeUtil;
import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class OffHeapMemory implements Deallocatable {
  private static final Log LOG = LogFactory.getLog(OffHeapMemory.class);

  protected static final Unsafe UNSAFE = UnsafeUtil.unsafe;

  protected ByteBuffer buffer;
  protected int memorySize;
  protected ResizableLimitSpec limitSpec;
  protected long address;

  @VisibleForTesting
  protected OffHeapMemory(ByteBuffer buffer, ResizableLimitSpec limitSpec) {
    this.buffer = buffer;
    this.address = ((DirectBuffer) buffer).address();
    this.memorySize = buffer.limit();
    this.limitSpec = limitSpec;
  }

  public OffHeapMemory(ResizableLimitSpec limitSpec) {
    this(ByteBuffer.allocateDirect((int) limitSpec.initialSize()).order(ByteOrder.nativeOrder()), limitSpec);
  }

  public long address() {
    return address;
  }

  public long size() {
    return memorySize;
  }

  public void resize(int newSize) {
    Preconditions.checkArgument(newSize > 0, "Size must be greater than 0 bytes");

    if (newSize > limitSpec.limit()) {
      throw new RuntimeException("Resize cannot exceed the size limit");
    }

    if (newSize < memorySize) {
      LOG.warn("The size reduction is ignored.");
    }

    int newBlockSize = UnsafeUtil.alignedSize(newSize);
    ByteBuffer newByteBuf = ByteBuffer.allocateDirect(newBlockSize);
    long newAddress = ((DirectBuffer)newByteBuf).address();

    UNSAFE.copyMemory(this.address, newAddress, memorySize);

    UnsafeUtil.free(buffer);
    this.memorySize = newSize;
    this.buffer = newByteBuf;
    this.address = newAddress;
  }

  public java.nio.Buffer nioBuffer() {
    return (ByteBuffer) buffer.position(0).limit(memorySize);
  }

  @Override
  public void release() {
    UnsafeUtil.free(this.buffer);
    this.buffer = null;
    this.address = 0;
    this.memorySize = 0;
  }

  public String toString() {
    return "memory=" + FileUtil.humanReadableByteCount(memorySize, false) + "," + limitSpec;
  }
}
