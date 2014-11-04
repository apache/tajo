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
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.tuple.offheap.HeapTuple;
import org.apache.tajo.tuple.offheap.OffHeapRowWriter;
import org.apache.tajo.tuple.offheap.ZeroCopyTuple;
import org.apache.tajo.unit.StorageUnit;
import org.apache.tajo.util.Deallocatable;
import org.apache.tajo.util.FileUtil;
import org.apache.tajo.util.UnsafeUtil;
import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class BaseTupleBuilder extends OffHeapRowWriter implements TupleBuilder, Deallocatable {
  private static final Log LOG = LogFactory.getLog(BaseTupleBuilder.class);

  private static final Unsafe UNSAFE = UnsafeUtil.unsafe;

  // buffer
  private ByteBuffer buffer;
  private long address;

  public BaseTupleBuilder(Schema schema) {
    super(SchemaUtil.toDataTypes(schema));
    buffer = ByteBuffer.allocateDirect(64 * StorageUnit.KB).order(ByteOrder.nativeOrder());
    address = UnsafeUtil.getAddress(buffer);
  }

  @Override
  public long address() {
    return address;
  }

  public void ensureSize(int size) {
    if (buffer.remaining() - size < 0) { // check the remain size
      // enlarge new buffer and copy writing data
      int newBlockSize = UnsafeUtil.alignedSize(buffer.capacity() * 2);
      ByteBuffer newByteBuf = ByteBuffer.allocateDirect(newBlockSize);
      long newAddress = ((DirectBuffer)newByteBuf).address();
      UNSAFE.copyMemory(this.address, newAddress, buffer.limit());
      LOG.debug("Increase the buffer size to " + FileUtil.humanReadableByteCount(newBlockSize, false));

      // release existing buffer and replace variables
      UnsafeUtil.free(buffer);
      buffer = newByteBuf;
      address = newAddress;
    }
  }

  @Override
  public int position() {
    return 0;
  }

  @Override
  public void forward(int length) {
  }

  @Override
  public void endRow() {
    super.endRow();
    buffer.position(0).limit(offset());
  }

  @Override
  public Tuple build() {
    return buildToHeapTuple();
  }

  public HeapTuple buildToHeapTuple() {
    byte [] bytes = new byte[buffer.limit()];
    UNSAFE.copyMemory(null, address, bytes, UnsafeUtil.ARRAY_BOOLEAN_BASE_OFFSET, buffer.limit());
    return new HeapTuple(bytes, dataTypes());
  }

  public ZeroCopyTuple buildToZeroCopyTuple() {
    ZeroCopyTuple zcTuple = new ZeroCopyTuple();
    zcTuple.set(buffer, 0, buffer.limit(), dataTypes());
    return zcTuple;
  }

  public void release() {
    UnsafeUtil.free(buffer);
    buffer = null;
    address = 0;
  }
}
