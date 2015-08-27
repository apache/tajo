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

import io.netty.util.internal.PlatformDependent;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.common.TajoDataTypes.DataType;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.tuple.memory.*;
import org.apache.tajo.unit.StorageUnit;
import org.apache.tajo.util.Deallocatable;

public class BaseTupleBuilder extends OffHeapRowWriter implements TupleBuilder, Deallocatable {
  private static final Log LOG = LogFactory.getLog(BaseTupleBuilder.class);

  // buffer
  private MemoryBlock memoryBlock;

  public BaseTupleBuilder(DataType[] schema) {
    super(schema);
    this.memoryBlock = new OffHeapMemoryBlock(new ResizableLimitSpec(64 * StorageUnit.KB));
  }

  @Override
  public long address() {
    return memoryBlock.address();
  }

  public void ensureSize(int size) {
    memoryBlock.ensureSize(size);
  }

  @Override
  public int position() {
    return memoryBlock.writerPosition();
  }

  @Override
  public void forward(int length) {
    memoryBlock.writerPosition(memoryBlock.writerPosition() + length);
  }

  @Override
  public boolean startRow() {
    return super.startRow();
  }

  @Override
  public void endRow() {
    super.endRow();
  }

  @Override
  public Tuple build() {
    return buildToHeapTuple();
  }

  public HeapTuple buildToHeapTuple() {
    byte[] bytes = new byte[memoryBlock.readableBytes()];
    PlatformDependent.copyMemory(memoryBlock.address(), bytes, memoryBlock.readerPosition(), bytes.length);
    memoryBlock.writerPosition(0);
    return new HeapTuple(bytes, dataTypes());
  }

  public ZeroCopyTuple buildToZeroCopyTuple() {
    ZeroCopyTuple zcTuple = new ZeroCopyTuple();
    zcTuple.set(memoryBlock, memoryBlock.readerPosition(), memoryBlock.readableBytes(), dataTypes());
    memoryBlock.writerPosition(0);
    return zcTuple;
  }

  public void release() {
    memoryBlock.release();
  }
}
