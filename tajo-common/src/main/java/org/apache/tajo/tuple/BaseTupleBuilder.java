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

import org.apache.tajo.common.TajoDataTypes.DataType;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.tuple.memory.*;
import org.apache.tajo.unit.StorageUnit;
import org.apache.tajo.util.Deallocatable;
import org.apache.tajo.util.TUtil;

public class BaseTupleBuilder extends OffHeapRowWriter implements TupleBuilder, Deallocatable {

  private MemoryBlock memoryBlock;

  public BaseTupleBuilder(DataType[] schema) {
    super(schema);
    this.memoryBlock = new ResizableMemoryBlock(new ResizableLimitSpec(64 * StorageUnit.KB), true);
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
    memoryBlock.writerPosition(0);
    return super.startRow();
  }

  @Override
  public void endRow() {
    super.endRow();
  }

  @Override
  public void addTuple(Tuple tuple) {
    if (tuple instanceof UnSafeTuple) {
      UnSafeTuple unSafeTuple = TUtil.checkTypeAndGet(tuple, UnSafeTuple.class);
      addTuple(unSafeTuple);
    } else {
      OffHeapRowBlockUtils.convert(tuple, this);
    }
  }

  @Override
  public Tuple build() {
    return buildToHeapTuple();
  }

  public HeapTuple buildToHeapTuple() {
    return buildToZeroCopyTuple().toHeapTuple();
  }

  public UnSafeTuple buildToZeroCopyTuple() {
    UnSafeTuple zcTuple = new UnSafeTuple();
    zcTuple.set(memoryBlock, memoryBlock.readerPosition(), memoryBlock.readableBytes(), dataTypes());
    return zcTuple;
  }

  public void release() {
    memoryBlock.release();
  }
}
