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

import io.netty.buffer.ByteBuf;
import org.apache.tajo.storage.BufferPool;
import org.apache.tajo.tuple.RowBlockReader;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestHeapTuple {

  @Test
  public void testHeapTupleFromOffheap() {
    MemoryRowBlock rowBlock = TestMemoryRowBlock.createRowBlock(1024);
    assertTrue(rowBlock.getMemory().getBuffer().isDirect());
    assertTrue(rowBlock.getMemory().hasAddress());

    RowBlockReader reader = rowBlock.getReader();
    assertEquals(OffHeapRowBlockReader.class, reader.getClass());

    UnSafeTuple zcTuple = new UnSafeTuple();
    int i = 0;
    while (reader.next(zcTuple)) {

      HeapTuple heapTuple = zcTuple.toHeapTuple();
      TestMemoryRowBlock.validateTupleResult(i, heapTuple);
      TestMemoryRowBlock.validateTupleResult(i, zcTuple);
      TestMemoryRowBlock.validateTupleResult(i, zcTuple.toHeapTuple());
      i++;
    }

    assertEquals(rowBlock.rows(), i);
    rowBlock.release();
  }

  @Test
  public void testHeapTupleFromHeap() throws CloneNotSupportedException {
    MemoryRowBlock rowBlock = TestMemoryRowBlock.createRowBlock(1024);
    int length = rowBlock.getMemory().writerPosition();
    //write rows to heap
    ByteBuf heapBuffer = BufferPool.heapBuffer(length, length);
    heapBuffer.writeBytes(rowBlock.getMemory().getBuffer());
    assertFalse(heapBuffer.isDirect());

    ResizableMemoryBlock memoryBlock =
        new ResizableMemoryBlock(heapBuffer);
    assertFalse(memoryBlock.hasAddress());


    RowBlockReader reader = new HeapRowBlockReader(memoryBlock, rowBlock.getDataTypes(), rowBlock.rows());
    assertEquals(HeapRowBlockReader.class, reader.getClass());
    HeapTuple heapTuple = new HeapTuple();
    int i = 0;
    while (reader.next(heapTuple)) {

      TestMemoryRowBlock.validateTupleResult(i, heapTuple);
      TestMemoryRowBlock.validateTupleResult(i, heapTuple.clone());
      i++;
    }
    assertEquals(rowBlock.rows(), i);
    rowBlock.release();
    memoryBlock.release();
  }
}