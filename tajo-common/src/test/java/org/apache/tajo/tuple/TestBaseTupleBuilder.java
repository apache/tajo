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

package org.apache.tajo.tuple;

import org.apache.tajo.tuple.memory.*;
import org.junit.Test;

public class TestBaseTupleBuilder {

  @Test
  public void testBuild() {
    BaseTupleBuilder builder = new BaseTupleBuilder(TestMemoryRowBlock.schema);

    MemoryRowBlock rowBlock = TestMemoryRowBlock.createRowBlock(10248);
    RowBlockReader reader = rowBlock.getReader();

    ZeroCopyTuple inputTuple = new UnSafeTuple();

    HeapTuple heapTuple;
    ZeroCopyTuple zcTuple;
    int i = 0;
    while(reader.next(inputTuple)) {
      OffHeapRowBlockUtils.convert(inputTuple, builder);

      zcTuple = builder.buildToZeroCopyTuple();
      TestMemoryRowBlock.validateTupleResult(i, zcTuple);

      heapTuple = builder.buildToHeapTuple();
      TestMemoryRowBlock.validateTupleResult(i, heapTuple);

      i++;
    }
    builder.release();
    rowBlock.release();
  }

  @Test
  public void testBuildWithNull() {
    BaseTupleBuilder builder = new BaseTupleBuilder(TestMemoryRowBlock.schema);

    MemoryRowBlock rowBlock = TestMemoryRowBlock.createRowBlockWithNull(10248);
    RowBlockReader reader = rowBlock.getReader();

    ZeroCopyTuple inputTuple = new UnSafeTuple();

    HeapTuple heapTuple;
    ZeroCopyTuple zcTuple;
    int i = 0;
    while(reader.next(inputTuple)) {
      OffHeapRowBlockUtils.convert(inputTuple, builder);

      heapTuple = builder.buildToHeapTuple();
      TestMemoryRowBlock.validateNullity(i, heapTuple);

      zcTuple = builder.buildToZeroCopyTuple();
      TestMemoryRowBlock.validateNullity(i, zcTuple);

      i++;
    }

    builder.release();
    rowBlock.release();
  }
}