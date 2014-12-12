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

import org.apache.tajo.storage.RowStoreUtil;
import org.apache.tajo.tuple.offheap.*;
import org.junit.Test;

public class TestBaseTupleBuilder {

  @Test
  public void testBuild() {
    BaseTupleBuilder builder = new BaseTupleBuilder(TestOffHeapRowBlock.schema);

    OffHeapRowBlock rowBlock = TestOffHeapRowBlock.createRowBlock(10248);
    OffHeapRowBlockReader reader = rowBlock.getReader();

    ZeroCopyTuple inputTuple = new ZeroCopyTuple();

    HeapTuple heapTuple = null;
    ZeroCopyTuple zcTuple = null;
    int i = 0;
    while(reader.next(inputTuple)) {
      RowStoreUtil.convert(inputTuple, builder);

      heapTuple = builder.buildToHeapTuple();
      TestOffHeapRowBlock.validateTupleResult(i, heapTuple);

      zcTuple = builder.buildToZeroCopyTuple();
      TestOffHeapRowBlock.validateTupleResult(i, zcTuple);

      i++;
    }
  }

  @Test
  public void testBuildWithNull() {
    BaseTupleBuilder builder = new BaseTupleBuilder(TestOffHeapRowBlock.schema);

    OffHeapRowBlock rowBlock = TestOffHeapRowBlock.createRowBlockWithNull(10248);
    OffHeapRowBlockReader reader = rowBlock.getReader();

    ZeroCopyTuple inputTuple = new ZeroCopyTuple();

    HeapTuple heapTuple = null;
    ZeroCopyTuple zcTuple = null;
    int i = 0;
    while(reader.next(inputTuple)) {
      RowStoreUtil.convert(inputTuple, builder);

      heapTuple = builder.buildToHeapTuple();
      TestOffHeapRowBlock.validateNullity(i, heapTuple);

      zcTuple = builder.buildToZeroCopyTuple();
      TestOffHeapRowBlock.validateNullity(i, zcTuple);

      i++;
    }
  }
}