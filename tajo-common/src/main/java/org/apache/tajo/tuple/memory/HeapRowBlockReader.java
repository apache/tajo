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

import org.apache.tajo.common.TajoDataTypes.DataType;
import org.apache.tajo.tuple.RowBlockReader;

public class HeapRowBlockReader implements RowBlockReader<HeapTuple> {
  private final DataType[] dataTypes;
  private final MemoryBlock memoryBlock;
  private final int rows;

  // Read States
  private int curRowIdxForRead;
  private int curPosForRead;

  public HeapRowBlockReader(MemoryRowBlock rowBlock) {
    this(rowBlock.getMemory(), rowBlock.getDataTypes(), rowBlock.rows());
  }

  public HeapRowBlockReader(MemoryBlock memoryBlock, DataType[] dataTypes, int rows) {
    this.dataTypes = dataTypes;
    this.rows = rows;
    this.memoryBlock = memoryBlock.duplicate();
  }

  public long remainForRead() {
    return memoryBlock.readableBytes();
  }

  @Override
  public boolean next(HeapTuple tuple) {
    if (curRowIdxForRead < rows) {

      int recordLen = memoryBlock.getInt(curPosForRead);
      tuple.set(memoryBlock, curPosForRead, recordLen, dataTypes);

      curPosForRead += recordLen;
      curRowIdxForRead++;
      memoryBlock.readerPosition(curPosForRead);

      return true;
    } else {
      return false;
    }
  }

  @Override
  public void reset() {
    curPosForRead = 0;
    curRowIdxForRead = 0;
    memoryBlock.readerPosition(curPosForRead);
  }
}
