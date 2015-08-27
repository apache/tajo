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

import io.netty.util.internal.PlatformDependent;
import org.apache.tajo.tuple.RowBlockReader;

public class OffHeapRowBlockReader implements RowBlockReader<ZeroCopyTuple> {
  private MemoryRowBlock rowBlock;

  // Read States
  private int curRowIdxForRead;
  private int curPosForRead;

  public OffHeapRowBlockReader(MemoryRowBlock rowBlock) {
    this.rowBlock = rowBlock;
  }

  public long remainForRead() {
    return rowBlock.capacity() - curPosForRead;
  }

  @Override
  public boolean next(ZeroCopyTuple tuple) {
    if (curRowIdxForRead < rowBlock.rows()) {

      long recordStartPtr = rowBlock.getMemory().address() + curPosForRead;
      int recordLen = PlatformDependent.getInt(recordStartPtr);
      tuple.set(rowBlock.getMemory(), curPosForRead, recordLen, rowBlock.getDataTypes());

      curPosForRead += recordLen;
      curRowIdxForRead++;
      rowBlock.getMemory().readerPosition(curPosForRead);

      return true;
    } else {
      return false;
    }
  }

  @Override
  public void reset() {
    curPosForRead = 0;
    curRowIdxForRead = 0;
  }
}
