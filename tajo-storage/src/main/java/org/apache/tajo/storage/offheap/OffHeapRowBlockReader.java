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

package org.apache.tajo.storage.offheap;

import org.apache.tajo.util.UnsafeUtil;
import sun.misc.Unsafe;

public class OffHeapRowBlockReader {
  private static final Unsafe UNSAFE = UnsafeUtil.unsafe;
  OffHeapRowBlock rowBlock;

  // Read States
  private int curRowIdxForRead;
  private int curPosForRead;

  public OffHeapRowBlockReader(OffHeapRowBlock rowBlock) {
    this.rowBlock = rowBlock;
  }

  public long remainForRead() {
    return rowBlock.memorySize - curPosForRead;
  }

  /**
   * Return for each tuple
   *
   * @return True if tuple block is filled with tuples. Otherwise, It will return false.
   */
  public boolean next(ZeroCopyTuple tuple) {
    if (curRowIdxForRead < rowBlock.rows()) {

      long recordStartPtr = rowBlock.address() + curPosForRead;
      int recordLen = UNSAFE.getInt(recordStartPtr);
      tuple.set(rowBlock.buffer, curPosForRead, recordLen, rowBlock.dataTypes);

      curPosForRead += recordLen;
      curRowIdxForRead++;

      return true;
    } else {
      return false;
    }
  }

  public void resetRowCursor() {
    curPosForRead = 0;
    curRowIdxForRead = 0;
  }

  public void free() {
    rowBlock = null;
  }
}
