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

import org.apache.tajo.common.TajoDataTypes;

/**
 *
 * Row Record Structure
 *
 * | row length (4 bytes) | field 1 offset | field 2 offset | ... | field N offset| field 1 | field 2| ... | field N |
 *                              4 bytes          4 bytes               4 bytes
 *
 */
public class OffHeapRowBlockWriter extends OffHeapRowWriter {
  OffHeapRowBlock rowBlock;

  OffHeapRowBlockWriter(OffHeapRowBlock rowBlock) {
    super(rowBlock.dataTypes);
    this.rowBlock = rowBlock;
  }

  public long address() {
    return rowBlock.address();
  }

  public int position() {
    return rowBlock.position();
  }

  @Override
  public void forward(int length) {
    rowBlock.position(position() + length);
  }

  public void ensureSize(int size) {
    rowBlock.ensureSize(size);
  }

  @Override
  public void endRow() {
    super.endRow();
    rowBlock.setRows(rowBlock.rows() + 1);
  }

  @Override
  public TajoDataTypes.DataType[] dataTypes() {
    return rowBlock.dataTypes;
  }
}
