/**
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

package org.apache.tajo.engine.planner.physical;

import com.google.common.collect.Lists;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SchemaUtil;
import org.apache.tajo.common.TajoDataTypes.DataType;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.tuple.memory.FixedSizeLimitSpec;
import org.apache.tajo.tuple.memory.MemoryRowBlock;
import org.apache.tajo.tuple.memory.UnSafeTuple;
import org.apache.tajo.unit.StorageUnit;

import java.util.ArrayList;
import java.util.List;

/**
 * In UnSafeTupleList, input tuples are copied to off-heap memory page whenever the add() method is called.
 * The memory pages are automatically added, if memory of a page are exceeded.
 * This instance must be released
 */
public class UnSafeTupleList extends ArrayList<Tuple> {

  private final DataType[] dataTypes;
  private List<MemoryRowBlock> rowBlocks;
  private MemoryRowBlock currentRowBlock;
  private int totalUsedMem;
  private int pageSize;

  public UnSafeTupleList(Schema schema, int initialArraySize) {
    this(schema, initialArraySize, StorageUnit.MB);

  }

  public UnSafeTupleList(Schema schema, int initialArraySize, int pageSize) {
    super(initialArraySize);
    this.dataTypes = SchemaUtil.toDataTypes(schema);
    this.pageSize = pageSize;
    this.rowBlocks = Lists.newArrayList();
    this.currentRowBlock = new MemoryRowBlock(dataTypes, new FixedSizeLimitSpec(pageSize), true);
    this.rowBlocks.add(currentRowBlock);

  }

  @Override
  public boolean add(Tuple tuple) {

    int prevPos = currentRowBlock.getMemory().writerPosition();
    if (currentRowBlock.getWriter().addTuple(tuple)) {
      UnSafeTuple unSafeTuple = new UnSafeTuple();
      unSafeTuple.set(currentRowBlock.getMemory(), prevPos, dataTypes);
      return super.add(unSafeTuple);
    } else {
      this.totalUsedMem += currentRowBlock.usedMem();
      this.currentRowBlock = new MemoryRowBlock(dataTypes, new FixedSizeLimitSpec(pageSize), true);
      this.rowBlocks.add(currentRowBlock);
      return this.add(tuple);
    }
  }

  /**
   * Release the cached pages
   */
  public void release() {
    for (MemoryRowBlock rowBlock : rowBlocks) {
      rowBlock.release();
    }
    super.clear();
    rowBlocks.clear();
    totalUsedMem = 0;
  }

  /**
   * Total used memory
   */
  public int usedMem() {
    return totalUsedMem + currentRowBlock.usedMem();
  }

  /**
   * Release and reset
   */
  @Override
  public void clear() {
    release();
    this.currentRowBlock = new MemoryRowBlock(dataTypes, new FixedSizeLimitSpec(pageSize), true);
    this.rowBlocks.add(currentRowBlock);
  }
}
