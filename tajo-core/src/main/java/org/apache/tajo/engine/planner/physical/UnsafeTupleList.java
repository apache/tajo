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

import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SchemaUtil;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.tuple.memory.MemoryRowBlock;

import java.util.ArrayList;

/**
 * In TupleList, input tuples are automatically cloned whenever the add() method is called.
 * This data structure is usually used in physical operators like hash join or hash aggregation.
 */
public class UnsafeTupleList extends ArrayList<Tuple> {

  private MemoryRowBlock rowBlock;

  public UnsafeTupleList(Schema schema) {
    super();
    this.rowBlock = new MemoryRowBlock(SchemaUtil.toDataTypes(schema));
  }

  public UnsafeTupleList(Schema schema, int initialCapacity) {
    super(10000);
    this.rowBlock = new MemoryRowBlock(SchemaUtil.toDataTypes(schema), initialCapacity);
  }

  @Override
  public boolean add(Tuple tuple) {
    return super.add(rowBlock.getWriter().addTuple(tuple));
  }

  public void release() {
    rowBlock.release();
    super.clear();
  }

  public int usedMem() {
    return rowBlock.usedMem();
  }

  public float usage() {
    return rowBlock.usage();
  }

  @Override
  public void clear() {
    super.clear();
    rowBlock.clear();
  }
}
