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

import org.apache.tajo.storage.Tuple;
import org.apache.tajo.tuple.memory.TupleList;

import java.util.ArrayList;

/**
 * In ReferenceTupleList, input tuples are not cloned whenever the add() method is called.
 */
public class ReferenceTupleList extends ArrayList<Tuple> implements TupleList<Tuple> {

  public ReferenceTupleList() {
    super();
  }

  public ReferenceTupleList(int initialCapacity) {
    super(initialCapacity);
  }

  @Override
  public boolean add(Tuple tuple) {
    return super.add(tuple);
  }

  @Override
  public boolean addTuple(Tuple tuple) {
    return add(tuple);
  }

  @Override
  public void release() {
    super.clear();
  }
}
