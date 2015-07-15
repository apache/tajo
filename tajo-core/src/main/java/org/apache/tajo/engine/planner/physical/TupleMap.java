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

import org.apache.tajo.annotation.Nullable;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;

import java.util.HashMap;

public class TupleMap<E> extends HashMap<Tuple, E> {

  public TupleMap() {
    super();
  }

  public TupleMap(int initialCapacity) {
    super(initialCapacity);
  }

  public TupleMap(TupleMap tupleMap){
    super(tupleMap);
  }

  @Override
  public E put(@Nullable Tuple key, E value) {
    if (key != null) {
      return super.put(new VTuple(key.getValues()), value);
    } else {
      return super.put(null, value);
    }
  }

  public E putWihtoutKeyCopy(@Nullable Tuple key, E value) {
    return super.put(key, value);
  }
}
