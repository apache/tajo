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

import java.util.HashMap;

/**
 * TupleMap is a map which uses KeyTuple with its key.
 * Please note that every put() call creates a copy of input KeyTuple.
 * This data structure is usually used in physical operators like hash join or hash aggregation.
 *
 * @param <E> value type
 */
public class TupleMap<E> extends HashMap<KeyTuple, E> {

  public TupleMap() {
    super();
  }

  public TupleMap(int initialCapacity) {
    super(initialCapacity);
  }

  public TupleMap(TupleMap tupleMap){
    super(tupleMap);
  }

  /**
   * Add a pair of (key, value).
   * The key is always copied.
   *
   * @param key
   * @param value
   * @return
   */
  @Override
  public E put(@Nullable KeyTuple key, E value) {
    if (key != null) {
      try {
        return super.put(key.clone(), value);
      } catch (CloneNotSupportedException e) {
        throw new RuntimeException(e);
      }
    } else {
      return super.put(null, value);
    }
  }

  /**
   * Add a pair of (key, value).
   * The key is not copied.
   *
   * @param key
   * @param value
   * @return
   */
  public E putWihtoutKeyCopy(@Nullable KeyTuple key, E value) {
    return super.put(key, value);
  }
}
