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

import org.apache.tajo.catalog.SortSpec;
import org.apache.tajo.exception.UnsupportedException;
import org.apache.tajo.storage.Tuple;

import java.util.Iterator;
import java.util.List;

public class VectorizedTupleSorter extends VectorizedSorter implements TupleSorter {

  private final Tuple[] tuples;         // source tuples

  public VectorizedTupleSorter(List<Tuple> source, SortSpec[] sortKeys, int[] keyIndex) {
    super(source.size(), sortKeys, keyIndex);
    tuples = source.toArray(new Tuple[source.size()]);   // wish it's array list
    for (Tuple tuple : tuples) {
      addTuple(tuple);
    }
  }

  @Override
  public Iterable<Tuple> sort() {
    final int[] mappings = sortTuples();
    return new Iterable<Tuple>() {
      @Override
      public Iterator<Tuple> iterator() {
        return new Iterator<Tuple>() {
          int index;
          public boolean hasNext() { return index < mappings.length; }
          public Tuple next() { return tuples[mappings[index++]]; }
          public void remove() { throw new UnsupportedException(); }
        };
      }
    };
  }
}
