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

import org.apache.hadoop.util.IndexedSortable;
import org.apache.hadoop.util.QuickSort;
import org.apache.tajo.catalog.SortSpec;
import org.apache.tajo.exception.UnsupportedException;
import org.apache.tajo.storage.Tuple;

import java.util.Iterator;
import java.util.List;

/**
 * Extract raw level values (primitive or String/byte[]) from each of key columns before sorting
 * Uses indirection for efficient swapping
 */
public class VectorizedSorter extends ComparableVector implements IndexedSortable, TupleSorter {

  private final int[] mappings;         // index indirection

  public VectorizedSorter(List<Tuple> source, SortSpec[] sortKeys, int[] keyIndex) {
    super(source.size(), sortKeys, keyIndex);
    source.toArray(tuples);   // wish it's array list
    mappings = new int[tuples.length];
    for (int i = 0; i < tuples.length; i++) {
      for (int j = 0; j < keyIndex.length; j++) {
        vectors[j].append(tuples[i], keyIndex[j]);
      }
      mappings[i] = i;
    }
  }

  @Override
  public int compare(int i1, int i2) {
    return super.compare(mappings[i1], mappings[i2]);
  }

  @Override
  public void swap(int i1, int i2) {
    int v1 = mappings[i1];
    mappings[i1] = mappings[i2];
    mappings[i2] = v1;
  }

  @Override
  public Iterable<Tuple> sort() {
    new QuickSort().sort(this, 0, mappings.length);
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
