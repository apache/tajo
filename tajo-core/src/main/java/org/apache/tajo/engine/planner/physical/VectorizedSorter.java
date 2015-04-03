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
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SortSpec;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.util.ComparableVector;

import java.util.Arrays;

/**
 * Extract raw level values (primitive or String/byte[]) from each of key columns before sorting
 * Uses indirection for efficient swapping
 */
public class VectorizedSorter extends ComparableVector implements IndexedSortable {

  private final int[] mappings;         // index indirection

  private int counter;

  public VectorizedSorter(int limit, SortSpec[] sortKeys, int[] keyIndex) {
    super(limit, sortKeys, keyIndex);
    mappings = new int[limit];
  }

  public VectorizedSorter(int limit, Schema schema, SortSpec[] sortKeys, int[] keyIndex) {
    super(limit, schema, sortKeys, keyIndex);
    mappings = new int[limit];
  }

  public boolean addTuple(Tuple tuple) {
    if (counter >= mappings.length) {
      return false;
    }
    append(tuple);
    mappings[counter] = counter;
    counter++;
    return true;
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

  public int[] sortTuples() {
    if (counter > 0) {
      new QuickSort().sort(this, 0, counter);
    }
    return mapping();
  }

  public int size() {
    return counter;
  }

  @Override
  public void reset() {
    super.reset();
    counter = 0;
  }

  public int[] mapping() {
    return Arrays.copyOf(mappings, counter);
  }
}
