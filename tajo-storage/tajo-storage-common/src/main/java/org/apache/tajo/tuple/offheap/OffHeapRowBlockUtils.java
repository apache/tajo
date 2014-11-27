/*
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

package org.apache.tajo.tuple.offheap;

import com.google.common.collect.Lists;
import org.apache.tajo.storage.Tuple;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class OffHeapRowBlockUtils {

  public static List<Tuple> sort(OffHeapRowBlock rowBlock, Comparator<Tuple> comparator) {
    List<Tuple> tupleList = Lists.newArrayList();
    ZeroCopyTuple zcTuple = new ZeroCopyTuple();
    OffHeapRowBlockReader reader = new OffHeapRowBlockReader(rowBlock);
    while(reader.next(zcTuple)) {
      tupleList.add(zcTuple);
      zcTuple = new ZeroCopyTuple();
    }
    Collections.sort(tupleList, comparator);
    return tupleList;
  }

  public static Tuple[] sortToArray(OffHeapRowBlock rowBlock, Comparator<Tuple> comparator) {
    Tuple[] tuples = new Tuple[rowBlock.rows()];
    ZeroCopyTuple zcTuple = new ZeroCopyTuple();
    OffHeapRowBlockReader reader = new OffHeapRowBlockReader(rowBlock);
    for (int i = 0; i < rowBlock.rows() && reader.next(zcTuple); i++) {
      tuples[i] = zcTuple;
      zcTuple = new ZeroCopyTuple();
    }
    Arrays.sort(tuples, comparator);
    return tuples;
  }
}
