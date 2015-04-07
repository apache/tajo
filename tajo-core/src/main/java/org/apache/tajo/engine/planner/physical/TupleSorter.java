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
import org.apache.tajo.storage.TupleComparator;

import java.util.Collections;
import java.util.List;

public interface TupleSorter {

  Iterable<Tuple> sort();

  public static class DefaultSorter implements TupleSorter {

    private final List<Tuple> target;
    private final TupleComparator comparator;

    public DefaultSorter(List<Tuple> target, TupleComparator comparator) {
      this.target = target;
      this.comparator = comparator;
    }

    @Override
    public Iterable<Tuple> sort() {
      Collections.sort(target, comparator);
      return target;
    }
  }
}
