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

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import org.apache.tajo.plan.expr.EvalNode;
import org.apache.tajo.storage.Tuple;

import java.util.Arrays;
import java.util.SortedMap;

@SuppressWarnings("unchecked")
public enum ThetaOperation {
  LE {
    @Override
    <T> Iterable<T> filter(SortedMap<Tuple, T> sortedMap, Tuple tuple) {
      Iterable<T> iterable = sortedMap.headMap(tuple).values();
      T equal = sortedMap.get(tuple);
      return equal == null ? iterable : Iterables.concat(iterable, Arrays.asList(equal));
    }
  },
  LT {
    @Override
    <T> Iterable<T> filter(SortedMap<Tuple, T> sortedMap, Tuple tuple) {
      return sortedMap.headMap(tuple).values();
    }
  },
  GE {
    @Override
    <T> Iterable<T> filter(SortedMap<Tuple, T> sortedMap, Tuple tuple) {
      return sortedMap.tailMap(tuple).values();
    }
  },
  GT {
    @Override
    <T> Iterable<T> filter(SortedMap<Tuple, T> sortedMap, Tuple tuple) {
      Iterable<T> iterable = sortedMap.tailMap(tuple).values();
      final T equal = sortedMap.get(tuple);
      return equal == null ? iterable : Iterables.filter(iterable,
          new Predicate<T>() { public boolean apply(T input) { return input != equal; } }
      );
    }
  },
  NE {
    @Override
    <T> Iterable<T> filter(SortedMap<Tuple, T> sortedMap, Tuple tuple) {
      Iterable<T> iterable = sortedMap.values();
      final T equal = sortedMap.get(tuple);
      return equal == null ? iterable : Iterables.filter(iterable,
          new Predicate<T>() { public boolean apply(T input) { return input != equal; } }
      );
    }
  };
  abstract <T> Iterable<T> filter(SortedMap<Tuple, T> sortedMap, Tuple tuple);

  public static ThetaOperation valueOf(EvalNode eval) {
    switch (eval.getType()) {
      case NOT_EQUAL: return NE;
      case LTH: return LT;
      case LEQ: return LE;
      case GTH: return GT;
      case GEQ: return GE;
      default: throw new IllegalArgumentException("Unsupported operation " + eval.getType());
    }
  }
}
