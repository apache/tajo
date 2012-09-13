/*
 * Copyright 2012 Database Lab., Korea Univ.
 *
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

package tajo.engine.planner.physical;

import com.google.common.base.Preconditions;
import tajo.catalog.Schema;
import tajo.datum.Datum;
import tajo.datum.DatumType;
import tajo.engine.parser.QueryBlock.SortSpec;
import tajo.storage.Tuple;

import java.util.Comparator;

/**
 * The Comparator class for Tuples
 * 
 * @see Tuple
 */
public class TupleComparator implements Comparator<Tuple> {
  private final int[] sortKeyIds;
  private final boolean[] asc;
  @SuppressWarnings("unused")
  private final boolean[] nullFirsts;  

  private Datum left;
  private Datum right;
  private int compVal;

  public TupleComparator(Schema schema, SortSpec[] sortKeys) {
    Preconditions.checkArgument(sortKeys.length > 0, 
        "At least one sort key must be specified.");
    
    this.sortKeyIds = new int[sortKeys.length];
    this.asc = new boolean[sortKeys.length];
    this.nullFirsts = new boolean[sortKeys.length];
    for (int i = 0; i < sortKeys.length; i++) {
      this.sortKeyIds[i] = schema.getColumnId(sortKeys[i].getSortKey().getQualifiedName());
          
      this.asc[i] = sortKeys[i].isAscending();
      this.nullFirsts[i]= sortKeys[i].isNullFirst();
    }
  }

  @Override
  public int compare(Tuple tuple1, Tuple tuple2) {
    for (int i = 0; i < sortKeyIds.length; i++) {
      left = tuple1.get(sortKeyIds[i]);
      right = tuple2.get(sortKeyIds[i]);

      if (left.type() == DatumType.NULL || right.type() == DatumType.NULL) {
        if (!left.equals(right)) {
          if (left.type() == DatumType.NULL) {
            compVal = 1;
          } else if (right.type() == DatumType.NULL) {
            compVal = -1;
          }
          if (nullFirsts[i]) {
            if (compVal != 0) {
              compVal *= -1;
            }
          }
        } else {
          compVal = 0;
        }
      } else {
        if (asc[i]) {
          compVal = left.compareTo(right);
        } else {
          compVal = right.compareTo(left);
        }
      }

      if (compVal < 0 || compVal > 0) {
        return compVal;
      }
    }
    return 0;
  }
}