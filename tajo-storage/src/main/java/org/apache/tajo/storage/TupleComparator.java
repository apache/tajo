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

package org.apache.tajo.storage;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SortSpec;
import org.apache.tajo.common.ProtoObject;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.NullDatum;

import java.util.Comparator;

import static org.apache.tajo.catalog.proto.CatalogProtos.TupleComparatorSpecProto;
import static org.apache.tajo.index.IndexProtos.TupleComparatorProto;

/**
 * The Comparator class for Tuples
 * 
 * @see Tuple
 */
public class TupleComparator implements Comparator<Tuple>, ProtoObject<TupleComparatorProto> {
  private final int[] sortKeyIds;
  private final boolean[] asc;
  @SuppressWarnings("unused")
  private final boolean[] nullFirsts;  

  /**
   * @param schema The schema of input tuples
   * @param sortKeys The description of sort keys
   */
  public TupleComparator(Schema schema, SortSpec[] sortKeys) {
    Preconditions.checkArgument(sortKeys.length > 0, 
        "At least one sort key must be specified.");

    this.sortKeyIds = new int[sortKeys.length];
    this.asc = new boolean[sortKeys.length];
    this.nullFirsts = new boolean[sortKeys.length];
    for (int i = 0; i < sortKeys.length; i++) {
      if (sortKeys[i].getSortKey().hasQualifier()) {
        this.sortKeyIds[i] = schema.getColumnId(sortKeys[i].getSortKey().getQualifiedName());
      } else {
        this.sortKeyIds[i] = schema.getColumnIdByName(sortKeys[i].getSortKey().getSimpleName());
      }
          
      this.asc[i] = sortKeys[i].isAscending();
      this.nullFirsts[i]= sortKeys[i].isNullFirst();
    }
  }

  public TupleComparator(TupleComparatorProto proto) {
    this.sortKeyIds = new int[proto.getCompSpecsCount()];
    this.asc = new boolean[proto.getCompSpecsCount()];
    this.nullFirsts = new boolean[proto.getCompSpecsCount()];

    for (int i = 0; i < proto.getCompSpecsCount(); i++) {
      TupleComparatorSpecProto sortSepcProto = proto.getCompSpecs(i);
      sortKeyIds[i] = sortSepcProto.getColumnId();
      asc[i] = sortSepcProto.getAscending();
      nullFirsts[i] = sortSepcProto.getNullFirst();
    }
  }

  public boolean isAscendingFirstKey() {
    return this.asc[0];
  }

  @Override
  public int compare(Tuple tuple1, Tuple tuple2) {
    Datum left = null;
    Datum right = null;
    int compVal = 0;

    for (int i = 0; i < sortKeyIds.length; i++) {
      left = tuple1.get(sortKeyIds[i]);
      right = tuple2.get(sortKeyIds[i]);

      if (left instanceof NullDatum || right instanceof NullDatum) {
        if (!left.equals(right)) {
          if (left instanceof NullDatum) {
            compVal = 1;
          } else if (right instanceof NullDatum) {
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

  @Override
  public int hashCode() {
    return Objects.hashCode(sortKeyIds);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof TupleComparator) {
      TupleComparator other = (TupleComparator) obj;
      if (sortKeyIds.length != other.sortKeyIds.length) {
        return false;
      }

      for (int i = 0; i < sortKeyIds.length; i++) {
        if (sortKeyIds[i] != other.sortKeyIds[i] ||
            asc[i] != other.asc[i] ||
            nullFirsts[i] != other.nullFirsts[i]) {
          return false;
        }
      }

      return true;
    } else {
      return false;
    }
  }

  @Override
  public TupleComparatorProto getProto() {
    TupleComparatorProto.Builder builder = TupleComparatorProto.newBuilder();
    TupleComparatorSpecProto.Builder sortSpecBuilder;

    for (int i = 0; i < sortKeyIds.length; i++) {
      sortSpecBuilder = TupleComparatorSpecProto.newBuilder();
      sortSpecBuilder.setColumnId(sortKeyIds[i]);
      sortSpecBuilder.setAscending(asc[i]);
      sortSpecBuilder.setNullFirst(nullFirsts[i]);
      builder.addCompSpecs(sortSpecBuilder);
    }

    return builder.build();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();

    String prefix = "";
    for (int i = 0; i < sortKeyIds.length; i++) {
      sb.append(prefix).append("SortKeyId=").append(sortKeyIds[i])
        .append(",Asc=").append(asc[i])
        .append(",NullFirst=").append(nullFirsts[i]);
      prefix = " ,";
    }
    return sb.toString();
  }
}