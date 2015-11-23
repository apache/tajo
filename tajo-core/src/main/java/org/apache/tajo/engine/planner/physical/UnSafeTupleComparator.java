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

import com.google.common.base.Preconditions;
import com.google.common.primitives.*;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.SortSpec;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.exception.TajoRuntimeException;
import org.apache.tajo.exception.UnsupportedException;
import org.apache.tajo.tuple.memory.UnSafeTuple;
import org.apache.tajo.tuple.memory.UnSafeTupleBytesComparator;

import java.util.Arrays;
import java.util.Comparator;

/**
 * The Comparator class for UnSafeTuples
 * 
 * @see UnSafeTuple
 */
public class UnSafeTupleComparator implements Comparator<UnSafeTuple> {
  private final Schema schema;
  private final SortSpec[] sortSpecs;
  private final int[] sortKeyIds;
  private final Type[] sortKeyTypes;
  private final boolean[] asc;
  @SuppressWarnings("unused")
  private final boolean[] nullFirsts;

  /**
   * @param schema   The schema of input tuples
   * @param sortKeys The description of sort keys
   */
  public UnSafeTupleComparator(Schema schema, SortSpec[] sortKeys) {
    Preconditions.checkArgument(sortKeys.length > 0,
        "At least one sort key must be specified.");

    this.schema = schema;
    this.sortSpecs = sortKeys;
    this.sortKeyIds = new int[sortKeys.length];
    this.sortKeyTypes = new Type[sortKeys.length];
    this.asc = new boolean[sortKeys.length];
    this.nullFirsts = new boolean[sortKeys.length];
    for (int i = 0; i < sortKeys.length; i++) {
      if (sortKeys[i].getSortKey().hasQualifier()) {
        this.sortKeyIds[i] = schema.getColumnId(sortKeys[i].getSortKey().getQualifiedName());
      } else {
        this.sortKeyIds[i] = schema.getColumnIdByName(sortKeys[i].getSortKey().getSimpleName());
      }

      this.asc[i] = sortKeys[i].isAscending();
      this.nullFirsts[i] = sortKeys[i].isNullsFirst();
      this.sortKeyTypes[i] = sortKeys[i].getSortKey().getDataType().getType();
    }
  }

  public Schema getSchema() {
    return schema;
  }

  public SortSpec[] getSortSpecs() {
    return sortSpecs;
  }

  @Override
  public int compare(UnSafeTuple tuple1, UnSafeTuple tuple2) {
    for (int i = 0; i < sortKeyIds.length; i++) {
      int compare = compareColumn(tuple1, tuple2, sortKeyIds[i], asc[i]);
      if (compare != 0) {
        return compare;
      }
    }
    return 0;
  }

  private final int compareColumn(UnSafeTuple tuple1, UnSafeTuple tuple2, int index, boolean ascending) {
    final boolean n1 = tuple1.isBlankOrNull(index);
    final boolean n2 = tuple2.isBlankOrNull(index);
    if (n1 && n2) {
      return 0;
    }

    if (n1 ^ n2) {
      return nullFirsts[index] ? (n1 ? -1 : 1) : (n1 ? 1 : -1);
    }

    int compare;
    switch (sortKeyTypes[index]) {
    case BOOLEAN:
      compare = Booleans.compare(tuple1.getBool(index), tuple2.getBool(index));
      break;
    case BIT:
      compare = tuple1.getByte(index) - tuple2.getByte(index);
      break;
    case INT1:
    case INT2:
      compare = Shorts.compare(tuple1.getInt2(index), tuple2.getInt2(index));
      break;
    case DATE:
    case INET4:
    case INT4:
      compare = Ints.compare(tuple1.getInt4(index), tuple2.getInt4(index));
      break;
    case TIME:
    case TIMESTAMP:
    case INT8:
      compare = Longs.compare(tuple1.getInt8(index), tuple2.getInt8(index));
      break;
    case FLOAT4:
      compare = Floats.compare(tuple1.getFloat4(index), tuple2.getFloat4(index));
      break;
    case FLOAT8:
      compare = Doubles.compare(tuple1.getFloat8(index), tuple2.getFloat8(index));
      break;
    case CHAR:
    case TEXT:
    case BLOB:
      compare = UnSafeTupleBytesComparator.compare(tuple1.getFieldAddr(index), tuple2.getFieldAddr(index));
      break;
    default:
      throw new TajoRuntimeException(
          new UnsupportedException("unknown data type '" + sortKeyTypes[index].name() + "'"));
    }
    return ascending ? compare : -compare;
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(sortSpecs);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof UnSafeTupleComparator) {
      UnSafeTupleComparator other = (UnSafeTupleComparator) obj;
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