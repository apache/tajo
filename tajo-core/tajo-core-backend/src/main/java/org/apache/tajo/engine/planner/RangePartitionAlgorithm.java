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

package org.apache.tajo.engine.planner;

import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.SortSpec;
import org.apache.tajo.common.TajoDataTypes.DataType;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.TupleRange;

import java.math.BigDecimal;

public abstract class RangePartitionAlgorithm {
  protected SortSpec [] sortSpecs;
  protected TupleRange range;
  protected final BigDecimal totalCard;
  /** true if the end of the range is inclusive. Otherwise, it should be false. */
  protected final boolean inclusive;

  /**
   *
   * @param sortSpecs The array of sort keys
   * @param totalRange The total range to be partition
   * @param inclusive true if the end of the range is inclusive. Otherwise, false.
   */
  public RangePartitionAlgorithm(SortSpec [] sortSpecs, TupleRange totalRange, boolean inclusive) {
    this.sortSpecs = sortSpecs;
    this.range = totalRange;
    this.inclusive = inclusive;
    this.totalCard = computeCardinalityForAllColumns(sortSpecs, totalRange, inclusive);
  }

  /**
   * It computes the value cardinality of a tuple range.
   *
   * @param dataType
   * @param start
   * @param end
   * @return
   */
  public static BigDecimal computeCardinality(DataType dataType, Datum start, Datum end,
                                              boolean inclusive, boolean isAscending) {
    BigDecimal columnCard;

    switch (dataType.getType()) {
      case BOOLEAN:
        columnCard = new BigDecimal(2);
        break;
      case CHAR:
        if (isAscending) {
          columnCard = new BigDecimal(end.asChar() - start.asChar());
        } else {
          columnCard = new BigDecimal(start.asChar() - end.asChar());
        }
        break;
      case BIT:
        if (isAscending) {
          columnCard = new BigDecimal(end.asByte() - start.asByte());
        } else {
          columnCard = new BigDecimal(start.asByte() - end.asByte());
        }
        break;
      case INT2:
        if (isAscending) {
          columnCard = new BigDecimal(end.asInt2() - start.asInt2());
        } else {
          columnCard = new BigDecimal(start.asInt2() - end.asInt2());
        }
        break;
      case INT4:
        if (isAscending) {
          columnCard = new BigDecimal(end.asInt4() - start.asInt4());
        } else {
          columnCard = new BigDecimal(start.asInt4() - end.asInt4());
        }
        break;
      case INT8:
        if (isAscending) {
          columnCard = new BigDecimal(end.asInt8() - start.asInt8());
        } else {
          columnCard = new BigDecimal(start.asInt8() - end.asInt8());
        }
        break;
      case FLOAT4:
        if (isAscending) {
          columnCard = new BigDecimal(end.asInt4() - start.asInt4());
        } else {
          columnCard = new BigDecimal(start.asInt4() - end.asInt4());
        }
        break;
      case FLOAT8:
        if (isAscending) {
          columnCard = new BigDecimal(end.asInt8() - start.asInt8());
        } else {
          columnCard = new BigDecimal(start.asInt8() - end.asInt8());
        }
        break;
      case TEXT:
        if (isAscending) {
          columnCard = new BigDecimal(end.asChars().charAt(0) - start.asChars().charAt(0));
        } else {
          columnCard = new BigDecimal(start.asChars().charAt(0) - end.asChars().charAt(0));
        }
        break;
      case DATE:
        if (isAscending) {
          columnCard = new BigDecimal(end.asInt4() - start.asInt4());
        } else {
          columnCard = new BigDecimal(start.asInt4() - end.asInt4());
        }
        break;
      case TIME:
      case TIMESTAMP:
        if (isAscending) {
          columnCard = new BigDecimal(end.asInt8() - start.asInt8());
        } else {
          columnCard = new BigDecimal(start.asInt8() - end.asInt8());
        }
        break;
      default:
        throw new UnsupportedOperationException(dataType + " is not supported yet");
    }

    return inclusive ? columnCard.add(new BigDecimal(1)) : columnCard;
  }

  /**
   * It computes the value cardinality of a tuple range.
   * @return
   */
  public static BigDecimal computeCardinalityForAllColumns(SortSpec[] sortSpecs, TupleRange range, boolean inclusive) {
    Tuple start = range.getStart();
    Tuple end = range.getEnd();
    Column col;

    BigDecimal cardinality = new BigDecimal(1);
    BigDecimal columnCard;
    for (int i = 0; i < sortSpecs.length; i++) {
      columnCard = computeCardinality(sortSpecs[i].getSortKey().getDataType(), start.get(i), end.get(i), inclusive,
          sortSpecs[i].isAscending());

      if (new BigDecimal(0).compareTo(columnCard) < 0) {
        cardinality = cardinality.multiply(columnCard);
      }
    }

    return cardinality;
  }

  public BigDecimal getTotalCardinality() {
    return totalCard;
  }

  /**
   *
   * @param partNum the number of desired partitions, but it may return the less partitions.
   * @return the end of intermediate ranges are exclusive, and the end of final range is inclusive.
   */
  public abstract TupleRange[] partition(int partNum);
}
