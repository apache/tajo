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

import org.apache.tajo.catalog.SortSpec;
import org.apache.tajo.common.TajoDataTypes.DataType;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.TupleRange;
import org.apache.tajo.util.Bytes;

import java.math.BigInteger;

public abstract class RangePartitionAlgorithm {
  protected SortSpec [] sortSpecs;
  protected TupleRange mergedRange;
  protected final BigInteger totalCard;
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
    this.mergedRange = totalRange;
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
  public static BigInteger computeCardinality(DataType dataType, Datum start, Datum end,
                                              boolean inclusive, boolean isAscending) {
    BigInteger columnCard;

    switch (dataType.getType()) {
      case BOOLEAN:
        columnCard = BigInteger.valueOf(2);
        break;
      case CHAR:
        if (isAscending) {
          columnCard = BigInteger.valueOf((int)end.asChar() - (int)start.asChar());
        } else {
          columnCard = BigInteger.valueOf(start.asChar() - end.asChar());
        }
        break;
      case BIT:
        if (isAscending) {
          columnCard = BigInteger.valueOf(end.asByte() - start.asByte());
        } else {
          columnCard = BigInteger.valueOf(start.asByte() - end.asByte());
        }
        break;
      case INT2:
        if (isAscending) {
          columnCard = BigInteger.valueOf(end.asInt2() - start.asInt2());
        } else {
          columnCard = BigInteger.valueOf(start.asInt2() - end.asInt2());
        }
        break;
      case INT4:
        if (isAscending) {
          columnCard = BigInteger.valueOf(end.asInt4() - start.asInt4());
        } else {
          columnCard = BigInteger.valueOf(start.asInt4() - end.asInt4());
        }
        break;
    case INT8:
    case TIME:
    case TIMESTAMP:
        if (isAscending) {
          columnCard = BigInteger.valueOf(end.asInt8() - start.asInt8());
        } else {
          columnCard = BigInteger.valueOf(start.asInt8() - end.asInt8());
        }
        break;
      case FLOAT4:
        if (isAscending) {
          columnCard = BigInteger.valueOf(end.asInt4() - start.asInt4());
        } else {
          columnCard = BigInteger.valueOf(start.asInt4() - end.asInt4());
        }
        break;
      case FLOAT8:
        if (isAscending) {
          columnCard = BigInteger.valueOf(end.asInt8() - start.asInt8());
        } else {
          columnCard = BigInteger.valueOf(start.asInt8() - end.asInt8());
        }
        break;
      case TEXT: {
        byte [] a;
        byte [] b;
        if (isAscending) {
          a = start.asByteArray();
          b = end.asByteArray();
        } else {
          b = start.asByteArray();
          a = end.asByteArray();
        }

        byte [] prependHeader = {1, 0};
        final BigInteger startBI = new BigInteger(Bytes.add(prependHeader, a));
        final BigInteger stopBI = new BigInteger(Bytes.add(prependHeader, b));
        BigInteger diffBI = stopBI.subtract(startBI);
        columnCard = diffBI;
        break;
      }
      case DATE:
        if (isAscending) {
          columnCard = BigInteger.valueOf(end.asInt4() - start.asInt4());
        } else {
          columnCard = BigInteger.valueOf(start.asInt4() - end.asInt4());
        }
        break;
      case INET4:
        if (isAscending) {
          columnCard = BigInteger.valueOf(end.asInt4() - start.asInt4());
        } else {
          columnCard = BigInteger.valueOf(start.asInt4() - end.asInt4());
        }
        break;
      default:
        throw new UnsupportedOperationException(dataType + " is not supported yet");
    }

    return inclusive ? columnCard.add(BigInteger.valueOf(1)).abs() : columnCard.abs();
  }

  /**
   * It computes the value cardinality of a tuple range.
   * @return
   */
  public static BigInteger computeCardinalityForAllColumns(SortSpec[] sortSpecs, TupleRange range, boolean inclusive) {
    Tuple start = range.getStart();
    Tuple end = range.getEnd();

    BigInteger cardinality = BigInteger.ONE;
    BigInteger columnCard;
    for (int i = 0; i < sortSpecs.length; i++) {
      columnCard = computeCardinality(sortSpecs[i].getSortKey().getDataType(), start.get(i), end.get(i), inclusive,
          sortSpecs[i].isAscending());

      if (BigInteger.ZERO.compareTo(columnCard) < 0) {
        cardinality = cardinality.multiply(columnCard);
      }
    }

    return cardinality;
  }

  public BigInteger getTotalCardinality() {
    return totalCard;
  }

  /**
   *
   * @param partNum the number of desired partitions, but it may return the less partitions.
   * @return the end of intermediate ranges are exclusive, and the end of final range is inclusive.
   */
  public abstract TupleRange[] partition(int partNum);
}
