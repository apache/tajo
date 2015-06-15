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
import org.apache.tajo.util.BytesUtils;
import org.apache.tajo.util.StringUtils;

import java.math.BigInteger;

public abstract class RangePartitionAlgorithm {
  protected SortSpec [] sortSpecs;
  protected TupleRange mergedRange;
  protected final BigInteger totalCard; // total cardinality
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
    try {
      this.mergedRange = totalRange.clone();
    } catch (CloneNotSupportedException e) {
      throw new RuntimeException(e);
    }
    this.inclusive = inclusive;
    this.totalCard = computeCardinalityForAllColumns(sortSpecs, totalRange, inclusive);
  }

  /**
   * It computes the value cardinality of a tuple range.
   *
   * @param dataType
   * @param range
   * @param i
   * @return
   */
  public static BigInteger computeCardinality(DataType dataType, TupleRange range, int i,
                                              boolean inclusive, boolean isAscending) {
    BigInteger columnCard;

    Tuple start = range.getStart();
    Tuple end = range.getEnd();
    switch (dataType.getType()) {
      case BOOLEAN:
        columnCard = BigInteger.valueOf(2);
        break;
      case CHAR:
        if (isAscending) {
          columnCard = BigInteger.valueOf((int)end.getChar(i) - (int)start.getChar(i));
        } else {
          columnCard = BigInteger.valueOf(start.getChar(i) - end.getChar(i));
        }
        break;
      case BIT:
        if (isAscending) {
          columnCard = BigInteger.valueOf(end.getByte(i) - start.getByte(i));
        } else {
          columnCard = BigInteger.valueOf(start.getByte(i) - end.getByte(i));
        }
        break;
      case INT2:
        if (isAscending) {
          columnCard = BigInteger.valueOf(end.getInt2(i) - start.getInt2(i));
        } else {
          columnCard = BigInteger.valueOf(start.getInt2(i) - end.getInt2(i));
        }
        break;
      case INT4:
        if (isAscending) {
          columnCard = BigInteger.valueOf(end.getInt4(i) - start.getInt4(i));
        } else {
          columnCard = BigInteger.valueOf(start.getInt4(i) - end.getInt4(i));
        }
        break;
    case INT8:
    case TIME:
    case TIMESTAMP:
        if (isAscending) {
          columnCard = BigInteger.valueOf(end.getInt8(i) - start.getInt8(i));
        } else {
          columnCard = BigInteger.valueOf(start.getInt8(i) - end.getInt8(i));
        }
        break;
      case FLOAT4:
        if (isAscending) {
          columnCard = BigInteger.valueOf(end.getInt4(i) - start.getInt4(i));
        } else {
          columnCard = BigInteger.valueOf(start.getInt4(i) - end.getInt4(i));
        }
        break;
      case FLOAT8:
        if (isAscending) {
          columnCard = BigInteger.valueOf(end.getInt8(i) - start.getInt8(i));
        } else {
          columnCard = BigInteger.valueOf(start.getInt8(i) - end.getInt8(i));
        }
        break;
      case TEXT: {
        boolean isPureAscii = StringUtils.isPureAscii(start.getText(i)) && StringUtils.isPureAscii(end.getText(i));

        if (isPureAscii) {
          byte[] a;
          byte[] b;
          if (isAscending) {
            a = start.getBytes(i);
            b = end.getBytes(i);
          } else {
            b = start.getBytes(i);
            a = end.getBytes(i);
          }

          byte [][] padded = BytesUtils.padBytes(a, b);
          a = padded[0];
          b = padded[1];

          byte[] prependHeader = {1, 0};
          final BigInteger startBI = new BigInteger(Bytes.add(prependHeader, a));
          final BigInteger stopBI = new BigInteger(Bytes.add(prependHeader, b));
          BigInteger diffBI = stopBI.subtract(startBI);
          columnCard = diffBI;
        } else {
          char [] a;
          char [] b;

          if (isAscending) {
            a = start.getUnicodeChars(i);
            b = end.getUnicodeChars(i);
          } else {
            b = start.getUnicodeChars(i);
            a = end.getUnicodeChars(i);
          }

          BigInteger startBI = UniformRangePartition.charsToBigInteger(a);
          BigInteger stopBI = UniformRangePartition.charsToBigInteger(b);

          BigInteger diffBI = stopBI.subtract(startBI);
          columnCard = diffBI;
        }
        break;
      }
      case DATE:
        if (isAscending) {
          columnCard = BigInteger.valueOf(end.getInt4(i) - start.getInt4(i));
        } else {
          columnCard = BigInteger.valueOf(start.getInt4(i) - end.getInt4(i));
        }
        break;
      case INET4:
        if (isAscending) {
          columnCard = BigInteger.valueOf(end.getInt4(i) - start.getInt4(i));
        } else {
          columnCard = BigInteger.valueOf(start.getInt4(i) - end.getInt4(i));
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

    BigInteger cardinality = BigInteger.ONE;
    BigInteger columnCard;
    for (int i = 0; i < sortSpecs.length; i++) {
      columnCard = computeCardinality(sortSpecs[i].getSortKey().getDataType(), range, i, inclusive,
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
