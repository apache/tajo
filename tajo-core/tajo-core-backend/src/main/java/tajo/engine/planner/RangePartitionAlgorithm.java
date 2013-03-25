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

package tajo.engine.planner;

import tajo.catalog.Column;
import tajo.catalog.Schema;
import tajo.catalog.proto.CatalogProtos;
import tajo.datum.Datum;
import tajo.storage.Tuple;
import tajo.storage.TupleRange;

import java.math.BigDecimal;

public abstract class RangePartitionAlgorithm {
  protected Schema schema;
  protected TupleRange range;
  protected final BigDecimal totalCard;
  /** true if the end of the range is inclusive. Otherwise, it should be false. */
  protected final boolean inclusive;

  /**
   *
   * @param schema the schema of the range tuples
   * @param range range to be partition
   * @param inclusive true if the end of the range is inclusive. Otherwise, false.
   */
  public RangePartitionAlgorithm(Schema schema, TupleRange range, boolean inclusive) {
    this.schema = schema;
    this.range = range;
    this.inclusive = inclusive;
    this.totalCard = computeCardinalityForAllColumns(schema, range, inclusive);
  }

  /**
   * It computes the value cardinality of a tuple range.
   *
   * @param dataType
   * @param start
   * @param end
   * @return
   */
  public static BigDecimal computeCardinality(CatalogProtos.DataType dataType, Datum start, Datum end,
                                              boolean inclusive) {
    BigDecimal columnCard;

    switch (dataType) {
      case CHAR:
        columnCard = new BigDecimal(end.asChar() - start.asChar());
        break;
      case BYTE:
        columnCard = new BigDecimal(end.asByte() - start.asByte());
        break;
      case SHORT:
        columnCard = new BigDecimal(end.asShort() - start.asShort());
        break;
      case INT:
        columnCard = new BigDecimal(end.asInt() - start.asInt());
        break;
      case LONG:
        columnCard = new BigDecimal(end.asLong() - start.asLong());
        break;
      case FLOAT:
        columnCard = new BigDecimal(end.asInt() - start.asInt());
        break;
      case DOUBLE:
        columnCard = new BigDecimal(end.asLong() - start.asLong());
        break;
      case STRING:
        columnCard = new BigDecimal(end.asChars().charAt(0) - start.asChars().charAt(0));
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
  public static BigDecimal computeCardinalityForAllColumns(Schema schema, TupleRange range, boolean inclusive) {
    Tuple start = range.getStart();
    Tuple end = range.getEnd();
    Column col;

    BigDecimal cardinality = new BigDecimal(1);
    BigDecimal columnCard;
    for (int i = 0; i < schema.getColumnNum(); i++) {
      col = schema.getColumn(i);
      columnCard = computeCardinality(col.getDataType(), start.get(i), end.get(i), inclusive);

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
