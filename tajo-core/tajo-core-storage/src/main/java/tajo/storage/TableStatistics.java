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

package tajo.storage;

import tajo.catalog.Schema;
import tajo.catalog.proto.CatalogProtos;
import tajo.catalog.statistics.ColumnStat;
import tajo.catalog.statistics.TableStat;
import tajo.datum.Datum;
import tajo.datum.DatumType;

/**
 * This class is not thread-safe.
 */
public class TableStatistics {
  private Schema schema;
  private Tuple minValues;
  private Tuple maxValues;
  private long [] numNulls;
  private long numRows = 0;
  private long numBytes = 0;


  private boolean [] comparable;

  public TableStatistics(Schema schema) {
    this.schema = schema;
    minValues = new VTuple(schema.getColumnNum());
    maxValues = new VTuple(schema.getColumnNum());
    /*for (int i = 0; i < schema.getColumnNum(); i++) {
      minValues[i] = Long.MAX_VALUE;
      maxValues[i] = Long.MIN_VALUE;
    }*/

    numNulls = new long[schema.getColumnNum()];
    comparable = new boolean[schema.getColumnNum()];

    CatalogProtos.DataType type;
    for (int i = 0; i < schema.getColumnNum(); i++) {
      type = schema.getColumn(i).getDataType();
      if (type == CatalogProtos.DataType.ARRAY) {
        comparable[i] = false;
      } else {
        comparable[i] = true;
      }
    }
  }

  public Schema getSchema() {
    return this.schema;
  }

  public void incrementRow() {
    numRows++;
  }

  public long getNumRows() {
    return this.numRows;
  }

  public void setNumBytes(long bytes) {
    this.numBytes = bytes;
  }

  public long getNumBytes() {
    return this.numBytes;
  }

  public void analyzeField(int idx, Datum datum) {
    if (datum.type() == DatumType.NULL) {
      numNulls[idx]++;
      return;
    }

    if (datum.type() != DatumType.ARRAY) {
      if (comparable[idx]) {
        if (!maxValues.contains(idx) ||
            maxValues.get(idx).compareTo(datum) < 0) {
          maxValues.put(idx, datum);
        }
        if (!minValues.contains(idx) ||
            minValues.get(idx).compareTo(datum) > 0) {
          minValues.put(idx, datum);
        }
      }
    }
  }

  public TableStat getTableStat() {
    TableStat stat = new TableStat();

    ColumnStat columnStat;
    for (int i = 0; i < schema.getColumnNum(); i++) {
      columnStat = new ColumnStat(schema.getColumn(i));
      columnStat.setNumNulls(numNulls[i]);
      columnStat.setMinValue(minValues.get(i));
      columnStat.setMaxValue(maxValues.get(i));
      stat.addColumnStat(columnStat);
    }

    stat.setNumRows(this.numRows);
    stat.setNumBytes(this.numBytes);

    return stat;
  }
}
