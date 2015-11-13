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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.TajoConstants;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.statistics.ColumnStats;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.datum.Datum;

/**
 * This class is not thread-safe.
 */
public class TableStatistics {
  private static final Log LOG = LogFactory.getLog(TableStatistics.class);
  private final Schema schema;
  private final VTuple minValues;
  private final VTuple maxValues;
  private final long [] numNulls;
  private long numRows = 0;
  private long numBytes = TajoConstants.UNKNOWN_LENGTH;

  private final boolean[] columnStatsEnabled;

  public TableStatistics(Schema schema, boolean[] columnStatsEnabled) {
    this.schema = schema;
    minValues = new VTuple(schema.size());
    maxValues = new VTuple(schema.size());

    numNulls = new long[schema.size()];

    this.columnStatsEnabled = columnStatsEnabled;
    for (int i = 0; i < schema.size(); i++) {
      if (schema.getColumn(i).getDataType().getType().equals(Type.PROTOBUF)) {
        columnStatsEnabled[i] = false;
      }
    }
  }

  public Schema getSchema() {
    return this.schema;
  }

  public void incrementRow() {
    numRows++;
  }

  public void incrementRows(long num) {
    numRows += num;
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

  public void analyzeField(int idx, Tuple tuple) {
    if (columnStatsEnabled[idx]) {
      if (tuple.isBlankOrNull(idx)) {
        numNulls[idx]++;
        return;
      }

      Datum datum = tuple.asDatum(idx);
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

  public TableStats getTableStat() {
    TableStats stat = new TableStats();

    for (int i = 0; i < schema.size(); i++) {
      if (columnStatsEnabled[i]) {
        Column column = schema.getColumn(i);
        ColumnStats columnStats = new ColumnStats(column);
        columnStats.setNumNulls(numNulls[i]);
        if (minValues.isBlank(i) || column.getDataType().getType() == minValues.type(i)) {
          columnStats.setMinValue(minValues.get(i));
        } else {
          LOG.warn("Wrong statistics column type (" + minValues.type(i) +
              ", expected=" + column.getDataType().getType() + ")");
        }
        if (minValues.isBlank(i) || column.getDataType().getType() == maxValues.type(i)) {
          columnStats.setMaxValue(maxValues.get(i));
        } else {
          LOG.warn("Wrong statistics column type (" + maxValues.type(i) +
              ", expected=" + column.getDataType().getType() + ")");
        }
        stat.addColumnStat(columnStats);
      }
    }

    stat.setNumRows(this.numRows);
    stat.setNumBytes(this.numBytes);

    return stat;
  }
}
