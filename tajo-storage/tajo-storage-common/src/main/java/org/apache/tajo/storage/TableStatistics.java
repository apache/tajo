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
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.statistics.ColumnStats;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.common.TajoDataTypes.DataType;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.NullDatum;

/**
 * This class is not thread-safe.
 */
public class TableStatistics {
  private static final Log LOG = LogFactory.getLog(TableStatistics.class);
  private Schema schema;
  private Tuple minValues;
  private Tuple maxValues;
  private long [] numNulls;
  private long numRows = 0;
  private long numBytes = 0;

  private boolean [] comparable;

  public TableStatistics(Schema schema) {
    this.schema = schema;
    minValues = new VTuple(schema.size());
    maxValues = new VTuple(schema.size());

    numNulls = new long[schema.size()];
    comparable = new boolean[schema.size()];

    DataType type;
    for (int i = 0; i < schema.size(); i++) {
      type = schema.getColumn(i).getDataType();
      if (type.getType() == Type.PROTOBUF) {
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
    if (datum instanceof NullDatum) {
      numNulls[idx]++;
      return;
    }

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

  public TableStats getTableStat() {
    TableStats stat = new TableStats();

    ColumnStats columnStats;
    for (int i = 0; i < schema.size(); i++) {
      columnStats = new ColumnStats(schema.getColumn(i));
      columnStats.setNumNulls(numNulls[i]);
      if (minValues.get(i) == null || schema.getColumn(i).getDataType().getType() == minValues.get(i).type()) {
        columnStats.setMinValue(minValues.get(i));
      } else {
        LOG.warn("Wrong statistics column type (" + minValues.get(i).type() +
            ", expected=" + schema.getColumn(i).getDataType().getType() + ")");
      }
      if (maxValues.get(i) == null || schema.getColumn(i).getDataType().getType() == maxValues.get(i).type()) {
        columnStats.setMaxValue(maxValues.get(i));
      } else {
        LOG.warn("Wrong statistics column type (" + maxValues.get(i).type() +
            ", expected=" + schema.getColumn(i).getDataType().getType() + ")");
      }
      stat.addColumnStat(columnStats);
    }

    stat.setNumRows(this.numRows);
    stat.setNumBytes(this.numBytes);

    return stat;
  }
}
