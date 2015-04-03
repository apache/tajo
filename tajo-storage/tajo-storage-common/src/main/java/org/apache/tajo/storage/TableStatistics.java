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
import org.apache.tajo.catalog.SchemaUtil;
import org.apache.tajo.catalog.statistics.ColumnStats;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.common.TajoDataTypes.DataType;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.util.ComparableVector;

/**
 * This class is not thread-safe.
 */
public class TableStatistics {
  private static final Log LOG = LogFactory.getLog(TableStatistics.class);
  private Schema schema;
  private MinMaxStats minMaxValues;
  private long [] numNulls;
  private long numRows = 0;
  private long numBytes = 0;

  private boolean [] comparable;

  public TableStatistics(Schema schema) {
    this.schema = schema;
    this.minMaxValues = new MinMaxStats();

    numNulls = new long[schema.size()];
    comparable = new boolean[schema.size()];

    for (int i = 0; i < schema.size(); i++) {
      DataType type = schema.getColumn(i).getDataType();
      comparable[i] = type.getType() != Type.PROTOBUF;
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

  public void analyzeField(ReadableTuple tuple) {
    minMaxValues.update(tuple);
  }

  public void analyzeNullField() {
    minMaxValues.updateNull();
  }

  public TableStats getTableStat() {
    TableStats stat = new TableStats();

    ColumnStats columnStats;
    for (int i = 0; i < schema.size(); i++) {
      columnStats = new ColumnStats(schema.getColumn(i));
      columnStats.setNumNulls(numNulls[i]);
      columnStats.setMinValue(minMaxValues.getMinDatum(i));
      columnStats.setMaxValue(minMaxValues.getMaxDatum(i));
      stat.addColumnStat(columnStats);
    }

    stat.setNumRows(this.numRows);
    stat.setNumBytes(this.numBytes);

    return stat;
  }

  private class MinMaxStats extends ComparableVector {

    private static final int MAX_INDEX = 0;
    private static final int MIN_INDEX = 1;
    private static final int TMP_INDEX = 2;

    private final boolean[] warned;
    private final Type[] types;

    public MinMaxStats() {
      super(3, schema);
      this.types = SchemaUtil.toTypes(schema);
      this.warned = new boolean[types.length];
      setNull(MAX_INDEX);
      setNull(MIN_INDEX);
    }

    public void update(ReadableTuple tuple) {
      set(TMP_INDEX, tuple);
      for (int i = 0; i < types.length; i++) {
        if (tuple.isNull(i)) {
          numNulls[i]++;
          continue;
        }
        if (warned[i] || !comparable[i]) {
          continue;
        }
        if (tuple.type(i) != types[i]) {
          LOG.warn("Wrong statistics column type (" + tuple.type(i) +
            ", expected=" + types[i] + ")");
          warned[i] = true;
          continue;
        }
        if (vectors[i].isNull(MAX_INDEX) || vectors[i].compare(MAX_INDEX, TMP_INDEX) < 0) {
          vectors[i].set(MAX_INDEX, tuple, i);
        }
        if (vectors[i].isNull(MIN_INDEX) || vectors[i].compare(MIN_INDEX, TMP_INDEX) > 0) {
          vectors[i].set(MIN_INDEX, tuple, i);
        }
      }
    }

    public void updateNull() {
      for (int i = 0; i < types.length; i++) {
        numNulls[i]++;
      }
    }

    public Datum getMaxDatum(int field) {
      return getDatum(MAX_INDEX, field);
    }

    public Datum getMinDatum(int field) {
      return getDatum(MIN_INDEX, field);
    }

    public Datum getDatum(int index, int field) {
      return vectors[field].isNull(index) ? null : vectors[field].toDatum(index);
    }
  }
}
