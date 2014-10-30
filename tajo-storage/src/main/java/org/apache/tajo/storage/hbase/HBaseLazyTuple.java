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

package org.apache.tajo.storage.hbase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.datum.ProtobufDatum;
import org.apache.tajo.datum.TextDatum;
import org.apache.tajo.exception.UnsupportedException;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.util.BytesUtils;

import java.util.Arrays;
import java.util.NavigableMap;

public class HBaseLazyTuple implements Tuple, Cloneable {
  private static final Log LOG = LogFactory.getLog(HBaseLazyTuple.class);

  private Datum[] values;
  private Result result;
  private byte[][][] mappingColumnFamilies;
  private boolean[] isRowKeyMappings;
  private boolean[] isBinaryColumns;
  private int[] rowKeyFieldIndexes;
  private char rowKeyDelimiter;
  private Column[] schemaColumns;

  public HBaseLazyTuple(ColumnMapping columnMapping,
                        Column[] schemaColumns,
                        int[] targetIndexes,
                        Result result) {
    values = new Datum[schemaColumns.length];
    mappingColumnFamilies = columnMapping.getMappingColumns();
    isRowKeyMappings = columnMapping.getIsRowKeyMappings();
    isBinaryColumns = columnMapping.getIsBinaryColumns();
    rowKeyDelimiter = columnMapping.getRowKeyDelimiter();
    rowKeyFieldIndexes = columnMapping.getRowKeyFieldIndexes();

    this.result = result;
    this.schemaColumns = schemaColumns;
  }

  @Override
  public int size() {
    return values.length;
  }

  @Override
  public boolean contains(int fieldid) {
    return false;
  }

  @Override
  public boolean isNull(int fieldid) {
    return false;
  }

  @Override
  public void clear() {
    values = new Datum[schemaColumns.length];
  }

  @Override
  public void put(int fieldId, Datum value) {
    values[fieldId] = value;
  }

  @Override
  public void put(int fieldId, Datum[] values) {
    for (int i = fieldId, j = 0; j < values.length; i++, j++) {
      this.values[i] = values[j];
    }
  }

  @Override
  public void put(int fieldId, Tuple tuple) {
    for (int i = fieldId, j = 0; j < tuple.size(); i++, j++) {
      values[i] = tuple.get(j);
    }
  }

  @Override
  public void put(Datum[] values) {
    System.arraycopy(values, 0, this.values, 0, size());
  }

  @Override
  public Datum get(int fieldId) {
    if (values[fieldId] != null) {
      return values[fieldId];
    }

    byte[] value = null;
    if (isRowKeyMappings[fieldId]) {
      value = result.getRow();
      if (!isBinaryColumns[fieldId] && rowKeyFieldIndexes[fieldId] >= 0) {
        int rowKeyFieldIndex = rowKeyFieldIndexes[fieldId];

        byte[][] rowKeyFields = BytesUtils.splitPreserveAllTokens(value, rowKeyDelimiter);

        if (rowKeyFields.length < rowKeyFieldIndex) {
          values[fieldId] = NullDatum.get();
          return values[fieldId];
        } else {
          value = rowKeyFields[rowKeyFieldIndex];
        }
      }
    } else {
      if (mappingColumnFamilies[fieldId][1] == null) {
        NavigableMap<byte[], byte[]> cfMap = result.getFamilyMap(mappingColumnFamilies[fieldId][0]);
        if (cfMap != null && !cfMap.isEmpty()) {
          int count = 0;
          String delim = "";

          StringBuilder sb = new StringBuilder();
          sb.append("{");
          for (NavigableMap.Entry<byte[], byte[]> entry: cfMap.entrySet()) {
            byte[] entryKey = entry.getKey();
            byte[] entryValue = entry.getValue();

            String keyText = new String(entryKey);
            String valueText = null;
            if (entryValue != null) {
              try {
                if (isBinaryColumns[fieldId]) {
                  valueText = HBaseBinarySerializerDeserializer.deserialize(schemaColumns[fieldId], entryValue).asChars();
                } else {
                  valueText = HBaseTextSerializerDeserializer.deserialize(schemaColumns[fieldId], entryValue).asChars();
                }
              } catch (Exception e) {
                LOG.error(e.getMessage(), e);
                throw new RuntimeException(e.getMessage(), e);
              }
            }
            sb.append(delim).append("\"").append(keyText).append("\":\"").append(valueText).append("\"");
            delim = ", ";
            if (count > 100) {
              break;
            }
          }
          sb.append("}");
          values[fieldId] = new TextDatum(sb.toString());
          return values[fieldId];
        } else {
          value = null;
        }
      } else {
        value = result.getValue(mappingColumnFamilies[fieldId][0], mappingColumnFamilies[fieldId][1]);
      }
    }

    if (value == null) {
      values[fieldId] = NullDatum.get();
    } else {
      try {
        if (isBinaryColumns[fieldId]) {
          values[fieldId] = HBaseBinarySerializerDeserializer.deserialize(schemaColumns[fieldId], value);
        } else {
          values[fieldId] = HBaseTextSerializerDeserializer.deserialize(schemaColumns[fieldId], value);
        }
      } catch (Exception e) {
        LOG.error(e.getMessage(), e);
        throw new RuntimeException(e.getMessage(), e);
      }
    }

    return values[fieldId];
  }

  @Override
  public void setOffset(long offset) {
  }

  @Override
  public long getOffset() {
    return 0;
  }

  @Override
  public boolean getBool(int fieldId) {
    return get(fieldId).asBool();
  }

  @Override
  public byte getByte(int fieldId) {
    return get(fieldId).asByte();
  }

  @Override
  public char getChar(int fieldId) {
    return get(fieldId).asChar();
  }

  @Override
  public byte [] getBytes(int fieldId) {
    return get(fieldId).asByteArray();
  }

  @Override
  public short getInt2(int fieldId) {
    return get(fieldId).asInt2();
  }

  @Override
  public int getInt4(int fieldId) {
    return get(fieldId).asInt4();
  }

  @Override
  public long getInt8(int fieldId) {
    return get(fieldId).asInt8();
  }

  @Override
  public float getFloat4(int fieldId) {
    return get(fieldId).asFloat4();
  }

  @Override
  public double getFloat8(int fieldId) {
    return get(fieldId).asFloat8();
  }

  @Override
  public String getText(int fieldId) {
    return get(fieldId).asChars();
  }

  @Override
  public ProtobufDatum getProtobufDatum(int fieldId) {
    throw new UnsupportedException();
  }

  @Override
  public char[] getUnicodeChars(int fieldId) {
    return get(fieldId).asUnicodeChars();
  }

  public String toString() {
    boolean first = true;
    StringBuilder str = new StringBuilder();
    str.append("(");
    Datum d;
    for (int i = 0; i < values.length; i++) {
      d = get(i);
      if (d != null) {
        if (first) {
          first = false;
        } else {
          str.append(", ");
        }
        str.append(i)
            .append("=>")
            .append(d);
      }
    }
    str.append(")");
    return str.toString();
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(values);
  }

  @Override
  public Datum[] getValues() {
    Datum[] datums = new Datum[values.length];
    for (int i = 0; i < values.length; i++) {
      datums[i] = get(i);
    }
    return datums;
  }

  @Override
  public Tuple clone() throws CloneNotSupportedException {
    HBaseLazyTuple lazyTuple = (HBaseLazyTuple) super.clone();
    lazyTuple.values = getValues(); //shallow copy
    return lazyTuple;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof Tuple) {
      Tuple other = (Tuple) obj;
      return Arrays.equals(getValues(), other.getValues());
    }
    return false;
  }
}
