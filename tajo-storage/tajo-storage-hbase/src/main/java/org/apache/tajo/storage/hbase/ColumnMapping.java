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

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.exception.InvalidTablePropertyException;
import org.apache.tajo.exception.MissingTablePropertyException;
import org.apache.tajo.exception.TajoException;
import org.apache.tajo.util.BytesUtils;
import org.apache.tajo.util.KeyValueSet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ColumnMapping {
  private Schema schema;
  private KeyValueSet tableProperty;

  private char rowKeyDelimiter;
  private String hbaseTableName;

  private int[] rowKeyFieldIndexes;
  private boolean[] isRowKeyMappings;
  private boolean[] isBinaryColumns;
  private boolean[] isColumnKeys;
  private boolean[] isColumnValues;

  // schema order -> 0: cf name, 1: column name -> name bytes
  private byte[][][] mappingColumns;

  private int numRowKeys;

  public ColumnMapping(Schema schema, KeyValueSet tableProperty)
      throws MissingTablePropertyException, InvalidTablePropertyException {
    this.schema = schema;
    this.tableProperty = tableProperty;
    init();
  }

  public void init() throws MissingTablePropertyException, InvalidTablePropertyException {
    hbaseTableName = tableProperty.get(HBaseStorageConstants.META_TABLE_KEY);
    String delim = tableProperty.get(HBaseStorageConstants.META_ROWKEY_DELIMITER, "").trim();
    if (delim.length() > 0) {
      rowKeyDelimiter = delim.charAt(0);
    }
    isRowKeyMappings = new boolean[schema.size()];
    rowKeyFieldIndexes = new int[schema.size()];
    isBinaryColumns = new boolean[schema.size()];
    isColumnKeys = new boolean[schema.size()];
    isColumnValues = new boolean[schema.size()];

    mappingColumns = new byte[schema.size()][][];

    for (int i = 0; i < schema.size(); i++) {
      rowKeyFieldIndexes[i] = -1;
    }

    String columnMapping = tableProperty.get(HBaseStorageConstants.META_COLUMNS_KEY, "");
    if (columnMapping == null || columnMapping.isEmpty()) {
      throw new MissingTablePropertyException(HBaseStorageConstants.META_COLUMNS_KEY, hbaseTableName);
    }

    String[] columnMappingTokens = columnMapping.split(",");

    if (columnMappingTokens.length != schema.getRootColumns().size()) {
      throw new InvalidTablePropertyException(
          "mapping column pairs must be more than number of columns in the schema", hbaseTableName);
    }

    int index = 0;
    for (String eachToken: columnMappingTokens) {
      mappingColumns[index] = new byte[2][];

      byte[][] mappingTokens = BytesUtils.splitTrivial(eachToken.trim().getBytes(), (byte)':');

      if (mappingTokens.length == 3) {
        if (mappingTokens[0].length == 0) {
          // cfname
          throw new InvalidTablePropertyException(eachToken +
              " 'column' attribute should be '<cfname>:key:' or '<cfname>:key:#b' " +
              "or '<cfname>:value:' or '<cfname>:value:#b'", hbaseTableName);
        }
        //<cfname>:key: or <cfname>:value:
        if (mappingTokens[2].length != 0) {
          String binaryOption = new String(mappingTokens[2]);
          if ("#b".equals(binaryOption)) {
            isBinaryColumns[index] = true;
          } else {
            throw new InvalidTablePropertyException(eachToken +
                " 'column' attribute should be '<cfname>:key:' or '<cfname>:key:#b' " +
                "or '<cfname>:value:' or '<cfname>:value:#b'", hbaseTableName);
          }
        }
        mappingColumns[index][0] = mappingTokens[0];
        String keyOrValue = new String(mappingTokens[1]);
        if (HBaseStorageConstants.KEY_COLUMN_MAPPING.equalsIgnoreCase(keyOrValue)) {
          isColumnKeys[index] = true;
        } else if (HBaseStorageConstants.VALUE_COLUMN_MAPPING.equalsIgnoreCase(keyOrValue)) {
          isColumnValues[index] = true;
        } else {
          throw new InvalidTablePropertyException(eachToken +
              " 'column' attribute should be '<cfname>:key:' or '<cfname>:value:'",
              hbaseTableName);
        }
      } else if (mappingTokens.length == 2) {
        //<cfname>: or <cfname>:<qualifier> or :key
        String cfName = new String(mappingTokens[0]);
        String columnName = new String(mappingTokens[1]);
        RowKeyMapping rowKeyMapping = getRowKeyMapping(cfName, columnName);
        if (rowKeyMapping != null) {
          isRowKeyMappings[index] = true;
          numRowKeys++;
          isBinaryColumns[index] = rowKeyMapping.isBinary();
          if (!cfName.isEmpty()) {
            if (rowKeyDelimiter == 0) {
              throw new InvalidTablePropertyException("hbase.rowkey.delimiter is required.",
                  hbaseTableName);
            }
            rowKeyFieldIndexes[index] = Integer.parseInt(cfName);
          } else {
            rowKeyFieldIndexes[index] = -1; //rowkey is mapped a single column.
          }
        } else {
          if (cfName.isEmpty()) {
            throw new InvalidTablePropertyException(eachToken +
                " 'column' attribute should be '<cfname>:key:' or '<cfname>:value:'",
                hbaseTableName);
          }
          if (cfName != null) {
            mappingColumns[index][0] = Bytes.toBytes(cfName);
          }

          if (columnName != null && !columnName.isEmpty()) {
            String[] columnNameTokens = columnName.split("#");
            if (columnNameTokens[0].isEmpty()) {
              mappingColumns[index][1] = null;
            } else {
              mappingColumns[index][1] = Bytes.toBytes(columnNameTokens[0]);
            }
            if (columnNameTokens.length == 2 && "b".equals(columnNameTokens[1])) {
              isBinaryColumns[index] = true;
            }
          }
        }
      } else {
        throw new InvalidTablePropertyException(eachToken + " 'column' attribute '[cfname]:[qualfier]:'"
            , hbaseTableName);
      }

      index++;
    } // for loop
  }

  public List<String> getColumnFamilyNames() {
    List<String> cfNames = new ArrayList<>();

    for (byte[][] eachCfName: mappingColumns) {
      if (eachCfName != null && eachCfName.length > 0 && eachCfName[0] != null) {
        String cfName = new String(eachCfName[0]);
        if (!cfNames.contains(cfName)) {
          cfNames.add(cfName);
        }
      }
    }

    return cfNames;
  }

  private RowKeyMapping getRowKeyMapping(String cfName, String columnName) {
    if (columnName == null || columnName.isEmpty()) {
      return null;
    }

    String[] tokens = columnName.split("#");
    if (!tokens[0].equalsIgnoreCase(HBaseStorageConstants.KEY_COLUMN_MAPPING)) {
      return null;
    }

    RowKeyMapping rowKeyMapping = new RowKeyMapping();

    if (tokens.length == 2 && "b".equals(tokens[1])) {
      rowKeyMapping.setBinary(true);
    }

    if (cfName != null && !cfName.isEmpty()) {
      rowKeyMapping.setKeyFieldIndex(Integer.parseInt(cfName));
    }
    return rowKeyMapping;
  }

  public char getRowKeyDelimiter() {
    return rowKeyDelimiter;
  }

  public int[] getRowKeyFieldIndexes() {
    return rowKeyFieldIndexes;
  }

  public boolean[] getIsRowKeyMappings() {
    return isRowKeyMappings;
  }

  public byte[][][] getMappingColumns() {
    return mappingColumns;
  }

  public Schema getSchema() {
    return schema;
  }

  public boolean[] getIsBinaryColumns() {
    return isBinaryColumns;
  }

  public String getHbaseTableName() {
    return hbaseTableName;
  }

  public boolean[] getIsColumnKeys() {
    return isColumnKeys;
  }

  public int getNumRowKeys() {
    return numRowKeys;
  }

  public int getNumColumns() {
    return schema.size();
  }
  
  public boolean[] getIsColumnValues() {
    return isColumnValues;
  }
}
