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
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.util.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ColumnMapping {
  private TableMeta tableMeta;
  private Schema schema;
  private char rowKeyDelimiter;

  private int[] rowKeyFieldIndexes;
  private boolean[] isRowKeyMappings;
  private boolean[] isBinaryColumns;

  // schema order -> 0: cf name, 1: column name -> name bytes
  private byte[][][] mappingColumns;

  public ColumnMapping(Schema schema, TableMeta tableMeta) throws IOException {
    this.schema = schema;
    this.tableMeta = tableMeta;

    init();
  }

  public void init() throws IOException {
    String delim = tableMeta.getOption(HBaseStorageManager.META_ROWKEY_DELIMITER, "").trim();
    if (delim.length() > 0) {
      rowKeyDelimiter = delim.charAt(0);
    }
    isRowKeyMappings = new boolean[schema.size()];
    rowKeyFieldIndexes = new int[schema.size()];
    isBinaryColumns = new boolean[schema.size()];

    mappingColumns = new byte[schema.size()][][];

    for (int i = 0; i < schema.size(); i++) {
      isRowKeyMappings[i] = false;
      rowKeyFieldIndexes[i] = -1;
      isBinaryColumns[i] = false;
    }

    List<Pair<String, String>> hbaseColumnMappings = parseColumnMapping(tableMeta);
    if (hbaseColumnMappings == null || hbaseColumnMappings.isEmpty()) {
      throw new IOException("columns property is required.");
    }

    if (hbaseColumnMappings.size() != schema.getColumns().size()) {
      throw new IOException("The number of mapped HBase columns is great than the number of Tajo table columns");
    }

    int index = 0;
    for (Pair<String, String> eachMapping: hbaseColumnMappings) {
      String cfName = eachMapping.getFirst();
      String columnName = eachMapping.getSecond();

      mappingColumns[index] = new byte[2][];

      RowKeyMapping rowKeyMapping = HBaseStorageManager.getRowKeyMapping(cfName, columnName);
      if (rowKeyMapping != null) {
        isRowKeyMappings[index] = true;
        isBinaryColumns[index] = rowKeyMapping.isBinary();
        if (!cfName.isEmpty()) {
          if (rowKeyDelimiter == 0) {
            throw new IOException("hbase.rowkey.delimiter is required.");
          }
          rowKeyFieldIndexes[index] = Integer.parseInt(cfName);
        } else {
          rowKeyFieldIndexes[index] = -1; //rowkey is mapped a single column.
        }
      } else {
        isRowKeyMappings[index] = false;

        if (cfName != null) {
          mappingColumns[index][0] = Bytes.toBytes(cfName);
        }

        if (columnName != null) {
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

      index++;
    }
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

  /**
   * Get column mapping data from tableMeta's option.
   * First value of return is column family name and second value is column name which can be null.
   * @param tableMeta
   * @return
   * @throws java.io.IOException
   */
  public static List<Pair<String, String>> parseColumnMapping(TableMeta tableMeta) throws IOException {
    List<Pair<String, String>> columnMappings = new ArrayList<Pair<String, String>>();

    String columnMapping = tableMeta.getOption(HBaseStorageManager.META_COLUMNS_KEY, "");
    if (columnMapping == null || columnMapping.trim().isEmpty()) {
      return columnMappings;
    }

    for (String eachToken: columnMapping.split(",")) {
      String[] cfToken = eachToken.split(":");

      String cfName = cfToken[0];
      String columnName = null;
      if (cfToken.length == 2 && !cfToken[1].trim().isEmpty()) {
        columnName = cfToken[1].trim();
      }
      Pair<String, String> mappingEntry = new Pair<String, String>(cfName, columnName);
      columnMappings.add(mappingEntry);
    }

    return columnMappings;
  }
}
