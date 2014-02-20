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

package org.apache.tajo.catalog;

import org.apache.hadoop.fs.Path;
import org.apache.tajo.TajoConstants;
import org.apache.tajo.catalog.partition.PartitionMethodDesc;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.catalog.proto.CatalogProtos.ColumnProto;
import org.apache.tajo.catalog.proto.CatalogProtos.SchemaProto;
import org.apache.tajo.catalog.proto.CatalogProtos.TableDescProto;
import org.apache.tajo.common.TajoDataTypes.DataType;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;

import static org.apache.tajo.catalog.proto.CatalogProtos.StoreType;
import static org.apache.tajo.common.TajoDataTypes.Type;

public class CatalogUtil {
  public final static String IDENTIFIER_DELIMITER = ".";
  public final static String IDENTIFIER_DELIMITER_REGEXP = "\\.";

  /**
   * Normalize an identifier
   *
   * @param identifier The identifier to be normalized
   * @return The normalized identifier
   */
  public static String normalizeIdentifier(String identifier) {
    return identifier.toLowerCase();
  }

  /**
   * Extract a qualification name from an identifier.
   *
   * For example, consider a table identifier like 'database1.table1'.
   * In this case, this method extracts 'database1'.
   *
   * @param name The identifier to be extracted
   * @return The extracted qualifier
   */
  public static String extractQualifier(String name) {
    int lastDelimiterIdx = name.lastIndexOf(IDENTIFIER_DELIMITER);
    if (lastDelimiterIdx > -1) {
      return name.substring(0, lastDelimiterIdx);
    } else {
      return TajoConstants.EMPTY_STRING;
    }
  }

  /**
   * Extract a simple name from an identifier.
   *
   * For example, consider a table identifier like 'database1.table1'.
   * In this case, this method extracts 'table1'.
   *
   * @param name The identifier to be extracted
   * @return The extracted simple name
   */
  public static String extractSimpleName(String name) {
    int lastDelimiterIdx = name.lastIndexOf(IDENTIFIER_DELIMITER);
    if (lastDelimiterIdx > -1) {
      // plus one means skipping a delimiter.
      return name.substring(lastDelimiterIdx + 1, name.length());
    } else {
      return name;
    }
  }

  public static String getCanonicalName(String signature, Collection<DataType> paramTypes) {
    DataType [] types = paramTypes.toArray(new DataType[paramTypes.size()]);
    return getCanonicalName(signature, types);
  }
  public static String getCanonicalName(String signature, DataType...paramTypes) {
    StringBuilder sb = new StringBuilder(signature);
    sb.append("(");
    int i = 0;
    for (DataType type : paramTypes) {
      sb.append(type.getType().name().toLowerCase());
      if(i < paramTypes.length - 1) {
        sb.append(",");
      }
      i++;
    }
    sb.append(")");
    return sb.toString();
  }

  public static StoreType getStoreType(final String typeStr) {
    if (typeStr.equalsIgnoreCase(StoreType.CSV.name())) {
      return StoreType.CSV;
    } else if (typeStr.equalsIgnoreCase(StoreType.RAW.name())) {
      return StoreType.RAW;
    } else if (typeStr.equalsIgnoreCase(StoreType.CSV.name())) {
      return StoreType.CSV;
    } else if (typeStr.equalsIgnoreCase(StoreType.ROWFILE.name())) {
      return StoreType.ROWFILE;
    }else if (typeStr.equalsIgnoreCase(StoreType.RCFILE.name())) {
      return StoreType.RCFILE;
    } else if (typeStr.equalsIgnoreCase(StoreType.TREVNI.name())) {
      return StoreType.TREVNI;
    } else {
      return null;
    }
  }

  public static TableMeta newTableMeta(StoreType type) {
    return new TableMeta(type, new Options());
  }

  public static TableMeta newTableMeta(StoreType type, Options options) {
    return new TableMeta(type, options);
  }

  public static TableDesc newTableDesc(String tableName, Schema schema, TableMeta meta, Path path) {
    return new TableDesc(tableName, schema, meta, path);
  }

  public static TableDesc newTableDesc(TableDescProto proto) {
    return new TableDesc(proto);
  }

  public static PartitionMethodDesc newPartitionMethodDesc(CatalogProtos.PartitionMethodProto proto) {
    return new PartitionMethodDesc(proto);
  }

  public static TableDesc newTableDesc(String tableName, Schema schema, StoreType type, Options options, Path path) {
    return new TableDesc(tableName, schema, type, options, path);
  }

  /**
  * This method transforms the unqualified names of a given schema into
  * the qualified names.
  *
  * @param tableName a table name to be prefixed
  * @param schema a schema to be transformed
  *
  * @return
  */
  public static SchemaProto getQualfiedSchema(String tableName, SchemaProto schema) {
    SchemaProto.Builder revisedSchema = SchemaProto.newBuilder(schema);
    revisedSchema.clearFields();
    for (ColumnProto col : schema.getFieldsList()) {
      ColumnProto.Builder builder = ColumnProto.newBuilder(col);
      builder.setName(tableName + IDENTIFIER_DELIMITER + extractSimpleName(col.getName()));
      revisedSchema.addFields(builder.build());
    }

    return revisedSchema.build();
  }

  public static DataType newDataType(Type type, String code) {
    return newDataType(type, code, 0);
  }

  public static DataType newDataType(Type type, String code, int len) {
    DataType.Builder builder = DataType.newBuilder();
    builder.setType(type)
        .setCode(code)
        .setLength(len);
    return builder.build();
  }

  public static DataType newSimpleDataType(Type type) {
    return DataType.newBuilder().setType(type).build();
  }

  public static DataType [] newSimpleDataTypeArray(Type... types) {
    DataType [] dataTypes = new DataType[types.length];
    for (int i = 0; i < types.length; i++) {
      dataTypes[i] = DataType.newBuilder().setType(types[i]).build();
    }
    return dataTypes;
  }

  public static DataType newDataTypeWithLen(Type type, int length) {
    return DataType.newBuilder().setType(type).setLength(length).build();
  }

  public static String columnToDDLString(Column column) {
    StringBuilder sb = new StringBuilder(column.getSimpleName());
    sb.append(" ").append(column.getDataType().getType());
    if (column.getDataType().hasLength()) {
      sb.append(" (").append(column.getDataType().getLength()).append(")");
    }
    return sb.toString();
  }

  public static void closeQuietly(Connection conn) {
    try {
      if (conn != null)
        conn.close();
    } catch (SQLException se) {
    }
  }

  public static void closeQuietly(Statement stmt)  {
    try {
      if (stmt != null)
        stmt.close();
    } catch (SQLException se) {
    }
  }

  public static void closeQuietly(ResultSet res) {
    try {
      if (res != null)
        res.close();
    } catch (SQLException se) {
    }
  }

  public static void closeQuietly(Connection conn, Statement stmt)  {
    try {
      closeQuietly(stmt);
    } finally {
      closeQuietly(conn);
    }
  }

  public static void closeQuietly(Connection conn, ResultSet res) {
    try {
      closeQuietly(res);
    } finally {
      closeQuietly(conn);
    }
  }

  public static void closeQuietly(Connection conn, Statement stmt, ResultSet res) {
    try {
      closeQuietly(res);
    } finally {
      try {
        closeQuietly(stmt);
      } finally {
        closeQuietly(conn);
      }
    }
  }
}
