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
import org.apache.tajo.util.StringUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import parquet.hadoop.ParquetOutputFormat;

import static org.apache.tajo.catalog.proto.CatalogProtos.StoreType;
import static org.apache.tajo.common.TajoDataTypes.Type;

public class CatalogUtil {
  public final static String IDENTIFIER_DELIMITER = ".";
  public final static String IDENTIFIER_DELIMITER_REGEXP = "\\.";

  /**
   * Normalize an identifier. Normalization means a translation from a identifier to be a refined identifier name.
   *
   * Identifier can be composed of multiple parts as follows:
   * <pre>
   *   database_name.table_name.column_name
   * </pre>
   *
   * Each regular identifier part can be composed alphabet ([a-z][A-Z]), number([0-9]), and underscore([_]).
   * Also, the first letter must be an alphabet character.
   *
   * <code>normalizeIdentifier</code> normalizes each part of an identifier.
   *
   * In detail, for each part, it performs as follows:
   * <ul>
   *   <li>changing a part without double quotation to be lower case letters</li>
   *   <li>eliminating double quotation marks from identifier</li>
   * </ul>
   *
   * @param identifier The identifier to be normalized
   * @return The normalized identifier
   */
  public static String normalizeIdentifier(String identifier) {
    String [] splitted = identifier.split(IDENTIFIER_DELIMITER_REGEXP);

    StringBuilder sb = new StringBuilder();
    boolean first = true;
    for (String part : splitted) {
      if (first) {
        first = false;
      } else {
        sb.append(IDENTIFIER_DELIMITER);
      }
      sb.append(normalizeIdentifierPart(part));
    }
    return sb.toString();
  }

  public static String normalizeIdentifierPart(String part) {
    return isDelimited(part) ? stripQuote(part) : part.toLowerCase();
  }

  /**
   * Denormalize an identifier. Denormalize means a translation from a stored identifier
   * to be a printable identifier name.
   *
   * In detail, for each part, it performs as follows:
   * <ul>
   *   <li>changing a part including upper case character or non-ascii character to be lower case letters</li>
   *   <li>eliminating double quotation marks from identifier</li>
   * </ul>
   *
   * @param identifier The identifier to be normalized
   * @return The denormalized identifier
   */
  public static String denormalizeIdentifier(String identifier) {
    String [] splitted = identifier.split(IDENTIFIER_DELIMITER_REGEXP);

    StringBuilder sb = new StringBuilder();
    boolean first = true;
    for (String part : splitted) {
      if (first) {
        first = false;
      } else {
        sb.append(IDENTIFIER_DELIMITER);
      }
      sb.append(denormalizePart(part));
    }
    return sb.toString();
  }

  public static String denormalizePart(String identifier) {
    if (isShouldBeQuoted(identifier)) {
      return StringUtils.doubleQuote(identifier);
    } else {
      return identifier;
    }
  }

  public static boolean isShouldBeQuoted(String columnName) {
    for (char character : columnName.toCharArray()) {
      if (Character.isUpperCase(character)) {
        return true;
      }

      if (!StringUtils.isPartOfAnsiSQLIdentifier(character)) {
        return true;
      }

      if (RESERVED_KEYWORDS_SET.contains(columnName.toUpperCase())) {
        return true;
      }
    }

    return false;
  }

  public static String stripQuote(String str) {
    return str.substring(1, str.length() - 1);
  }

  public static boolean isDelimited(String identifier) {
    boolean openQuote = identifier.charAt(0) == '"';
    boolean closeQuote = identifier.charAt(identifier.length() - 1) == '"';

    // if at least one quote mark exists, the identifier must be grater than equal to 2 characters,
    if (openQuote ^ closeQuote && identifier.length() < 2) {
      throw new IllegalArgumentException("Invalid Identifier: " + identifier);
    }

    // does not allow the empty identifier (''),
    if (openQuote && closeQuote && identifier.length() == 2) {
      throw new IllegalArgumentException("zero-length delimited identifier: " + identifier);
    }

    // Ensure the quote open and close
    return openQuote && closeQuote;
  }

  public static boolean isFQColumnName(String tableName) {
    return tableName.split(IDENTIFIER_DELIMITER_REGEXP).length == 3;
  }

  public static boolean isFQTableName(String tableName) {
    int lastDelimiterIdx = tableName.lastIndexOf(IDENTIFIER_DELIMITER);
    return lastDelimiterIdx > -1;
  }

  public static String [] splitFQTableName(String qualifiedName) {
    String [] splitted = CatalogUtil.splitTableName(qualifiedName);
    if (splitted.length == 1) {
      throw new IllegalArgumentException("createTable() requires a qualified table name, but it is \""
          + qualifiedName + "\".");
    }
    return splitted;
  }

  public static String [] splitTableName(String tableName) {
    int lastDelimiterIdx = tableName.lastIndexOf(IDENTIFIER_DELIMITER);
    if (lastDelimiterIdx > -1) {
      return new String [] {
          tableName.substring(0, lastDelimiterIdx),
          tableName.substring(lastDelimiterIdx + 1, tableName.length())
      };
    } else {
      return new String [] {tableName};
    }
  }

  public static String buildFQName(String... identifiers) {
    boolean first = true;
    StringBuilder sb = new StringBuilder();
    for(String id : identifiers) {
      if (first) {
        first = false;
      } else {
        sb.append(IDENTIFIER_DELIMITER);
      }

      sb.append(id);
    }

    return sb.toString();
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

  public static String getCanonicalTableName(String databaseName, String tableName) {
    StringBuilder sb = new StringBuilder(databaseName);
    sb.append(IDENTIFIER_DELIMITER);
    sb.append(tableName);
    return sb.toString();
  }

  public static String getCanonicalSignature(String functionName, Collection<DataType> paramTypes) {
    DataType [] types = paramTypes.toArray(new DataType[paramTypes.size()]);
    return getCanonicalSignature(functionName, types);
  }
  public static String getCanonicalSignature(String signature, DataType... paramTypes) {
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
    } else if (typeStr.equalsIgnoreCase(StoreType.ROWFILE.name())) {
      return StoreType.ROWFILE;
    } else if (typeStr.equalsIgnoreCase(StoreType.RCFILE.name())) {
      return StoreType.RCFILE;
    } else if (typeStr.equalsIgnoreCase(StoreType.TREVNI.name())) {
      return StoreType.TREVNI;
    } else if (typeStr.equalsIgnoreCase(StoreType.PARQUET.name())) {
      return StoreType.PARQUET;
    } else if (typeStr.equalsIgnoreCase(StoreType.SEQUENCEFILE.name())) {
      return StoreType.SEQUENCEFILE;
    } else {
      return null;
    }
  }
  public static Options newOptionsWithDefault(StoreType type) {
    Options options = new Options();
    if(StoreType.CSV == type){
      options.put(CatalogConstants.CSVFILE_DELIMITER, CatalogConstants.DEFAULT_FIELD_DELIMITER);
    } else if(StoreType.RCFILE == type){
      options.put(CatalogConstants.RCFILE_SERDE, CatalogConstants.DEFAULT_BINARY_SERDE);
    } else if(StoreType.SEQUENCEFILE == type){
      options.put(CatalogConstants.SEQUENCEFILE_SERDE, CatalogConstants.DEFAULT_TEXT_SERDE);
      options.put(CatalogConstants.SEQUENCEFILE_DELIMITER, CatalogConstants.DEFAULT_FIELD_DELIMITER);
    } else if (type == StoreType.PARQUET) {
      options.put(ParquetOutputFormat.BLOCK_SIZE,
          CatalogConstants.PARQUET_DEFAULT_BLOCK_SIZE);
      options.put(ParquetOutputFormat.PAGE_SIZE,
          CatalogConstants.PARQUET_DEFAULT_PAGE_SIZE);
      options.put(ParquetOutputFormat.COMPRESSION,
          CatalogConstants.PARQUET_DEFAULT_COMPRESSION_CODEC_NAME);
      options.put(ParquetOutputFormat.ENABLE_DICTIONARY,
          CatalogConstants.PARQUET_DEFAULT_IS_DICTIONARY_ENABLED);
      options.put(ParquetOutputFormat.VALIDATION,
          CatalogConstants.PARQUET_DEFAULT_IS_VALIDATION_ENABLED);
    }

    return options;
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
    StringBuilder sb = new StringBuilder(denormalizeIdentifier(column.getSimpleName()));
    sb.append(" ").append(column.getDataType().getType());
    if (column.getDataType().hasLength()) {
      sb.append(" (").append(column.getDataType().getLength()).append(")");
    }
    return sb.toString();
  }

  public static CatalogProtos.TableIdentifierProto buildTableIdentifier(String databaseName, String tableName) {
    CatalogProtos.TableIdentifierProto.Builder builder = CatalogProtos.TableIdentifierProto.newBuilder();
    builder.setDatabaseName(databaseName);
    builder.setTableName(tableName);
    return builder.build();
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

  public static void closeQuietly(Statement stmt, ResultSet res) {
    try {
      closeQuietly(res);
    } finally {
      closeQuietly(stmt);
    }
  }

  public static final Set<String> RESERVED_KEYWORDS_SET = new HashSet<String>();

  static final String [] RESERVED_KEYWORDS = {
      "AS", "ALL", "AND", "ANY", "ASYMMETRIC", "ASC",
      "BOTH",
      "CASE", "CAST", "CREATE", "CROSS",
      "DESC", "DISTINCT",
      "END", "ELSE", "EXCEPT",
      "FALSE", "FULL", "FROM",
      "GROUP",
      "HAVING",
      "ILIKE", "IN", "INNER", "INTERSECT", "INTO", "IS",
      "JOIN",
      "LEADING", "LEFT", "LIKE", "LIMIT",
      "NATURAL", "NOT", "NULL",
      "ON", "OUTER", "OR", "ORDER",
      "RIGHT",
      "SELECT", "SOME", "SYMMETRIC",
      "TABLE", "THEN", "TRAILING", "TRUE",
      "UNION", "UNIQUE", "USING",
      "WHEN", "WHERE", "WITH"
  };

  static {
    for (String keyword : RESERVED_KEYWORDS) {
      RESERVED_KEYWORDS_SET.add(keyword);
    }
  }

  public static AlterTableDesc renameColumn(String tableName, String oldColumName, String newColumName, AlterTableType alterTableType) {
    final AlterTableDesc alterTableDesc = new AlterTableDesc();
    alterTableDesc.setTableName(tableName);
    alterTableDesc.setColumnName(oldColumName);
    alterTableDesc.setNewColumnName(newColumName);
    alterTableDesc.setAlterTableType(alterTableType);
    return alterTableDesc;
  }

  public static AlterTableDesc renameTable(String tableName, String newTableName, AlterTableType alterTableType) {
    final AlterTableDesc alterTableDesc = new AlterTableDesc();
    alterTableDesc.setTableName(tableName);
    alterTableDesc.setNewTableName(newTableName);
    alterTableDesc.setAlterTableType(alterTableType);
    return alterTableDesc;
  }

  public static AlterTableDesc addNewColumn(String tableName, Column column, AlterTableType alterTableType) {
    final AlterTableDesc alterTableDesc = new AlterTableDesc();
    alterTableDesc.setTableName(tableName);
    alterTableDesc.setAddColumn(column);
    alterTableDesc.setAlterTableType(alterTableType);
    return alterTableDesc;
  }
}
