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

import com.google.common.collect.Maps;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.DataTypeUtil;
import org.apache.tajo.TajoConstants;
import org.apache.tajo.catalog.partition.PartitionMethodDesc;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.catalog.proto.CatalogProtos.ColumnProto;
import org.apache.tajo.catalog.proto.CatalogProtos.SchemaProto;
import org.apache.tajo.catalog.proto.CatalogProtos.TableDescProto;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.common.TajoDataTypes.DataType;
import org.apache.tajo.exception.InvalidOperationException;
import org.apache.tajo.util.KeyValueSet;
import org.apache.tajo.util.StringUtils;
import org.apache.tajo.util.TUtil;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

import static org.apache.tajo.catalog.proto.CatalogProtos.StoreType;
import static org.apache.tajo.common.TajoDataTypes.Type;

public class CatalogUtil {

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
    if (identifier == null || identifier.equals("")) {
      return identifier;
    }
    String [] splitted = identifier.split(CatalogConstants.IDENTIFIER_DELIMITER_REGEXP);

    StringBuilder sb = new StringBuilder();
    boolean first = true;
    for (String part : splitted) {
      if (first) {
        first = false;
      } else {
        sb.append(CatalogConstants.IDENTIFIER_DELIMITER);
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
    String [] splitted = identifier.split(CatalogConstants.IDENTIFIER_DELIMITER_REGEXP);

    StringBuilder sb = new StringBuilder();
    boolean first = true;
    for (String part : splitted) {
      if (first) {
        first = false;
      } else {
        sb.append(CatalogConstants.IDENTIFIER_DELIMITER);
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
    return tableName.split(CatalogConstants.IDENTIFIER_DELIMITER_REGEXP).length == 3;
  }

  public static boolean isFQTableName(String tableName) {
    int lastDelimiterIdx = tableName.lastIndexOf(CatalogConstants.IDENTIFIER_DELIMITER);
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
    int lastDelimiterIdx = tableName.lastIndexOf(CatalogConstants.IDENTIFIER_DELIMITER);
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
        sb.append(CatalogConstants.IDENTIFIER_DELIMITER);
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
    int lastDelimiterIdx = name.lastIndexOf(CatalogConstants.IDENTIFIER_DELIMITER);
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
    int lastDelimiterIdx = name.lastIndexOf(CatalogConstants.IDENTIFIER_DELIMITER);
    if (lastDelimiterIdx > -1) {
      // plus one means skipping a delimiter.
      return name.substring(lastDelimiterIdx + 1, name.length());
    } else {
      return name;
    }
  }

  public static String getCanonicalTableName(String databaseName, String tableName) {
    StringBuilder sb = new StringBuilder(databaseName);
    sb.append(CatalogConstants.IDENTIFIER_DELIMITER);
    sb.append(tableName);
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
    } else if (typeStr.equalsIgnoreCase(StoreType.AVRO.name())) {
      return StoreType.AVRO;
    } else if (typeStr.equalsIgnoreCase(StoreType.HBASE.name())) {
      return StoreType.HBASE;
    } else {
      return null;
    }
  }

  public static TableMeta newTableMeta(StoreType type) {
    return new TableMeta(type, new KeyValueSet());
  }

  public static TableMeta newTableMeta(StoreType type, KeyValueSet options) {
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
      builder.setName(tableName + CatalogConstants.IDENTIFIER_DELIMITER + extractSimpleName(col.getName()));
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

  public static boolean isArrayType(TajoDataTypes.Type type) {
    return type.toString().endsWith("_ARRAY");
  }

  /**
   * Checking if the given parameter types are compatible to the defined types of the function.
   * It also considers variable-length function invocations.
   *
   * @param definedTypes The defined function types
   * @param givenTypes The given parameter types
   * @return True if the parameter types are compatible to the defined types.
   */
  public static boolean isMatchedFunction(List<DataType> definedTypes, List<DataType> givenTypes) {

    // below check handles the following cases:
    //
    //   defined arguments          given params    RESULT
    //        ()                        ()            T
    //        ()                       (arg1)         F

    if (definedTypes == null || definedTypes.isEmpty()) { // if no defined argument
      if (givenTypes == null || givenTypes.isEmpty()) {
        return true; // if no defined argument as well as no given parameter
      } else {
        return false; // if no defined argument but there is one or more given parameters.
      }
    }

    // if the number of the given parameters are less than, the invocation is invalid.
    // It should already return false.
    //
    // below check handles the following example cases:
    //
    //   defined             given arguments
    //     (a)                    ()
    //    (a,b)                   (a)

    int definedSize = definedTypes == null ? 0 : definedTypes.size();
    int givenParamSize = givenTypes == null ? 0 : givenTypes.size();
    int paramDiff = givenParamSize - definedSize;
    if (paramDiff < 0) {
      return false;
    }

    // if the lengths of both defined and given parameter types are the same to each other
    if (paramDiff == 0) {
      return checkIfMatchedNonVarLengthFunction(definedTypes, givenTypes, definedSize, true);

    } else { // if variable length parameter match is suspected

      // Invocation parameters can be divided into two parts: non-variable part and variable part.
      //
      // For example, the function definition is as follows:
      //
      // c = func(a int, b float, c text[])
      //
      // If the function invocation is as follows. We can divided them into two parts as we mentioned above.
      //
      // func(  1,   3.0,           'b',   'c',   'd',   ...)
      //      <------------>       <----------------------->
      //     non-variable part          variable part

      // check if the candidate function is a variable-length function.
      if (!checkIfVariableLengthParamDefinition(definedTypes)) {
        return false;
      }

      // check only non-variable part
      if (!checkIfMatchedNonVarLengthFunction(definedTypes, givenTypes, definedSize - 1, false)) {
        return false;
      }

      ////////////////////////////////////////////////////////////////////////////////
      // The below code is for checking the variable part of a candidate function.
      ////////////////////////////////////////////////////////////////////////////////

      // Get a primitive type of the last defined parameter (array)
      TajoDataTypes.Type primitiveTypeOfLastDefinedParam =
          getPrimitiveTypeOf(definedTypes.get(definedTypes.size() - 1).getType());

      Type basisTypeOfVarLengthType = null;
      Type [] varLengthTypesOfGivenParams = new Type[paramDiff + 1]; // including last parameter
      for (int i = 0,j = (definedSize - 1); j < givenParamSize; i++, j++) {
        varLengthTypesOfGivenParams[i] = givenTypes.get(j).getType();

        // chooses the first non-null type as the basis type.
        if (givenTypes.get(j).getType() != Type.NULL_TYPE && basisTypeOfVarLengthType == null) {
          basisTypeOfVarLengthType = givenTypes.get(j).getType();
        } else if (basisTypeOfVarLengthType != null) {
          // If there are more than one type, we choose the most widen type as the basis type.
          basisTypeOfVarLengthType =
              getWidestType(CatalogUtil.newSimpleDataTypeArray(basisTypeOfVarLengthType, givenTypes.get(j).getType()))
              .getType();
        }
      }

      // If basis type is null, it means that all params in variable part is NULL_TYPE.
      // In this case, we set NULL_TYPE to the basis type.
      if (basisTypeOfVarLengthType == null) {
        basisTypeOfVarLengthType = Type.NULL_TYPE;
      }

      // Check if a basis param type is compatible to the variable parameter in the function definition.
      if (!(primitiveTypeOfLastDefinedParam == basisTypeOfVarLengthType ||
          isCompatibleType(primitiveTypeOfLastDefinedParam, basisTypeOfVarLengthType))) {
        return false;
      }

      // If all parameters are equivalent to the basis type
      for (TajoDataTypes.Type type : varLengthTypesOfGivenParams) {
        if (type != Type.NULL_TYPE && !isCompatibleType(primitiveTypeOfLastDefinedParam, type)) {
          return false;
        }
      }

      return true;
    }
  }

  /**
   * It is used when the function definition and function invocation whose the number of parameters are the same.
   *
   * @param definedTypes The parameter definition of a function
   * @param givenTypes invoked parameters
   * @param n Should how many types be checked?
   * @param lastArrayCompatible variable-length compatibility is allowed if true.
   * @return True the parameter definition and invoked definition are compatible to each other
   */
  public static boolean checkIfMatchedNonVarLengthFunction(List<DataType> definedTypes, List<DataType> givenTypes,
                                                           int n, boolean lastArrayCompatible) {
    for (int i = 0; i < n; i++) {
      Type definedType = definedTypes.get(i).getType();
      Type givenType = givenTypes.get(i).getType();

      if (lastArrayCompatible) {
        if (!CatalogUtil.checkIfCompatibleIncludingArray(definedType, givenType)) {
          return false;
        }
      } else {
        if (!CatalogUtil.isCompatibleType(definedType, givenType)) {
          return false;
        }
      }
    }

    return true;
  }

  /**
   * Check if both are compatible to each other. This function includes
   * the compatibility between primitive type and array type.
   * For example, INT8 and INT8_ARRAY is compatible.
   * This method is used to find variable-length functions.
   *
   * @param defined One parameter of the function definition
   * @param given One parameter of the invoked parameters
   * @return True if compatible.
   */
  public static boolean checkIfCompatibleIncludingArray(Type defined, Type given) {
    boolean compatible = isCompatibleType(defined, given);

    if (compatible) {
      return true;
    }

    if (isArrayType(defined)) {
      TajoDataTypes.Type primitiveTypeOfDefined = getPrimitiveTypeOf(defined);
      return isCompatibleType(primitiveTypeOfDefined, given);
    } else {
      return false;
    }
  }

  /**
   * Check if the parameter definition can be variable length.
   *
   * @param definedTypes The parameter definition of a function.
   * @return True if the parameter definition can be variable length.
   */
  public static boolean checkIfVariableLengthParamDefinition(List<DataType> definedTypes) {
    if (definedTypes.size() < 1) { // if no parameter function
      return false;
    }

    // Get the last param type of the function definition.
    Type lastDefinedParamType = definedTypes.get(definedTypes.size() - 1).getType();

    // Check if this function is variable function.
    // if the last defined parameter is not array, it is not a variable length function. It will be false.
    return CatalogUtil.isArrayType(lastDefinedParamType);
  }

  public static Type getPrimitiveTypeOf(Type type) {
    if (!isArrayType(type)) { // If the type is already primitive, it will just return the type.
      return type;
    }
    switch (type) {
    case BOOLEAN_ARRAY: return Type.BOOLEAN;
    case UINT1_ARRAY: return Type.UINT1;
    case UINT2_ARRAY: return Type.UINT2;
    case UINT4_ARRAY: return Type.UINT4;
    case UINT8_ARRAY: return Type.UINT8;
    case INT1_ARRAY: return Type.INT1;
    case INT2_ARRAY: return Type.INT2;
    case INT4_ARRAY: return Type.INT4;
    case INT8_ARRAY: return Type.INT8;
    case FLOAT4_ARRAY: return Type.FLOAT4;
    case FLOAT8_ARRAY: return Type.FLOAT8;
    case NUMERIC_ARRAY: return Type.NUMERIC;
    case CHAR_ARRAY: return Type.CHAR;
    case NCHAR_ARRAY: return Type.NCHAR;
    case VARCHAR_ARRAY: return Type.VARCHAR;
    case NVARCHAR_ARRAY: return Type.NVARCHAR;
    case TEXT_ARRAY: return Type.TEXT;
    case DATE_ARRAY: return Type.DATE;
    case TIME_ARRAY: return Type.TIME;
    case TIMEZ_ARRAY: return Type.TIMEZ;
    case TIMESTAMP_ARRAY: return Type.TIMESTAMP;
    case TIMESTAMPZ_ARRAY: return Type.TIMESTAMPZ;
    case INTERVAL_ARRAY: return Type.INTERVAL;
    default: throw new InvalidOperationException("Invalid array type: " + type.name());
    }
  }

  public static boolean isCompatibleType(final Type definedType, final Type givenType) {

    // No point in going forward because the data type cannot be upper casted.
    if (givenType.getNumber() > definedType.getNumber()) {
      return false;
    }

    boolean flag = definedType == givenType; // if both are the same to each other
    flag |= givenType == Type.NULL_TYPE;     // NULL value is acceptable in any parameter type
    flag |= definedType == Type.ANY;         // ANY can accept any given value.

    if (flag) {
      return true;
    }

    // Checking if a given type can be casted to a corresponding defined type.

    Type actualType = definedType;
    // if definedType is variable length or array, get a primitive type.
    if (CatalogUtil.isArrayType(definedType)) {
      actualType = CatalogUtil.getPrimitiveTypeOf(definedType);
    }

    flag |= DataTypeUtil.isUpperCastable(actualType, givenType);

    return flag;
  }

  /**
   * It picks out the widest range type among given <code>types</code>.
   *
   * Example:
   * <ul>
   *   <li>int, int8  -> int8 </li>
   *   <li>int4, int8, float4  -> float4 </li>
   *   <li>float4, float8 -> float8</li>
   *   <li>float4, text -> exception!</li>
   * </ul>
   *
   * @param types A list of DataTypes
   * @return The widest DataType
   */
  public static DataType getWidestType(DataType...types) {
    DataType widest = types[0];
    for (int i = 1; i < types.length; i++) {

      if (widest.getType() == Type.NULL_TYPE) { // if null, skip this type
        widest = types[i];
        continue;
      }

      if (types[i].getType() != Type.NULL_TYPE) {
        Type candidate = TUtil.getFromNestedMap(OPERATION_CASTING_MAP, widest.getType(), types[i].getType());
        if (candidate == null) {
          throw new InvalidOperationException("No matched operation for those types: " + TUtil.arrayToString
              (types));
        }
        widest = newSimpleDataType(candidate);
      }
    }

    return widest;
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
      "CASE", "CAST", "CREATE", "CROSS", "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP",
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
      "OVER",
      "UNION", "UNIQUE", "USING",
      "WHEN", "WHERE", "WINDOW", "WITH"
  };

  static {
    for (String keyword : RESERVED_KEYWORDS) {
      RESERVED_KEYWORDS_SET.add(keyword);
    }
  }

  public static AlterTableDesc renameColumn(String tableName, String oldColumName, String newColumName,
                                            AlterTableType alterTableType) {
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

  /* It is the relationship graph of type conversions. */
  public static final Map<Type, Map<Type, Type>> OPERATION_CASTING_MAP = Maps.newHashMap();

  static {
    // Type Conversion Map
    TUtil.putToNestedMap(OPERATION_CASTING_MAP, Type.BOOLEAN, Type.BOOLEAN, Type.BOOLEAN);

    TUtil.putToNestedMap(OPERATION_CASTING_MAP, Type.INT1, Type.INT1, Type.INT1);
    TUtil.putToNestedMap(OPERATION_CASTING_MAP, Type.INT1, Type.INT2, Type.INT2);
    TUtil.putToNestedMap(OPERATION_CASTING_MAP, Type.INT1, Type.INT4, Type.INT4);
    TUtil.putToNestedMap(OPERATION_CASTING_MAP, Type.INT1, Type.INT8, Type.INT8);
    TUtil.putToNestedMap(OPERATION_CASTING_MAP, Type.INT1, Type.FLOAT4, Type.FLOAT4);
    TUtil.putToNestedMap(OPERATION_CASTING_MAP, Type.INT1, Type.FLOAT8, Type.FLOAT8);
    TUtil.putToNestedMap(OPERATION_CASTING_MAP, Type.INT1, Type.TEXT, Type.TEXT);

    TUtil.putToNestedMap(OPERATION_CASTING_MAP, Type.INT2, Type.INT1, Type.INT2);
    TUtil.putToNestedMap(OPERATION_CASTING_MAP, Type.INT2, Type.INT2, Type.INT2);
    TUtil.putToNestedMap(OPERATION_CASTING_MAP, Type.INT2, Type.INT4, Type.INT4);
    TUtil.putToNestedMap(OPERATION_CASTING_MAP, Type.INT2, Type.INT8, Type.INT8);
    TUtil.putToNestedMap(OPERATION_CASTING_MAP, Type.INT2, Type.FLOAT4, Type.FLOAT4);
    TUtil.putToNestedMap(OPERATION_CASTING_MAP, Type.INT2, Type.FLOAT8, Type.FLOAT8);
    TUtil.putToNestedMap(OPERATION_CASTING_MAP, Type.INT2, Type.TEXT, Type.TEXT);

    TUtil.putToNestedMap(OPERATION_CASTING_MAP, Type.INT4, Type.INT1, Type.INT4);
    TUtil.putToNestedMap(OPERATION_CASTING_MAP, Type.INT4, Type.INT2, Type.INT4);
    TUtil.putToNestedMap(OPERATION_CASTING_MAP, Type.INT4, Type.INT4, Type.INT4);
    TUtil.putToNestedMap(OPERATION_CASTING_MAP, Type.INT4, Type.INT8, Type.INT8);
    TUtil.putToNestedMap(OPERATION_CASTING_MAP, Type.INT4, Type.FLOAT4, Type.FLOAT4);
    TUtil.putToNestedMap(OPERATION_CASTING_MAP, Type.INT4, Type.FLOAT8, Type.FLOAT8);
    TUtil.putToNestedMap(OPERATION_CASTING_MAP, Type.INT4, Type.TEXT, Type.TEXT);

    TUtil.putToNestedMap(OPERATION_CASTING_MAP, Type.INT8, Type.INT1, Type.INT8);
    TUtil.putToNestedMap(OPERATION_CASTING_MAP, Type.INT8, Type.INT2, Type.INT8);
    TUtil.putToNestedMap(OPERATION_CASTING_MAP, Type.INT8, Type.INT4, Type.INT8);
    TUtil.putToNestedMap(OPERATION_CASTING_MAP, Type.INT8, Type.INT8, Type.INT8);
    TUtil.putToNestedMap(OPERATION_CASTING_MAP, Type.INT8, Type.FLOAT4, Type.FLOAT4);
    TUtil.putToNestedMap(OPERATION_CASTING_MAP, Type.INT8, Type.FLOAT8, Type.FLOAT8);
    TUtil.putToNestedMap(OPERATION_CASTING_MAP, Type.INT8, Type.TEXT, Type.TEXT);

    TUtil.putToNestedMap(OPERATION_CASTING_MAP, Type.FLOAT4, Type.INT1, Type.FLOAT4);
    TUtil.putToNestedMap(OPERATION_CASTING_MAP, Type.FLOAT4, Type.INT2, Type.FLOAT4);
    TUtil.putToNestedMap(OPERATION_CASTING_MAP, Type.FLOAT4, Type.INT4, Type.FLOAT4);
    TUtil.putToNestedMap(OPERATION_CASTING_MAP, Type.FLOAT4, Type.INT8, Type.FLOAT4);
    TUtil.putToNestedMap(OPERATION_CASTING_MAP, Type.FLOAT4, Type.FLOAT4, Type.FLOAT4);
    TUtil.putToNestedMap(OPERATION_CASTING_MAP, Type.FLOAT4, Type.FLOAT8, Type.FLOAT8);
    TUtil.putToNestedMap(OPERATION_CASTING_MAP, Type.FLOAT4, Type.TEXT, Type.TEXT);

    TUtil.putToNestedMap(OPERATION_CASTING_MAP, Type.FLOAT8, Type.INT1, Type.FLOAT8);
    TUtil.putToNestedMap(OPERATION_CASTING_MAP, Type.FLOAT8, Type.INT2, Type.FLOAT8);
    TUtil.putToNestedMap(OPERATION_CASTING_MAP, Type.FLOAT8, Type.INT4, Type.FLOAT8);
    TUtil.putToNestedMap(OPERATION_CASTING_MAP, Type.FLOAT8, Type.INT8, Type.FLOAT8);
    TUtil.putToNestedMap(OPERATION_CASTING_MAP, Type.FLOAT8, Type.FLOAT4, Type.FLOAT8);
    TUtil.putToNestedMap(OPERATION_CASTING_MAP, Type.FLOAT8, Type.FLOAT8, Type.FLOAT8);
    TUtil.putToNestedMap(OPERATION_CASTING_MAP, Type.FLOAT8, Type.TEXT, Type.TEXT);

    TUtil.putToNestedMap(OPERATION_CASTING_MAP, Type.TEXT, Type.TIMESTAMP, Type.TIMESTAMP);
    TUtil.putToNestedMap(OPERATION_CASTING_MAP, Type.TIMESTAMP, Type.TIMESTAMP, Type.TIMESTAMP);
    TUtil.putToNestedMap(OPERATION_CASTING_MAP, Type.TIMESTAMP, Type.TEXT, Type.TEXT);
    TUtil.putToNestedMap(OPERATION_CASTING_MAP, Type.TEXT, Type.TEXT, Type.TEXT);

    TUtil.putToNestedMap(OPERATION_CASTING_MAP, Type.TIME, Type.TIME, Type.TIME);
    TUtil.putToNestedMap(OPERATION_CASTING_MAP, Type.DATE, Type.DATE, Type.DATE);

    TUtil.putToNestedMap(OPERATION_CASTING_MAP, Type.INET4, Type.INET4, Type.INET4);
  }
}
