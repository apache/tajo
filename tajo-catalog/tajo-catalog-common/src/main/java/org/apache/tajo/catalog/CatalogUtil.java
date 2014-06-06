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
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.common.TajoDataTypes.DataType;
import org.apache.tajo.exception.InvalidOperationException;
import org.apache.tajo.util.KeyValueSet;
import org.apache.tajo.util.StringUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
    } else if (typeStr.equalsIgnoreCase(StoreType.AVRO.name())) {
      return StoreType.AVRO;
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

        // chooses one basis type for checking the variable part
        if (givenTypes.get(j).getType() != Type.NULL_TYPE) {
          basisTypeOfVarLengthType = givenTypes.get(j).getType();
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
        if (type != Type.NULL_TYPE && type != basisTypeOfVarLengthType) {
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
    boolean flag = false;
    if (givenType == Type.NULL_TYPE) {
      flag = true;
    } else if (definedType == Type.ANY) {
      flag = true;
    } else if (givenType.getNumber() > definedType.getNumber()) {
      // NO POINT IN GOING FORWARD BECAUSE THE DATA TYPE CANNOT BE UPPER CASTED
      flag = false;
    } else {
      //argType.getNumber() < exitingType.getNumber()
      int exitingTypeNumber = definedType.getNumber();
      int argTypeNumber = givenType.getNumber();

      if (Type.INT1.getNumber() <= exitingTypeNumber && exitingTypeNumber <= Type.INT8.getNumber()) {
        // INT1 ==> INT2 ==> INT4 ==> INT8
        if (Type.INT1.getNumber() <= argTypeNumber && argTypeNumber <= exitingTypeNumber) {
          flag = true;
        }
      } else if (Type.UINT1.getNumber() <= exitingTypeNumber && exitingTypeNumber <= Type.UINT8.getNumber()) {
        // UINT1 ==> UINT2 ==> UINT4 ==> UINT8
        if (Type.UINT1.getNumber() <= argTypeNumber && argTypeNumber <= exitingTypeNumber) {
          flag = true;
        }
      } else if (Type.FLOAT4.getNumber() <= exitingTypeNumber && exitingTypeNumber <= Type.NUMERIC.getNumber()) {
        // FLOAT4 ==> FLOAT8 ==> NUMERIC ==> DECIMAL
        if (Type.FLOAT4.getNumber() <= argTypeNumber && argTypeNumber <= exitingTypeNumber) {
          flag = true;
        }
      } else if (Type.CHAR.getNumber() <= exitingTypeNumber && exitingTypeNumber <= Type.TEXT.getNumber()) {
        // CHAR ==> NCHAR ==> VARCHAR ==> NVARCHAR ==> TEXT
        if (Type.CHAR.getNumber() <= argTypeNumber && argTypeNumber <= exitingTypeNumber) {
          flag = true;
        }
      } else if (Type.BIT.getNumber() <= exitingTypeNumber && exitingTypeNumber <= Type.VARBINARY.getNumber()) {
        // BIT ==> VARBIT ==> BINARY ==> VARBINARY
        if (Type.BIT.getNumber() <= argTypeNumber && argTypeNumber <= exitingTypeNumber) {
          flag = true;
        }
      } else if (Type.INT1_ARRAY.getNumber() <= exitingTypeNumber
          && exitingTypeNumber <= Type.INT8_ARRAY.getNumber()) {
        // INT1_ARRAY ==> INT2_ARRAY ==> INT4_ARRAY ==> INT8_ARRAY
        if (Type.INT1_ARRAY.getNumber() <= argTypeNumber && argTypeNumber <= exitingTypeNumber) {
          flag = true;
        }
      } else if (Type.UINT1_ARRAY.getNumber() <= exitingTypeNumber
          && exitingTypeNumber <= Type.UINT8_ARRAY.getNumber()) {
        // UINT1_ARRAY ==> UINT2_ARRAY ==> UINT4_ARRAY ==> UINT8_ARRAY
        if (Type.UINT1_ARRAY.getNumber() <= argTypeNumber && argTypeNumber <= exitingTypeNumber) {
          flag = true;
        }
      } else if (Type.FLOAT4_ARRAY.getNumber() <= exitingTypeNumber
          && exitingTypeNumber <= Type.FLOAT8_ARRAY.getNumber()) {
        // FLOAT4_ARRAY ==> FLOAT8_ARRAY ==> NUMERIC_ARRAY ==> DECIMAL_ARRAY
        if (Type.FLOAT4_ARRAY.getNumber() <= argTypeNumber && argTypeNumber <= exitingTypeNumber) {
          flag = true;
        }
      } else if (Type.CHAR_ARRAY.getNumber() <= exitingTypeNumber
          && exitingTypeNumber <= Type.TEXT_ARRAY.getNumber()) {
        // CHAR_ARRAY ==> NCHAR_ARRAY ==> VARCHAR_ARRAY ==> NVARCHAR_ARRAY ==> TEXT_ARRAY
        if (Type.TEXT_ARRAY.getNumber() <= argTypeNumber && argTypeNumber <= exitingTypeNumber) {
          flag = true;
        }
      } else if (givenType == Type.BOOLEAN && (definedType == Type.BOOLEAN || definedType == Type.BOOLEAN_ARRAY)) {
        flag = true;
      } else if (givenType == Type.DATE && (definedType == Type.DATE || definedType == Type.DATE_ARRAY)) {
        flag = true;
      } else if (givenType == Type.TIME && (definedType == Type.TIME || definedType == Type.TIME_ARRAY)) {
        flag = true;
      } else if (givenType == Type.TIMESTAMP && (definedType == Type.TIMESTAMP || definedType == Type.TIMESTAMP_ARRAY)) {
        flag = true;
      }
    }
    return flag;
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
}
