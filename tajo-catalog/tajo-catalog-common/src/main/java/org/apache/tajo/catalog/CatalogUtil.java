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
import com.google.common.collect.Sets;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.BuiltinStorages;
import org.apache.tajo.DataTypeUtil;
import org.apache.tajo.annotation.Nullable;
import org.apache.tajo.catalog.partition.PartitionDesc;
import org.apache.tajo.catalog.partition.PartitionMethodDesc;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.catalog.proto.CatalogProtos.PartitionKeyProto;
import org.apache.tajo.catalog.proto.CatalogProtos.SchemaProto;
import org.apache.tajo.catalog.proto.CatalogProtos.TableDescProto;
import org.apache.tajo.catalog.proto.CatalogProtos.TableIdentifierProto;
import org.apache.tajo.common.TajoDataTypes;
import org.apache.tajo.common.TajoDataTypes.DataType;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.datum.NullDatum;
import org.apache.tajo.exception.InvalidOperationException;
import org.apache.tajo.exception.UndefinedOperatorException;
import org.apache.tajo.schema.IdentifierUtil;
import org.apache.tajo.storage.StorageConstants;
import org.apache.tajo.util.KeyValueSet;
import org.apache.tajo.util.Pair;
import org.apache.tajo.util.StringUtils;
import org.apache.tajo.util.TUtil;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

import static org.apache.tajo.common.TajoDataTypes.Type;

public class CatalogUtil {

  public static String getBackwardCompitableDataFormat(String dataFormat) {
    final String upperDataFormat = dataFormat.toUpperCase();
    switch (upperDataFormat) {
      case "CSV":
      case "TEXTFILE":
        return BuiltinStorages.TEXT;
      default:
        return dataFormat;
    }
  }

  public static TableMeta newTableMeta(String dataFormat, TajoConf conf) {
    KeyValueSet defaultProperties = CatalogUtil.newDefaultProperty(dataFormat, conf);
    return new TableMeta(dataFormat, defaultProperties);
  }

  public static TableMeta newTableMeta(String dataFormat, KeyValueSet options) {
    return new TableMeta(dataFormat, options);
  }

  public static TableDesc newTableDesc(String tableName, Schema schema, TableMeta meta, Path path) {
    return new TableDesc(tableName, schema, meta, path.toUri());
  }

  public static TableDesc newTableDesc(TableDescProto proto) {
    return new TableDesc(proto);
  }

  public static PartitionMethodDesc newPartitionMethodDesc(CatalogProtos.PartitionMethodProto proto) {
    return new PartitionMethodDesc(proto);
  }

  /**
  * This method transforms the unqualified names of a schema to the qualified names.
  *
  * @param tableName a table name to be prefixed
  * @param schema a schema to be transformed
  *
  * @return
  */
  public static SchemaProto getQualfiedSchema(String tableName, SchemaProto schema) {
    Schema restored = SchemaFactory.newV1(schema);
    restored.setQualifier(tableName);
    return restored.getProto();
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
		if (type != Type.CHAR) {
			return DataType.newBuilder().setType(type).build();
		} else {
			return newDataTypeWithLen(Type.CHAR, 1);
		}
  }

  /**
   * Create a record type
   *
   * @param nestedFieldNum The number of nested fields
   * @return RECORD DataType
   */
  public static DataType newRecordType(int nestedFieldNum) {
    DataType.Builder builder = DataType.newBuilder();
    builder.setType(Type.RECORD);
    builder.setNumNestedFields(nestedFieldNum);
    return builder.build();
  }

  public static DataType [] newSimpleDataTypeArray(Type... types) {
    DataType [] dataTypes = new DataType[types.length];
    for (int i = 0; i < types.length; i++) {
      dataTypes[i] = newSimpleDataType(types[i]);
    }
    return dataTypes;
  }

  public static DataType newDataTypeWithLen(Type type, int length) {
    return DataType.newBuilder().setType(type).setLength(length).build();
  }

  public static String columnToDDLString(Column column) {
    StringBuilder sb = new StringBuilder(IdentifierUtil.denormalizeIdentifier(column.getSimpleName()));
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

    int definedSize = definedTypes.size();
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
          try {
            basisTypeOfVarLengthType =
                getWidestType(CatalogUtil.newSimpleDataTypeArray(basisTypeOfVarLengthType, givenTypes.get(j).getType()))
                .getType();
          } catch (UndefinedOperatorException e) {
            continue;
          }
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
  public static DataType getWidestType(DataType...types) throws UndefinedOperatorException {
    DataType widest = types[0];
    for (int i = 1; i < types.length; i++) {

      if (widest.getType() == Type.NULL_TYPE) { // if null, skip this type
        widest = types[i];
        continue;
      }

      if (types[i].getType() != Type.NULL_TYPE) {
        Type candidate = TUtil.getFromNestedMap(OPERATION_CASTING_MAP, widest.getType(), types[i].getType());
        if (candidate == null) {
          throw new UndefinedOperatorException(StringUtils.join(types));
        }
        widest = newSimpleDataType(candidate);
      }
    }

    return widest;
  }

  public static TableIdentifierProto buildTableIdentifier(String databaseName, String tableName) {
    return TableIdentifierProto.newBuilder()
        .setDatabaseName(databaseName)
        .setTableName(tableName)
        .build();
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

  static {
    Collections.addAll(IdentifierUtil.RESERVED_KEYWORDS_SET, IdentifierUtil.RESERVED_KEYWORDS);
  }

  public static AlterTableDesc renameColumn(String tableName, String oldColumName, String newColumName) {
    final AlterTableDesc alterTableDesc = new AlterTableDesc();
    alterTableDesc.setTableName(tableName);
    alterTableDesc.setColumnName(oldColumName);
    alterTableDesc.setNewColumnName(newColumName);
    alterTableDesc.setAlterTableType(AlterTableType.RENAME_COLUMN);
    return alterTableDesc;
  }

  public static AlterTableDesc renameTable(String tableName, String newTableName, @Nullable Path newTablePath) {
    final AlterTableDesc alterTableDesc = new AlterTableDesc();
    alterTableDesc.setTableName(tableName);
    alterTableDesc.setNewTableName(newTableName);
    alterTableDesc.setAlterTableType(AlterTableType.RENAME_TABLE);
    if (newTablePath != null) {
      alterTableDesc.setNewTablePath(newTablePath);
    }
    return alterTableDesc;
  }

  public static AlterTableDesc addNewColumn(String tableName, Column column) {
    final AlterTableDesc alterTableDesc = new AlterTableDesc();
    alterTableDesc.setTableName(tableName);
    alterTableDesc.setAddColumn(column);
    alterTableDesc.setAlterTableType(AlterTableType.ADD_COLUMN);
    return alterTableDesc;
  }

  public static AlterTableDesc setProperty(String tableName, KeyValueSet params) {
    final AlterTableDesc alterTableDesc = new AlterTableDesc();
    alterTableDesc.setTableName(tableName);
    alterTableDesc.setProperties(params);
    alterTableDesc.setAlterTableType(AlterTableType.SET_PROPERTY);
    return alterTableDesc;
  }

  public static AlterTableDesc unsetProperty(String tableName, String[] propertyKeys) {
    final AlterTableDesc alterTableDesc = new AlterTableDesc();
    alterTableDesc.setTableName(tableName);
    alterTableDesc.setUnsetPropertyKey(Sets.newHashSet(propertyKeys));
    alterTableDesc.setAlterTableType(AlterTableType.UNSET_PROPERTY);
    return alterTableDesc;
  }

  /**
   * Converts passed parameters to a AlterTableDesc. This method would be called when adding a partition or dropping
   * a table. This creates AlterTableDesc that is a wrapper class for protocol buffer.
   * *
   * @param tableName
   * @param columns
   * @param values
   * @param path
   * @param alterTableType
   * @return
   */
  public static AlterTableDesc addOrDropPartition(String tableName, String[] columns, String[] values, @Nullable
    String path, AlterTableType alterTableType) {
    return addOrDropPartition(tableName, columns, values, path, alterTableType, 0L);
  }
  /**
   * Converts passed parameters to a AlterTableDesc. This method would be called when adding a partition or dropping
   * a table. This creates AlterTableDesc that is a wrapper class for protocol buffer.
   *
   * @param tableName table name
   * @param columns partition column names
   * @param values partition values
   * @param path partition directory path
   * @param alterTableType ADD_PARTITION or DROP_PARTITION
   * @param numBytes contents length
   * @return AlterTableDesc
   */
  public static AlterTableDesc addOrDropPartition(String tableName, String[] columns, String[] values,
    @Nullable String path, AlterTableType alterTableType, long numBytes) {

    final AlterTableDesc alterTableDesc = new AlterTableDesc();
    alterTableDesc.setTableName(tableName);

    PartitionDesc partitionDesc = new PartitionDesc();
    Pair<List<PartitionKeyProto>, String> pair = getPartitionKeyNamePair(columns, values);

    partitionDesc.setPartitionKeys(pair.getFirst());
    partitionDesc.setPartitionName(pair.getSecond());

    if (alterTableType.equals(AlterTableType.ADD_PARTITION)) {
      if (path != null) {
        partitionDesc.setPath(path);
      }
      partitionDesc.setNumBytes(numBytes);
    }

    alterTableDesc.setPartitionDesc(partitionDesc);
    alterTableDesc.setAlterTableType(alterTableType);
    return alterTableDesc;
  }

  /**
   * Get partition key/value list and partition name
   *
   * ex) partition key/value list :
   *   - col1, 2015-07-01
   *   - col2, tajo
   *     partition name : col1=2015-07-01/col2=tajo
   *
   * @param columns partition column names
   * @param values partition values
   * @return partition key/value list and partition name
   */
  public static Pair<List<PartitionKeyProto>, String> getPartitionKeyNamePair(String[] columns, String[] values) {
    Pair<List<PartitionKeyProto>, String> pair = null;
    List<PartitionKeyProto> partitionKeyList = new ArrayList<>();

    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < columns.length; i++) {
      PartitionKeyProto.Builder builder = PartitionKeyProto.newBuilder();
      builder.setColumnName(columns[i]);
      builder.setPartitionValue(values[i]);

      if (i > 0) {
        sb.append("/");
      }
      sb.append(columns[i]).append("=").append(values[i]);
      partitionKeyList.add(builder.build());
    }

    pair = new Pair<>(partitionKeyList, sb.toString());
    return pair;
  }

  /**
   * It is the relationship graph of type conversions.
   * It contains tuples, each of which (LHS type, RHS type, Result type).
   */

  public static final Map<Type, Map<Type, Type>> OPERATION_CASTING_MAP = Maps.newHashMap();

  /**
   * It is the casting direction of relationship graph
   */
  private static final Map<Type, Map<Type, Direction>> CASTING_DIRECTION_MAP = Maps.newHashMap();

  public static Direction getCastingDirection(Type lhs, Type rhs) {
    Direction direction = TUtil.getFromNestedMap(CatalogUtil.CASTING_DIRECTION_MAP, lhs, rhs);
    if (direction == null) {
      return Direction.BOTH;
    }
    return direction;
  }

  public enum Direction {
    LHS,
    RHS,
    BOTH
  }

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
    TUtil.putToNestedMap(OPERATION_CASTING_MAP, Type.TEXT, Type.TEXT, Type.TEXT);
    TUtil.putToNestedMap(OPERATION_CASTING_MAP, Type.TEXT, Type.VARCHAR, Type.TEXT);

    TUtil.putToNestedMap(OPERATION_CASTING_MAP, Type.VARCHAR, Type.TIMESTAMP, Type.TIMESTAMP);
    TUtil.putToNestedMap(OPERATION_CASTING_MAP, Type.VARCHAR, Type.TEXT, Type.TEXT);
    TUtil.putToNestedMap(OPERATION_CASTING_MAP, Type.VARCHAR, Type.VARCHAR, Type.TEXT);

    TUtil.putToNestedMap(OPERATION_CASTING_MAP, Type.TIMESTAMP, Type.TIMESTAMP, Type.TIMESTAMP);
    TUtil.putToNestedMap(OPERATION_CASTING_MAP, Type.TIMESTAMP, Type.TEXT, Type.TEXT);
    TUtil.putToNestedMap(OPERATION_CASTING_MAP, Type.TIMESTAMP, Type.VARCHAR, Type.TEXT);

    TUtil.putToNestedMap(OPERATION_CASTING_MAP, Type.TIME, Type.TIME, Type.TIME);
    TUtil.putToNestedMap(OPERATION_CASTING_MAP, Type.TIME, Type.DATE, Type.TIMESTAMP);
    TUtil.putToNestedMap(CASTING_DIRECTION_MAP, Type.TIME, Type.DATE, Direction.RHS);

    TUtil.putToNestedMap(OPERATION_CASTING_MAP, Type.DATE, Type.DATE, Type.DATE);
    TUtil.putToNestedMap(OPERATION_CASTING_MAP, Type.DATE, Type.TIME, Type.TIMESTAMP);
    TUtil.putToNestedMap(CASTING_DIRECTION_MAP, Type.DATE, Type.TIME, Direction.LHS);

    TUtil.putToNestedMap(OPERATION_CASTING_MAP, Type.DATE, Type.INTERVAL, Type.TIMESTAMP);
    TUtil.putToNestedMap(CASTING_DIRECTION_MAP, Type.DATE, Type.INTERVAL, Direction.LHS);

    TUtil.putToNestedMap(OPERATION_CASTING_MAP, Type.INTERVAL, Type.DATE, Type.TIMESTAMP);
    TUtil.putToNestedMap(CASTING_DIRECTION_MAP, Type.INTERVAL, Type.DATE, Direction.RHS);
  }

  // table default properties
  public static final String BLOCK_SIZE           = "parquet.block.size";
  public static final String PAGE_SIZE            = "parquet.page.size";
  public static final String COMPRESSION          = "parquet.compression";
  public static final String ENABLE_DICTIONARY    = "parquet.enable.dictionary";
  public static final String VALIDATION           = "parquet.validation";

  /**
   * Create new table property with default configs. It is used to make TableMeta to be stored in Catalog.
   *
   * @param dataFormat DataFormat
   * @return Table properties
   */
  public static KeyValueSet newDefaultProperty(String dataFormat, TajoConf conf) {
    KeyValueSet options = new KeyValueSet();
    // set default timezone to the system timezone
    options.set(StorageConstants.TIMEZONE, conf.getSystemTimezone().getID());

    if (dataFormat.equalsIgnoreCase(BuiltinStorages.TEXT)) {
      options.set(StorageConstants.TEXT_DELIMITER, StorageConstants.DEFAULT_FIELD_DELIMITER);
      options.set(StorageConstants.TEXT_NULL, NullDatum.DEFAULT_TEXT);
    } else if (dataFormat.equalsIgnoreCase(BuiltinStorages.JSON)) {
      options.set(StorageConstants.TEXT_SERDE_CLASS, StorageConstants.DEFAULT_JSON_SERDE_CLASS);
    } else if (dataFormat.equalsIgnoreCase(BuiltinStorages.REGEX)) {
      options.set(StorageConstants.TEXT_SERDE_CLASS, StorageConstants.DEFAULT_REGEX_SERDE_CLASS);
    } else if (dataFormat.equalsIgnoreCase(BuiltinStorages.RCFILE)) {
      options.set(StorageConstants.RCFILE_SERDE, StorageConstants.DEFAULT_BINARY_SERDE);
    } else if (dataFormat.equalsIgnoreCase(BuiltinStorages.SEQUENCE_FILE)) {
      options.set(StorageConstants.SEQUENCEFILE_SERDE, StorageConstants.DEFAULT_TEXT_SERDE);
      options.set(StorageConstants.TEXT_DELIMITER, StorageConstants.DEFAULT_FIELD_DELIMITER);
    } else if (dataFormat.equalsIgnoreCase(BuiltinStorages.PARQUET)) {
      options.set(BLOCK_SIZE, StorageConstants.PARQUET_DEFAULT_BLOCK_SIZE);
      options.set(PAGE_SIZE, StorageConstants.PARQUET_DEFAULT_PAGE_SIZE);
      options.set(COMPRESSION, StorageConstants.PARQUET_DEFAULT_COMPRESSION_CODEC_NAME);
      options.set(ENABLE_DICTIONARY, StorageConstants.PARQUET_DEFAULT_IS_DICTIONARY_ENABLED);
      options.set(VALIDATION, StorageConstants.PARQUET_DEFAULT_IS_VALIDATION_ENABLED);
    }

    return options;
  }

  /**
   * Make a unique name by concatenating column names.
   * The concatenation is performed in sequence of columns' occurrence in the relation schema.
   *
   * @param originalSchema original relation schema
   * @param columnNames column names which will be unified
   * @return unified name
   */
  public static String getUnifiedSimpleColumnName(Schema originalSchema, String[] columnNames) {
    String[] simpleNames = new String[columnNames.length];
    for (int i = 0; i < simpleNames.length; i++) {
      String[] identifiers = columnNames[i].split(IdentifierUtil.IDENTIFIER_DELIMITER_REGEXP);
      simpleNames[i] = identifiers[identifiers.length-1];
    }
    Arrays.sort(simpleNames, new ColumnPosComparator(originalSchema));
    StringBuilder sb = new StringBuilder();
    for (String colName : simpleNames) {
      sb.append(colName).append(",");
    }
    sb.deleteCharAt(sb.length()-1);
    return sb.toString();
  }

  /**
   * Given column names, compare the position of columns in the relation schema.
   */
  public static class ColumnPosComparator implements Comparator<String> {

    private Schema originlSchema;
    public ColumnPosComparator(Schema originalSchema) {
      this.originlSchema = originalSchema;
    }

    @Override
    public int compare(String o1, String o2) {
      return originlSchema.getColumnId(o1) - originlSchema.getColumnId(o2);
    }
  }
}
