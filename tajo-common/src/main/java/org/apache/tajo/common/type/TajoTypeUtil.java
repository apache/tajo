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

package org.apache.tajo.common.type;

import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.datum.*;

import java.sql.DatabaseMetaData;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

public class TajoTypeUtil {
  private static List<Datum[]> typeInfos = new ArrayList<>();

  public static List<Datum[]> getTypeInfos() {
    synchronized (typeInfos) {
      if (typeInfos.isEmpty()) {
        for (Type eachType : Type.values()) {
          if (!isUserDataType(eachType)) {
            continue;
          }
          Datum[] datums = new Datum[18];

          int index = 0;
          datums[index++] = new TextDatum(eachType.name());    //TYPE_NAME
          datums[index++] = new Int2Datum((short) getJavaSqlType(eachType));     //DATA_TYPE
          datums[index++] = new Int4Datum(getPrecision(eachType));     //PRECISION
          datums[index++] = new TextDatum(getLiteralPrefix(eachType));    //LITERAL_PREFIX
          datums[index++] = new TextDatum(getLiteralPrefix(eachType));    //LITERAL_SUFFIX
          datums[index++] = new TextDatum("");    //CREATE_PARAMS
          datums[index++] = new Int2Datum((short) DatabaseMetaData.typeNullable);    //NULLABLE
          datums[index++] = BooleanDatum.TRUE;    //CASE_SENSITIVE
          datums[index++] = new Int2Datum(getSearchable(eachType));    //SEARCHABLE
          datums[index++] = BooleanDatum.FALSE;    //UNSIGNED_ATTRIBUTE
          datums[index++] = BooleanDatum.FALSE;    //FIXED_PREC_SCALE
          datums[index++] = BooleanDatum.FALSE;    //AUTO_INCREMENT
          datums[index++] = new TextDatum(eachType.name());    //LOCAL_TYPE_NAME
          datums[index++] = new Int2Datum((short) 0);    //MINIMUM_SCALE
          datums[index++] = new Int2Datum((short) 0);    //MAXIMUM_SCALE
          datums[index++] = NullDatum.get();    //SQL_DATA_TYPE
          datums[index++] = NullDatum.get();    //SQL_DATETIME_SUB
          datums[index++] = new Int4Datum(getNumPrecRadix(eachType));    //NUM_PREC_RADIX

          typeInfos.add(datums);
        }
      }
      return typeInfos;
    }
  }

  public static boolean isUserDataType(Type type) {
    switch (type) {
      case INT1:
      case INT2:
      case INT4:
      case INT8:
      case FLOAT4:
      case FLOAT8:
      case DATE:
      case TIME:
      case TIMESTAMP:
      case NUMERIC:
      case VARCHAR:
      case TEXT: return true;
      default: return false;
    }
  }

  public static int getJavaSqlType(Type type) {
    switch (type) {
      case INT1: return Types.TINYINT;
      case INT2: return Types.SMALLINT;
      case INT4: return Types.INTEGER;
      case INT8: return Types.BIGINT;
      case FLOAT4: return Types.FLOAT;
      case FLOAT8: return Types.DOUBLE;
      case VARCHAR:
      case TEXT: return Types.VARCHAR;
      case DATE: return Types.DATE;
      case TIME: return Types.TIME;
      case TIMESTAMP: return Types.TIMESTAMP;
      case NUMERIC: return Types.DECIMAL;
      default: return Types.VARCHAR;
    }
  }

  public static int getPrecision(Type type) {
    switch (type) {
      case INT1: return 3;
      case INT2: return 5;
      case INT4: return 10;
      case INT8: return 19;
      case FLOAT4: return 7;
      case FLOAT8: return 15;
      case DATE:
      case TIME:
      case TIMESTAMP: return 0;
      default: return Integer.MAX_VALUE;
    }
  }

  public static String getLiteralPrefix(Type type) {
    switch (type) {
      case VARCHAR:
      case TEXT: return "'";
      default: return "";
    }
  }

  public static short getSearchable(Type type) {
    /*
     * DatabaseMetaData.typePredNone - No support
     * DatabaseMetaData.typePredChar - Only support with WHERE .. LIKE
     * DatabaseMetaData.typePredBasic - Supported except for WHERE .. LIKE
     * DatabaseMetaData.typeSearchable - Supported for all WHERE ..
     */
    switch (type) {
      case INT1:
      case INT2:
      case INT4:
      case INT8:
      case FLOAT4:
      case FLOAT8:
      case DATE:
      case TIME:
      case TIMESTAMP:
      case NUMERIC: return DatabaseMetaData.typePredBasic;
      case VARCHAR:
      case TEXT: return DatabaseMetaData.typeSearchable;
      default: return DatabaseMetaData.typePredBasic;
    }
  }

  public static int getNumPrecRadix(Type type) {
    switch (type) {
      case INT1:
      case INT2:
      case INT4:
      case INT8:
        return 10;
      case FLOAT4:
      case FLOAT8:
        return 2;
      default:
        return 0;
    }
  }

  public static boolean isSigned(Type type) {
    switch (type) {
      case INT1:
      case INT2:
      case INT4:
      case INT8:
      case FLOAT4:
      case FLOAT8:
        return true;
      case DATE:
      case TIME:
      case TIMESTAMP:
      case VARCHAR:
      case CHAR:
      case TEXT: return false;
      default: return true;
    }
  }

  public static boolean isNumeric(org.apache.tajo.type.Type type) {
    return isNumber(type) || isReal(type.kind());
  }

  public static boolean isNumber(org.apache.tajo.type.Type type) {
    return
        type.kind() == Type.INT2 ||
        type.kind() == Type.INT4 ||
        type.kind() == Type.INT8;
  }

  public static boolean isReal(Type type) {
    return type == Type.FLOAT4|| type == Type.FLOAT8;
  }
}
